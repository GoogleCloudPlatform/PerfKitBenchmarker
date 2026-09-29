# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs multichase and multiload simultaneously to measure loaded latency.

This benchmark characterizes the memory subsystem performance by measuring
pointer-chasing memory latency under varying levels of background memory
bandwidth load.

Measurement Methodology:
1. Dynamic Hardware Topology Discovery:
   - The CPUs available to the benchmark, their physical cores, and NUMA nodes
     are discovered via vm.GetCpusAllowedSet(), vm.CheckProcCpu(), and
     numactl --hardware.
   - The last core (core N-1) and all SMT sibling threads sharing its physical
     core are reserved exclusively for multichase to avoid both SMT interference
     and CPU 0 kernel housekeeping interrupt noise.
   - Multiload threads are pinned to cores 0 to N-2 (either 1 thread per
     physical core or all remaining vCPUs).
2. Idle Latency Baseline:
   - Multichase runs alone pinned to the last core (N-1) and its local NUMA node
     to measure unloaded (idle) memory latency.
3. Memory Traffic Generation & Synchronization:
   - Multiload generates continuous background memory bandwidth using streaming
     traffic patterns (e.g. stream-triad-nontemporal-injection-delay) with
     an injection delay parameter (-d).
   - Multiload runs continuously (-n 0), warming memory buffers until
   steady-state
     bandwidth reporting begins.
   - Multichase then executes concurrent pointer chasing.
   - Bandwidth samples during the concurrent execution are captured, and
   multiload
     is safely terminated via SIGTERM.
4. Adaptive Delay Curve Sampling:
   - Probe phase: Evaluates delay d=0 (peak bandwidth) and exponentially doubles
     delay until bandwidth drops below a fraction of peak bandwidth (e.g. 2%).
   - Refinement phase: Uses a priority queue (max-heap) of Euclidean distances
     in normalized (0.001 * MB/s, ns) space to bisect intervals with the largest
     gaps, capturing the full knee of the latency-bandwidth curve efficiently
     within a sample budget.
"""

import collections
import dataclasses
import heapq
import logging
import math
import posixpath
import re
import statistics
from typing import Any, Callable
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import multichase
from perfkitbenchmarker.linux_packages import numactl

BENCHMARK_NAME = 'loaded_latency'
BENCHMARK_CONFIG = """
loaded_latency:
  description: >
      Run multichase and multiload simultaneously to measure loaded memory latency.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n2-standard-64
        AWS:
          machine_type: m5.16xlarge
        Azure:
          machine_type: Standard_D64s_v3
  flags:
    enable_transparent_hugepages: True
"""

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'loaded_latency_max_delay',
    20000,
    'Upper bound on injection delay -d for adaptive delay sweep.',
)
flags.DEFINE_float(
    'loaded_latency_idle_bw_fraction',
    0.02,
    'Stop probing once load BW falls below this fraction of peak (d=0) BW.',
)
flags.DEFINE_integer(
    'loaded_latency_max_samples',
    30,
    'Adaptive sampling budget (including probes).',
)
flags.DEFINE_float(
    'loaded_latency_gap_tolerance',
    10.0,
    'Stop refining once neighboring points are closer than this Euclidean'
    ' distance in (GB/s, ns) space.',
)
flags.DEFINE_string(
    'loaded_latency_multichase_numactl',
    None,
    'numactl arguments for pinning multichase. When None, dynamically binds to'
    ' the last selected CPU (core N-1) and its associated NUMA node (-C'
    ' <last_cpu> -m <numa_node>).',
)
flags.DEFINE_string(
    'loaded_latency_multichase_memory_size',
    '2g',
    '-m parameter for multichase.',
)
flags.DEFINE_integer(
    'loaded_latency_multichase_num_samples',
    10,
    '-n parameter for multichase.',
)
flags.DEFINE_integer(
    'loaded_latency_multichase_stride_size',
    1024,
    '-s parameter for multichase.',
)
flags.DEFINE_string(
    'loaded_latency_multichase_chase_type',
    'simple',
    '-c parameter for multichase.',
)
flags.DEFINE_string(
    'loaded_latency_multichase_additional_flags',
    '-t 1 -T 8m -a -H -L',
    'additional flags for multichase.',
)
flags.DEFINE_string(
    'loaded_latency_multiload_numactl',
    None,
    'optional override for multiload numactl options. When None, automatically'
    ' bind to active cores 0 to N-2 (-C <core_list>).',
)
flags.DEFINE_bool(
    'loaded_latency_physical_cores_only',
    True,
    'If True, select only physical cores (1 thread per physical core,'
    ' excluding SMT siblings). If False, select all vCPUs (excluding the last'
    ' core N-1 and its SMT siblings, which run multichase).',
)
flags.DEFINE_integer(
    'loaded_latency_num_cores',
    None,
    'Optional number of cores N to use (where multichase runs on core N-1'
    ' and multiload runs on cores 0 to N-2).',
)
flags.DEFINE_string(
    'loaded_latency_multiload_buffer_size',
    '1G',
    '-m parameter for multiload.',
)
flags.DEFINE_string(
    'loaded_latency_multiload_traffic_pattern',
    'stream-triad-nontemporal-injection-delay',
    '-l parameter for multiload.',
)


@dataclasses.dataclass(frozen=True, order=True)
class DelayPoint:
  """A single measurement on the loaded latency curve.

  Attributes:
    delay: multiload injection delay (-d).
    load_avg_mibs: Average multiload bandwidth in MiB/s while multichase ran.
    latency_ns: multichase pointer-chase latency in ns.
  """

  delay: int
  load_avg_mibs: float
  latency_ns: float


_MULTILOAD_OUT = 'multiload.out'
_BW_SAMPLE_MARKER = 'Total(MiB/s)='
_SAMPLE_WAIT_TIMEOUT_SEC = 300
_SAMPLE_POLL_INTERVAL_SEC = 0.5


class _MultiloadNotReadyError(Exception):
  """Raised while multiload has not yet written enough bandwidth samples."""


def _StartMultiload(vm, multiload_cmd: str) -> int:
  """Starts multiload in the background on the VM and returns its PID."""
  stdout, _ = vm.RemoteCommand(
      f'nohup {multiload_cmd} > {_MULTILOAD_OUT} 2>&1 < /dev/null & echo $!'
  )
  return int(stdout.strip())


def _StopMultiload(vm, pid: int) -> None:
  """Sends SIGTERM to multiload and waits for it to exit."""
  vm.RemoteCommand(
      f'kill -TERM {pid} 2>/dev/null; '
      f'while kill -0 {pid} 2>/dev/null; do sleep 0.1; done',
      ignore_failure=True,
  )


def _CountBandwidthSamples(vm) -> int:
  """Returns the number of bandwidth samples multiload has written so far."""
  stdout, _ = vm.RemoteCommand(
      f'grep -c "{_BW_SAMPLE_MARKER}" {_MULTILOAD_OUT} || true',
      ignore_failure=True,
  )
  return int(stdout.strip() or 0)


def _WaitForBandwidthSamples(vm, min_count: int) -> int:
  """Waits until multiload has written at least min_count bandwidth samples.

  Args:
    vm: The VM running multiload.
    min_count: Minimum number of bandwidth samples to wait for.

  Returns:
    The number of bandwidth samples written when the wait finished.

  Raises:
    errors.Benchmarks.RunError: If the samples do not appear within
      _SAMPLE_WAIT_TIMEOUT_SEC.
  """

  @vm_util.Retry(
      poll_interval=_SAMPLE_POLL_INTERVAL_SEC,
      timeout=_SAMPLE_WAIT_TIMEOUT_SEC,
      max_retries=-1,
      fuzz=0,
      log_errors=False,
      retryable_exceptions=(_MultiloadNotReadyError,),
  )
  def _Poll() -> int:
    count = _CountBandwidthSamples(vm)
    if count < min_count:
      raise _MultiloadNotReadyError(f'{count} < {min_count}')
    return count

  try:
    return _Poll()
  except vm_util.RetryError as e:
    raise errors.Benchmarks.RunError(
        f'multiload did not write {min_count} bandwidth samples within'
        f' {_SAMPLE_WAIT_TIMEOUT_SEC}s.'
    ) from e


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _GetCoresForMultiload(vm) -> tuple[int, int, list[int]]:
  """Selects the multichase CPU/NUMA node and the multiload CPUs.

  Only CPUs allowed for the benchmark process (vm.GetCpusAllowedSet()) are
  considered. Multichase runs on the last selected core (N-1) and its NUMA
  node; multiload runs on cores 0 to N-2, excluding SMT siblings of core N-1.

  Args:
    vm: VirtualMachine to inspect.

  Returns:
    A tuple of (multichase_cpu, multichase_numa_node, multiload_cores).

  Raises:
    errors.Config.InvalidValue: If loaded_latency_num_cores is invalid.
  """
  allowed_cpus = vm.GetCpusAllowedSet()
  proc_cpu_mappings = vm.CheckProcCpu().mappings
  cpu_to_node = {}
  for node, cpus in numactl.GetNumaCpus(vm).items():
    for cpu in cpus:
      cpu_to_node[cpu] = node

  core_to_cpus = collections.defaultdict(list)
  cpu_to_core_key = {}
  all_cpus = sorted(allowed_cpus)
  for cpu_id in all_cpus:
    cpu_info = proc_cpu_mappings.get(cpu_id, {})
    # 'core id'/'physical id' may be absent (e.g. ARM without SMT); treat each
    # CPU as its own physical core.
    socket_id = int(cpu_info.get('physical id', 0))
    core_id = int(cpu_info.get('core id', cpu_id))
    core_key = (socket_id, core_id)
    cpu_to_core_key[cpu_id] = core_key
    core_to_cpus[core_key].append(cpu_id)

  total_vcpus = len(all_cpus)

  if FLAGS.loaded_latency_physical_cores_only:
    # 1 hardware thread (min CPU ID) per physical core, sorted ascending
    pool = sorted(min(cpus) for cpus in core_to_cpus.values())
  else:
    pool = all_cpus

  if FLAGS.loaded_latency_num_cores is not None:
    n = FLAGS.loaded_latency_num_cores
    if n < 2:
      raise errors.Config.InvalidValue(
          f'loaded_latency_num_cores must be at least 2, got {n}'
      )
    if n > total_vcpus:
      raise errors.Config.InvalidValue(
          f'loaded_latency_num_cores ({n}) cannot exceed total vCPUs'
          f' ({total_vcpus})'
      )
    if n > len(pool):
      raise errors.Config.InvalidValue(
          f'loaded_latency_num_cores ({n}) exceeds available cores'
          f' ({len(pool)}) when physical_cores_only='
          f'{FLAGS.loaded_latency_physical_cores_only}'
      )
    selected_pool = pool[:n]
  else:
    selected_pool = pool

  # Run multichase on the last core (index N-1) and multiload on cores 0..N-2
  # (excluding any SMT sibling of the multichase physical core).
  multichase_cpu = selected_pool[-1]
  multichase_numa_node = cpu_to_node.get(multichase_cpu, 0)
  reserved_core = cpu_to_core_key[multichase_cpu]
  multiload_cores = [
      cpu for cpu in selected_pool[:-1] if cpu_to_core_key[cpu] != reserved_core
  ]

  return multichase_cpu, multichase_numa_node, multiload_cores


def _ComputeGap(s1: DelayPoint, s2: DelayPoint) -> float:
  """Computes Euclidean distance in (0.001 * MiB/s, latency_ns) space.

  Bandwidth in MiB/s
  (typically 0 to ~400,000 MiB/s) is scaled by 0.001 (~GB/s, 0 to ~400) so its
  numerical range is comparable to memory latency in nanoseconds (~80 to ~350
  ns). This ensures neither axis dominates when selecting which interval on the
  bandwidth-vs-latency curve has the largest gap.

  Args:
    s1: First sample.
    s2: Second sample.

  Returns:
    Euclidean distance between s1 and s2 in (0.001 * MiB/s, ns) space.
  """
  return math.hypot(
      0.001 * (s1.load_avg_mibs - s2.load_avg_mibs),
      s1.latency_ns - s2.latency_ns,
  )


def _ProbeDelayRange(
    measure: Callable[[int], DelayPoint],
    max_delay: int,
    idle_bw_fraction: float,
    first_probe: int = 64,
) -> tuple[int, list[DelayPoint]]:
  """Finds x_end: smallest doubling delay whose BW < fraction * BW(d=0).

  Phase 1 of adaptive delay sampling:
    1. Measure d=0 to establish maximum memory saturation bandwidth (peak_bw).
    2. Compute the near-idle bandwidth threshold (idle_bw_fraction * peak_bw).
    3. Probe exponentially doubling delays (64, 128, 256, 512, ...) until
       either the measured load bandwidth drops below the near-idle threshold
       or max_delay is reached. Every probed point is saved and reused as an
       initial seed for Phase 2 (_AdaptiveSampling).

  Args:
    measure: Function that takes an integer injection delay d and returns the
      measured DelayPoint.
    max_delay: Hard upper bound on the injection delay (-d).
    idle_bw_fraction: Fraction of peak_bw below which the memory subsystem is
      considered effectively unloaded (e.g. 0.02 = 2% of d=0 bandwidth).
    first_probe: Starting non-zero delay for exponential doubling (default 64,
      which also guarantees d=64 is always sampled).

  Returns:
    A tuple (x_end, probes) where x_end is the upper bound delay and probes is
    the list of DelayPoints measured during probing.
  """
  probes = [measure(0)]
  peak_bw = probes[0].load_avg_mibs
  threshold = idle_bw_fraction * peak_bw
  d = first_probe
  while d < max_delay:
    s = measure(d)
    probes.append(s)
    if s.load_avg_mibs < threshold:
      return d, probes
    d *= 2
  probes.append(measure(max_delay))
  return max_delay, probes


def _AdaptiveSampling(
    measure: Callable[[int], DelayPoint],
    x_start: int,
    x_end: int,
    initial_samples: list[DelayPoint],
    max_samples: int = 30,
    gap_tolerance: float = 10.0,
    min_dx: int = 1,
) -> list[DelayPoint]:
  """Performs adaptive bisection using a max-heap of Euclidean gaps.

  Phase 2 of adaptive delay sampling:
  Instead of sweeping a fixed grid of delays, this function iteratively bisects
  whichever delay interval [left_delay, right_delay] currently has the largest
  Euclidean distance (_ComputeGap) in (bandwidth, latency) space. This
  automatically concentrates measurements around the steep saturation knee of
  the bandwidth-vs-latency curve while taking very few samples in flat regions.

  Args:
    measure: Function that takes an integer injection delay d and returns the
      measured DelayPoint.
    x_start: Lower delay boundary (typically 0 for peak load).
    x_end: Upper delay boundary discovered by _ProbeDelayRange.
    initial_samples: Seed samples already collected by _ProbeDelayRange so they
      are not re-measured.
    max_samples: Maximum total delay points to measure (including seed points).
    gap_tolerance: Stop bisecting once the largest remaining Euclidean gap
      across all adjacent intervals is below this threshold.
    min_dx: Minimum delay difference (right_delay - left_delay) eligible for
      bisection (1 = stop subdividing adjacent integer delays).

  Returns:
    List of all measured DelayPoints sorted by ascending injection delay.
  """
  # Seed the lookup map (delay -> DelayPoint) with points already measured
  # during exponential range probing to avoid duplicate VM runs.
  sample_map: dict[int, DelayPoint] = {s.delay: s for s in initial_samples}

  # Ensure both boundaries [x_start, x_end] and the canonical d=64 reference
  # point are present in sample_map before building intervals.
  if x_start not in sample_map:
    sample_map[x_start] = measure(x_start)
  if 64 not in sample_map:
    sample_map[64] = measure(64)
  if x_end not in sample_map:
    sample_map[x_end] = measure(x_end)

  # Sort initial seed points by delay and push every adjacent interval
  # (samples[i], samples[i + 1]) onto a max-heap priority queue keyed by
  # Euclidean gap. Because Python's heapq is a min-heap, we store -gap so
  # heappop() always yields the interval with the largest gap first.
  samples = sorted(sample_map.values(), key=lambda s: s.delay)
  pq = []
  for i in range(len(samples) - 1):
    gap = _ComputeGap(samples[i], samples[i + 1])
    heapq.heappush(pq, (-gap, samples[i], samples[i + 1]))

  # Greedily bisect the interval with the largest (bandwidth, latency) gap
  # until we either reach the sample budget (max_samples) or all remaining
  # intervals have a gap smaller than gap_tolerance.
  while len(sample_map) < max_samples and pq:
    neg_gap, left, right = heapq.heappop(pq)
    gap = -neg_gap

    # Since the heap orders intervals by descending gap, if the largest gap is
    # already below gap_tolerance, every other interval in pq is also below
    # gap_tolerance and we can terminate early.
    if gap < gap_tolerance:
      break

    # Skip intervals whose integer delays are already adjacent (diff <= min_dx)
    # and therefore cannot be subdivided further; continue checking other
    # intervals still in the priority queue.
    if (right.delay - left.delay) <= min_dx:
      continue

    # Compute the integer midpoint delay and guard against degenerate or
    # already-sampled delays.
    x_mid = int(round((left.delay + right.delay) / 2.0))
    if x_mid <= left.delay or x_mid >= right.delay:
      continue
    if x_mid in sample_map:
      continue

    # Measure bandwidth and latency at the midpoint delay on the target VM.
    s_mid = measure(x_mid)
    sample_map[x_mid] = s_mid

    # Split [left, right] into [left, s_mid] and [s_mid, right], compute the
    # Euclidean gap for each new sub-interval, and push both onto the heap.
    gap_l = _ComputeGap(left, s_mid)
    gap_r = _ComputeGap(s_mid, right)
    heapq.heappush(pq, (-gap_l, left, s_mid))
    heapq.heappush(pq, (-gap_r, s_mid, right))

  return sorted(sample_map.values(), key=lambda s: s.delay)


def _MeasureDelayPoint(
    vm,
    delay: int,
    multiload_path: str,
    multichase_path: str,
    multiload_numactl: str,
    multichase_numactl: str,
    multiload_cores: list[int],
) -> DelayPoint:
  """Measures a single point on the loaded latency curve."""
  multiload_threads = len(multiload_cores)
  multiload_cmd = (
      f'numactl {multiload_numactl} {multiload_path} -l'
      f' {FLAGS.loaded_latency_multiload_traffic_pattern} -t'
      f' {multiload_threads} -n 0 -m'
      f' {FLAGS.loaded_latency_multiload_buffer_size} -H -v -d {delay}'
  )

  multichase_cmd_parts = [
      'numactl',
      multichase_numactl,
      multichase_path,
      '-m',
      FLAGS.loaded_latency_multichase_memory_size,
      '-n',
      str(FLAGS.loaded_latency_multichase_num_samples),
  ]
  if FLAGS.loaded_latency_multichase_additional_flags:
    multichase_cmd_parts.append(
        FLAGS.loaded_latency_multichase_additional_flags
    )
  multichase_cmd_parts.extend([
      '-s',
      str(FLAGS.loaded_latency_multichase_stride_size),
      '-c',
      FLAGS.loaded_latency_multichase_chase_type,
  ])
  multichase_cmd = ' '.join(multichase_cmd_parts)

  pid = _StartMultiload(vm, multiload_cmd)
  try:
    # Wait for multiload to allocate and fault in its buffers and report its
    # first steady-state bandwidth sample.
    start_idx = _WaitForBandwidthSamples(vm, min_count=1)
    # Measure latency while multiload is generating traffic.
    multichase_stdout, _ = vm.RemoteCommand(multichase_cmd)
    # Wait for one more sample so the chase window is fully covered.
    _WaitForBandwidthSamples(vm, min_count=start_idx + 1)
  finally:
    _StopMultiload(vm, pid)

  multiload_stdout, _ = vm.RemoteCommand(f'cat {_MULTILOAD_OUT}')

  latency_ns = float(multichase_stdout.strip().split()[-1])
  bws = [
      float(x)
      for x in re.findall(r'Total\(MiB/s\)=\s*([0-9.]+)', multiload_stdout)
  ]
  # Samples from the last warm-up sample onward overlap the chase window.
  relevant_bws = bws[start_idx - 1 :] or bws or [0.0]
  load_avg_mibs = statistics.fmean(relevant_bws)

  return DelayPoint(delay, load_avg_mibs, latency_ns)


def Prepare(benchmark_spec) -> None:
  """Installs multichase and numactl on the VM."""
  vm = benchmark_spec.vms[0]
  vm.Install('multichase')
  vm.Install('numactl')


def Run(benchmark_spec) -> list[sample.Sample]:
  """Runs multichase and multiload simultaneously to measure loaded latency.

  Args:
    benchmark_spec: BenchmarkSpec.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]
  multichase_cpu, multichase_numa_node, multiload_cores = _GetCoresForMultiload(
      vm
  )

  multichase_path = posixpath.join(multichase.INSTALL_PATH, 'multichase')
  multiload_path = posixpath.join(multichase.INSTALL_PATH, 'multiload')

  if FLAGS.loaded_latency_multichase_numactl is not None:
    multichase_numactl = FLAGS.loaded_latency_multichase_numactl
  else:
    multichase_numactl = f'-C {multichase_cpu} -m {multichase_numa_node}'

  if FLAGS.loaded_latency_multiload_numactl is not None:
    multiload_numactl = FLAGS.loaded_latency_multiload_numactl
    m_ml_cpus = re.search(
        r'(?:-C|--physcpubind[=\s])\s*([0-9,-]+)', multiload_numactl
    )
    if m_ml_cpus:
      pinned_cpus = linux_virtual_machine.ParseRangeList(m_ml_cpus.group(1))
      if len(pinned_cpus) < len(multiload_cores):
        logging.warning(
            'multiload thread count (%d) exceeds the number of CPUs (%d)'
            ' specified in loaded_latency_multiload_numactl (%r), which may'
            ' cause thread oversubscription.',
            len(multiload_cores),
            len(pinned_cpus),
            multiload_numactl,
        )
  else:
    multiload_numactl = f'-C {",".join(str(c) for c in multiload_cores)}'

  # 1. Measure Idle Latency
  idle_chase_cmd_parts = [
      'numactl',
      multichase_numactl,
      multichase_path,
      '-m',
      FLAGS.loaded_latency_multichase_memory_size,
      '-n',
      str(FLAGS.loaded_latency_multichase_num_samples),
  ]
  if FLAGS.loaded_latency_multichase_additional_flags:
    idle_chase_cmd_parts.append(
        FLAGS.loaded_latency_multichase_additional_flags
    )
  idle_chase_cmd_parts.extend([
      '-s',
      str(FLAGS.loaded_latency_multichase_stride_size),
      '-c',
      FLAGS.loaded_latency_multichase_chase_type,
  ])
  idle_chase_cmd = ' '.join(idle_chase_cmd_parts)
  stdout, _ = vm.RemoteCommand(idle_chase_cmd)
  idle_latency_ns = float(stdout.strip().split()[-1])

  def _Measure(d: int) -> DelayPoint:
    return _MeasureDelayPoint(
        vm,
        d,
        multiload_path,
        multichase_path,
        multiload_numactl,
        multichase_numactl,
        multiload_cores,
    )

  x_end, probes = _ProbeDelayRange(
      _Measure,
      FLAGS.loaded_latency_max_delay,
      FLAGS.loaded_latency_idle_bw_fraction,
  )
  all_points = _AdaptiveSampling(
      _Measure,
      0,
      x_end,
      probes,
      max_samples=FLAGS.loaded_latency_max_samples,
      gap_tolerance=FLAGS.loaded_latency_gap_tolerance,
  )

  loaded_latency_d64 = next(
      (p.latency_ns for p in all_points if p.delay == 64), None
  )

  base_metadata = {
      'multichase_numactl': multichase_numactl,
      'multichase_cpu': multichase_cpu,
      'multichase_numa_node': multichase_numa_node,
      'multiload_numactl': multiload_numactl,
      'multiload_physical_cores': multiload_cores,
      'multiload_threads': len(multiload_cores),
      'multiload_active_cores': multiload_cores,
      'idle_latency_ns': idle_latency_ns,
      'loaded_latency_d64': loaded_latency_d64,
      'loaded_latency_multichase_numactl': (
          FLAGS.loaded_latency_multichase_numactl
      ),
      'loaded_latency_multichase_memory_size': (
          FLAGS.loaded_latency_multichase_memory_size
      ),
      'loaded_latency_multichase_num_samples': (
          FLAGS.loaded_latency_multichase_num_samples
      ),
      'loaded_latency_multichase_stride_size': (
          FLAGS.loaded_latency_multichase_stride_size
      ),
      'loaded_latency_multichase_chase_type': (
          FLAGS.loaded_latency_multichase_chase_type
      ),
      'loaded_latency_multichase_additional_flags': (
          FLAGS.loaded_latency_multichase_additional_flags
      ),
      'loaded_latency_num_cores': FLAGS.loaded_latency_num_cores,
      'loaded_latency_physical_cores_only': (
          FLAGS.loaded_latency_physical_cores_only
      ),
      'loaded_latency_multiload_buffer_size': (
          FLAGS.loaded_latency_multiload_buffer_size
      ),
      'loaded_latency_multiload_traffic_pattern': (
          FLAGS.loaded_latency_multiload_traffic_pattern
      ),
      'loaded_latency_max_delay': FLAGS.loaded_latency_max_delay,
      'loaded_latency_idle_bw_fraction': FLAGS.loaded_latency_idle_bw_fraction,
      'loaded_latency_max_samples': FLAGS.loaded_latency_max_samples,
      'loaded_latency_gap_tolerance': FLAGS.loaded_latency_gap_tolerance,
  }

  samples = [
      sample.Sample('idle_latency', idle_latency_ns, 'ns', base_metadata.copy())
  ]

  for point in all_points:
    d, bw, lat = point.delay, point.load_avg_mibs, point.latency_ns
    point_metadata = base_metadata.copy()
    point_metadata.update({
        'injection_delay': d,
        'loaded_latency_ns': lat,
        'LdAvgMibs': bw,
    })
    samples.extend([
        sample.Sample('loaded_latency', lat, 'ns', point_metadata.copy()),
        sample.Sample('LdAvgMibs', bw, 'Mibs', point_metadata.copy()),
    ])
    if d == 64:
      samples.append(
          sample.Sample('loaded_latency_d64', lat, 'ns', point_metadata.copy())
      )

  return samples


def Cleanup(benchmark_spec) -> None:
  """Stops any multiload process left behind by an interrupted run."""
  vm = benchmark_spec.vms[0]
  vm.RemoteCommand('pkill -TERM -x multiload', ignore_failure=True)
