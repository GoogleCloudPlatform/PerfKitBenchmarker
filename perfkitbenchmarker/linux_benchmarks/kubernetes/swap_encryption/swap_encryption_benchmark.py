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
"""GKE vs. AWS EKS Swap Encryption and LSSD Performance Benchmark.

Methodology: go/swap-encryption-and-lssd-performance-comparison:gke-vs-aws

== Architecture ==

Provisions a real GKE (GCP) or EKS (AWS) Kubernetes cluster via PKB's
container_cluster abstraction, then deploys a privileged DaemonSet whose
pod has full host-device access (/dev, /sys, hostPID).  All benchmark
phases execute inside this pod via kubectl exec, so measurements reflect
actual cluster-node behaviour including Kubernetes overhead (kubelet,
containerd cgroup hierarchy, etc.).

  GKE nodes  ── dm-crypt with ephemeral key (go/node:swap-encryption)
                 swap device: /dev/mapper/swap_encrypted (over dedicated
                 hyperdisk or LSSD RAID-0 /dev/md0).
  EKS nodes  ── NVMe Instance Store, Nitro hardware-offloaded encryption.

== Phases ==

  Tier 1 (Gate 1) — fio microbenchmarks (Phase 1)
    Raw I/O ceiling of the swap device.

  Tier 2 (Gate 2) — stress-ng CPU overhead + I/O interference (Phase 2a/2b)
    Requires an active swap device (Gate 1 must pass).

  Phase 3b — kernel build under memory-capped cgroup
    Downloads and compiles Linux kernel; compares constrained vs.
    unconstrained build time to quantify swap pressure impact.

Phase 2 flags (_STRESS_*) are defined in phases.py alongside the phase code.
Phase 3b flags (_KERNEL_*) are defined here (benchmark-level config).
"""

import logging
import textwrap
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import phases as _phases
from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import utils as _utils
from perfkitbenchmarker.resources.container_service import (
    swap_daemonset as _ds_mod,
)

FLAGS = flags.FLAGS

_BenchmarkSpec = bm_spec_lib.BenchmarkSpec

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------

BENCHMARK_NAME = 'swap_encryption'

BENCHMARK_CONFIG = """
swap_encryption:
  description: >
    CPU/IO overhead + kernel build benchmarks (Tiers 2 + 3b) on swap-encrypted
    GKE/EKS nodes.
    Swap-enabled 'benchmark' nodepool declared in BENCHMARK_CONFIG;
    GKE cluster creation applies --system-config-from-file (dm-crypt
    swapConfig) automatically via swap_config field on NodepoolSpec.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-medium
        boot_disk_size: 20
        zone: us-central1-a
    nodepools:
      benchmark:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n4-highmem-32
            boot_disk_type: hyperdisk-balanced
            boot_disk_size: 500
            zone: us-central1-a
        swap_config:
          enabled: true
          swappiness: 100
          min_free_kbytes: 200
          watermark_scale_factor: 500
          boot_disk_iops: 160000
          boot_disk_throughput: 2400
"""

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------

_FIO_RUNTIME_SEC = flags.DEFINE_integer(
    'swap_encryption_fio_runtime_sec',
    60,
    'Wall-clock seconds each fio job runs in Phase 1.',
)

_NODEPOOL = flags.DEFINE_string(
    'swap_encryption_nodepool',
    'benchmark',
    'Name of the nodepool where the benchmark DaemonSet runs.',
)

_INSTANCE_SIZE_LABEL = flags.DEFINE_string(
    'swap_encryption_instance_size_label',
    '',
    'Human-readable instance size label stored in sample metadata.',
)

_COLLECT_COST = flags.DEFINE_boolean(
    'swap_encryption_collect_cost',
    False,
    'Emit a cost_estimate_usd sample at the end of the run.',
)

_FAIL_ON_DEGRADED = flags.DEFINE_boolean(
    'swap_encryption_fail_on_degraded',
    False,
    'Raise RunError if the run is marked degraded.',
)

_PHASES = flags.DEFINE_list(
    'swap_encryption_phases',
    ['all'],
    "Comma-separated phases: fio, 2a, 2b, 3b. Default 'all' runs every phase.",
)

_BENCHMARK_MACHINE_TYPE = flags.DEFINE_string(
    'swap_encryption_benchmark_machine_type',
    '',
    'Machine type for cost estimation when cloud metadata is unavailable.',
)

_KERNEL_VERSION = flags.DEFINE_string(
    'swap_encryption_kernel_version',
    '6.1.38',
    'Linux kernel version to download and compile for the build workload.',
)

_KERNEL_MEMORY_MB = flags.DEFINE_integer(
    'swap_encryption_kernel_memory_mb',
    512,
    'cgroup memory limit in MB applied during the constrained kernel build.',
)


# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

_DS_NAME = 'pkb-swap-benchmark'
_DS_NAMESPACE = 'default'
_DS_LABEL = 'pkb-swap-benchmark'
_BENCHMARK_NODEPOOL = 'benchmark'

# Set by _EnsureIo2Volume(); passed to SetupSwap for serial-based detection.
_IO2_VOLUME_ID = ''

# ---------------------------------------------------------------------------
# PKB entry points
# ---------------------------------------------------------------------------


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=invalid-name
  """Load and return benchmark config spec."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(spec: _BenchmarkSpec) -> None:  # pylint: disable=invalid-name
  """Deploy DaemonSet, tune kernel swap settings, and configure swap device.

  PKB cluster creation automatically provisions the swap-enabled 'benchmark'
  nodepool (swap_config declared in BENCHMARK_CONFIG). This function:
    1. Deploys the privileged SwapDaemonSet and waits for Running.
    2. Deletes the cheap e2-medium default-pool.
    3. Tunes kernel swap aggressiveness (swappiness, min_free_kbytes).
    4. Unlocks container cgroup swap limits.
    5. Optionally enables zswap.
    6. Configures cloud-specific swap via SwapDaemonSet.SetupSwap().

  Args:
    spec: The benchmark spec.
  """
  cluster = spec.container_cluster

  logging.info('[swap_encryption] Deploying privileged DaemonSet')
  daemonset_class = resource.GetResourceClass(
      _ds_mod.SwapDaemonSet, CLOUD=spec.container_cluster.CLOUD
  )
  daemonset = daemonset_class(
      name=_DS_NAME,
      namespace=_DS_NAMESPACE,
      label=_DS_LABEL,
      nodepool=_BENCHMARK_NODEPOOL,
      image=FLAGS.swap_encryption_daemonset_image,
  )
  spec.resources.append(daemonset)
  daemonset.Create()
  logging.info('[swap_encryption] Benchmark pod ready: %s', daemonset.pod_name)
  _DeleteDefaultPool(cluster)
  daemonset.WaitForPod()
  logging.info(
      '[swap_encryption] Benchmark pod (post-deletion): %s', daemonset.pod_name
  )

  # Tune kernel swap aggressiveness.
  daemonset.PodExec('sysctl -w vm.swappiness=100')
  if FLAGS.swap_encryption_min_free_kbytes > 0:
    daemonset.PodExec(
        f'sysctl -w vm.min_free_kbytes={FLAGS.swap_encryption_min_free_kbytes}'
    )

  # Unlock container cgroup swap.
  daemonset.PodExec(
      textwrap.dedent("""
    PKB_CG=$(awk -F: '/^0::/{print $3; exit}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_CG" ] && [ -f "/sys/fs/cgroup${PKB_CG}/memory.swap.max" ]; then
      echo max > "/sys/fs/cgroup${PKB_CG}/memory.swap.max" 2>/dev/null || true
    fi
    PKB_CG1=$(awk -F: '/:memory:/{print $3; exit}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_CG1" ] && \
       [ -f "/sys/fs/cgroup/memory${PKB_CG1}/memory.memsw.limit_in_bytes" ]; then
      echo -1 > "/sys/fs/cgroup/memory${PKB_CG1}/memory.memsw.limit_in_bytes" \
        2>/dev/null || true
    fi
  """),
      ignore_failure=True,
  )

  # Enable zswap if requested.
  if FLAGS.swap_encryption_enable_zswap:
    daemonset.EnableZswap()

  # Configure cloud-specific swap via the daemonset abstraction.
  daemonset.SetupSwap(
      swap_type=FLAGS.swap_encryption_swap_type,
      enable_dmcrypt=FLAGS.swap_encryption_enable_dmcrypt,
      swap_size_gb=FLAGS.swap_encryption_swap_size_gb,
      io2_volume_id=_IO2_VOLUME_ID,
  )


def Run(spec: _BenchmarkSpec) -> list[sample.Sample]:  # pylint: disable=invalid-name
  """Execute all benchmark phases with gate logic.

  Tier 1 (Gate 1): fio microbenchmarks — raw I/O ceiling of the swap device.
  Tier 2 (Gate 2): stress-ng CPU overhead (2a) + IO interference (2b).
    Requires Gate 1 to pass (no point measuring app-level swap when the raw
    device is inaccessible).
  Phase 3b: kernel build under memory-capped cgroup (independent gate).

  Args:
    spec: The benchmark spec.

  Returns:
    A list of sample.Sample objects.
  """
  daemonset = _GetDaemonset(spec)

  pod = daemonset.WaitForPod()
  daemonset.oom_events.clear()
  daemonset.pod_lost.clear()
  original_pod = pod
  degraded_reasons: list[str] = []

  swap_dev = daemonset.GetActiveSwapDevice(FLAGS.swap_encryption_swap_device)
  base_meta = _utils.BuildMetadata(
      daemonset,
      swap_dev,
      swap_type=FLAGS.swap_encryption_swap_type,
      enable_dmcrypt=FLAGS.swap_encryption_enable_dmcrypt,
      node_image_type=FLAGS.swap_encryption_node_image_type,
      boot_disk_type=FLAGS.swap_encryption_boot_disk_type,
      boot_disk_iops=FLAGS.swap_encryption_boot_disk_iops,
      benchmark_machine_type=_BENCHMARK_MACHINE_TYPE.value,
      enable_zswap=FLAGS.swap_encryption_enable_zswap,
      min_free_kbytes=FLAGS.swap_encryption_min_free_kbytes,
      fio_runtime_sec=_FIO_RUNTIME_SEC.value,
      nodepool=_NODEPOOL.value,
      instance_size_label=_INSTANCE_SIZE_LABEL.value,
  )
  results: list[sample.Sample] = []
  t_run_start = time.time()

  logging.info('[swap_encryption] swap device: %s', swap_dev)

  # ── Tier 1 / Gate 1: fio microbenchmarks ──────────────────────────────────
  tier1_results = []
  if _PhaseSelected('fio'):
    logging.info('[swap_encryption] ── Tier 1 / Gate 1: fio microbenchmarks ──')
    try:
      tier1_results = _phases.RunPhase1Fio(
          daemonset,
          swap_dev,
          base_meta,
          _FIO_RUNTIME_SEC.value,
          FLAGS.swap_encryption_swap_type,
      )
      results += tier1_results
    except Exception as e:  # pylint: disable=broad-except
      logging.error('[swap_encryption] Gate 1 FAILED — fio phase error: %s', e)
      return results

    if not tier1_results:
      logging.info(
          '[swap_encryption] Gate 1 produced no samples '
          '(loop-device skip or parse error) — continuing to Tier 2'
      )
  else:
    logging.info(
        '[swap_encryption] Skipping Tier 1 (fio)'
        ' — not in --swap_encryption_phases'
    )

  if tier1_results or swap_dev.startswith('/dev/loop'):
    # ── Tier 2 / Gate 2: stress-ng CPU + I/O interference ────────────────────
    logging.info('[swap_encryption] ── Tier 2 / Gate 2: stress-ng ──')
    try:
      if _PhaseSelected('2a'):
        logging.info('[swap_encryption] Phase 2a: CPU overhead')
        results += _phases.RunPhase2a(daemonset, base_meta, degraded_reasons)
      if _PhaseSelected('2b'):
        logging.info('[swap_encryption] Phase 2b: I/O interference')
        results += _phases.RunPhase2b(daemonset, base_meta)
    except Exception as e:  # pylint: disable=broad-except
      logging.error(
          '[swap_encryption] Gate 2 FAILED — stress phase error: %s', e
      )

    # ── Tier 3: Real-world workloads ────────────────────────────────────────
    logging.info('[swap_encryption] ── Tier 3: Real-world workloads ──')

    if _PhaseSelected('3a'):
      logging.info('[swap_encryption] Phase 3a: redis benchmark')
      try:
        results += _phases.RunPhase3aRedis(daemonset, base_meta)
      except Exception as e:  # pylint: disable=broad-except
        logging.error('[swap_encryption] Phase 3a FAILED: %s', e)

    if _PhaseSelected('3b'):
      logging.info('[swap_encryption] Phase 3b: kernel build under memory cap')
      try:
        results += _phases.RunPhase3b(
            daemonset,
            base_meta,
            kernel_version=_KERNEL_VERSION.value,
            kernel_memory_mb=_KERNEL_MEMORY_MB.value,
        )
      except Exception as e:  # pylint: disable=broad-except
        logging.error('[swap_encryption] Phase 3b FAILED: %s', e)

  # ── Cost estimate ──────────────────────────────────────────────────────────
  if _COLLECT_COST.value:
    elapsed = time.time() - t_run_start
    results += _utils.CollectCostSample(
        daemonset,
        elapsed,
        base_meta,
        _INSTANCE_SIZE_LABEL.value,
        _BENCHMARK_MACHINE_TYPE.value,
    )

  # ── Final degradation gate ────────────────────────────────────────────────
  if daemonset.pod_name and daemonset.pod_name != original_pod:
    degraded_reasons.append(
        f'benchmark pod was replaced during the run ({original_pod} →'
        f' {daemonset.pod_name}) — OOM-evicted under swap pressure'
    )
  if daemonset.pod_lost:
    degraded_reasons.append(
        'benchmark pod(s) went NotFound during the run'
        f' ({", ".join(daemonset.pod_lost)})'
    )
  if daemonset.oom_events:
    degraded_reasons.append(
        'OOM kill(s) (rc=137) occurred during the run on pod(s) '
        f'{", ".join([e for e in daemonset.oom_events if e])}'
    )

  if _PhaseSelected('fio') and not tier1_results:
    if swap_dev.startswith('/dev/loop'):
      logging.info(
          '[swap_encryption] Gate 1 (fio) skipped (loop) — Tier 2 valid: %s',
          swap_dev,
      )
    else:
      degraded_reasons.append(
          'Gate 1 (fio microbenchmarks) produced no samples'
      )

  degraded = bool(degraded_reasons)
  results.append(
      sample.Sample(
          'swap_encryption_run_status',
          0.0 if degraded else 1.0,
          'status',
          dict(
              base_meta,
              degraded=degraded,
              degraded_reasons='; '.join(degraded_reasons) or 'none',
              num_samples=len(results) + 1,
          ),
      )
  )

  if degraded:
    msg = '[swap_encryption] RUN DEGRADED — ' + '; '.join(degraded_reasons)
    logging.error(msg)
    if _FAIL_ON_DEGRADED.value:
      raise errors.Benchmarks.RunError(msg)
  else:
    logging.info(
        '[swap_encryption] Run completed cleanly (%d samples)', len(results)
    )

  return results


def Cleanup(spec: _BenchmarkSpec) -> None:  # pylint: disable=unused-argument,invalid-name
  """Resources in spec.resources are auto-deleted by the PKB framework.

  SwapDaemonSet._Delete() runs in-pod teardown (swapoff, dmsetup remove,
  losetup cleanup, pkill fio/stress-ng) then deletes the DaemonSet.

  Args:
    spec: The benchmark spec.
  """


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _DeleteDefaultPool(cluster: Any) -> None:
  """Delete the dummy e2-medium default-pool once the benchmark pod is Running.

  GKE requires at least one nodepool at cluster creation time; the e2-medium
  default-pool satisfies that requirement. Deleting it before the DaemonSet
  pod is Running can trigger a brief API-server timeout while two concurrent
  nodepool operations are in progress.

  Args:
    cluster: The GKE cluster object.
  """
  try:
    cluster.DeleteNodePool('default-pool')
  except Exception as e:  # pylint: disable=broad-except
    logging.warning('[swap_encryption] Could not delete default-pool: %s', e)


def _GetDaemonset(spec: _BenchmarkSpec) -> _ds_mod.SwapDaemonSet:
  """Retrieve the SwapDaemonSet resource from spec.resources."""
  daemonset = next(
      (r for r in spec.resources if isinstance(r, _ds_mod.SwapDaemonSet)),
      None,
  )
  if daemonset is None:
    raise errors.Benchmarks.RunError(
        '[swap_encryption] SwapDaemonSet not found in spec.resources —'
        ' was Prepare() called?'
    )
  return daemonset


def _PhaseSelected(token: str) -> bool:
  """Return True if phase token should run given --swap_encryption_phases."""
  selected = [p.strip().lower() for p in _PHASES.value if p.strip()]
  return (not selected) or ('all' in selected) or (token.lower() in selected)


def _ConfigureEksKubeletSwap(spec) -> None:  # pylint: disable=unused-argument
  """Configure EKS kubelet for LimitedSwap (deferred — blocked on PR #6780)."""
  raise NotImplementedError(
      '[swap_encryption] EKS kubelet LimitedSwap config via nodeadm is '
      'deferred (blocked on PR #6780).'
  )


def _EnsureIo2Volume() -> None:
  """Create and attach an io2 EBS volume for swap on EKS."""
  if FLAGS.swap_encryption_swap_type != 'io2':
    return
  logging.info('[swap_encryption] io2 swap volume provisioning deferred')
