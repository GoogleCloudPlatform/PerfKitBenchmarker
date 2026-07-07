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
"""Phase helper functions for swap_encryption_benchmark.

Contains fio phase runner (Phase 1) and stress-ng CPU/IO overhead phases
(Phase 2a / 2b) used by swap_encryption_benchmark.
Mirrors the pattern used by fio/utils.py for the fio benchmark.

PR4 adds Phase 2a (stress-ng CPU overhead sweep) and Phase 2b (IO interference
under concurrent swap pressure) on top of the PR3 Phase 1 fio microbenchmarks.
Phase 2 flags are defined here to keep this module self-contained.
"""

import json
import logging
import re
import textwrap
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample


# ---------------------------------------------------------------------------
# Phase 2 flags (defined here; benchmark.py defines general execution flags).
# ---------------------------------------------------------------------------

_STRESS_TIMEOUT_SEC = flags.DEFINE_integer(
    'swap_encryption_stress_timeout_sec',
    120,
    'Duration in seconds of each stress-ng memory-pressure phase.',
)

_STRESS_VM_BYTES = flags.DEFINE_string(
    'swap_encryption_stress_vm_bytes',
    '28G',
    'Combined stress-ng working-set size (total in-flight footprint, not '
    'per-worker).  It is divided equally across --swap_encryption_stress_vm_'
    'workers before being passed to stress-ng, so the total memory touched '
    'equals this value regardless of worker count.',
)

_STRESS_VM_BYTES_LIST = flags.DEFINE_string(
    'swap_encryption_stress_vm_bytes_list',
    '',
    'Comma-separated list of stress-ng --vm-bytes values to iterate over '
    'in Phase 2a (pressure-curve sweep, gap 5).  When non-empty, '
    'this overrides --swap_encryption_stress_vm_bytes and Phase 2a is run '
    'once per listed value.',
)

_STRESS_VM_WORKERS = flags.DEFINE_integer(
    'swap_encryption_stress_vm_workers',
    4,
    'Number of parallel stress-ng --vm workers for Phase 2a.  The total '
    'vm_bytes is divided by this number per worker.',
)

_MIN_SWAP_OUT_PAGES = flags.DEFINE_integer(
    'swap_encryption_min_swap_out_pages',
    500,
    'Minimum peak swap-out pages/s required to consider a Phase 2a attempt '
    'valid.  If all attempts fall below this threshold the run is marked '
    'degraded.',
)

# ---------------------------------------------------------------------------
# fio job definitions for Phase 1.
# Each entry: (name, rw_mode, block_size, iodepth, human_label)
# ---------------------------------------------------------------------------

_FIO_JOBS = (
    ('rand_write_iops', 'randwrite', '4k', 256, 'Random write IOPS'),
    ('rand_read_iops', 'randread', '4k', 256, 'Random read IOPS'),
    ('rand_rw_mixed', 'randrw', '4k', 256, 'Mixed random R/W (50/50)'),
    ('seq_write_bw', 'write', '1m', 64, 'Sequential write bandwidth'),
    ('seq_read_bw', 'read', '1m', 64, 'Sequential read bandwidth'),
    ('lat_write', 'randwrite', '4k', 1, 'Random write latency'),
    ('lat_read', 'randread', '4k', 1, 'Random read latency'),
)

# GiB to pre-fill before read jobs — enough to warm up the dm-crypt pipeline.
_PREFILL_GIB = 20

# ---------------------------------------------------------------------------
# Phase 2 constants.
# ---------------------------------------------------------------------------

# Mutable single-element list used as a module-level cache for the detected
# stress-ng --vm-method value.  Populated on first call to _get_stress_vm_method.
_stress_vm_method: list[str] = []

# Kernel thread names associated with swap encryption.
_CRYPTO_PROCS = ('kswapd', 'kworker', 'kcryptd', 'dmcrypt_write')


# ===========================================================================
# Phase 1: fio microbenchmarks
# ===========================================================================

def RunPhase1Fio(
    daemonset,
    swap_dev: str,
    base_meta: dict[str, Any],
    fio_runtime_sec: int,
    swap_type: str = '',
) -> list[sample.Sample]:
  """Run fio microbenchmarks on the raw swap block device (Phase 1).

  Skipped only for an UNINTENTIONAL loop fallback (a single-disk node with no
  dedicated swap disk, where fio on the loop would measure the boot ext4
  filesystem rather than the swap stack).  When the user explicitly selects the
  boot_disk target, the loop over the boot disk IS the device under test.

  Pre-fills the device before read tests so measurements reflect real data
  patterns rather than zero-filled pages.  Each fio job writes output to a
  temporary JSON file inside the pod then reads it back separately, avoiding
  a potential truncation race on large outputs.

  Args:
    daemonset: Active SwapDaemonSet resource.
    swap_dev: Block device path, e.g. /dev/mapper/swap_encrypted.
    base_meta: Shared metadata dict from BuildMetadata().
    fio_runtime_sec: Wall-clock seconds each fio job runs.
    swap_type: Storage target value (e.g. 'boot_disk', 'hyperdisk').  Used to
      decide whether a loop device is intentional.

  Returns:
    List of Sample objects with IOPS, bandwidth and latency metrics, or an
    empty list if the phase is skipped.
  """
  if swap_dev.startswith('/dev/loop') and swap_type != 'boot_disk':
    logging.info(
        '[swap_encryption] Phase 1 (fio) SKIPPED for plain loop device %s'
        ' (unintentional single-disk fallback).',
        swap_dev,
    )
    return []

  results = []

  daemonset.PodExec(f'swapoff {swap_dev}', ignore_failure=True)

  prefill_timeout = _PREFILL_GIB * 1024 // 150 + 60
  prefill_timeout = max(prefill_timeout, 300)
  logging.info(
      '[swap_encryption] Pre-filling %d GiB of %s', _PREFILL_GIB, swap_dev
  )
  daemonset.PodExec(
      (
          f'fio --name=prefill --filename={swap_dev} --ioengine=libaio'
          f' --direct=1 --rw=write --bs=1m --size={_PREFILL_GIB}g --verify=0'
          ' --output=/tmp/pkb_fio_prefill.log'
      ),
      timeout=prefill_timeout,
      ignore_failure=True,
  )

  fio_run_timeout = fio_runtime_sec + 90
  fio_read_timeout = 60

  for name, rw, bs, depth, label in _FIO_JOBS:
    logging.info('[swap_encryption] fio: %s', name)
    out_file = f'/tmp/pkb_fio_{name}.json'
    daemonset.PodExec(
        f'rm -f {out_file}',
        ignore_failure=True,
        _retries=0,
        timeout=15,
    )
    run_cmd = (
        f'fio --name={name} --filename={swap_dev} '
        '--ioengine=libaio --direct=1 --verify=0 --randrepeat=0 '
        f'--bs={bs} --iodepth={depth} --rw={rw} '
        f'--time_based --runtime={fio_runtime_sec}s '
        f'--output-format=json --output={out_file}'
    )
    _, err = daemonset.PodExec(
        run_cmd,
        timeout=fio_run_timeout,
        ignore_failure=True,
        _retries=0,
    )
    if 'connection reset by peer' in err:
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] fio {name}: kubectl exec connection reset.'
      )
    out, _ = daemonset.PodExec(
        f'cat {out_file} 2>/dev/null || echo ""',
        timeout=fio_read_timeout,
        ignore_failure=True,
    )
    results += ParseFioJson(out, name, base_meta, label)

  daemonset.PodExec(
      f'mkswap {swap_dev} && swapon {swap_dev}',
      ignore_failure=True,
      timeout=120,
  )

  logging.info(
      '[swap_encryption] Phase 1 complete (%d samples)', len(results)
  )
  return results


def ParseFioJson(
    fio_output: str,
    job_name: str,
    base_meta: dict[str, Any],
    label: str = '',
) -> list[sample.Sample]:
  """Parse fio --output-format=json output into PKB Sample objects.

  Args:
    fio_output: Raw stdout from fio with --output-format=json.
    job_name: Short identifier embedded in metric names.
    base_meta: Shared metadata dict copied into each sample.
    label: Human-readable job description stored in sample metadata.

  Returns:
    List of Sample objects; empty if output cannot be parsed or all zero.
  """
  results = []
  try:
    data = json.loads(fio_output)
  except (json.JSONDecodeError, ValueError) as e:
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] fio JSON parse failed for {job_name}: {e}'
    ) from e

  meta = dict(base_meta, fio_job=job_name, fio_label=label)
  for job in data.get('jobs', []):
    for direction in ('read', 'write'):
      d = job.get(direction, {})
      if not d or d.get('io_bytes', 0) == 0:
        continue
      iops = float(d.get('iops', 0))
      bw_kib = float(d.get('bw', 0))
      clat = d.get('clat_ns', {})
      pct = clat.get('percentile', {})
      lat_mean = float(clat.get('mean', 0)) / 1000.0
      lat_p50 = float(pct.get('50.000000', 0)) / 1000.0
      lat_p99 = float(pct.get('99.000000', 0)) / 1000.0
      lat_p999 = float(pct.get('99.900000', 0)) / 1000.0
      m = dict(meta, direction=direction)
      results += [
          sample.Sample(f'{job_name}_{direction}_iops', iops, 'iops', m),
          sample.Sample(
              f'{job_name}_{direction}_bw_mbps', bw_kib / 1024, 'MB/s', m
          ),
          sample.Sample(
              f'{job_name}_{direction}_lat_mean', lat_mean, 'us', m
          ),
          sample.Sample(f'{job_name}_{direction}_lat_p50', lat_p50, 'us', m),
          sample.Sample(f'{job_name}_{direction}_lat_p99', lat_p99, 'us', m),
          sample.Sample(
              f'{job_name}_{direction}_lat_p999', lat_p999, 'us', m
          ),
      ]
  return results


# ===========================================================================
# Phase 2 helpers
# ===========================================================================

def _parse_vm_bytes_to_mb(vm_bytes: str) -> float:
  """Parse a vm-bytes string like '28G', '512M', '1024k' into megabytes."""
  vm_bytes = vm_bytes.strip()
  if not vm_bytes:
    return 0.0
  suffix = vm_bytes[-1].upper()
  try:
    value = float(vm_bytes[:-1])
  except ValueError:
    return 0.0
  if suffix == 'G':
    return value * 1024.0
  elif suffix == 'M':
    return value
  elif suffix == 'K':
    return value / 1024.0
  elif suffix == 'T':
    return value * 1024.0 * 1024.0
  else:
    try:
      return float(vm_bytes) / (1024.0 * 1024.0)
    except ValueError:
      return 0.0


def _per_worker_vm_bytes(total_vm_bytes: str, workers: int) -> str:
  """Split a total vm-bytes target across N stress-ng --vm workers."""
  workers = max(1, int(workers))
  total_mb = _parse_vm_bytes_to_mb(total_vm_bytes)
  if total_mb <= 0:
    return total_vm_bytes
  per_worker_mb = max(1, int(total_mb / workers))
  return f'{per_worker_mb}M'


def _cgroup_swap_limit_mb(daemonset) -> float:
  """Return the swap budget (in MB) that the benchmark cgroup can use.

  Returns float('inf') when uncapped, limit in MB when capped, 0.0 when
  fully locked, -1.0 when the limit could not be read.
  """
  probe = textwrap.dedent("""
    mypid=$$
    for procf in $(find /sys/fs/cgroup -path '*kubepods*' -name cgroup.procs 2>/dev/null)
    do
      if grep -qx "$mypid" "$procf" 2>/dev/null
      then
        d=$(dirname "$procf")
        if [ -f "$d/memory.swap.max" ]
        then
          echo "V2=$(cat "$d/memory.swap.max" 2>/dev/null)"
        elif [ -f "$d/memory.memsw.limit_in_bytes" ] && [ -f "$d/memory.limit_in_bytes" ]
        then
          echo "MEMSW=$(cat "$d/memory.memsw.limit_in_bytes" 2>/dev/null) MEM=$(cat "$d/memory.limit_in_bytes" 2>/dev/null)"
        fi
        break
      fi
    done
  """)
  try:
    out, _ = daemonset.PodExec(probe, timeout=20, ignore_failure=True)
  except Exception as e:  # pylint: disable=broad-except
    logging.info('[swap_encryption] cgroup swap-limit probe failed: %s', e)
    return -1.0

  text = (out or '').strip()
  m = re.search(r'V2=(\S+)', text)
  if m:
    val = m.group(1)
    if val == 'max':
      return float('inf')
    try:
      return int(val) / (1024.0 * 1024.0)
    except ValueError:
      return -1.0
  m = re.search(r'MEMSW=(\S+)\s+MEM=(\S+)', text)
  if m:
    try:
      memsw = int(m.group(1))
      mem = int(m.group(2))
    except ValueError:
      return -1.0
    if memsw >= (1 << 62):
      return float('inf')
    return max(0.0, (memsw - mem) / (1024.0 * 1024.0))
  return -1.0


def _autoscale_vm_bytes(
    daemonset,
    vm_bytes: str,
    degraded_reasons: list[str],
) -> str:
  """Ensure vm_bytes forces real swap I/O without hard-crashing the container.

  Target formula: target = RAM + min(swap_size x 0.25, 64 GB).
  Hard ceiling: RAM + swap_size - 4 GB headroom.
  """
  try:
    meminfo_out, _ = daemonset.PodExec('cat /proc/meminfo', timeout=15)
    node_ram_kb = 0
    swap_total_kb = 0
    for line in meminfo_out.splitlines():
      if line.startswith('MemTotal:'):
        parts = line.split()
        if len(parts) >= 2:
          node_ram_kb = int(parts[1])
      elif line.startswith('SwapTotal:'):
        parts = line.split()
        if len(parts) >= 2:
          swap_total_kb = int(parts[1])
      if node_ram_kb and swap_total_kb:
        break

    if node_ram_kb <= 0:
      logging.info(
          '[swap_encryption] Could not read MemTotal; using vm_bytes=%s',
          vm_bytes,
      )
      return vm_bytes

    node_ram_mb = node_ram_kb / 1024.0
    swap_total_mb = swap_total_kb / 1024.0
    requested_mb = _parse_vm_bytes_to_mb(vm_bytes)
    if requested_mb <= 0:
      return vm_bytes

    cgroup_swap_mb = _cgroup_swap_limit_mb(daemonset)
    usable_swap_mb = swap_total_mb
    if cgroup_swap_mb == 0.0:
      safe_gb = max(1, int(node_ram_mb * 0.9 / 1024))
      msg = (
          'cgroup swap is locked (memory.swap.max=0); the'
          f' {swap_total_mb/1024:.0f} GB node swap device is unreachable.'
          f' Capping stress-ng vm_bytes {vm_bytes} → {safe_gb}G (0.9 x'
          ' RAM) to keep the pod alive — swap-encryption overhead will'
          ' NOT be measured this run'
      )
      logging.error('[swap_encryption] %s', msg)
      degraded_reasons.append(msg)
      return f'{safe_gb}G'
    if 0.0 < cgroup_swap_mb < float('inf'):
      usable_swap_mb = min(swap_total_mb, cgroup_swap_mb)

    overflow_mb = max(min(usable_swap_mb * 0.25, 64.0 * 1024), 4.0 * 1024)
    target_mb = node_ram_mb + overflow_mb

    if usable_swap_mb > 0:
      ceiling_mb = node_ram_mb + usable_swap_mb - 4096.0
      target_mb = min(target_mb, ceiling_mb)
    else:
      target_mb = min(target_mb, node_ram_mb * 0.9)

    target_gb = max(1, int(target_mb / 1024))

    if requested_mb < node_ram_mb * 0.95:
      new_vm_bytes = f'{target_gb}G'
      logging.info(
          '[swap_encryption] Auto-scaling vm_bytes UP: %s → %s '
          '(RAM %.0f GB, swap %.0f GB)',
          vm_bytes, new_vm_bytes,
          node_ram_mb / 1024, swap_total_mb / 1024,
      )
      return new_vm_bytes

    if requested_mb > target_mb:
      new_vm_bytes = f'{target_gb}G'
      logging.info(
          '[swap_encryption] Capping vm_bytes DOWN: %s → %s '
          '(RAM %.0f GB, swap %.0f GB)',
          vm_bytes, new_vm_bytes,
          node_ram_mb / 1024, swap_total_mb / 1024,
      )
      return new_vm_bytes

    return vm_bytes
  except Exception as e:  # pylint: disable=broad-except
    logging.info(
        '[swap_encryption] _autoscale_vm_bytes failed (%s); using %s', e, vm_bytes
    )
    return vm_bytes


def _get_stress_vm_method(daemonset) -> str:
  """Detect the best --vm-method for stress-ng on this node (cached)."""
  if _stress_vm_method:
    return _stress_vm_method[0]

  try:
    from perfkitbenchmarker.resources.container_service import kubectl  # pylint: disable=g-import-not-at-top
    out, _, _ = kubectl.RunKubectlCommand(
        [
            'exec', daemonset.pod_name,
            '-n', 'default',
            '--',
            'bash', '-c',
            (
                'stress-ng --vm 1 --vm-bytes 1M --vm-method __invalid__'
                ' --timeout 1s 2>&1 || true'
            ),
        ],
        raise_on_failure=False,
        timeout=15,
    )
    combined = out.lower()
    if 'rand-set' in combined:
      method = 'rand-set'
    elif 'mmap' in combined:
      method = 'mmap'
    elif 'write64' in combined:
      method = 'write64'
    else:
      method = ''
    logging.info(
        '[swap_encryption] stress-ng vm-method detected: %r',
        method or '(default)',
    )
  except Exception as e:  # pylint: disable=broad-except
    logging.info(
        '[swap_encryption] vm-method detection failed (%s); using rand-set', e
    )
    method = 'rand-set'

  _stress_vm_method.append(method)
  return method


def _stress_vm_method_flag(daemonset) -> str:
  """Return the --vm-method <method> flag string, or empty string if none."""
  method = _get_stress_vm_method(daemonset)
  return f'--vm-method {method}' if method else ''


def _parse_vmstat(output: str, base_meta: dict) -> list[sample.Sample]:
  """Parse vmstat output for swap rates and CPU utilisation.

  Standard vmstat column layout (non-header data lines, 0-indexed):
    r b swpd free buff cache  si  so  bi  bo  in  cs  us  sy  id  wa  st
    0 1    2    3    4     5   6   7   8   9  10  11  12  13  14  15  16
  """
  si_vals, so_vals = [], []
  us_vals, sy_vals, wa_vals = [], [], []

  for line in output.splitlines():
    parts = line.split()
    if len(parts) < 17 or not parts[0].isdigit():
      continue
    try:
      si_vals.append(float(parts[6]))
      so_vals.append(float(parts[7]))
      us_vals.append(float(parts[12]))
      sy_vals.append(float(parts[13]))
      wa_vals.append(float(parts[15]))
    except (ValueError, IndexError):
      pass

  if not si_vals:
    return []

  meta = dict(base_meta, metric_source='vmstat')

  def _mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

  def _peak(lst):
    return max(lst) if lst else 0.0

  total_active = [u + s + w for u, s, w in zip(us_vals, sy_vals, wa_vals)]

  return [
      sample.Sample('swap_in_pages_per_sec', _mean(si_vals), 'pages/s', meta),
      sample.Sample('swap_in_pages_per_sec_max', _peak(si_vals), 'pages/s', meta),
      sample.Sample('swap_out_pages_per_sec', _mean(so_vals), 'pages/s', meta),
      sample.Sample('swap_out_pages_per_sec_max', _peak(so_vals), 'pages/s', meta),
      sample.Sample('total_cpu_pct_avg', _mean(total_active), '%', meta),
      sample.Sample('total_cpu_pct_max', _peak(total_active), '%', meta),
      sample.Sample('system_time_pct_avg', _mean(sy_vals), '%', meta),
      sample.Sample('system_time_pct_max', _peak(sy_vals), '%', meta),
      sample.Sample('user_cpu_pct_avg', _mean(us_vals), '%', meta),
      sample.Sample('iowait_cpu_pct_avg', _mean(wa_vals), '%', meta),
  ]


def _parse_pidstat(output: str, base_meta: dict) -> list[sample.Sample]:
  """Parse CPU % for swap/encryption-related kernel threads from pidstat."""
  cpu_by_proc: dict[str, list[float]] = {}
  for line in output.splitlines():
    parts = line.split()
    if len(parts) < 9:
      continue
    proc = parts[-1]
    if not any(t in proc for t in _CRYPTO_PROCS):
      continue
    try:
      cpu_by_proc.setdefault(proc, []).append(float(parts[7]))
    except (ValueError, IndexError):
      pass
  results = []
  meta = dict(base_meta, metric_source='pidstat')
  for proc, vals in cpu_by_proc.items():
    m = dict(meta, process=proc)
    results += [
        sample.Sample(f'cpu_pct_avg_{proc}', sum(vals) / len(vals), '%', m),
        sample.Sample(f'cpu_pct_max_{proc}', max(vals), '%', m),
    ]
  return results


def _launch_confined_bg_stress(daemonset, timeout_s: int, logfile: str) -> None:
  """Launch Phase 2b background swap stressor in its own memory-capped cgroup."""
  method = _stress_vm_method_flag(daemonset)
  vm_bytes = _STRESS_VM_BYTES.value
  daemonset.PodExec(
      textwrap.dedent(f"""
    nohup bash -c '
      BG=/sys/fs/cgroup/pkb_bgstress
      mkdir -p "$BG" 2>/dev/null || true
      echo +memory > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
      echo max > "$BG/memory.swap.max" 2>/dev/null || true
      MT_KB=$(grep -m1 MemTotal /proc/meminfo | tr -s " " | cut -d" " -f2)
      echo $(( MT_KB * 1024 * 60 / 100 )) > "$BG/memory.max" 2>/dev/null || true
      echo $$ > "$BG/cgroup.procs" 2>/dev/null || true
      exec stress-ng --vm 1 --vm-bytes {vm_bytes} {method} --timeout {timeout_s}s
    ' >{logfile} 2>&1 &
    disown
    echo STRESS_STARTED
  """),
      timeout=30,
  )


def _set_memory_high_guard(daemonset, fraction: float = 0.9) -> None:
  """Cap the container cgroup memory.high at fraction x RAM (soft OOM guard)."""
  daemonset.PodExec(
      textwrap.dedent(f"""
    PKB_MCG=$(awk -F: '/^0::/{{print $3}}' /proc/self/cgroup 2>/dev/null)
    MT_KB=$(awk '/MemTotal/{{print $2}}' /proc/meminfo)
    HIGH=$(( MT_KB * 1024 / 100 * {int(fraction * 100)} ))
    if [ -n "$PKB_MCG" ] && [ -f "/sys/fs/cgroup$PKB_MCG/memory.high" ]; then
      echo $HIGH > "/sys/fs/cgroup$PKB_MCG/memory.high" 2>/dev/null || true
    fi
  """),
      ignore_failure=True,
      timeout=30,
      _retries=0,
  )


def _reset_memory_high_guard(daemonset) -> None:
  """Restore memory.high to max after a guarded phase."""
  daemonset.PodExec(
      textwrap.dedent("""
    PKB_MCG=$(awk -F: '/^0::/{print $3}' /proc/self/cgroup 2>/dev/null)
    if [ -n "$PKB_MCG" ] && [ -f "/sys/fs/cgroup$PKB_MCG/memory.high" ]; then
      echo max > "/sys/fs/cgroup$PKB_MCG/memory.high" 2>/dev/null || true
    fi
  """),
      ignore_failure=True,
      timeout=30,
      _retries=0,
  )


# ===========================================================================
# Phase 2a: stress-ng CPU overhead
# ===========================================================================

def RunPhase2a(
    daemonset,
    base_meta: dict[str, Any],
    degraded_reasons: list[str],
) -> list[sample.Sample]:
  """Measure CPU cost of dm-crypt/Nitro while stress-ng drives swap I/O.

  If --swap_encryption_stress_vm_bytes_list is set the phase is run once per
  listed intensity value (pressure-curve sweep).  Otherwise the single value
  from --swap_encryption_stress_vm_bytes is used.

  Args:
    daemonset: Active SwapDaemonSet resource.
    base_meta: Shared metadata dict from BuildMetadata().
    degraded_reasons: Mutable list; appended to on degradation conditions.

  Returns:
    List of Sample objects with swap rates, CPU utilisation, and bogo-ops.
  """
  if _STRESS_VM_BYTES_LIST.value.strip():
    intensities = [
        v.strip()
        for v in _STRESS_VM_BYTES_LIST.value.split(',')
        if v.strip()
    ]
  else:
    intensities = [_STRESS_VM_BYTES.value]

  results = []
  for vm_bytes in intensities:
    scaled = _autoscale_vm_bytes(daemonset, vm_bytes, degraded_reasons)
    logging.info('[swap_encryption] Phase 2a: stress-ng intensity %s', scaled)
    results += _run_cpu_overhead_sweep(
        daemonset, base_meta, scaled, degraded_reasons
    )
  return results


def _run_cpu_overhead_sweep(
    daemonset,
    base_meta: dict,
    vm_bytes: str,
    degraded_reasons: list[str],
) -> list[sample.Sample]:
  """Phase 2a stressor sweep with retry for flaky swap."""
  meta = dict(base_meta, phase='cpu_overhead', stress_vm_bytes=vm_bytes)
  timeout = _STRESS_TIMEOUT_SEC.value
  interval = 2
  n_samples = timeout // interval + 10
  vmstat_log = f'/tmp/pkb_vmstat_{vm_bytes}.log'
  pidstat_log = f'/tmp/pkb_pidstat_{vm_bytes}.log'
  workers = max(1, _STRESS_VM_WORKERS.value)
  per_worker = _per_worker_vm_bytes(vm_bytes, workers)
  min_so = _MIN_SWAP_OUT_PAGES.value
  method_flag = _stress_vm_method_flag(daemonset)
  max_attempts = 3
  best = None

  for attempt in range(1, max_attempts + 1):
    t0 = time.time()
    stress_out, _ = daemonset.PodExec(
        textwrap.dedent(f"""
      echo 2 > /sys/kernel/mm/ksm/run 2>/dev/null || true
      echo 0 > /sys/kernel/mm/ksm/run 2>/dev/null || true
      sysctl -w vm.swappiness=100 >/dev/null 2>&1 || true
      PKB_MCG=$(awk -F: '/^0::/{{print $3}}' /proc/self/cgroup 2>/dev/null)
      echo "[pkb] phase2a attempt={attempt}/{max_attempts} workers={workers} per_worker={per_worker}"
      vmstat {interval} {n_samples} > {vmstat_log} 2>&1 &
      VMSTAT_PID=$!
      pidstat -u {interval} {n_samples} -p ALL > {pidstat_log} 2>&1 &
      PISTAT_PID=$!
      stress-ng --vm {workers} \\
        --vm-bytes {per_worker} \\
        {method_flag} \\
        --timeout {timeout}s \\
        --metrics-brief 2>&1 || true
      kill $VMSTAT_PID $PISTAT_PID 2>/dev/null || true
    """),
        timeout=timeout + 60,
        ignore_failure=True,
    )
    elapsed = time.time() - t0

    completed_cleanly = (
        'successful run completed' in stress_out.lower()
        or 'metrics-brief' in stress_out.lower()
        or 'bogo-ops' in stress_out.lower()
    )
    oom_killed = (not completed_cleanly) and elapsed < timeout * 0.8
    vmstat_out, _ = daemonset.PodExec(
        f'cat {vmstat_log}', ignore_failure=True
    )
    pidstat_out, _ = daemonset.PodExec(
        f'cat {pidstat_log}', ignore_failure=True
    )
    vmstat_samples = _parse_vmstat(vmstat_out, meta)
    swap_out_max = max(
        (
            s.value
            for s in vmstat_samples
            if s.metric in (
                'swap_out_pages_per_sec', 'swap_out_pages_per_sec_max'
            )
        ),
        default=0.0,
    )
    bogo = None
    for line in stress_out.splitlines():
      mm = re.search(r'vm\s+\d+\s+(\d+)\s+\S+\s+bogo-ops', line)
      if mm:
        bogo = float(mm.group(1))
        break
    logging.info(
        '[swap_encryption] Phase 2a attempt %d/%d: peak swap-out '
        '%.0f pages/s (completed=%s, oom=%s)',
        attempt, max_attempts, swap_out_max, completed_cleanly, oom_killed,
    )
    if best is None or swap_out_max > best['swap_out_max']:
      best = dict(
          elapsed=elapsed,
          oom_killed=oom_killed,
          swap_out_max=swap_out_max,
          vmstat_samples=vmstat_samples,
          pidstat_out=pidstat_out,
          bogo=bogo,
      )
    if oom_killed or swap_out_max >= min_so:
      break
    if attempt < max_attempts:
      logging.info(
          '[swap_encryption] Phase 2a swap-out %.0f < %d — retrying (%d/%d)',
          swap_out_max, min_so, attempt + 1, max_attempts,
      )
      daemonset.PodExec(
          textwrap.dedent("""
        echo -1000 > /proc/self/oom_score_adj 2>/dev/null || true
        pkill -9 stress-ng 2>/dev/null || true
        sleep 3; sync; echo 1 > /proc/sys/vm/drop_caches 2>/dev/null || true
      """),
          ignore_failure=True,
          timeout=60,
      )

  results = [
      sample.Sample('stress_ng_duration_sec', best['elapsed'], 's', meta),
      sample.Sample(
          'stress_ng_completed',
          0.0 if best['oom_killed'] else 1.0,
          'status',
          meta,
      ),
  ]
  if best['bogo'] is not None:
    results.append(
        sample.Sample('stress_ng_bogo_ops', best['bogo'], 'ops', meta)
    )
  results += best['vmstat_samples']
  results += _parse_pidstat(best['pidstat_out'], meta)

  if best['oom_killed']:
    msg = (
        f'stress-ng (vm_bytes={vm_bytes}) was OOM-killed — the cgroup could'
        ' not page anonymous memory out to swap; swap-encryption overhead'
        ' was not measured'
    )
    logging.error('[swap_encryption] %s', msg)
    degraded_reasons.append(msg)
  elif best['swap_out_max'] < min_so:
    msg = (
        f'stress-ng (vm_bytes={vm_bytes}) peak swap-out was only '
        f'{best["swap_out_max"]:.0f} pages/s (< {min_so} threshold) after '
        f'{max_attempts} attempts — the working set never meaningfully '
        f'paged to swap'
    )
    logging.error('[swap_encryption] %s', msg)
    degraded_reasons.append(msg)

  return results


# ===========================================================================
# Phase 2b: IO interference
# ===========================================================================

def RunPhase2b(
    daemonset,
    base_meta: dict[str, Any],
) -> list[sample.Sample]:
  """Quantify drop in application I/O when swap is under simultaneous pressure.

  Runs fio twice — once baseline (no swap pressure), once with a confined
  background stress-ng stressor — and reports the difference.

  Args:
    daemonset: Active SwapDaemonSet resource.
    base_meta: Shared metadata dict from BuildMetadata().

  Returns:
    List of Sample objects with baseline and under-pressure app fio metrics.
  """
  results = []
  app_file = '/mnt/stateful_partition/pkb_app_io'
  timeout = _STRESS_TIMEOUT_SEC.value
  meta = dict(base_meta, phase='io_interference')

  _set_memory_high_guard(daemonset)

  daemonset.PodExec(
      textwrap.dedent("""
    command -v fio >/dev/null 2>&1 || {
      apt-get install -y -qq fio 2>/dev/null || true
    }
  """),
      ignore_failure=True,
      timeout=120,
  )

  daemonset.PodExec(
      textwrap.dedent("""
    pkill -9 stress-ng 2>/dev/null || true
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    sleep 2
  """),
      ignore_failure=True,
      timeout=60,
  )

  daemonset.PodExec(
      (
          f'fio --name=create --filename={app_file} '
          '--rw=write --bs=1m --size=4G --verify=0 --direct=1'
      ),
      timeout=600,
      ignore_failure=True,
  )

  def _run_app_fio(pressure_label: str) -> list[sample.Sample]:
    cmd = (
        f'fio --name=app_io --filename={app_file} '
        '--ioengine=libaio --direct=1 '
        '--rw=randrw --bs=4k --iodepth=32 --size=4G --verify=0 '
        '--time_based --runtime=60s --output-format=json'
    )
    out, _ = daemonset.PodExec(cmd, ignore_failure=True)
    return ParseFioJson(
        out,
        'app_io',
        dict(meta, pressure=pressure_label),
        f'App I/O ({pressure_label})',
    )

  logging.info('[swap_encryption] I/O interference: baseline (no pressure)')
  results += _run_app_fio('no_pressure')

  logging.info('[swap_encryption] I/O interference: under swap pressure')
  _launch_confined_bg_stress(daemonset, timeout, '/tmp/pkb_stress_io.log')
  time.sleep(10)
  results += _run_app_fio('with_swap_pressure')

  daemonset.PodExec(
      'pkill -9 stress-ng 2>/dev/null || true',
      ignore_failure=True,
      _retries=0,
      timeout=15,
  )
  _reset_memory_high_guard(daemonset)
  return results
