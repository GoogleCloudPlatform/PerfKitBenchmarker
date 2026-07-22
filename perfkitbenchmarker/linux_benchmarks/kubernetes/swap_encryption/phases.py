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

Contains fio phase runner and JSON parser used by swap_encryption_benchmark.
Mirrors the pattern used by fio/utils.py for the fio benchmark.

PR3 update: RunPhase1Fio gains a pre-fill step, file-based per-job JSON output,
and explicit loop-device skip logic.  ParseFioJson gains a label parameter and
uses io_bytes for zero-result detection.
"""

import json
import logging
from typing import Any

from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

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
    empty list if the phase is skipped (loop device + unintentional fallback).
  """
  # Skip only the unintentional loop-device fallback.
  if swap_dev.startswith('/dev/loop') and swap_type != 'boot_disk':
    logging.info(
        '[swap_encryption] Phase 1 (fio) SKIPPED for plain loop device %s'
        ' (unintentional single-disk fallback). fio on a loop-backed device'
        ' measures the underlying ext4 filesystem (stateful_partition), not'
        ' the swap stack. Use c4-*-lssd, --swap_encryption_add_swap_disk,'
        ' or --swap_encryption_swap_type=boot_disk for fio data.',
        swap_dev,
    )
    return []

  results = []

  # Disable swap before fio so measurements reflect the raw hardware /
  # encryption ceiling without swap-daemon overhead.
  daemonset.PodExec(f'swapoff {swap_dev}', ignore_failure=True)

  # Pre-fill device so read tests have real data.
  # Cap at _PREFILL_GIB — enough to warm up the dm-crypt pipeline.
  prefill_timeout = _PREFILL_GIB * 1024 // 150 + 60
  prefill_timeout = max(prefill_timeout, 300)
  logging.info(
      '[swap_encryption] Pre-filling %d GiB of %s', _PREFILL_GIB, swap_dev
  )
  daemonset.PodExec(
      (
          f'fio --name=prefill --filename={swap_dev} --ioengine=libaio'
          f' --direct=1 --rw=write --bs=1m --size={_PREFILL_GIB}g --verify=0'
          ' --output={vm_util.VM_TMP_DIR}/pkb_fio_prefill.log'
      ),
      timeout=prefill_timeout,
      ignore_failure=True,
  )

  # Each fio job: runtime + 90 s buffer (run + JSON write + file read).
  fio_run_timeout = fio_runtime_sec + 90
  fio_read_timeout = 60

  for name, rw, bs, depth, label in _FIO_JOBS:
    logging.info('[swap_encryption] fio: %s', name)
    out_file = f'{vm_util.VM_TMP_DIR}/pkb_fio_{name}.json'
    # Remove stale output first to avoid silently reusing a previous result.
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

  # fio prefill overwrites the entire device, destroying the mkswap header.
  # Re-stamp and re-enable before the remaining phases need active swap.
  daemonset.PodExec(
      f'mkswap {swap_dev} && swapon {swap_dev}',
      timeout=120,
  )

  logging.info('[swap_encryption] Phase 1 complete (%d samples)', len(results))
  return results


def ParseFioJson(
    fio_output: str,
    job_name: str,
    base_meta: dict[str, Any],
    label: str = '',
) -> list[sample.Sample]:
  """Parse fio --output-format=json output into PKB Sample objects.

  Extracts per-direction (read/write) IOPS, bandwidth (MB/s) and completion
  latency (mean + p50/p99/p999 percentiles) for each job in the output.

  Skips directions where io_bytes == 0 (fio did no work in that direction),
  which is normal for pure read or write jobs.

  Args:
    fio_output: Raw stdout from fio with --output-format=json.
    job_name: Short identifier embedded in metric names, e.g. 'rand_read_iops'.
    base_meta: Shared metadata dict copied into each sample.
    label: Human-readable description of the job (e.g. 'Random write IOPS').
      Stored in sample metadata as fio_label.

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
          sample.Sample(f'{job_name}_{direction}_lat_mean', lat_mean, 'us', m),
          sample.Sample(f'{job_name}_{direction}_lat_p50', lat_p50, 'us', m),
          sample.Sample(f'{job_name}_{direction}_lat_p99', lat_p99, 'us', m),
          sample.Sample(f'{job_name}_{direction}_lat_p999', lat_p999, 'us', m),
      ]
  return results
