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

Only Phase 1 (fio microbenchmarks) utilities live here in PR2; later PRs
add additional phase helpers (stress-ng CPU overhead, Redis, kernel build,
OpenSearch) as phases are introduced.
"""

import json
import logging
from typing import Any

from perfkitbenchmarker import errors
from perfkitbenchmarker import sample


def RunPhase1Fio(
    daemonset,
    swap_dev: str,
    base_meta: dict[str, Any],
    fio_runtime_sec: int,
) -> list[sample.Sample]:
  """Run fio microbenchmarks on the raw swap block device (Phase 1).

  Calls swapoff before running fio so measurements reflect the raw
  hardware + encryption ceiling with no swap-daemon overhead.  Re-enables
  swap unconditionally after all jobs complete.

  Jobs:
    4k_randread   iodepth=32  → random read IOPS
    4k_randwrite  iodepth=32  → random write IOPS
    1m_seqread    iodepth=8   → sequential read bandwidth
    1m_seqwrite   iodepth=8   → sequential write bandwidth
    4k_lat_read   iodepth=1   → completion latency floor (read)

  Args:
    daemonset: Active SwapDaemonSet resource.
    swap_dev: Block device path, e.g. /dev/mapper/swap_encrypted.
    base_meta: Shared metadata dict from benchmark's _build_metadata().
    fio_runtime_sec: Wall-clock seconds each fio job runs.

  Returns:
    List of Sample objects with IOPS, bandwidth and latency metrics.
  """
  samples: list[sample.Sample] = []

  # swapoff before fio — running fio with --direct=1 on an active swap device
  # races with kernel page-reclaim on the same dm-crypt target.
  logging.info('[swap_encryption] Phase 1: swapoff %s', swap_dev)
  daemonset.PodExec(
      f'swapoff {swap_dev} 2>/dev/null || swapoff -a 2>/dev/null || true',
      timeout=30,
      ignore_failure=True,
  )

  # (name, rw_mode, block_size, iodepth)
  fio_jobs = [
      ('4k_randread', 'randread', '4k', 32),
      ('4k_randwrite', 'randwrite', '4k', 32),
      ('1m_seqread', 'read', '1m', 8),
      ('1m_seqwrite', 'write', '1m', 8),
      ('4k_lat_read', 'randread', '4k', 1),
  ]

  try:
    for name, rw, bs, iodepth in fio_jobs:
      cmd = (
          f'fio --name={name} --filename={swap_dev}'
          f' --rw={rw} --bs={bs} --iodepth={iodepth}'
          ' --ioengine=libaio --direct=1'
          f' --runtime={fio_runtime_sec} --time_based --group_reporting'
          ' --output-format=json 2>/dev/null'
      )
      logging.info('[swap_encryption] Phase 1: fio job %s', name)
      out, _ = daemonset.PodExec(cmd, timeout=fio_runtime_sec + 120)
      samples += ParseFioJson(out, name, base_meta)
  finally:
    # Always re-enable swap so subsequent phases can drive swap I/O.
    logging.info('[swap_encryption] Phase 1: swapon %s', swap_dev)
    daemonset.PodExec(
        f'swapon {swap_dev} 2>/dev/null || true',
        timeout=30,
        ignore_failure=True,
    )

  logging.info(
      '[swap_encryption] Phase 1 complete (%d samples)', len(samples)
  )
  return samples


def ParseFioJson(
    fio_output: str,
    job_name: str,
    base_meta: dict[str, Any],
) -> list[sample.Sample]:
  """Parse fio --output-format=json output into PKB Sample objects.

  Extracts per-direction (read/write) IOPS, bandwidth (MB/s) and completion
  latency (mean + p50/p99/p999 percentiles).

  Args:
    fio_output: Raw stdout from fio with --output-format=json.
    job_name: Short identifier embedded in metric names, e.g. '4k_randread'.
    base_meta: Shared metadata dict copied into each sample.

  Returns:
    List of Sample objects; empty if output cannot be parsed or is zero.
  """
  # fio sometimes emits kernel warnings before the JSON object.
  json_start = fio_output.find('{')
  if json_start == -1:
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] Phase 1: no JSON in fio output for {job_name}'
    )

  try:
    data = json.loads(fio_output[json_start:])
  except json.JSONDecodeError as e:
    raise errors.Benchmarks.RunError(
        f'[swap_encryption] Phase 1: fio JSON parse error ({job_name}): {e}'
    ) from e

  jobs = data.get('jobs', [])
  if not jobs:
    return []

  job = jobs[0]
  samples: list[sample.Sample] = []
  meta = dict(base_meta, fio_job=job_name)

  for direction in ('read', 'write'):
    d = job.get(direction, {})
    iops = float(d.get('iops', 0))
    bw_kbps = float(d.get('bw', 0))  # fio reports KiB/s
    bw_mbps = bw_kbps / 1024.0

    # Skip directions with near-zero throughput.
    if iops < 1 and bw_kbps < 1:
      continue

    prefix = f'phase1_fio_{job_name}_{direction}'
    samples.append(sample.Sample(f'{prefix}_iops', iops, 'IOPS', meta))
    samples.append(
        sample.Sample(f'{prefix}_bw_mbps', bw_mbps, 'MB/s', meta)
    )

    # Completion latency — fio reports nanoseconds; emit microseconds.
    clat = d.get('clat_ns', d.get('lat_ns', {}))
    lat_mean_ns = float(clat.get('mean', 0))
    if lat_mean_ns > 0:
      samples.append(
          sample.Sample(
              f'{prefix}_lat_mean_us', lat_mean_ns / 1000.0, 'us', meta
          )
      )
      for pct_key, label in (
          ('50.000000', 'p50'),
          ('99.000000', 'p99'),
          ('99.900000', 'p999'),
      ):
        val_ns = clat.get('percentile', {}).get(pct_key, 0)
        if val_ns:
          samples.append(
              sample.Sample(
                  f'{prefix}_lat_{label}_us',
                  val_ns / 1000.0,
                  'us',
                  meta,
              )
          )

  return samples
