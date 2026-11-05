#!/usr/bin/env python3
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
"""On-VM workload driver for https://github.com/fsspec (gcsfs, s3fs, adlfs).

Parses a splittable scenario string of the form:
  <pattern>_<block_size>_<operation>_<file_size>[_files-<N>][_ops-<M>][_workers-<W>]

Examples:
  - seq_16M_read_100G
  - rand_16M_read_100G_ops-1000_workers-16
  - seq_8K_read_8K_files-10000_workers-16
  - seq_16M_write_50G
"""

import argparse
from concurrent import futures
import dataclasses
import json
import os
import random
import threading
import time
from typing import Any

import fsspec
import numpy as np

_SIZE_UNITS = {'B': 1, 'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}


@dataclasses.dataclass(frozen=True)
class ScenarioSpec:
  """Parsed parameters from a splittable scenario string."""

  raw: str
  pattern: str  # 'seq' or 'rand'
  block_size_str: str
  block_bytes: int
  operation: str  # 'read' or 'write'
  file_size_str: str
  file_bytes: int
  num_files: int = 1
  num_ops: int = 10000
  num_workers: int = 1


def parse_scenario(scenario_str: str) -> ScenarioSpec:
  """Parses <pattern>_<bs>_<op>_<size>[_files-<N>][_ops-<M>][_workers-<W>]."""
  tokens = scenario_str.split('_')
  pattern, bs, operation, fs = tokens[:4]
  extras = dict(extra.split('-', 1) for extra in tokens[4:])
  num_files = int(extras.get('files', 1))
  block_bytes = int(bs[:-1]) * _SIZE_UNITS[bs[-1].upper()]
  file_bytes = int(fs[:-1]) * _SIZE_UNITS[fs[-1].upper()]
  if file_bytes < block_bytes:
    raise ValueError(
        f'file_size ({fs}) must be >= block_size ({bs}) in {scenario_str}'
    )
  return ScenarioSpec(
      raw=scenario_str,
      pattern=pattern,
      block_size_str=bs,
      block_bytes=block_bytes,
      operation=operation,
      file_size_str=fs,
      file_bytes=file_bytes,
      num_files=num_files,
      num_ops=int(extras.get('ops', max(10000, num_files))),
      num_workers=int(extras.get('workers', 1)),
  )


def _get_filesystem_and_protocol(
    cloud: str, cache_type: str, block_size_bytes: int
) -> tuple[fsspec.AbstractFileSystem, str, str]:
  """Initializes a fresh cloud-specific fsspec filesystem per GitHub docs."""
  common_kwargs = {
      'default_block_size': block_size_bytes,
      'default_cache_type': cache_type,
      'skip_instance_cache': True,
  }
  if cloud == 'GCP':
    import gcsfs  # pylint: disable=g-import-not-at-top

    return (
        gcsfs.GCSFileSystem(**common_kwargs),
        'gs',
        getattr(gcsfs, '__version__', 'unknown'),
    )
  elif cloud == 'AWS':
    import s3fs  # pylint: disable=g-import-not-at-top

    return (
        s3fs.S3FileSystem(**common_kwargs),
        's3',
        getattr(s3fs, '__version__', 'unknown'),
    )
  elif cloud == 'Azure':
    import adlfs  # pylint: disable=g-import-not-at-top

    return (
        adlfs.AzureBlobFileSystem(
            account_name=os.environ.get('AZURE_STORAGE_ACCOUNT_NAME'),
            account_key=os.environ.get('AZURE_STORAGE_ACCOUNT_KEY'),
            **common_kwargs,
        ),
        'abfs',
        getattr(adlfs, '__version__', 'unknown'),
    )
  raise ValueError(f'Unsupported cloud provider: {cloud}')


def _dataset_uris(proto: str, bucket: str, spec: ScenarioSpec) -> list[str]:
  """Returns the canonical object URIs for a parsed ScenarioSpec."""
  prefix = f'{proto}://{bucket}/data_{spec.file_size_str}'
  return [f'{prefix}/file_{i}.bin' for i in range(spec.num_files)]


def _stream_write_file(
    fs: fsspec.AbstractFileSystem,
    uri: str,
    total_bytes: int,
    block_bytes: int,
    chunk_buf: bytes | None = None,
) -> None:
  """Streams a file of total_bytes to uri using an in-memory block buffer."""
  if chunk_buf is None:
    chunk_buf = os.urandom(block_bytes)
  remaining = total_bytes
  with fs.open(uri, 'wb', block_size=block_bytes) as f:
    while remaining > 0:
      to_write = min(remaining, block_bytes)
      f.write(chunk_buf[:to_write])
      remaining -= to_write


def _build_metrics(
    prefix: str,
    duration_sec: float,
    total_bytes: int,
    latencies_ms: list[float] | None = None,
    latency_name: str = 'latency',
) -> list[dict[str, Any]]:
  """Builds duration, throughput, and latency percentile metrics."""
  total_mb = total_bytes / (1024.0 * 1024.0)
  metrics = [
      {
          'metric': f'{prefix}_duration',
          'value': duration_sec,
          'unit': 'seconds',
      },
      {
          'metric': f'{prefix}_throughput',
          'value': total_mb / duration_sec,
          'unit': 'MB/s',
      },
  ]
  if latencies_ms:
    for pct, val in zip(
        (50, 90, 99, 100), np.percentile(latencies_ms, [50, 90, 99, 100])
    ):
      metrics.append({
          'metric': f'{prefix}_{latency_name}_p{pct}',
          'value': val,
          'unit': 'ms',
      })
  return metrics


def prepare_dataset(
    fs: fsspec.AbstractFileSystem,
    proto: str,
    args: argparse.Namespace,
    spec: ScenarioSpec,
) -> None:
  """Populates the bucket with the scenario dataset using fsspec directly."""
  uris = _dataset_uris(proto, args.bucket, spec)
  # Warm up the async session on the main thread before spawning workers.
  fs.exists(uris[0])

  payload = os.urandom(spec.block_bytes)
  upload_fn = lambda uri: _stream_write_file(
      fs, uri, spec.file_bytes, spec.block_bytes, payload
  )
  with futures.ThreadPoolExecutor(
      max_workers=min(spec.num_workers, len(uris))
  ) as pool:
    list(pool.map(upload_fn, uris))


def _warm_or_clear_metadata_cache(
    fs: fsspec.AbstractFileSystem,
    proto: str,
    bucket: str,
    spec: ScenarioSpec,
    enable_metadata_cache: bool,
) -> None:
  """Warms the async session and populates or clears the metadata dircache."""
  prefix = f'{proto}://{bucket}/data_{spec.file_size_str}'
  if enable_metadata_cache:
    fs.ls(prefix, detail=True)
  else:
    fs.exists(f'{prefix}/file_0.bin')
    fs.invalidate_cache()


def run_seq_read(
    fs: fsspec.AbstractFileSystem,
    proto: str,
    args: argparse.Namespace,
    spec: ScenarioSpec,
    cache_type: str,
) -> list[dict[str, Any]]:
  """Runs sequential read across spec.num_files in spec.block_bytes chunks."""
  uris = _dataset_uris(proto, args.bucket, spec)
  _warm_or_clear_metadata_cache(
      fs, proto, args.bucket, spec, args.enable_metadata_cache
  )

  def _read_file(uri: str) -> tuple[list[float], int]:
    bytes_read = 0
    block_latencies_ms = []
    with fs.open(
        uri, 'rb', block_size=spec.block_bytes, cache_type=cache_type
    ) as f:
      while True:
        t0 = time.perf_counter()
        chunk = f.read(spec.block_bytes)
        if not chunk:
          break
        block_latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        bytes_read += len(chunk)
    return block_latencies_ms, bytes_read

  start = time.perf_counter()
  with futures.ThreadPoolExecutor(max_workers=spec.num_workers) as pool:
    results = list(pool.map(_read_file, uris))
  duration_sec = time.perf_counter() - start
  all_latencies_ms = []
  total_bytes = 0
  for latencies, nbytes in results:
    all_latencies_ms.extend(latencies)
    total_bytes += nbytes
  return _build_metrics(
      spec.raw,
      duration_sec,
      total_bytes,
      latencies_ms=all_latencies_ms,
      latency_name='block_latency',
  )


def run_rand_read(
    fs: fsspec.AbstractFileSystem,
    proto: str,
    args: argparse.Namespace,
    spec: ScenarioSpec,
    cache_type: str,
) -> list[dict[str, Any]]:
  """Runs random byte-range reads of spec.block_bytes across spec.file_bytes."""
  uris = _dataset_uris(proto, args.bucket, spec)
  _warm_or_clear_metadata_cache(
      fs, proto, args.bucket, spec, args.enable_metadata_cache
  )

  rng = random.Random(42)
  tasks = [
      (
          uris[rng.randrange(len(uris))],
          rng.randint(0, spec.file_bytes - spec.block_bytes),
      )
      for _ in range(spec.num_ops)
  ]

  thread_local = threading.local()
  opened_files = []
  opened_files_lock = threading.Lock()

  def _get_thread_file(uri: str) -> Any:
    f = getattr(thread_local, 'file', None)
    if f is None:
      f = fs.open(uri, 'rb', block_size=spec.block_bytes, cache_type=cache_type)
      thread_local.file = f
      with opened_files_lock:
        opened_files.append(f)
    return f

  def _read_range(task: tuple[str, int]) -> tuple[float, int]:
    uri, offset = task
    if spec.num_files == 1:
      f = _get_thread_file(uri)
      t0 = time.perf_counter()
      f.seek(offset)
      chunk = f.read(spec.block_bytes)
      return (time.perf_counter() - t0) * 1000.0, len(chunk)

    t0 = time.perf_counter()
    with fs.open(
        uri, 'rb', block_size=spec.block_bytes, cache_type=cache_type
    ) as f:
      f.seek(offset)
      chunk = f.read(spec.block_bytes)
    return (time.perf_counter() - t0) * 1000.0, len(chunk)

  start = time.perf_counter()
  try:
    with futures.ThreadPoolExecutor(max_workers=spec.num_workers) as pool:
      results = list(pool.map(_read_range, tasks))
  finally:
    for f in opened_files:
      f.close()
  duration_sec = time.perf_counter() - start
  return _build_metrics(
      spec.raw,
      duration_sec,
      sum(r[1] for r in results),
      latencies_ms=[r[0] for r in results],
  )


def run_write(
    fs: fsspec.AbstractFileSystem,
    proto: str,
    args: argparse.Namespace,
    spec: ScenarioSpec,
) -> list[dict[str, Any]]:
  """Runs streaming write of spec.file_bytes in spec.block_bytes chunks."""
  target_uri = f'{proto}://{args.bucket}/write_{spec.raw}.bin'
  start = time.perf_counter()
  _stream_write_file(fs, target_uri, spec.file_bytes, spec.block_bytes)
  duration_sec = time.perf_counter() - start
  fs.rm(target_uri)
  return _build_metrics(spec.raw, duration_sec, spec.file_bytes)


def main() -> None:
  parser = argparse.ArgumentParser(
      description='https://github.com/fsspec (gcsfs, s3fs, adlfs) runner.'
  )
  parser.add_argument('--mode', default='run', choices=['prepare', 'run'])
  parser.add_argument(
      '--cloud', required=True, choices=['GCP', 'AWS', 'Azure']
  )
  parser.add_argument('--bucket', required=True)
  parser.add_argument('--scenario', required=True)
  parser.add_argument('--cache_type', default='readahead')
  parser.add_argument(
      '--enable_metadata_cache',
      type=lambda v: v.lower() == 'true',
      default=False,
  )
  args = parser.parse_args()

  spec = parse_scenario(args.scenario)
  cache_type = args.cache_type if spec.pattern == 'seq' else 'none'
  fs, proto, backend_version = _get_filesystem_and_protocol(
      args.cloud, cache_type, spec.block_bytes
  )

  if args.mode == 'prepare':
    prepare_dataset(fs, proto, args, spec)
    return

  if spec.operation == 'write':
    raw_metrics = run_write(fs, proto, args, spec)
  elif spec.pattern == 'rand':
    raw_metrics = run_rand_read(fs, proto, args, spec, cache_type)
  else:
    raw_metrics = run_seq_read(fs, proto, args, spec, cache_type)
  base_meta = {
      'fsspec_version': fsspec.__version__,
      'backend_version': backend_version,
      'protocol': proto,
      'scenario': spec.raw,
      'access_pattern': spec.pattern,
      'operation': spec.operation,
      'block_size': spec.block_size_str,
      'file_size': spec.file_size_str,
      'num_files': spec.num_files,
      'cache_type': cache_type,
      'enable_metadata_cache': args.enable_metadata_cache,
      'num_workers': spec.num_workers,
  }
  for m in raw_metrics:
    m['metadata'] = base_meta

  print(json.dumps(raw_metrics))


if __name__ == '__main__':
  main()
