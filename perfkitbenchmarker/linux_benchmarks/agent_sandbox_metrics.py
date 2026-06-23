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
"""Aggregates load-generator records into PerfKitBenchmarker samples."""

import collections
import math

from perfkitbenchmarker import sample

_PERCENTILES = (50, 90, 95, 99)



def percentile(values, pct):
  """Linear-interpolated percentile. Returns 0.0 for an empty input."""
  if not values:
    return 0.0
  ordered = sorted(values)
  rank = (len(ordered) - 1) * (pct / 100.0)
  low = math.floor(rank)
  high = math.ceil(rank)
  if low == high:
    return float(ordered[int(rank)])
  return float(ordered[low] + (ordered[high] - ordered[low]) * (rank - low))


def build_samples(records, peak_concurrency, metadata):
  """Builds the sample list for one benchmark Run.

  Args:
    records: List of ClaimRecord, one per submitted claim.
    peak_concurrency: Maximum in-flight claim count observed.
    metadata: Dict attached to every emitted sample.

  Returns:
    A list of sample.Sample.
  """
  successes = [r for r in records if r.error is None and r.ready_at is not None]
  startup_times = [r.startup_time_s for r in successes]
  errors = collections.Counter(r.error for r in records if r.error is not None)

  samples = []
  for pct in _PERCENTILES:
    samples.append(
        sample.Sample(
            f'startup_time_p{pct}',
            percentile(startup_times, pct),
            'seconds',
            metadata,
        )
    )
  samples.append(
      sample.Sample(
          'startup_time_max',
          max(startup_times) if startup_times else 0.0,
          'seconds',
          metadata,
      )
  )

  exec_durations = sorted(
      r.exec_duration_s
      for r in successes
      if getattr(r, 'exec_duration_s', None) is not None
  )
  if exec_durations:
    for pct in _PERCENTILES:
      samples.append(
          sample.Sample(
              f'exec_duration_s_p{pct}',
              percentile(exec_durations, pct),
              'seconds',
              metadata,
          )
      )
    samples.append(
        sample.Sample(
            'exec_duration_s_max', exec_durations[-1], 'seconds', metadata
        )
    )

  lifecycles = sorted(
      r.total_lifecycle_s
      for r in successes
      if getattr(r, 'total_lifecycle_s', None) is not None
  )
  if lifecycles:
    for pct in _PERCENTILES:
      samples.append(
          sample.Sample(
              f'total_lifecycle_s_p{pct}',
              percentile(lifecycles, pct),
              'seconds',
              metadata,
          )
      )
    samples.append(
        sample.Sample(
            'total_lifecycle_s_max', lifecycles[-1], 'seconds', metadata
        )
    )

  request_times = [r.requested_at for r in records]
  ready_times = [r.ready_at for r in successes]
  # Submit QPS over the request window; 0.0 fallback when all requests
  # share a timestamp.
  submit_span = (max(request_times) - min(request_times)) if records else 0.0
  submit_qps = (len(records) / submit_span) if submit_span > 0 else 0.0
  # Completion QPS over the full experiment wall clock: first request to
  # last ready.
  completion_span = (
      (max(ready_times) - min(request_times)) if ready_times else 0.0
  )
  completion_qps = (
      (len(successes) / completion_span) if completion_span > 0 else 0.0
  )

  warm = [r for r in successes if r.warm_served]
  warm_fraction = (len(warm) / len(successes)) if successes else 0.0

  samples.append(sample.Sample('submit_qps', submit_qps, 'count/sec', metadata))
  samples.append(
      sample.Sample('completion_qps', completion_qps, 'count/sec', metadata)
  )
  samples.append(
      sample.Sample('peak_concurrency', peak_concurrency, 'count', metadata)
  )
  samples.append(
      sample.Sample('warm_served_fraction', warm_fraction, 'fraction', metadata)
  )
  samples.append(
      sample.Sample('success_count', len(successes), 'count', metadata)
  )
  samples.append(
      sample.Sample('error_count', sum(errors.values()), 'count', metadata)
  )
  for error_type, count in errors.items():
    error_metadata = dict(metadata, error_type=error_type)
    samples.append(
        sample.Sample(
            f'error_count_{error_type}', count, 'count', error_metadata
        )
    )
  return samples
