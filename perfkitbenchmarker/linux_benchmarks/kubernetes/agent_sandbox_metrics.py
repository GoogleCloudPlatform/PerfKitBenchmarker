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
"""Aggregates load-generator records into PerfKitBenchmarker samples.

Note: RunShape, resolve_run_shape, and ClaimRecord are intentionally mirrored
from data/agent_sandbox/agent_sandbox_loadgen.py because the load-generator
script executes standalone inside an in-cluster Kubernetes Job pod where the
perfkitbenchmarker package is not installed.
"""

import collections
import dataclasses
from typing import Any, Optional

from perfkitbenchmarker import errors
from perfkitbenchmarker import sample


@dataclasses.dataclass(frozen=True)
class RunShape:
  """A fully resolved load shape: rate, window, and total claim count."""

  qps: float
  duration: float
  total: int


def resolve_run_shape(
    qps: Optional[float] = None,
    duration: Optional[float] = None,
    total: Optional[int] = None,
) -> RunShape:
  """Resolves any two of qps, duration, total into a complete RunShape."""
  if qps is not None and duration is not None and total is not None:
    pass
  elif qps is not None and duration is not None:
    total = int(round(qps * duration))
  elif qps is not None and total is not None:
    duration = total / qps
  elif duration is not None and total is not None:
    qps = total / duration
  else:
    raise ValueError('Provide at least two of qps, duration, total.')
  return RunShape(qps=float(qps), duration=float(duration), total=int(total))


@dataclasses.dataclass
class ClaimRecord:
  """Timing record for one SandboxClaim."""

  name: str
  requested_at: float
  ready_at: Optional[float] = None
  error: Optional[str] = None
  warm_served: Optional[bool] = None
  exec_started_at: Optional[float] = None
  exec_completed_at: Optional[float] = None
  released_at: Optional[float] = None

  @property
  def startup_time_s(self) -> Optional[float]:
    if self.ready_at is None:
      return None
    return self.ready_at - self.requested_at

  @property
  def exec_duration_s(self) -> Optional[float]:
    if self.exec_started_at is None or self.exec_completed_at is None:
      return None
    return self.exec_completed_at - self.exec_started_at

  @property
  def delete_duration_s(self) -> Optional[float]:
    if self.exec_completed_at is None or self.released_at is None:
      return None
    return self.released_at - self.exec_completed_at

  @property
  def total_lifecycle_s(self) -> Optional[float]:
    if self.released_at is None:
      return None
    return self.released_at - self.requested_at


_PERCENTILES = (50, 90, 95, 99)
_PERCENTILE_METRICS = [sample.Percentile(pct) for pct in _PERCENTILES] + [
    sample.Aggregation.MAX
]
_MAX_FAILURE_RATE = 0.5


def _summarize_metric(
    metric_prefix: str, values: list[float], metadata: dict[str, Any]
) -> list[sample.Sample]:
  """Summarizes a list of timing values into p50/p90/p95/p99 and max samples."""
  if not values:
    samples = [
        sample.Sample(f'{metric_prefix}p{pct}', 0.0, 'seconds', metadata)
        for pct in _PERCENTILES
    ]
    samples.append(
        sample.Sample(f'{metric_prefix}max', 0.0, 'seconds', metadata)
    )
    return samples
  base_sample = sample.Sample(metric_prefix, 0.0, 'seconds', metadata)
  return sample.SummarizePercentiles(values, base_sample, _PERCENTILE_METRICS)


def build_samples(
    records: list[ClaimRecord],
    peak_concurrency: int,
    metadata: dict[str, Any],
) -> list[sample.Sample]:
  """Builds the sample list for one benchmark Run.

  Args:
    records: List of ClaimRecord, one per submitted claim.
    peak_concurrency: Maximum in-flight claim count observed.
    metadata: Dict attached to every emitted sample.

  Returns:
    A list of sample.Sample.
  """
  successes = [r for r in records if r.error is None and r.ready_at is not None]
  error_counts = collections.Counter(
      r.error for r in records if r.error is not None
  )

  num_failures = len(records) - len(successes)
  if records and (num_failures / len(records)) >= _MAX_FAILURE_RATE:
    raise errors.Benchmarks.RunError(
        f'{num_failures}/{len(records)} SandboxClaims failed '
        f'(>= {_MAX_FAILURE_RATE:.0%} threshold). '
        f'Errors: {dict(error_counts)}'
    )

  startup_times = [
      r.startup_time_s for r in successes if r.startup_time_s is not None
  ]
  samples = _summarize_metric('startup_time_', startup_times, metadata)

  warm_startup_times = [
      r.startup_time_s
      for r in successes
      if r.warm_served and r.startup_time_s is not None
  ]
  if warm_startup_times:
    samples.extend(
        _summarize_metric('warm_startup_time_', warm_startup_times, metadata)
    )

  cold_startup_times = [
      r.startup_time_s
      for r in successes
      if r.warm_served is not None
      and not r.warm_served
      and r.startup_time_s is not None
  ]
  if cold_startup_times:
    samples.extend(
        _summarize_metric('cold_startup_time_', cold_startup_times, metadata)
    )

  exec_durations = [
      r.exec_duration_s for r in successes if r.exec_duration_s is not None
  ]
  if exec_durations:
    samples.extend(
        _summarize_metric('exec_duration_s_', exec_durations, metadata)
    )

  delete_durations = [
      r.delete_duration_s for r in successes if r.delete_duration_s is not None
  ]
  if delete_durations:
    samples.extend(
        _summarize_metric('delete_duration_s_', delete_durations, metadata)
    )

  lifecycles = [
      r.total_lifecycle_s for r in successes if r.total_lifecycle_s is not None
  ]
  if lifecycles:
    samples.extend(
        _summarize_metric('total_lifecycle_s_', lifecycles, metadata)
    )

  pod_min_request: dict[str, float] = {}
  for r in records:
    prefix = r.name.rsplit('-', 1)[0] if '-' in r.name else ''
    if (
        prefix not in pod_min_request
        or r.requested_at < pod_min_request[prefix]
    ):
      pod_min_request[prefix] = r.requested_at

  request_times = [
      r.requested_at
      - pod_min_request[r.name.rsplit('-', 1)[0] if '-' in r.name else '']
      for r in records
  ]
  ready_times = [
      r.ready_at
      - pod_min_request[r.name.rsplit('-', 1)[0] if '-' in r.name else '']
      for r in successes
      if r.ready_at is not None
  ]
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

  samples.extend([
      sample.Sample('submit_qps', submit_qps, 'count/sec', metadata),
      sample.Sample('completion_qps', completion_qps, 'count/sec', metadata),
      sample.Sample('peak_concurrency', peak_concurrency, 'count', metadata),
      sample.Sample(
          'warm_served_fraction', warm_fraction, 'fraction', metadata
      ),
      sample.Sample('success_count', len(successes), 'count', metadata),
      sample.Sample(
          'error_count', sum(error_counts.values()), 'count', metadata
      ),
  ])
  for error_type, count in error_counts.items():
    error_metadata = dict(metadata, error_type=error_type)
    samples.append(
        sample.Sample(
            f'error_count_{error_type}', count, 'count', error_metadata
        )
    )
  return samples


_PROMETHEUS_METRICS_ALLOWLIST = frozenset({
    'agent_sandbox_build_info',
    'agent_sandbox_claim_controller_startup_latency_ms_sum',
    'agent_sandbox_claim_controller_startup_latency_ms_count',
    'agent_sandbox_claim_creation_total',
    'agent_sandbox_claim_startup_latency_ms_sum',
    'agent_sandbox_claim_startup_latency_ms_count',
    'agent_sandbox_client_claim_startup_latency_ms_sum',
    'agent_sandbox_client_claim_startup_latency_ms_count',
    'agent_sandbox_creation_latency_ms_sum',
    'agent_sandbox_creation_latency_ms_count',
    'agent_sandbox_warmpool_size',
    'agent_sandboxes',
    'rest_client_requests_total',
    'rest_client_request_duration_seconds_sum',
    'rest_client_request_duration_seconds_count',
    'rest_client_rate_limiter_duration_seconds_sum',
    'rest_client_request_retries_total',
    'workqueue_depth',
    'workqueue_adds_total',
    'workqueue_retries_total',
    'controller_runtime_reconcile_total',
    'controller_runtime_reconcile_errors_total',
    'controller_runtime_active_workers',
})


def parse_controller_prometheus_metrics(
    prometheus_text: str,
    metadata: dict[str, Any] | None = None,
) -> list[sample.Sample]:
  """Parses scraped controller Prometheus metrics text into PKB Samples."""
  meta = dict(metadata) if metadata else {}
  samples: list[sample.Sample] = []
  for raw_line in prometheus_text.splitlines():
    line = raw_line.strip()
    if not line or line.startswith('#'):
      continue
    parts = line.split()
    if len(parts) < 2:
      continue
    metric_and_labels, val_str = parts[0], parts[1]
    if '{' in metric_and_labels:
      metric_name, label_part = metric_and_labels.split('{', 1)
      label_part = label_part.rstrip('}')
    else:
      metric_name, label_part = metric_and_labels, ''
    if metric_name not in _PROMETHEUS_METRICS_ALLOWLIST:
      continue
    try:
      val = float(val_str)
    except ValueError:
      continue
    sample_meta = dict(meta)
    if label_part:
      sample_meta['prom_labels'] = label_part
    samples.append(
        sample.Sample(
            f'controller_{metric_name}',
            val,
            'count',
            sample_meta,
        )
    )
  return samples
