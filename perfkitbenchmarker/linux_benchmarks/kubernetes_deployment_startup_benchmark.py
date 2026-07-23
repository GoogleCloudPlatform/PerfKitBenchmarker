# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Benchmark for measuring time to start up a deployment on Kubernetes."""

import collections
from collections.abc import Callable
import logging
import threading
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.container_service import kubernetes_conditions

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_deployment_startup'
BENCHMARK_CONFIG = """
kubernetes_deployment_startup:
  description: >
    Measures the time it takes for a slow-starting JVM application
    to become ready in a Kubernetes cluster.
  workload: jvm
  scenario: baseline
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_spec: *default_dual_core
  container_specs:
    kubernetes_deployment_startup:
      image: slowjvmstartup
  container_registry:
    cloud: GCP
    spec:
      GCP:
        zone: 'us-central1'
"""

DEPLOYMENT_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_yaml',
    'container/kubernetes_deployment_startup/slowjvmstartup.yaml.j2',
    'Deployment yaml',
)
DEPLOYMENT_IMAGE = flags.DEFINE_string(
    'kubernetes_deployment_startup_image',
    None,
    'Image name. If omitted, "slowjvmstartup" will be used',
)

# Interval between successive CPU polls (seconds).
_CPU_POLL_INTERVAL_SECS = 5


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Returns merged benchmark config."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if DEPLOYMENT_IMAGE.value is not None:
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = DEPLOYMENT_IMAGE.value
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the Kubernetes cluster for the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark and collects the results.

  Collects three categories of metrics:
    1. max_pod_ready_time
    2. per_pod_ready_time  — one Sample per pod with pod name in metadata.
    3. cpu_utilization_*   — peak/mean/count via background collector.

  All samples carry scenario/workload/cloud/replicas metadata.

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    RuntimeError: Raised if no pods are ready after the deployment rolls out.

  Returns:
    A list of sample.Sample objects.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image
  deployment_name = 'startup'

  base_metadata: Dict[str, Any] = {
      'scenario': 'baseline',
      'workload': 'jvm',
      'cloud': FLAGS.cloud,
  }

  # ── CPU background collector ──────────────────────────────────
  all_samples: List[sample.Sample] = []
  stop = threading.Event()
  cpu_collector = _CpuUtilizationCollector(
      all_samples, stop, deployment_name, base_metadata
  )
  collector_thread = None

  try:
    kubernetes_commands.ApplyManifest(
        DEPLOYMENT_YAML.value,
        name=deployment_name,
        image=image,
    )

    # Run CPU collector in parallel with the rollout wait, exactly like
    # KubernetesMetricsCollector in kubernetes_hpa_benchmark.py.
    collector_thread = threading.Thread(
        target=cpu_collector.ObserveCpuUtilization,
        daemon=True,
    )
    collector_thread.start()

    kubernetes_commands.WaitForRollout(
        f'deployment/{deployment_name}', timeout=600
    )

  finally:
    stop.set()
    if collector_thread is not None:
      collector_thread.join(timeout=_CPU_POLL_INTERVAL_SECS * 3)

    pod_name_to_start_end_times: dict[str, tuple[int, int]] = (
        collections.defaultdict(lambda: (0, 0))
    )
  for c in kubernetes_conditions.GetStatusConditionsForResourceType('pod'):
    if c.event == 'PodReadyToStartContainers':
      prev_end_time = pod_name_to_start_end_times[c.resource_name][1]
      pod_name_to_start_end_times[c.resource_name] = (
          c.epoch_time,
          prev_end_time,
      )
    elif c.event == 'Ready':
      prev_start_time = pod_name_to_start_end_times[c.resource_name][0]
      pod_name_to_start_end_times[c.resource_name] = (
          prev_start_time,
          c.epoch_time,
      )

  if not pod_name_to_start_end_times:
    raise RuntimeError('No pods became ready')

  # ── Metric 1: max_pod_ready_time
  max_pod_ready_t = -1
  for _, times in pod_name_to_start_end_times.items():
    t = times[1] - times[0]
    max_pod_ready_t = max(max_pod_ready_t, t)

  if max_pod_ready_t < 0:
    raise RuntimeError('No pods became ready')

  all_samples.append(
      sample.Sample(
          'max_pod_ready_time',
          max_pod_ready_t,
          'seconds',
          {**base_metadata},
      )
  )

  # ── Metric 2: per_pod_ready_time

  for pod_name, (start_t, end_t) in pod_name_to_start_end_times.items():
    pod_ready_t = end_t - start_t
    if pod_ready_t >= 0:
      all_samples.append(
          sample.Sample(
              'per_pod_ready_time',
              pod_ready_t,
              'seconds',
              {**base_metadata, 'pod_name': pod_name},
          )
      )

  logging.info(
      '[startup] max_pod_ready_time=%.2fs across %d pod(s)',
      max_pod_ready_t,
      len(pod_name_to_start_end_times),
  )

  return all_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up the Kubernetes cluster after the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec


class _CpuUtilizationCollector:
  """Polls CPU utilization in a background thread during the startup window.

  Follows the KubernetesMetricsCollector / _Observe pattern from
  kubernetes_hpa_benchmark.py exactly:
  - _Observe(fn) loops calling fn() and appending results to self._samples.
  - Stops when self._stop is signalled.
  - Ignores IssueCommandError / IssueCommandTimeoutError (gaps in data OK).

  Emits three samples on completion:
    cpu_utilization_peak_millicores   — maximum reading during startup window.
    cpu_utilization_mean_millicores   — mean across all polls.
    cpu_utilization_reading_count     — number of successful polls.
  """

  def __init__(
      self,
      samples: List[sample.Sample],
      stop: threading.Event,
      deployment_name: str = 'startup',
      base_metadata: Dict[str, Any] | None = None,
  ):
    """Initialises the collector.

    Args:
      samples: Shared sample list.  CPU samples are appended here when
        ObserveCpuUtilization() finishes.
      stop: Threading event.  Collector loops until this is set.
      deployment_name: The deployment name.
      base_metadata: The base metadata to append to generated CPU samples.
    """
    self._samples = samples
    self._stop = stop
    self._deployment_name = deployment_name
    self._base_metadata = base_metadata or {}
    self._readings: List[float] = []
    self._lock = threading.Lock()

  def ObserveCpuUtilization(self) -> None:
    """Polls CPU millicores until stop is set; appends aggregate samples.

    Intended to be run in a background thread alongside WaitForRollout().
    Matches the ObserveNumReplicas / ObserveNumNodes pattern in
    kubernetes_hpa_benchmark.py.
    """
    self._Observe(self._PollCpuMillicoresSample)

    # Emit aggregate samples after the loop ends.
    with self._lock:
      readings = list(self._readings)

    if not readings:
      logging.warning('[startup/cpu] No CPU readings collected.')
      return

    peak = max(readings)
    mean = sum(readings) / len(readings)
    count = len(readings)

    logging.info(
        '[startup/cpu] peak=%.1f mean=%.1f count=%d millicores',
        peak, mean, count,
    )

    self._samples.extend([
        sample.Sample(
            'cpu_utilization_peak_millicores',
            peak,
            'millicores',
            {**self._base_metadata},
        ),
        sample.Sample(
            'cpu_utilization_mean_millicores',
            mean,
            'millicores',
            {**self._base_metadata},
        ),
        sample.Sample(
            'cpu_utilization_reading_count',
            len(readings),
            'count',
            {**self._base_metadata},
        ),
    ])

  def _PollCpuMillicoresSample(self) -> List[sample.Sample]:
    """Issues kubectl top pods and returns a transient sample list.

    The return value is a list so _Observe() can call self._samples.extend()
    on it (matching the KubernetesMetricsCollector interface).  The actual
    reading is also stored in self._readings for aggregate computation.

    Returns:
      A single-element list with the current CPU reading, or empty on error.
    """
    cpu_m = kubernetes_commands.GetTotalCpuMillicores(
        f'app={self._deployment_name}'
    )
    if cpu_m is None:
      return []
    with self._lock:
      self._readings.append(cpu_m)
    # Return an empty list — we do NOT emit a per-poll sample (too noisy).
    # Aggregates are emitted in ObserveCpuUtilization() after the loop.
    return []

  def _Observe(
      self,
      observe_fn: Callable[[], List[sample.Sample]],
  ) -> None:
    """Calls observe_fn in a loop until self._stop is set.

    Copied verbatim from KubernetesMetricsCollector._Observe() in
    kubernetes_hpa_benchmark.py — same error handling, same 1 s wait.

    Args:
      observe_fn: Function returning a list of samples to extend into
        self._samples.
    """
    success_count = 0
    failure_count = 0
    while True:
      try:
        self._samples.extend(observe_fn())
        success_count += 1
      except (
          errors.VmUtil.IssueCommandError,
          errors.VmUtil.IssueCommandTimeoutError,
      ) as e:
        logging.warning(
            '[startup/cpu] Ignoring poll error (gap in data): %s', e
        )
        failure_count += 1

      if self._stop.wait(timeout=_CPU_POLL_INTERVAL_SECS):
        logging.info(
            '[startup/cpu] Stopping after %d successes / %d failures',
            success_count, failure_count,
        )
        return

