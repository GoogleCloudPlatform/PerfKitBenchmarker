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
from typing import Any

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
    Measures the time it takes for a slow-starting JVM application or vLLM
    to become ready in a Kubernetes cluster.
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

# Flags
_DEPLOYMENT_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_yaml',
    'container/kubernetes_deployment_startup/slowjvmstartup.yaml.j2',
    'Deployment yaml for JVM workload.',
)
_IMAGE = flags.DEFINE_string(
    'kubernetes_deployment_startup_image',
    None,
    'Container image for the workload. If omitted, defaults to the image '
    'configured in the benchmark config (e.g. "slowjvmstartup") for the '
    'JVM workload, and '
    '"public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:latest" for the vLLM '
    'workload.',
)

_WORKLOAD = flags.DEFINE_enum(
    'kubernetes_deployment_startup_workload',
    'jvm',
    ['jvm', 'vllm'],
    'Workload type to deploy.',
)

_VLLM_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_yaml',
    'container/kubernetes_deployment_startup/vllm.yaml.j2',
    'Deployment yaml for the vLLM workload.',
)
_VLLM_MEMORY_LIMIT = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_memory_limit',
    '8Gi',
    "vLLM container's requests/limits.memory (Kubernetes quantity, e.g."
    + ' "8Gi"). vLLM OOMKills (exit 137) during its "Warming up model for'
    + ' the compilation..." phase against too small a limit -- 8Gi keeps'
    + ' the pod Guaranteed QoS (requests == limits) and fits comfortably'
    + ' on the n2-standard-4 nodes used for baseline vLLM runs (~16GiB'
    + ' allocatable).',
)

_JVM_DEPLOYMENT_NAME = 'startup'
_VLLM_DEPLOYMENT_NAME = 'vllm-startup'

# Interval between successive CPU polls (seconds).
_CPU_POLL_INTERVAL_SECS = 5


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Returns merged benchmark config.

  For workload=vllm, swaps the container spec's image to VLLM_IMAGE.

  Args:
    user_config: User-supplied configuration.

  Returns:
    Loaded benchmark configuration.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  image = _IMAGE.value
  if image is None and _WORKLOAD.value == 'vllm':
    image = 'public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:latest'

  if image is not None:
    config['container_specs']['kubernetes_deployment_startup']['image'] = image

  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the Kubernetes cluster for the benchmark.

  Deploys the JVM or vLLM workload depending on WORKLOAD.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec  # Unused.


def _ParsePodMetrics(base_metadata: dict[str, Any]) -> list[sample.Sample]:
  """Parses pod conditions to generate startup samples."""
  samples: list[sample.Sample] = []
  # ── Parse pod conditions ──────────────────────────────────────────────
  # max_pod_ready_time uses PodReadyToStartContainers -> Ready (existing).
  # startup_latency uses PodRunning -> Ready (container process started ->
  # app passed its readiness probe). PodRunning is synthesized by
  # kubernetes_conditions from containerStatuses[].state.running.startedAt,
  # since it isn't a real pod condition.
  pod_times: dict[str, dict[str, int]] = collections.defaultdict(
      lambda: collections.defaultdict(int)
  )
  for c in kubernetes_conditions.GetStatusConditionsForResourceType('pod'):
    if c.event == 'PodReadyToStartContainers':
      pod_times[c.resource_name]['start_time'] = c.epoch_time
    elif c.event == 'PodRunning':
      pod_times[c.resource_name]['running_time'] = c.epoch_time
    elif c.event == 'Ready':
      pod_times[c.resource_name]['ready_time'] = c.epoch_time

  if not pod_times or all(
      'ready_time' not in times for times in pod_times.values()
  ):
    raise RuntimeError('No pods became ready')

  # ── Metric 1: max_pod_ready_time ─────────────────────────────────────
  max_pod_ready_t = -1
  for _, times in pod_times.items():
    if 'start_time' not in times or 'ready_time' not in times:
      continue
    t = times['ready_time'] - times['start_time']
    max_pod_ready_t = max(max_pod_ready_t, t)

  if max_pod_ready_t < 0:
    raise RuntimeError('No pods became ready')

  samples.append(
      sample.Sample(
          'max_pod_ready_time', max_pod_ready_t, 'seconds', {**base_metadata}
      )
  )

  # ── Metric 2: per_pod_ready_time ──────────────────────────────
  for pod_name, times in pod_times.items():
    if 'start_time' not in times or 'ready_time' not in times:
      continue
    pod_ready_t = times['ready_time'] - times['start_time']
    if pod_ready_t < 0:
      continue
    samples.append(
        sample.Sample(
            'per_pod_ready_time',
            pod_ready_t,
            'seconds',
            {**base_metadata, 'pod_name': pod_name},
        )
    )

  # ── Metric 3: startup_latency (PodRunning -> Ready) ──────────────────
  max_startup_latency = -1
  for pod_name, times in pod_times.items():
    if 'running_time' not in times or 'ready_time' not in times:
      continue
    latency = times['ready_time'] - times['running_time']
    if latency < 0:
      continue
    max_startup_latency = max(max_startup_latency, latency)
    samples.append(
        sample.Sample(
            'per_pod_startup_latency',
            latency,
            'seconds',
            {**base_metadata, 'pod_name': pod_name},
        )
    )

  if max_startup_latency < 0:
    raise RuntimeError(
        'Could not compute startup_latency: no pod had both a'
        ' PodRunning and Ready timestamp (container runtime may not report'
        ' containerStatuses[].state.running.startedAt on this cluster).'
    )

  samples.append(
      sample.Sample(
          'startup_latency',
          max_startup_latency,
          'seconds',
          {**base_metadata},
      )
  )

  logging.info(
      '[startup] workload=%s max_pod_ready_time=%.2fs'
      + ' startup_latency=%.2fs pods=%d',
      base_metadata['workload'],
      max_pod_ready_t,
      max_startup_latency,
      len(pod_times),
  )

  return samples


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark and collects startup metrics.

  Collects all metrics (max_pod_ready_time, per_pod_ready_time,
  startup_latency/per_pod_startup_latency, cpu_utilization_*) against
  whichever workload's Deployment is active.

  Required metrics fail loudly rather than silently degrading: if a
  metric can't be computed at all for the whole run, this raises instead
  of logging a warning and returning partial results.

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    RuntimeError: If no pods become ready, if no pod had both a
      PodRunning and Ready timestamp (startup_latency uncomputable), or
      if zero CPU utilization readings were collected all run.

  Returns:
    List of sample.Sample objects.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image
  del benchmark_spec  # Image/deployment name are resolved via flags below.
  workload = _WORKLOAD.value
  deployment_name = (
      _VLLM_DEPLOYMENT_NAME if workload == 'vllm' else _JVM_DEPLOYMENT_NAME
  )

  base_metadata: dict[str, Any] = {
      'scenario': 'baseline',
      'workload': workload,
      'cloud': FLAGS.cloud,
  }

  # ── CPU background collector ──────────────────────────────────
  all_samples: list[sample.Sample] = []
  stop = threading.Event()
  cpu_collector = _CpuUtilizationCollector(
      all_samples, stop, deployment_name, base_metadata
  )
  collector_errors: list[BaseException] = []
  collector_thread = None

  def _RunCollector() -> None:
    # Runs in a background thread: exceptions raised here (e.g. zero CPU
    # readings collected all run) don't propagate to the main thread on
    # their own, so capture and re-raise below once the thread is joined.
    try:
      cpu_collector.ObserveCpuUtilization()
    except Exception as e:  # pylint: disable=broad-except
      collector_errors.append(e)
  try:

    collector_thread = threading.Thread(
        target=_RunCollector,
        daemon=True,
    )
    collector_thread.start()

    if workload == 'vllm':
      logging.info('[startup] Deploying vLLM workload (image=%s)', image)
      kubernetes_commands.ApplyManifest(
          _VLLM_YAML.value,
          name=_VLLM_DEPLOYMENT_NAME,
          image=image,
          gpu_memory_utilization=0.5,
          memory_limit=_VLLM_MEMORY_LIMIT.value,
      )
    else:
      logging.info('[startup] Deploying JVM workload (image=%s)', image)
      kubernetes_commands.ApplyManifest(
          _DEPLOYMENT_YAML.value,
          name=_JVM_DEPLOYMENT_NAME,
          image=image,
      )

    kubernetes_commands.WaitForRollout(
        f'deployment/{deployment_name}', timeout=600
    )

  finally:
    stop.set()
    if collector_thread is not None:
      collector_thread.join(timeout=_CPU_POLL_INTERVAL_SECS * 3)

  if collector_errors:
    raise collector_errors[0]

  all_samples.extend(_ParsePodMetrics(base_metadata))

  return all_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up the Kubernetes cluster after the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec


class _CpuUtilizationCollector:
  """Polls CPU utilization in a background thread during the startup window.

  Emits three samples on completion:
    cpu_utilization_peak_millicores   - maximum reading during startup window.
    cpu_utilization_mean_millicores   - mean across all polls.
    cpu_utilization_reading_count     - number of successful polls.
  """

  def __init__(
      self,
      samples: list[sample.Sample],
      stop: threading.Event,
      deployment_name: str = 'startup',
      base_metadata: dict[str, Any] | None = None,
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
    self._readings: list[float] = []
    self._lock = threading.Lock()

  def ObserveCpuUtilization(self) -> None:
    """Polls CPU utilization for the duration of the run.

    Transient poll failures (e.g. the Kubernetes Metrics API still
    warming up on a freshly created cluster) are tolerated by _Observe
    and simply retried. This only raises if the metric ends up with zero
    data for the entire run.

    Raises:
      RuntimeError: If not a single CPU reading was collected all run.
    """
    self._Observe(self._PollCpuMillicoresSample)
    with self._lock:
      readings = list(self._readings)
    if not readings:
      raise RuntimeError(
          'Collected zero CPU utilization readings for the entire run --'
          ' the Kubernetes Metrics API may never have become available.'
          ' cpu_utilization_peak/mean_millicores cannot be computed.'
      )
    peak = max(readings)
    mean = sum(readings) / len(readings)
    self._samples.extend(
        [
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
        ]
    )

  def _PollCpuMillicoresSample(self) -> list[sample.Sample]:
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
    return []

  def _Observe(self, observe_fn: Callable[[], list[sample.Sample]]) -> None:
    while True:
      try:
        self._samples.extend(observe_fn())
      except (
          errors.VmUtil.IssueCommandError,
          errors.VmUtil.IssueCommandTimeoutError,
      ) as e:
        logging.warning('[startup/cpu] Poll error: %s', e)
      if self._stop.wait(timeout=_CPU_POLL_INTERVAL_SECS):
        return
