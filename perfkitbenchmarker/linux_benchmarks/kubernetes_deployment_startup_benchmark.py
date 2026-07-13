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
"""Benchmark for measuring time to start up a deployment on Kubernetes.

PR 2 — vLLM Workload Support (Layer 1)
========================================
Adds a second workload (vLLM) alongside the existing JVM slow-start app.

Changes vs PR 1:
  - VLLM_IMAGE flag: public vLLM CPU image (overridable).
  - VLLM_YAML: new vllm.yaml.j2 manifest (port 8000, /health readiness probe).
  - GetConfig(): sets container image from workload flag.
  - Prepare(): deploys JVM or vLLM manifest based on workload flag.
  - Run(): uses workload-appropriate deployment name and wait condition.

No changes to PR 1 metrics or PR 3 scenario logic.
Independent of PR 3 — can be reviewed/merged in any order relative to PR 3.
"""

import collections
import logging
import threading
from collections.abc import Callable
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.container_service import kubernetes_conditions
from perfkitbenchmarker.resources.container_service import kubectl

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

# ── Existing flags (PR 1) ─────────────────────────────────────────────────
DEPLOYMENT_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_yaml',
    'container/kubernetes_deployment_startup/slowjvmstartup.yaml.j2',
    'Deployment yaml for JVM workload.',
)
DEPLOYMENT_IMAGE = flags.DEFINE_string(
    'kubernetes_deployment_startup_image',
    None,
    'Image name for JVM workload. If omitted, "slowjvmstartup" will be used.',
)
WORKLOAD = flags.DEFINE_enum(
    'kubernetes_deployment_startup_workload',
    'jvm',
    ['jvm', 'vllm'],
    'Workload type to deploy. jvm deploys the slow-starting JVM app; '
    'vllm deploys a vLLM CPU inference server.',
)
SCENARIO = flags.DEFINE_enum(
    'kubernetes_deployment_startup_scenario',
    'baseline',
    ['baseline', 'optimized'],
    'Startup scenario. optimized (GKE CPU Startup Boost via VPA) added in PR 3.',
)

# ── New flags (PR 2) ──────────────────────────────────────────────────────
VLLM_IMAGE = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_image',
    'public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:latest',
    'Container image for the vLLM CPU workload.',
)
VLLM_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_yaml',
    'container/kubernetes_deployment_startup/vllm.yaml.j2',
    'Deployment yaml for the vLLM workload.',
)

# Deployment names — must match the `name:` field in each manifest.
_JVM_DEPLOYMENT_NAME = 'startup'
_VLLM_DEPLOYMENT_NAME = 'vllm-startup'

# CPU metrics polling interval in seconds.
_CPU_POLL_INTERVAL_SECS = 5


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Returns merged benchmark config.

  Sets the container image from the appropriate workload flag so the
  container_registry image-building pipeline uses the right source.

  Args:
    user_config: User-supplied configuration (flags and config file).

  Returns:
    Loaded benchmark configuration.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if WORKLOAD.value == 'vllm':
    # vLLM uses a pre-built public image — no registry build needed.
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = VLLM_IMAGE.value
  elif DEPLOYMENT_IMAGE.value is not None:
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = DEPLOYMENT_IMAGE.value
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the Kubernetes cluster for the benchmark.

  Deploys either the JVM slow-start app or the vLLM CPU server depending
  on --kubernetes_deployment_startup_workload.

  Args:
    benchmark_spec: The benchmark specification.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image

  if WORKLOAD.value == 'vllm':
    logging.info('[startup] Deploying vLLM workload (image=%s)', image)
    kubernetes_commands.ApplyManifest(
        VLLM_YAML.value,
        name=_VLLM_DEPLOYMENT_NAME,
        image=image,
    )
  else:
    logging.info('[startup] Deploying JVM workload (image=%s)', image)
    kubernetes_commands.ApplyManifest(
        DEPLOYMENT_YAML.value,
        name=_JVM_DEPLOYMENT_NAME,
        image=image,
    )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark and collects the results.

  Waits for the appropriate deployment to roll out, then collects:
    1. max_pod_ready_time  — existing metric (preserved).
    2. per_pod_ready_time  — one Sample per pod (PR 1).
    3. cpu_utilization_*   — background CPU collector (PR 1).

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    RuntimeError: Raised if no pods are ready after the rollout.

  Returns:
    A list of sample.Sample objects.
  """
  workload = WORKLOAD.value
  scenario = SCENARIO.value

  deployment_name = (
      _VLLM_DEPLOYMENT_NAME if workload == 'vllm' else _JVM_DEPLOYMENT_NAME
  )

  # Common metadata on every sample.
  base_metadata: Dict[str, Any] = {
      'scenario': scenario,
      'workload': workload,
      'cloud': FLAGS.cloud,
      'deployment_name': deployment_name,
  }

  # ── Start CPU background collector (PR 1) ─────────────────────────────
  all_samples: List[sample.Sample] = []
  stop = threading.Event()
  cpu_collector = _CpuUtilizationCollector(all_samples, stop)

  try:
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
    collector_thread.join(timeout=_CPU_POLL_INTERVAL_SECS * 3)

  # ── Parse pod conditions ───────────────────────────────────────────────
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

  # ── Metric 1: max_pod_ready_time (existing) ───────────────────────────
  max_pod_ready_t = -1
  for _, times in pod_name_to_start_end_times.items():
    t = times[1] - times[0]
    max_pod_ready_t = max(max_pod_ready_t, t)

  if max_pod_ready_t < 0:
    raise RuntimeError('No pods became ready')

  all_samples.append(
      sample.Sample(
          'max_pod_ready_time', max_pod_ready_t, 'seconds', {**base_metadata}
      )
  )

  # ── Metric 2: per_pod_ready_time (PR 1) ──────────────────────────────
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
      '[startup] workload=%s max_pod_ready_time=%.2fs pods=%d',
      workload, max_pod_ready_t, len(pod_name_to_start_end_times),
  )

  return all_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up the Kubernetes cluster after the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec


# ---------------------------------------------------------------------------
# CPU Utilization Background Collector (PR 1 — unchanged)
# ---------------------------------------------------------------------------


class _CpuUtilizationCollector:
  """Polls CPU utilization in a background thread during the startup window.

  Follows the KubernetesMetricsCollector / _Observe pattern from
  kubernetes_hpa_benchmark.py exactly.
  """

  def __init__(
      self,
      samples: List[sample.Sample],
      stop: threading.Event,
  ):
    self._samples = samples
    self._stop = stop
    self._readings: List[float] = []
    self._lock = threading.Lock()

  def ObserveCpuUtilization(self) -> None:
    """Polls CPU millicores until stop is set; appends aggregate samples."""
    self._Observe(self._PollCpuMillicoresSample)

    with self._lock:
      readings = list(self._readings)

    if not readings:
      logging.warning('[startup/cpu] No CPU readings collected.')
      return

    peak = max(readings)
    mean = sum(readings) / len(readings)
    count = len(readings)

    self._samples.extend([
        sample.Sample(
            'cpu_utilization_peak_millicores', peak, 'millicores', {}
        ),
        sample.Sample(
            'cpu_utilization_mean_millicores', mean, 'millicores', {}
        ),
        sample.Sample(
            'cpu_utilization_reading_count', count, 'count', {}
        ),
    ])

  def _PollCpuMillicoresSample(self) -> List[sample.Sample]:
    cpu_m = _GetTotalCpuMillicores()
    if cpu_m is None:
      return []
    with self._lock:
      self._readings.append(cpu_m)
    return []

  def _Observe(
      self,
      observe_fn: Callable[[], List[sample.Sample]],
  ) -> None:
    """Calls observe_fn in a loop until self._stop is set."""
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


def _GetTotalCpuMillicores() -> float | None:
  """Returns total CPU millicores across all pods via kubectl top pods."""
  try:
    stdout, _, rc = kubectl.RunKubectlCommand(
        ['top', 'pods', '--no-headers'],
        raise_on_failure=False,
    )
    if rc != 0 or not stdout.strip():
      return None

    total_m = 0.0
    for line in stdout.strip().splitlines():
      parts = line.split()
      if len(parts) < 2:
        continue
      cpu_str = parts[1]
      if cpu_str.endswith('m'):
        total_m += float(cpu_str[:-1])
      else:
        total_m += float(cpu_str) * 1000.0

    return total_m
  except (ValueError, IndexError) as e:
    logging.debug('[startup/cpu] Parse error: %s', e)
    return None
