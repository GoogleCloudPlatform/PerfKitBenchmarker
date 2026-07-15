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

PR 3 — Baseline/Optimized Scenario + CPU Startup Boost via VPA (Layer 2)
==========================================================================
Completes the 4-configuration benchmark matrix:

  Config 1: GKE  w/o cpuboost  (scenario=baseline,   cloud=GCP)
  Config 2: AWS  w/o cpuboost  (scenario=baseline,   cloud=AWS)
  Config 3: AKS  w/o cpuboost  (scenario=baseline,   cloud=Azure)
  Config 4: GKE  w/  cpuboost  (scenario=optimized,  cloud=GCP)

Changes vs PR 2:
  - BOOST_FACTOR flag (default 2, matching Kam's "factor of 2 or 3").
  - GetConfig(): enables VPA on the cluster config for scenario=optimized.
  - Prepare(): deploys a VerticalPodAutoscaler alongside the JVM manifest
    when scenario=optimized (GKE only). VPA manifest uses startup boost
    policy with the configured boost factor.
  - CheckPrerequisites(): raises if scenario=optimized is used on non-GCP.
  - Run(): adds scenario to all sample metadata for cross-config comparison.

Exact VPA startup-boost policy fields follow Kam's linked guide.
vLLM + optimized combination is not supported (GKE VPA boost targets the
JVM slow-start workload specifically).
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

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_deployment_startup'
BENCHMARK_CONFIG = """
kubernetes_deployment_startup:
  description: >
    Measures the time it takes for a slow-starting JVM application or vLLM
    to become ready in a Kubernetes cluster. Supports CPU Startup Boost via
    VPA on GKE (scenario=optimized).
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

# ── Existing flags (PR 1 + PR 2) ─────────────────────────────────────────
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
    'Workload type to deploy.',
)
SCENARIO = flags.DEFINE_enum(
    'kubernetes_deployment_startup_scenario',
    'baseline',
    ['baseline', 'optimized'],
    'Startup scenario. optimized enables GKE VPA CPU Startup Boost (GCP only).',
)
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

# ── New flags (PR 3) ──────────────────────────────────────────────────────
BOOST_FACTOR = flags.DEFINE_integer(
    'kubernetes_deployment_startup_boost_factor',
    2,
    'CPU Startup Boost factor for VPA (scenario=optimized only). '
    'Matches Kam\'s recommended "factor of 2 or 3". GCP only.',
    lower_bound=1,
    upper_bound=10,
)
VPA_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_vpa_yaml',
    'container/kubernetes_deployment_startup/slowjvmstartup_vpa.yaml.j2',
    'VPA manifest for CPU Startup Boost (scenario=optimized, GCP only).',
)

_JVM_DEPLOYMENT_NAME = 'startup'
_VLLM_DEPLOYMENT_NAME = 'vllm-startup'
_CPU_POLL_INTERVAL_SECS = 5


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Returns merged benchmark config.

  For scenario=optimized, enables VPA on the container cluster spec so
  PKB provisions a VPA-enabled GKE cluster.

  Args:
    user_config: User-supplied configuration.

  Returns:
    Loaded benchmark configuration.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if WORKLOAD.value == 'vllm':
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = VLLM_IMAGE.value
  elif DEPLOYMENT_IMAGE.value is not None:
    config['container_specs']['kubernetes_deployment_startup'][
        'image'
    ] = DEPLOYMENT_IMAGE.value

  # PR 3: enable VPA on the cluster for the optimized scenario.
  if SCENARIO.value == 'optimized':
    config['container_cluster']['enable_vpa'] = True
    logging.info(
        '[startup] scenario=optimized: enable_vpa=True on cluster config'
    )

  return config


def CheckPrerequisites(_) -> None:
  """Validates flag combinations before cluster creation.

  Args:
    _: Unused benchmark spec (required by PKB interface).

  Raises:
    ValueError: If scenario=optimized is used on non-GCP or with vLLM.
  """
  if SCENARIO.value == 'optimized':
    if FLAGS.cloud != 'GCP':
      raise ValueError(
          f'--kubernetes_deployment_startup_scenario=optimized requires '
          f'--cloud=GCP (GKE only). Got --cloud={FLAGS.cloud}.'
      )
    if WORKLOAD.value == 'vllm':
      raise ValueError(
          '--kubernetes_deployment_startup_scenario=optimized is only '
          'supported with --kubernetes_deployment_startup_workload=jvm. '
          'VPA CPU Startup Boost targets the JVM slow-start workload.'
      )


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the Kubernetes cluster for the benchmark.

  For scenario=optimized, also deploys a VerticalPodAutoscaler manifest
  with a startup boost policy targeting the JVM deployment.

  Args:
    benchmark_spec: The benchmark specification.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image
  workload = WORKLOAD.value
  scenario = SCENARIO.value

  if workload == 'vllm':
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

    # PR 3: apply VPA with startup boost for optimized scenario.
    if scenario == 'optimized':
      logging.info(
          '[startup] scenario=optimized: applying VPA with boost_factor=%d',
          BOOST_FACTOR.value,
      )
      kubernetes_commands.ApplyManifest(
          VPA_YAML.value,
          name=_JVM_DEPLOYMENT_NAME,
          boost_factor=BOOST_FACTOR.value,
      )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark and collects startup metrics.

  Collects all metrics required by the benchmark methodology doc, plus
  metadata from PR 3:
    1. max_pod_ready_time     — PodReadyToStartContainers -> Ready.
    2. startup_latency        — PodRunning -> Ready (per-pod:
       per_pod_startup_latency). PodRunning is synthesized in
       kubernetes_conditions from containerStatuses[].state.running.
       startedAt, since Kubernetes doesn't report it as a real condition.
    3. cpu_utilization_*       — background CPU collector (PR 1).
    (per_pod_ready_time is also emitted as a PR 1 bonus metric, not
    required by the doc but useful for percentile analysis across
    replicas.)

  For scenario=optimized, the VPA startup boost is already active from
  Prepare() — no additional Run() changes needed.

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    RuntimeError: If no pods become ready.

  Returns:
    List of sample.Sample objects.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image
  workload = WORKLOAD.value
  scenario = SCENARIO.value

  deployment_name = (
      _VLLM_DEPLOYMENT_NAME if workload == 'vllm' else _JVM_DEPLOYMENT_NAME
  )

  # PR 3: boost_factor in metadata so config 1 vs config 4 comparison is clear.
  base_metadata: Dict[str, Any] = {
      'scenario': scenario,
      'workload': workload,
      'cloud': FLAGS.cloud,
      'deployment_name': deployment_name,
      'boost_factor': BOOST_FACTOR.value if scenario == 'optimized' else 1,
  }

  # ── CPU background collector (PR 1) ──────────────────────────────────
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

  # ── Parse pod conditions ──────────────────────────────────────────────
  # max_pod_ready_time uses PodReadyToStartContainers -> Ready (existing).
  # startup_latency uses PodRunning -> Ready (container process started ->
  # app passed its readiness probe), per the requirements doc's Metrics
  # table. PodRunning is synthesized by kubernetes_conditions from
  # containerStatuses[].state.running.startedAt, since it isn't a real
  # pod condition.
  pod_name_to_start_end_times: dict[str, tuple[int, int]] = (
      collections.defaultdict(lambda: (0, 0))
  )
  pod_name_to_running_ready_times: dict[str, tuple[int, int]] = (
      collections.defaultdict(lambda: (0, 0))
  )
  for c in kubernetes_conditions.GetStatusConditionsForResourceType('pod'):
    if c.event == 'PodReadyToStartContainers':
      prev_end_time = pod_name_to_start_end_times[c.resource_name][1]
      pod_name_to_start_end_times[c.resource_name] = (
          c.epoch_time,
          prev_end_time,
      )
    elif c.event == 'PodRunning':
      prev_end_time = pod_name_to_running_ready_times[c.resource_name][1]
      pod_name_to_running_ready_times[c.resource_name] = (
          c.epoch_time,
          prev_end_time,
      )
    elif c.event == 'Ready':
      prev_start_time = pod_name_to_start_end_times[c.resource_name][0]
      pod_name_to_start_end_times[c.resource_name] = (
          prev_start_time,
          c.epoch_time,
      )
      prev_running_start_time = pod_name_to_running_ready_times[
          c.resource_name
      ][0]
      pod_name_to_running_ready_times[c.resource_name] = (
          prev_running_start_time,
          c.epoch_time,
      )

  if not pod_name_to_start_end_times:
    raise RuntimeError('No pods became ready')

  # ── Metric 1: max_pod_ready_time ─────────────────────────────────────
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

  # ── Metric 3: startup_latency (PodRunning -> Ready) ──────────────────
  # Required by the doc's Metrics table alongside Max Pod Ready Time and
  # CPU Utilization. Only computed for pods where both a PodRunning
  # timestamp and a Ready timestamp were observed.
  max_startup_latency = -1
  for pod_name, (running_t, ready_t) in pod_name_to_running_ready_times.items():
    if running_t <= 0 or ready_t <= 0:
      continue
    latency = ready_t - running_t
    if latency < 0:
      continue
    max_startup_latency = max(max_startup_latency, latency)
    all_samples.append(
        sample.Sample(
            'per_pod_startup_latency',
            latency,
            'seconds',
            {**base_metadata, 'pod_name': pod_name},
        )
    )

  if max_startup_latency >= 0:
    all_samples.append(
        sample.Sample(
            'startup_latency',
            max_startup_latency,
            'seconds',
            {**base_metadata},
        )
    )
  else:
    logging.warning(
        '[startup] Could not compute startup_latency: no pod had both a'
        ' PodRunning and Ready timestamp (container runtime may not report'
        ' containerStatuses[].state.running.startedAt on this cluster).'
    )

  logging.info(
      '[startup] scenario=%s workload=%s max_pod_ready_time=%.2fs'
      ' startup_latency=%s pods=%d',
      scenario,
      workload,
      max_pod_ready_t,
      max_startup_latency if max_startup_latency >= 0 else 'n/a',
      len(pod_name_to_start_end_times),
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
  """Polls CPU utilization in a background thread during the startup window."""

  def __init__(self, samples, stop):
    self._samples = samples
    self._stop = stop
    self._readings: List[float] = []
    self._lock = threading.Lock()

  def ObserveCpuUtilization(self) -> None:
    self._Observe(self._PollCpuMillicoresSample)
    with self._lock:
      readings = list(self._readings)
    if not readings:
      return
    peak = max(readings)
    mean = sum(readings) / len(readings)
    self._samples.extend([
        sample.Sample('cpu_utilization_peak_millicores', peak, 'millicores', {}),
        sample.Sample('cpu_utilization_mean_millicores', mean, 'millicores', {}),
        sample.Sample('cpu_utilization_reading_count', len(readings), 'count', {}),
    ])

  def _PollCpuMillicoresSample(self) -> List[sample.Sample]:
    cpu_m = _GetTotalCpuMillicores()
    if cpu_m is None:
      return []
    with self._lock:
      self._readings.append(cpu_m)
    return []

  def _Observe(self, observe_fn: Callable[[], List[sample.Sample]]) -> None:
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


def _GetTotalCpuMillicores() -> float | None:
  try:
    from perfkitbenchmarker.resources.container_service import kubectl  # pylint: disable=g-import-not-at-top
    stdout, _, rc = kubectl.RunKubectlCommand(
        ['top', 'pods', '--no-headers'], raise_on_failure=False
    )
    if rc != 0 or not stdout.strip():
      return None
    total_m = 0.0
    for line in stdout.strip().splitlines():
      parts = line.split()
      if len(parts) < 2:
        continue
      cpu_str = parts[1]
      total_m += float(cpu_str[:-1]) if cpu_str.endswith('m') else float(cpu_str) * 1000.0
    return total_m
  except (ValueError, IndexError):
    return None
