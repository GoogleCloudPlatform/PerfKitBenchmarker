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

PR 4 — CPU Startup Boost for vLLM
==========================================================================
scenario=optimized is no longer JVM-only. AI/inference workloads spend a
large share of startup loading models into memory, so the same GKE VPA
CPU Startup Boost mechanism used for the JVM workload is now wired into
the vLLM Prepare() path too -- same code, same
--kubernetes_deployment_startup_scenario / _workload flags, just toggled
per run.

  - CheckPrerequisites(): no longer raises for workload=vllm; only the
    cloud=GCP restriction remains for scenario=optimized.
  - Prepare(): VPA apply-before-deployment ordering now runs for either
    workload, targeting whichever Deployment name is active
    (startup / vllm-startup).
  - The VPA manifest's CPU ceiling (resourcePolicy.maxAllowed.cpu) and
    startupBoost.cpu.durationSeconds are now template parameters instead
    of hardcoded values -- vLLM's baseline CPU request (2 cores) already
    exceeds the JVM-tuned ceiling of "1", and model loading is expected
    to take longer than the JVM's ~67s baseline that the old fixed 120s
    duration was sized for. New optional flags
    (kubernetes_deployment_startup_vpa_max_cpu /
    _vpa_duration_seconds) override the per-workload defaults if they
    don't fit a given model/image.

PR 5 — Fix vLLM crash-loop from default GPU memory utilization
==========================================================================
Production runs of workload=vllm crash-looped indefinitely (readiness
probe never passed, deployment exceeded its progress deadline) with:
  ValueError: Available memory on node 0 (2.12/4.0 GiB) on startup is
  less than desired CPU memory utilization (0.92, 3.68 GiB).
vLLM's CPU backend defaults --gpu-memory-utilization to ~0.9 (despite the
GPU-sounding name, it also governs the CPU backend), which assumes far
more headroom than this benchmark's 4Gi container memory limit provides
once Python/PyTorch/runtime overhead is subtracted. Raising the memory
limit to compensate would require an impractically large allocation (the
observed ~1.88GiB of fixed overhead means the limit would need to be
~23GiB just to satisfy the default 92% reservation) -- the container's
memory limit was never the actual bottleneck, vLLM's default utilization
fraction was.

  - New VLLM_GPU_MEMORY_UTILIZATION flag
    (kubernetes_deployment_startup_vllm_gpu_memory_utilization, default
    0.5) passed to the vLLM container as a `--gpu-memory-utilization`
    arg in vllm.yaml.j2, keeping the reservation within the headroom
    actually available under the existing 4Gi limit.

PR 6 — Fix vLLM OOMKill during compile/warmup (4Gi limit was too small)
==========================================================================
PR 5's fix stopped the deterministic startup ValueError, but production
runs still OOMKilled (exit 137) a few minutes later, during the "Warming
up model for the compilation..." phase -- confirmed via `kubectl
describe pod` (State: Terminated, Reason: OOMKilled) and `kubectl top
pods` showing RSS plateau right at the 4Gi limit before the pod reset.
Lowering --gpu-memory-utilization from 0.5 to 0.3 (shrinking the KV
cache reservation by ~800MiB) made no measurable difference to the
observed peak (4041Mi vs 4044Mi) -- the overage comes from
compile/warmup buffers, a cost that's largely fixed regardless of KV
cache sizing, so gpu_memory_utilization alone can't fix it.

  - New VLLM_MEMORY_LIMIT flag (kubernetes_deployment_startup_vllm_
    memory_limit, default "8Gi") templated into vllm.yaml.j2's
    requests/limits.memory (kept equal to preserve Guaranteed QoS),
    replacing the old hardcoded 4Gi.

Exact VPA startup-boost policy fields follow Kam's linked guide.
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
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
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

# ── New flags (PR 5) ──────────────────────────────────────────────────────
VLLM_GPU_MEMORY_UTILIZATION = flags.DEFINE_float(
    'kubernetes_deployment_startup_vllm_gpu_memory_utilization',
    0.5,
    "Fraction of the vLLM container's memory limit to reserve for model "
    + "weights/KV cache -- vLLM's --gpu-memory-utilization flag, which "
    + 'despite the name also governs the CPU backend. vLLM defaults to '
    + "~0.9, which assumes far more headroom than this benchmark's 4Gi "
    + 'container memory limit provides once Python/PyTorch/runtime '
    + 'overhead is subtracted -- confirmed in production logs as a '
    + 'deterministic crash-loop ("ValueError: Available memory ... is '
    + 'less than desired CPU memory utilization"). 0.5 keeps the '
    + 'reservation within the observed available headroom on a 4Gi limit.',
    lower_bound=0.05,
    upper_bound=0.95,
)

# ── New flags (PR 6) ──────────────────────────────────────────────────────
VLLM_MEMORY_LIMIT = flags.DEFINE_string(
    'kubernetes_deployment_startup_vllm_memory_limit',
    '8Gi',
    "vLLM container's requests/limits.memory (Kubernetes quantity, e.g."
    + ' "8Gi"). Production runs OOMKilled (exit 137) against the prior'
    + ' hardcoded 4Gi limit during the "Warming up model for the'
    + ' compilation..." phase, even after lowering'
    + ' --gpu-memory-utilization from 0.5 to 0.3 -- observed peak RSS'
    + ' stayed ~4.0GiB in both cases (4041Mi vs 4044Mi), confirming the'
    + ' overage is compile/warmup overhead largely independent of the KV'
    + ' cache reservation, not something --gpu-memory-utilization alone'
    + ' can fix. 8Gi keeps the pod Guaranteed QoS (requests == limits)'
    + ' and fits comfortably on the n2-standard-4 nodes used for'
    + ' baseline vLLM runs (~16GiB allocatable).',
)

# ── New flags (PR 3) ──────────────────────────────────────────────────────
BOOST_FACTOR = flags.DEFINE_integer(
    'kubernetes_deployment_startup_boost_factor',
    2,
    'CPU Startup Boost factor for VPA (scenario=optimized only, GCP only). '
    + 'Matches Kam\'s recommended "factor of 2 or 3".',
    lower_bound=1,
    upper_bound=10,
)
VPA_YAML = flags.DEFINE_string(
    'kubernetes_deployment_startup_vpa_yaml',
    'container/kubernetes_deployment_startup/slowjvmstartup_vpa.yaml.j2',
    'VPA manifest for CPU Startup Boost (scenario=optimized, GCP only).',
)

# ── New flags (PR 4) ──────────────────────────────────────────────────────
VPA_MAX_CPU = flags.DEFINE_string(
    'kubernetes_deployment_startup_vpa_max_cpu',
    None,
    'Ceiling for the VPA CPU Startup Boost '
    + '(resourcePolicy.containerPolicies[0].maxAllowed.cpu). If unset, '
    + 'defaults to "1" for --kubernetes_deployment_startup_workload=jvm and '
    + '"4" for =vllm (scenario=optimized only) -- vLLM already requests 2 '
    + 'full cores at baseline, which exceeds the JVM-tuned "1" ceiling.',
)
VPA_DURATION_SECONDS = flags.DEFINE_integer(
    'kubernetes_deployment_startup_vpa_duration_seconds',
    None,
    'How long, in seconds, the VPA keeps the CPU boost applied before '
    + 'scaling back down (startupBoost.cpu.durationSeconds). If unset, '
    + 'defaults to 120 for workload=jvm and 300 for workload=vllm '
    + '(scenario=optimized only) -- sized to comfortably exceed each '
    + "workload's observed baseline startup time; a boost that reverts "
    + 'before the workload is ready defeats the point.',
    lower_bound=1,
)

_JVM_DEPLOYMENT_NAME = 'startup'
_VLLM_DEPLOYMENT_NAME = 'vllm-startup'
_CPU_POLL_INTERVAL_SECS = 5

_VPA_CRD_NAME = 'verticalpodautoscalers.autoscaling.k8s.io'
_VPA_CRD_WAIT_TIMEOUT_SECS = 180

# PR 4: per-workload VPA sizing defaults, overridable via VPA_MAX_CPU /
# VPA_DURATION_SECONDS. vLLM's baseline CPU request (2 cores, see
# vllm.yaml.j2) already exceeds the JVM-tuned "1" ceiling, and model
# loading is expected to take longer than the JVM's ~67s baseline that
# 120s was originally sized for.
_VPA_DEFAULT_MAX_CPU = {'jvm': '1', 'vllm': '4'}
_VPA_DEFAULT_DURATION_SECONDS = {'jvm': 120, 'vllm': 300}


def _GetVpaMaxCpu(workload: str) -> str:
  """Returns the VPA CPU ceiling for the given workload."""
  return VPA_MAX_CPU.value or _VPA_DEFAULT_MAX_CPU[workload]


def _GetVpaDurationSeconds(workload: str) -> int:
  """Returns the VPA boost duration (seconds) for the given workload."""
  return VPA_DURATION_SECONDS.value or _VPA_DEFAULT_DURATION_SECONDS[workload]


def _WaitForVpaCrd() -> None:
  """Waits for the VerticalPodAutoscaler CRD to be registered on the API server.

  Enabling VPA via --enable-vertical-pod-autoscaling at cluster creation
  triggers an asynchronous GKE addon install for the VPA CRDs. That install
  can lag behind the cluster's own RUNNING status and behind kube-dns
  readiness, so applying a VerticalPodAutoscaler manifest immediately after
  the cluster comes up can race the CRD registration -- confirmed in
  production logs where `kubectl apply` failed with "no matches for kind
  VerticalPodAutoscaler" just seconds after kube-dns reported ready.
  Poll for the CRD instead of assuming it's already there.

  Raises:
    RuntimeError: If the CRD never registers within the timeout.
  """

  @vm_util.Retry(
      timeout=_VPA_CRD_WAIT_TIMEOUT_SECS,
      retryable_exceptions=(errors.VmUtil.IssueCommandError,),
  )
  def _Poll():
    # Deliberately do NOT pass raise_on_failure=False here: kubectl.
    # RunKubectlCommand's suppress_failure wrapper rewrites a suppressed
    # failure's return code to 0 (see vm_util.IssueCommand), which would
    # silently defeat a `retcode != 0` check on the result -- this exact
    # bug let a failing "get crd" report success and skip straight to
    # applying the VPA manifest in production. Let a failing `get crd`
    # raise IssueCommandError naturally and use that as the retry signal.
    kubectl.RunKubectlCommand(['get', 'crd', _VPA_CRD_NAME])

  try:
    _Poll()
  except vm_util.RetryError as e:
    raise RuntimeError(
        f'VerticalPodAutoscaler CRD ({_VPA_CRD_NAME}) never registered'
        f' within {_VPA_CRD_WAIT_TIMEOUT_SECS}s. GKE installs VPA CRDs'
        ' asynchronously after --enable-vertical-pod-autoscaling; either'
        ' the addon install is unusually slow, or CPU Startup Boost is'
        ' not supported on this cluster configuration.'
    ) from e


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
    ValueError: If scenario=optimized is used on a non-GCP cloud. (PR 4:
      scenario=optimized is supported for both workload=jvm and
      workload=vllm -- only the GKE/cloud restriction remains.)
  """
  if SCENARIO.value == 'optimized' and FLAGS.cloud != 'GCP':
    raise ValueError(
        '--kubernetes_deployment_startup_scenario=optimized requires '
        f'--cloud=GCP (GKE only). Got --cloud={FLAGS.cloud}.'
    )


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the Kubernetes cluster for the benchmark.

  For scenario=optimized (either workload, PR 4), first deploys a
  VerticalPodAutoscaler manifest with a startup boost policy targeting
  the active Deployment, and only then deploys the Deployment itself.

  Ordering matters here: GKE's CPU Startup Boost takes effect via a
  mutating admission webhook that intercepts *new* pod creation events.
  If the Deployment (and its first pod) were applied before the VPA
  object exists, that pod's initial CPU request would never be boosted,
  and this benchmark would end up measuring an unboosted startup even
  though scenario=optimized was requested. The VPA is safe to create
  before its targetRef Deployment exists -- it simply waits for the
  target to appear.

  For scenario=optimized, also waits for the VerticalPodAutoscaler CRD to
  be registered before applying the VPA manifest -- GKE installs VPA CRDs
  asynchronously after cluster creation, and that install can still be in
  flight even once the cluster and kube-dns report ready (see
  _WaitForVpaCrd).

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    RuntimeError: If scenario=optimized and the VerticalPodAutoscaler CRD
      never registers within the wait timeout.
  """
  image = benchmark_spec.container_specs['kubernetes_deployment_startup'].image
  workload = WORKLOAD.value
  scenario = SCENARIO.value
  deployment_name = (
      _VLLM_DEPLOYMENT_NAME if workload == 'vllm' else _JVM_DEPLOYMENT_NAME
  )

  # PR 4: apply VPA with startup boost for optimized scenario BEFORE the
  # deployment, for either workload, so the boost's admission-time
  # mutation applies to the very first pod this benchmark measures (see
  # docstring above). Sizing (CPU ceiling / boost duration) is
  # per-workload since vLLM's baseline CPU footprint and model-load time
  # are both much larger than the JVM's.
  if scenario == 'optimized':
    _WaitForVpaCrd()
    logging.info(
        '[startup] scenario=optimized workload=%s: VPA boost_factor=%d'
        + ' applied first',
        workload,
        BOOST_FACTOR.value,
    )
    kubernetes_commands.ApplyManifest(
        VPA_YAML.value,
        name=deployment_name,
        boost_factor=BOOST_FACTOR.value,
        max_allowed_cpu=_GetVpaMaxCpu(workload),
        duration_seconds=_GetVpaDurationSeconds(workload),
    )

  if workload == 'vllm':
    logging.info('[startup] Deploying vLLM workload (image=%s)', image)
    kubernetes_commands.ApplyManifest(
        VLLM_YAML.value,
        name=_VLLM_DEPLOYMENT_NAME,
        image=image,
        gpu_memory_utilization=VLLM_GPU_MEMORY_UTILIZATION.value,
        memory_limit=VLLM_MEMORY_LIMIT.value,
    )
  else:
    logging.info('[startup] Deploying JVM workload (image=%s)', image)
    kubernetes_commands.ApplyManifest(
        DEPLOYMENT_YAML.value,
        name=_JVM_DEPLOYMENT_NAME,
        image=image,
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

  Required metrics fail loudly rather than silently degrading: if a
  metric can't be computed at all for the whole run, this raises instead
  of logging a warning and returning partial results. (A silent warning
  here is exactly what let a prior VPA-ordering bug ship a "successful"
  optimized-scenario run that never actually applied the CPU boost.)

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    RuntimeError: If no pods become ready, if no pod had both a
      PodRunning and Ready timestamp (startup_latency uncomputable), or
      if zero CPU utilization readings were collected all run.

  Returns:
    List of sample.Sample objects.
  """
  del benchmark_spec  # Image/deployment name are resolved via flags below.
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
  collector_errors: List[BaseException] = []

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

    kubernetes_commands.WaitForRollout(
        f'deployment/{deployment_name}', timeout=600
    )

  finally:
    stop.set()
    collector_thread.join(timeout=_CPU_POLL_INTERVAL_SECS * 3)

  if collector_errors:
    raise collector_errors[0]

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

  if max_startup_latency < 0:
    raise RuntimeError(
        'Could not compute startup_latency: no pod had both a'
        ' PodRunning and Ready timestamp (container runtime may not report'
        ' containerStatuses[].state.running.startedAt on this cluster).'
    )

  all_samples.append(
      sample.Sample(
          'startup_latency',
          max_startup_latency,
          'seconds',
          {**base_metadata},
      )
  )

  logging.info(
      '[startup] scenario=%s workload=%s max_pod_ready_time=%.2fs'
      + ' startup_latency=%.2fs pods=%d',
      scenario,
      workload,
      max_pod_ready_t,
      max_startup_latency,
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
    """Polls CPU utilization for the duration of the run.

    Transient poll failures (e.g. the Kubernetes Metrics API still
    warming up on a freshly created cluster) are tolerated by _Observe
    and simply retried. This only raises if the metric ends up with zero
    data for the entire run -- the same standard applied to
    startup_latency, so a total collection failure is surfaced as a
    benchmark failure instead of silently shipping incomplete results.

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
    self._samples.extend([
        sample.Sample(
            'cpu_utilization_peak_millicores', peak, 'millicores', {}
        ),
        sample.Sample(
            'cpu_utilization_mean_millicores', mean, 'millicores', {}
        ),
        sample.Sample(
            'cpu_utilization_reading_count', len(readings), 'count', {}
        ),
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
  """Returns summed CPU millicores across pods from `kubectl top`, or None.

  Returns None (rather than raising) on any parse/command failure so a
  single bad poll doesn't take down the whole collector loop; _Observe
  above retries on the next poll interval regardless.
  """
  try:
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
      if cpu_str.endswith('m'):
        total_m += float(cpu_str[:-1])
      else:
        total_m += float(cpu_str) * 1000.0
    return total_m
  except (ValueError, IndexError):
    return None
