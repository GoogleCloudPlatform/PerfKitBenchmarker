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
"""Cold node-autoprovisioning scaling latency benchmark (CCC / NAP / Karpenter).

Measures pod-scheduling and node-autoprovisioning latency across cold burst and
incremental scale-up scenarios (for example 0->1, 1->2, 0->5, 0->10, 0->100)
when nodes are provisioned dynamically via GKE NAP + Custom Compute Classes
(CCC), EKS Karpenter, or AKS NAP.

Reuses `ParseStatusChanges`, `CheckForFailures`, and `GetStartEndCountSamples`
from `kubernetes_scale_benchmark` for condition- and event-based metric
collection.
"""

import json
import time
from typing import Any, Callable, cast

from absl import flags
from absl import logging
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import kubernetes_scale_benchmark
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_node_autoprovisioning'
BENCHMARK_CONFIG = """
kubernetes_node_autoprovisioning:
  description: Cold node-autoprovisioning scaling latency (GKE NAP+CCC, EKS Karpenter, AKS NAP).
  container_cluster:
    cloud: GCP
    type: Kubernetes
    min_vm_count: 0
    max_vm_count: 100
    poll_for_events: true
"""

_DEPLOYMENT = 'deployment/provisioning'
_CPU_MANIFEST = 'container/kubernetes_node_autoprovisioning/cpu.yaml.j2'
_GPU_MANIFEST = 'container/kubernetes_node_autoprovisioning/gpu.yaml.j2'
_RESET_TIMEOUT_S = 30 * 60
_NODEPOOL_MANIFEST_FILENAME = 'provisioning-nodepool.yaml'
_DEFAULT_IMAGE = 'nvcr.io/nvidia/cuda:12.3.1-base-ubuntu22.04'

_WORKLOAD = flags.DEFINE_enum(
    'node_autoprovisioning_workload',
    'cpu',
    ['cpu', 'gpu'],
    'Experiment design: cpu or gpu.',
)
_START_REPLICAS = flags.DEFINE_integer(
    'node_autoprovisioning_start_replicas',
    0,
    'Replicas pre-placed in Prepare (0=cold; 1=incremental 1->2 scenario).',
)
_TARGET_REPLICAS = flags.DEFINE_integer(
    'node_autoprovisioning_target_replicas',
    10,
    'Scale target replica count (e.g. 1, 2, 5, 10, 100).',
)
_IMAGE = flags.DEFINE_string(
    'node_autoprovisioning_image',
    _DEFAULT_IMAGE,
    'Workload image. Must be a real image, not pause.',
)
_WORKER_LABEL = flags.DEFINE_string(
    'node_autoprovisioning_worker_label',
    'pkb_nodepool=provisioning',
    'Label selector for worker nodes in cold-start drain.',
)
_PLATFORM = flags.DEFINE_string(
    'node_autoprovisioning_platform',
    'gke_ccc',
    'Platform tag in sample metadata.',
)
_GPU_COUNT = flags.DEFINE_integer(
    'node_autoprovisioning_gpu_count', 1, 'GPUs per pod (GPU workload).'
)
_ONE_POD_PER_NODE = flags.DEFINE_boolean(
    'node_autoprovisioning_one_pod_per_node',
    True,
    'When True, enforces 1 pod per node via anti-affinity or spread constraints'
    ' rather than bin-packing.',
)
_POOL_STATE = flags.DEFINE_enum(
    'node_autoprovisioning_pool_state',
    'absent',
    ['absent', 'zero', 'warm'],
    'Target node pool state at the start of measurement: absent (delete'
    ' pool CR/nodepools and recreate), zero (keep pool, scale deployment'
    ' to 0 and wait for 0 nodes), or warm (scale deployment to 0 without'
    ' node drain wait).',
)
_SPREAD_MODE = flags.DEFINE_enum(
    'node_autoprovisioning_spread_mode',
    'anti_affinity',
    ['anti_affinity', 'tsc', 'none'],
    'Pod placement constraint across nodes: anti_affinity'
    ' (podAntiAffinity on kubernetes.io/hostname), tsc'
    ' (topologySpreadConstraints maxSkew=1), or none (pure resource'
    ' sizing / bin-packing).',
)
_POD_CPU_REQUEST = flags.DEFINE_string(
    'node_autoprovisioning_pod_cpu_request',
    '2000m',
    'CPU request for CPU workload pods.',
)
_POD_CPU_LIMIT = flags.DEFINE_string(
    'node_autoprovisioning_pod_cpu_limit',
    None,
    'CPU limit for CPU workload pods. Defaults to pod_cpu_request if unset.',
)
_POD_MEMORY_REQUEST = flags.DEFINE_string(
    'node_autoprovisioning_pod_memory_request',
    '8Gi',
    'Memory request for CPU workload pods.',
)
_POD_MEMORY_LIMIT = flags.DEFINE_string(
    'node_autoprovisioning_pod_memory_limit',
    None,
    'Memory limit for CPU workload pods. Defaults to pod_memory_request if'
    ' unset.',
)
_RUNTIME_CLASS = flags.DEFINE_string(
    'node_autoprovisioning_runtime_class',
    None,
    'Optional runtimeClassName (e.g., gvisor) for pods.',
)
_ARRIVAL_STAGGER_S = flags.DEFINE_float(
    'node_autoprovisioning_arrival_stagger_s',
    0.0,
    'Stagger delay in seconds between incremental pod arrivals when'
    ' scaling up.',
)
_CCC_PRIORITIES = flags.DEFINE_string(
    'node_autoprovisioning_ccc_priorities',
    None,
    'Optional JSON-encoded list of priority tier dicts for GKE'
    ' ComputeClass spec.priorities.',
)
_CCC_MIN_CORES = flags.DEFINE_integer(
    'node_autoprovisioning_ccc_min_cores',
    None,
    'Optional minCores floor for GKE ComputeClass default priority tier.',
)
_CCC_MACHINE_TYPE = flags.DEFINE_string(
    'node_autoprovisioning_ccc_machine_type',
    None,
    'Optional specific machineType for GKE ComputeClass default priority'
    ' tier (e.g. e2-standard-8). Overrides machineFamily.',
)
_CCC_MAX_PODS_PER_NODE = flags.DEFINE_integer(
    'node_autoprovisioning_ccc_max_pods_per_node',
    None,
    'Optional maxPodsPerNode limit for GKE ComputeClass default priority tier.',
)
_CCC_ACTIVE_MIGRATION = flags.DEFINE_boolean(
    'node_autoprovisioning_ccc_active_migration',
    False,
    'Whether to enable activeMigration.optimizeRulePriority in GKE'
    ' ComputeClass.',
)
_CCC_MAX_PARALLEL_IMAGE_PULLS = flags.DEFINE_integer(
    'node_autoprovisioning_ccc_max_parallel_image_pulls',
    0,
    'Optional kubelet maxParallelImagePulls setting via GKE ComputeClass'
    ' nodePoolConfig.nodeSystemConfig.',
)
_ALLOW_PARTIAL_ROLLOUT = flags.DEFINE_boolean(
    'node_autoprovisioning_allow_partial_rollout',
    False,
    'When True, if WaitForRollout times out (e.g. capacity stockout),'
    ' emit samples for the partially provisioned pods instead of failing.',
)


def _GetNodePoolManifestPath() -> str:
  """Returns the temp-dir path for the saved Karpenter NodePool manifest."""
  return vm_util.PrependTempDir(_NODEPOOL_MANIFEST_FILENAME)


def WaitForZeroWorkerNodes(
    worker_label: str, timeout_s: int, poll_s: int = 15
) -> None:
  """Blocks until no node matches worker_label."""
  start_time = time.time()

  @vm_util.Retry(
      timeout=timeout_s,
      poll_interval=poll_s,
      retryable_exceptions=(
          errors.Benchmarks.RunError,
          kubectl.RetryableKubectlError,
          errors.VmUtil.IssueCommandTimeoutError,
          errors.VmUtil.IssueCommandError,
      ),
      log_errors=False,
  )
  def _Wait() -> None:
    stdout, _, _ = kubectl.RunKubectlCommand(
        [
            'get',
            'nodes',
            '-l',
            worker_label,
            '-o',
            'jsonpath={.items[*].metadata.name}',
        ],
        suppress_logging=True,
    )
    if stdout.split():
      raise errors.Benchmarks.RunError(
          f'Still draining worker nodes; present: {stdout.split()[:10]}'
      )
    logging.info(
        'Cold-start confirmed: 0 worker nodes (%.1fs).',
        time.time() - start_time,
    )

  _Wait()


def ResetToColdState(
    worker_label: str,
    reset_pool_fn: Callable[[], None],
    timeout_s: int,
    poll_s: int = 15,
) -> None:
  """Scales to 0, drains, then deletes+recreates the benchmark node pool."""
  kubectl.RunKubectlCommand(['scale', '--replicas=0', _DEPLOYMENT])
  WaitForZeroWorkerNodes(worker_label, timeout_s, poll_s=poll_s)
  reset_pool_fn()


def MakeKarpenterPoolReset(
    nodepool_name: str, manifest_path: str
) -> Callable[[], None]:
  """Returns a callable that deletes then re-applies a Karpenter NodePool CR."""

  def _Reset() -> None:
    kubectl.RunKubectlCommand(
        [
            'delete',
            'nodepool.karpenter.sh',
            nodepool_name,
            '--ignore-not-found',
        ],
        raise_on_failure=False,
    )
    WaitForZeroWorkerNodes(
        f'pkb_nodepool={nodepool_name}', timeout_s=300, poll_s=10
    )
    kubectl.RunKubectlCommand(['apply', '-f', manifest_path])

  return _Reset


def _GetComputeClassPoolNames(class_name: str) -> list[str]:
  """Returns deduplicated GKE node pool names hosting compute-class nodes."""
  stdout, _, _ = kubectl.RunKubectlCommand(
      [
          'get',
          'nodes',
          '-l',
          f'cloud.google.com/compute-class={class_name}',
          '-o',
          (
              r'jsonpath={range'
              r' .items[*]}{.metadata.labels.cloud\.google\.com/gke-nodepool}{"\n"}{end}'
          ),
      ],
      suppress_logging=True,
  )
  return list(dict.fromkeys(n for n in stdout.splitlines() if n))


def _GetAllNapPoolNames() -> list[str]:
  """Returns all NAP-auto-provisioned pool names (nap-* prefix) from node labels."""
  stdout, _, _ = kubectl.RunKubectlCommand(
      [
          'get',
          'nodes',
          '-o',
          (
              r'jsonpath={range'
              r' .items[*]}{.metadata.labels.cloud\.google\.com/gke-nodepool}{"\n"}{end}'
          ),
      ],
      suppress_logging=True,
  )
  return list(
      dict.fromkeys(n for n in stdout.splitlines() if n.startswith('nap-'))
  )


def MakeGkeComputeClassPoolReset(
    cluster: Any, class_name: str = 'provisioning'
) -> Callable[[], None]:
  """Returns a callable that deletes GKE node pools for a ComputeClass and waits for 0 nodes."""

  def _Reset() -> None:
    cc_pools = _GetComputeClassPoolNames(class_name)
    nap_pools = _GetAllNapPoolNames()
    pools_to_delete = list(dict.fromkeys(cc_pools + nap_pools))
    if len(pools_to_delete) == 1:
      cluster.DeleteNodePool(pools_to_delete[0])
    elif len(pools_to_delete) > 1:
      background_tasks.RunThreaded(cluster.DeleteNodePool, pools_to_delete)
    WaitForZeroWorkerNodes(
        f'cloud.google.com/compute-class={class_name}', timeout_s=300, poll_s=15
    )

  return _Reset


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _ScenarioTag() -> str:
  return f'{_START_REPLICAS.value}to{_TARGET_REPLICAS.value}'


def CheckPrerequisites(bm_config: dict[str, Any]) -> None:
  del bm_config
  if _TARGET_REPLICAS.value <= _START_REPLICAS.value:
    raise errors.Config.InvalidValue(
        'node_autoprovisioning_target_replicas must be greater than'
        ' node_autoprovisioning_start_replicas.'
    )


def _EffectiveSpreadMode() -> str:
  """Returns the effective spread mode considering one_pod_per_node."""
  if not _ONE_POD_PER_NODE.value:
    return 'none'
  return _SPREAD_MODE.value


def _CommonMetadata() -> dict[str, Any]:
  """Returns common metadata dictionary attached to all emitted samples."""
  md: dict[str, Any] = {
      'platform': _PLATFORM.value,
      'workload': _WORKLOAD.value,
      'scenario': _ScenarioTag(),
      'start_replicas': _START_REPLICAS.value,
      'target_replicas': _TARGET_REPLICAS.value,
      'image': _IMAGE.value,
      'one_pod_per_node': _ONE_POD_PER_NODE.value,
      'pool_state': _POOL_STATE.value,
      'spread_mode': _EffectiveSpreadMode(),
      'pod_cpu_request': _POD_CPU_REQUEST.value,
      'pod_memory_request': _POD_MEMORY_REQUEST.value,
  }
  if _POD_CPU_LIMIT.value:
    md['pod_cpu_limit'] = _POD_CPU_LIMIT.value
  if _POD_MEMORY_LIMIT.value:
    md['pod_memory_limit'] = _POD_MEMORY_LIMIT.value
  if _RUNTIME_CLASS.value:
    md['runtime_class'] = _RUNTIME_CLASS.value
  if _ARRIVAL_STAGGER_S.value > 0:
    md['arrival_stagger_s'] = _ARRIVAL_STAGGER_S.value
  if _WORKLOAD.value == 'gpu':
    md['gpu_count'] = _GPU_COUNT.value
  return md


def CollectScaleSamples(
    cluster: kubernetes_cluster.KubernetesCluster,
    start_time: float,
    initial_nodes: set[str],
    initial_pods: set[str],
) -> list[sample.Sample]:
  """Collects pod/node status transition and resource count samples."""
  expected_new_pods = max(1, _TARGET_REPLICAS.value - _START_REPLICAS.value)
  pod_samples = kubernetes_scale_benchmark.ParseStatusChanges(
      'pod', start_time, resources_to_ignore=initial_pods
  )
  if _ALLOW_PARTIAL_ROLLOUT.value:
    if cluster.event_poller:
      cluster.event_poller.GetAndLogFailureEvents()
  else:
    kubernetes_scale_benchmark.CheckForFailures(
        cluster, pod_samples, expected_new_pods
    )
  node_samples = kubernetes_scale_benchmark.ParseStatusChanges(
      'node', start_time, resources_to_ignore=initial_nodes
  )
  count_samples = kubernetes_scale_benchmark.GetStartEndCountSamples(
      initial_nodes, initial_pods
  )
  samples = pod_samples + node_samples + count_samples
  common_md = _CommonMetadata()
  for s in samples:
    s.metadata.update(common_md)
  return samples


def _BuildGkePriorities() -> list[dict[str, Any]]:
  """Constructs the list of priority tier dicts for GKE ComputeClass."""
  if _CCC_PRIORITIES.value:
    return json.loads(_CCC_PRIORITIES.value)
  if _WORKLOAD.value == 'gpu':
    tier: dict[str, Any] = {
        'machine_family': 'g2',
        'gpu_type': 'nvidia-l4',
        'gpu_count': _GPU_COUNT.value,
    }
  else:
    tier = {}
    if _CCC_MACHINE_TYPE.value:
      tier['machine_type'] = _CCC_MACHINE_TYPE.value
    else:
      tier['machine_family'] = 'n4'
  if _CCC_MIN_CORES.value:
    tier['min_cores'] = _CCC_MIN_CORES.value
  if _CCC_MAX_PODS_PER_NODE.value:
    tier['max_pods_per_node'] = _CCC_MAX_PODS_PER_NODE.value
  return [tier]


def _ApplyGkeComputeClass(class_name: str = 'provisioning') -> None:
  """Renders and applies the benchmark-owned ComputeClass to the GKE cluster."""
  priorities = _BuildGkePriorities()
  yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
      'container/kubernetes_node_autoprovisioning/compute_class.yaml.j2',
      class_name=class_name,
      priorities=priorities,
      node_pool_auto_creation=True,
      active_migration=_CCC_ACTIVE_MIGRATION.value,
      max_parallel_image_pulls=_CCC_MAX_PARALLEL_IMAGE_PULLS.value,
  )
  list(kubernetes_commands.ApplyYaml(yaml_docs))
  logging.info(
      'Applied ComputeClass %r (priorities: %s)',
      class_name,
      priorities,
  )


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Applies the workload deployment; for 1->2 scenario waits for pre-placed pod."""
  bm_spec.always_call_cleanup = True
  cluster = cast(
      kubernetes_cluster.KubernetesCluster, bm_spec.container_cluster
  )
  if _PLATFORM.value == 'gke_ccc':
    _ApplyGkeComputeClass(class_name='provisioning')
  if _WORKLOAD.value == 'gpu':
    yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
        _GPU_MANIFEST,
        StartReplicas=_START_REPLICAS.value,
        Image=_IMAGE.value,
        GpuCount=_GPU_COUNT.value,
        OnePodPerNode=_ONE_POD_PER_NODE.value,
        SpreadMode=_EffectiveSpreadMode(),
    )
    gpu_pool = cluster.nodepools.get('provisioning')
    placement_machine_type = (
        gpu_pool.machine_type
        if gpu_pool
        else cluster.default_nodepool.machine_type
    )
    cluster.ModifyPodSpecPlacementYaml(
        yaml_docs, 'provisioning', placement_machine_type
    )
  else:
    yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
        _CPU_MANIFEST,
        StartReplicas=_START_REPLICAS.value,
        Image=_IMAGE.value,
        CpuRequest=_POD_CPU_REQUEST.value,
        MemoryRequest=_POD_MEMORY_REQUEST.value,
        CpuLimit=_POD_CPU_LIMIT.value or _POD_CPU_REQUEST.value,
        MemoryLimit=_POD_MEMORY_LIMIT.value or _POD_MEMORY_REQUEST.value,
        OnePodPerNode=_ONE_POD_PER_NODE.value,
        SpreadMode=_EffectiveSpreadMode(),
        RuntimeClassName=_RUNTIME_CLASS.value,
    )
    cluster.ModifyPodSpecPlacementYaml(
        yaml_docs, 'provisioning', cluster.default_nodepool.machine_type
    )
  if _PLATFORM.value == 'gke_ccc':
    for yaml_doc in yaml_docs:
      pod_spec = yaml_doc['spec']['template']['spec']
      pod_spec.setdefault('nodeSelector', {})[
          'cloud.google.com/compute-class'
      ] = 'provisioning'
  list(kubernetes_commands.ApplyYaml(yaml_docs))
  if _START_REPLICAS.value > 0:
    kubernetes_commands.WaitForRollout(_DEPLOYMENT, timeout=_RESET_TIMEOUT_S)
  if _PLATFORM.value == 'eks_karpenter':
    cluster.SaveNodePoolManifest('provisioning', _GetNodePoolManifestPath())


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Resets to target pool state (if 0-start), scales up, and returns samples."""
  cluster = cast(
      kubernetes_cluster.KubernetesCluster, bm_spec.container_cluster
  )
  if _PLATFORM.value == 'gke_ccc':
    reset_pool = MakeGkeComputeClassPoolReset(
        cluster, class_name='provisioning'
    )
  elif _PLATFORM.value in ('eks_auto', 'aks_nap'):
    reset_pool = lambda: None
  else:
    reset_pool = MakeKarpenterPoolReset(
        'provisioning', _GetNodePoolManifestPath()
    )

  if _START_REPLICAS.value == 0:
    kubectl.RunKubectlCommand(['scale', '--replicas=0', _DEPLOYMENT])
    if _POOL_STATE.value == 'absent':
      if _PLATFORM.value in ('eks_auto', 'aks_nap'):
        WaitForZeroWorkerNodes(_WORKER_LABEL.value, _RESET_TIMEOUT_S)
      reset_pool()
    elif _POOL_STATE.value == 'zero':
      WaitForZeroWorkerNodes(_WORKER_LABEL.value, _RESET_TIMEOUT_S)
    # For 'warm', skip deletion and zero-node drain wait.

  initial_nodes = kubernetes_commands.GetNodeNames()
  initial_pods = kubernetes_commands.GetPodNames()
  start_time = int(time.time())
  if (
      _ARRIVAL_STAGGER_S.value > 0
      and _TARGET_REPLICAS.value > _START_REPLICAS.value
  ):
    for r in range(_START_REPLICAS.value + 1, _TARGET_REPLICAS.value + 1):
      kubectl.RunKubectlCommand(['scale', f'--replicas={r}', _DEPLOYMENT])
      if r < _TARGET_REPLICAS.value:
        time.sleep(_ARRIVAL_STAGGER_S.value)
  else:
    kubectl.RunKubectlCommand(
        ['scale', f'--replicas={_TARGET_REPLICAS.value}', _DEPLOYMENT]
    )

  rollout_error: Exception | None = None
  try:
    kubernetes_commands.WaitForRollout(_DEPLOYMENT, timeout=_RESET_TIMEOUT_S)
  except (
      errors.VmUtil.IssueCommandError,
      errors.VmUtil.IssueCommandTimeoutError,
      vm_util.TimeoutExceededRetryError,
  ) as e:
    rollout_error = e
    logging.warning(
        'WaitForRollout timed out or failed (%s); inspecting status conditions'
        ' and cluster events.',
        e,
    )

  samples = CollectScaleSamples(
      cluster,
      start_time=start_time,
      initial_nodes=initial_nodes,
      initial_pods=initial_pods,
  )
  if rollout_error is not None and not _ALLOW_PARTIAL_ROLLOUT.value:
    raise rollout_error
  return samples


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Deletes the provisioning deployment and ComputeClass."""
  del bm_spec
  kubectl.RunKubectlCommand(
      ['delete', 'deployment', 'provisioning', '--ignore-not-found'],
      raise_on_failure=False,
  )
  if _PLATFORM.value == 'gke_ccc':
    kubectl.RunKubectlCommand(
        ['delete', 'computeclass', 'provisioning', '--ignore-not-found'],
        raise_on_failure=False,
    )
