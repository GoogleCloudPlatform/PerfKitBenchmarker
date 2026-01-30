"""Benchmark for Kubernetes node auto-scaling: scale up, down, then up again.

Deploys a Deployment with pod anti-affinity to force one pod per node, then
measures node provisioning and de-provisioning times across three phases:

  1. Scale up to NUM_NODES replicas.
  2. Scale down to 0 replicas and wait for nodes to be removed.
  3. Scale up to NUM_NODES replicas again.

Reuses ParseStatusChanges and CheckForFailures from kubernetes_scale_benchmark
for consistent metric collection.
"""

from absl import flags
from absl import logging
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_scale_benchmark
from perfkitbenchmarker.container_service import kubernetes_commands

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_node_scale'
BENCHMARK_CONFIG = """
kubernetes_node_scale:
  description: Test scaling an auto-scaled Kubernetes cluster by adding pods.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    min_vm_count: 1
    max_vm_count: 100
    vm_spec: *default_dual_core
    poll_for_events: true
"""

NUM_NODES = flags.DEFINE_integer(
    'kubernetes_scale_num_nodes', 5, 'Number of new nodes to create'
)

MANIFEST_TEMPLATE = 'container/kubernetes_scale/kubernetes_node_scale.yaml.j2'


def CheckPrerequisites(_):
  """Validates flags and config."""


def GetConfig(user_config):
  """Loads and returns benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec):
  """Applies the app deployment manifest with 0 replicas."""
  bm_spec.always_call_cleanup = True
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  manifest_kwargs = dict(
      cloud=FLAGS.cloud,
  )

  yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
      MANIFEST_TEMPLATE,
      **manifest_kwargs,
  )
  cluster.ModifyPodSpecPlacementYaml(
      yaml_docs,
      'app',
      cluster.default_nodepool.machine_type,
  )
  list(kubernetes_commands.ApplyYaml(yaml_docs))


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the scale-up, scale-down, scale-up benchmark sequence.

  Args:
    bm_spec: The benchmark specification.

  Returns:
    Combined samples from all three phases, each tagged with phase metadata.
  """
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  assert isinstance(cluster, container_service.KubernetesCluster)
  cluster: container_service.KubernetesCluster = cluster

  # Do one scale up, scale down, then scale up again.
  _ScaleDeploymentReplicas(NUM_NODES.value)
  _ScaleDeploymentReplicas(0)
  _ScaleDeploymentReplicas(NUM_NODES.value)
  return []


def _ScaleDeploymentReplicas(replicas: int) -> None:
  container_service.RunKubectlCommand([
      'scale',
      f'--replicas={replicas}',
      'deployment/app',
  ])
  kubernetes_commands.WaitForRollout(
      'deployment/app',
      timeout=kubernetes_scale_benchmark._GetScaleTimeout(),
  )


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec):
  """Cleanups scale benchmark. Runs before teardown."""
  container_service.RunRetryableKubectlCommand(
      ['delete', 'deployment', 'app'],
      timeout=kubernetes_scale_benchmark._GetScaleTimeout(),
      raise_on_failure=False,
  )
