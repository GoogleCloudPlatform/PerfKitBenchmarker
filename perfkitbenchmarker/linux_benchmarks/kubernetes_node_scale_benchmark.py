"""Benchmark which scales up to a number of nodes, then down, then back up.

Similar to kubernetes_scale, but only cares about node scaling & has additional
scaling up/down steps.
"""

import time

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
  """Validate flags and config."""
  pass


def GetConfig(user_config):
  """Loads and returns benchmark config."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec):
  """Sets additional spec attributes."""
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
  """Scales a large number of pods on kubernetes."""
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  assert isinstance(cluster, container_service.KubernetesCluster)
  cluster: container_service.KubernetesCluster = cluster

  initial_node_count = len(kubernetes_commands.GetNodeNames())
  start_time = time.time()

  # Do one scale up, scale down, then scale up again.
  _ScaleDeploymentReplicas(NUM_NODES.value)
  samples = kubernetes_scale_benchmark.ParseStatusChanges(
      'node',
      start_time,
      resources_to_ignore=set(),
  )
  _ScaleDeploymentReplicas(0)
  if _WaitForScaledNodesDeletion(initial_node_count):
    _ScaleDeploymentReplicas(NUM_NODES.value)
  else:
    logging.warning(
        'Skipping final scale up; scaled nodes not deleted within timeout.'
    )
  return samples


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


def _WaitForScaledNodesDeletion(initial_node_count: int) -> bool:
  timeout = 20 * 60 + kubernetes_scale_benchmark._GetScaleTimeout()
  start_time = time.monotonic()
  while True:
    current_node_count = len(kubernetes_commands.GetNodeNames())
    if current_node_count <= initial_node_count:
      logging.info('Node count returned to initial level.')
      return True
    elapsed = time.monotonic() - start_time
    if elapsed >= timeout:
      logging.warning(
          'Timed out waiting for scaled nodes to delete. Remaining nodes: %d',
          max(current_node_count - initial_node_count, 0),
      )
      return False
    logging.info(
        'Remaining scaled nodes: %d',
        max(current_node_count - initial_node_count, 0),
    )
    time.sleep(60)


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec):
  """Cleanups scale benchmark. Runs before teardown."""
  container_service.RunRetryableKubectlCommand(
      ['delete', 'deployment', 'app'],
      timeout=kubernetes_scale_benchmark._GetScaleTimeout(),
      raise_on_failure=False,
  )
