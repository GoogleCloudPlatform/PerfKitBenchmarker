"""Benchmark which scales up to a number of nodes, then down, then back up.

Similar to kubernetes_scale, but only cares about node scaling & has additional
scaling up/down steps.
"""


from absl import flags
from absl import logging
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_scale_benchmark

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


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Scales a large number of pods on kubernetes."""
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  assert isinstance(cluster, container_service.KubernetesCluster)
  cluster: container_service.KubernetesCluster = cluster

  # Warm up the cluster by creating a single pod. This compensates for
  # differences between Standard & Autopilot, where Standard already has 1 node
  # due to its starting nodepool but Autopilot does not.
  scale_one_samples, _ = kubernetes_scale_benchmark.ScaleUpPods(cluster, 1)
  if not scale_one_samples:
    logging.exception(
        'Failed to scale up to 1 pod; now investigating failure reasons.'
    )
    unused = 0
    pod_samples = kubernetes_scale_benchmark.ParseStatusChanges('pod', unused)
    # Log & check for quota failure.
    kubernetes_scale_benchmark.CheckForFailures(cluster, pod_samples, 1)

  # Do one scale up, scale down, then scale up again.
  return []


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec):
  """Cleanups scale benchmark. Runs before teardown."""
  # Might need to change if kubernetes-scaleup deployment not used.
  kubernetes_scale_benchmark.Cleanup(bm_spec)
