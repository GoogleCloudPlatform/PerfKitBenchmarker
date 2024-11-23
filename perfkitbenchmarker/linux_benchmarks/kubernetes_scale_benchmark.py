"""Benchmark which runs spins up a large number of pods on kubernetes."""

import collections
import json
import time

from absl import flags
from absl import logging
from dateutil import parser
import numpy as np
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_scale'
BENCHMARK_CONFIG = """
kubernetes_scale:
  description: Scales up a large number of pods on kubernetes.
  container_specs:
    hello_world:
      image: hello-world
  container_cluster:
    cloud: GCP
    type: Kubernetes
    min_vm_count: 1
    max_vm_count: 100
    vm_spec: *default_dual_core
"""
# Don't define container_registry so no GetOrBuild happens


NUM_NEW_INSTANCES = flags.DEFINE_integer(
    'kubernetes_goal_replicas', 5, 'Number of new instances to create'
)

# kubectl pods don't allow _'s.
SPEC_NAME = 'hello_world'
CONTAINER_NAME = 'hello-world'


def GetConfig(user_config):
  """Load and return benchmark config."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec):
  """Provision container, because it's not handled by provision stage."""
  del bm_spec


def _GetRolloutCreationTime(rollout_name: str) -> int:
  """Returns the time when the rollout was created."""
  out, _, _ = container_service.RunKubectlCommand([
      'rollout',
      'history',
      rollout_name,
      '-o',
      'jsonpath={.metadata.creationTimestamp}',
  ])
  return ConvertToEpochTime(out)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Scales a large number of pods on kubernetes."""
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  assert isinstance(cluster, container_service.KubernetesCluster)

  samples, rollout_name = ScaleUpPods(cluster)
  start_time = _GetRolloutCreationTime(rollout_name)
  samples += ParseEvents(cluster, start_time)
  return samples


def ScaleUpPods(
    cluster: container_service.KubernetesCluster,
) -> tuple[list[sample.Sample], str]:
  """Scales up pods on a kubernetes cluster. Returns samples & rollout name."""
  samples = []
  initial_pods = set(cluster.GetAllPodNames())
  logging.info('Initial pods: %s', initial_pods)

  # Request X new pods via YAML apply.
  num_new_instances = NUM_NEW_INSTANCES.value
  rollout_name = cluster.ApplyManifest(
      'container/kubernetes_scale/kubernetes_scale.yaml.j2',
      Name='pkb123',
      Replicas=num_new_instances,
      CpuRequest='250m',
      MemoryRequest='250M',
      EphemeralStorageRequest='10Mi',
  )

  start_polling_time = time.monotonic()

  cluster.WaitForRollout(rollout_name)

  all_new_pods = set(cluster.GetAllPodNames()) - initial_pods
  if len(all_new_pods) < num_new_instances:
    raise errors.Benchmarks.RunError(
        'Failed to scale up to %d pods, only found %d.'
        % (num_new_instances, len(all_new_pods))
    )
  cluster.WaitForResource(
      'pod',
      condition_name='Ready',
      timeout=60 * 10,
      wait_for_all=True,
  )
  end_polling_time = time.monotonic()
  logging.info(
      'In %d seconds, found all new pods %s',
      end_polling_time - start_polling_time,
      all_new_pods,
  )
  samples.append(
      sample.Sample(
          'pod_polling_duration',
          end_polling_time - start_polling_time,
          'seconds',
      )
  )
  return samples, rollout_name


def _GetPodStatusConditions(pod_name: str) -> list[dict[str, str]]:
  """Returns the status conditions for a pod.

  Args:
    pod_name: The name of the pod to get the status conditions for.

  Returns:
    A list of status condition, where each condition is a dict with type &
    lastTransitionTime.
  """
  out, _, _ = container_service.RunKubectlCommand([
      'get',
      'pod',
      pod_name,
      '-o',
      'jsonpath={.status.conditions[*]}',
  ])
  # Turn space separated individual json objects into a single json array.
  conditions = '[' + out.replace('} {', '},{') + ']'
  return json.loads(conditions)


def ConvertToEpochTime(timestamp: str) -> int:
  """Converts a timestamp to epoch time."""
  # Example: 2024-11-08T23:44:36Z
  return parser.parse(timestamp).timestamp()


def ParseEvents(
    cluster: container_service.KubernetesCluster,
    start_time: float,
) -> list[sample.Sample]:
  """Parses events into samples."""
  all_pods = cluster.GetAllPodNames()
  overall_times = collections.defaultdict(list)
  for pod in all_pods:
    conditions = _GetPodStatusConditions(pod)
    for condition in conditions:
      str_time = condition['lastTransitionTime']
      overall_times[condition['type']].append(ConvertToEpochTime(str_time))

  samples = []
  for event, timestamps in overall_times.items():
    summaries = _SummarizeTimestamps(timestamps)
    prefix = f'pod_{event}_'
    for percentile, value in summaries.items():
      samples.append(
          sample.Sample(
              prefix + percentile,
              value - start_time,
              'seconds',
          )
      )
    samples.append(
        sample.Sample(
            prefix + 'count',
            len(timestamps),
            'count',
        )
    )
  return samples


def _SummarizeTimestamps(timestamps: list[float]) -> dict[str, float]:
  """Returns a few metrics about a list of timestamps."""
  return {
      'p50': np.percentile(timestamps, 50),
      'p90': np.percentile(timestamps, 90),
      'p99.9': np.percentile(timestamps, 99.9),
      'p10': np.percentile(timestamps, 10),
      'mean': np.mean(timestamps),
  }


def Cleanup(_):
  """Cleanup scale benchmark."""
  pass
