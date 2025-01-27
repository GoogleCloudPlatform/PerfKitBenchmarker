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
  """Loads and returns benchmark config."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec):
  """Sets additional spec attributes."""
  bm_spec.always_call_cleanup = True


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


def _GetScaleTimeout() -> int:
  """Returns the timeout for the scale up & teardown."""
  base_timeout = 60 * 10
  per_pod_timeout = NUM_NEW_INSTANCES.value * 3
  proposed_timeout = base_timeout + per_pod_timeout
  max_timeout = 2 * 60 * 60  # 2 hours
  return min(proposed_timeout, max_timeout)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Scales a large number of pods on kubernetes."""
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  assert isinstance(cluster, container_service.KubernetesCluster)

  samples, rollout_name = ScaleUpPods(cluster)
  start_time = _GetRolloutCreationTime(rollout_name)
  samples += ParseEvents(cluster, 'pod', start_time)
  samples += ParseEvents(cluster, 'node', start_time)
  metadata = {'goal_replicas': NUM_NEW_INSTANCES.value}
  for s in samples:
    s.metadata.update(metadata)
  return samples


def ScaleUpPods(
    cluster: container_service.KubernetesCluster,
) -> tuple[list[sample.Sample], str]:
  """Scales up pods on a kubernetes cluster. Returns samples & rollout name."""
  samples = []
  initial_pods = set(cluster.GetPodNames())
  logging.info('Initial pods: %s', initial_pods)

  # Request X new pods via YAML apply.
  num_new_instances = NUM_NEW_INSTANCES.value
  max_wait_time = _GetScaleTimeout()
  resource_names = cluster.ApplyManifest(
      'container/kubernetes_scale/kubernetes_scale.yaml.j2',
      Name='kubernetes-scaleup',
      Replicas=num_new_instances,
      CpuRequest='250m',
      MemoryRequest='250M',
      EphemeralStorageRequest='10Mi',
      RolloutTimeout=max_wait_time,
      PodTimeout=max_wait_time + 60,
  )

  # Arbitrarily pick the first resource (it should be the only one.)
  assert resource_names
  rollout_name = next(resource_names)

  start_polling_time = time.monotonic()
  cluster.WaitForRollout(rollout_name, timeout=max_wait_time)

  all_new_pods = set(cluster.GetPodNames()) - initial_pods
  if len(all_new_pods) < num_new_instances:
    raise errors.Benchmarks.RunError(
        'Failed to scale up to %d pods, only found %d.'
        % (num_new_instances, len(all_new_pods))
    )
  try:
    cluster.WaitForResource(
        'pod',
        condition_name='Ready',
        timeout=max_wait_time,
        wait_for_all=True,
    )
  except errors.VmUtil.IssueCommandError as e:
    logging.info(
        'Failed to wait for all pods to be ready, even with retries: %s.'
        ' Failure will be checked later by number of pods with ready events.',
        e,
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


def _GetResourceStatusConditions(
    resource_type: str, resource_name: str
) -> list[dict[str, str]]:
  """Returns the status conditions for a resource.

  Args:
    resource_type: The type of the resource to get the status conditions for.
    resource_name: The name of the resource to get the status conditions for.
      Should not have a prefix (eg pods/).

  Returns:
    A list of status condition, where each condition is a dict with type &
    lastTransitionTime.
  """
  out, _, _ = container_service.RunKubectlCommand([
      'get',
      resource_type,
      resource_name,
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
    resource_type: str,
    start_time: float,
) -> list[sample.Sample]:
  """Parses events into samples."""
  all_resources = cluster.GetAllNamesForResourceType(resource_type + 's')
  overall_times = collections.defaultdict(list)
  for resource in all_resources:
    conditions = _GetResourceStatusConditions(resource_type, resource)
    for condition in conditions:
      str_time = condition['lastTransitionTime']
      overall_times[condition['type']].append(ConvertToEpochTime(str_time))

  samples = []
  for event, timestamps in overall_times.items():
    summaries = _SummarizeTimestamps(timestamps)
    prefix = f'{resource_type}_{event}_'
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
  percentiles = [0, 10, 50, 90, 95, 99.9, 100]
  summary = {
      'mean': np.mean(timestamps),
  }
  for percentile in percentiles:
    summary[f'p{percentile}'] = np.percentile(timestamps, percentile)
  return summary


def Cleanup(_):
  """Cleanups scale benchmark. Runs before teardown."""
  container_service.RunRetryableKubectlCommand(
      ['delete', '--all', 'pods', '-n', 'default'], timeout=_GetScaleTimeout()
  )
