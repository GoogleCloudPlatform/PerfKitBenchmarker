"""Benchmark which runs spins up a large number of pods on kubernetes."""

import collections
from collections import abc
import dataclasses
import json
import time
from typing import Any

from absl import flags
from absl import logging
from dateutil import parser
import numpy as np
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_scale'
BENCHMARK_CONFIG = """
kubernetes_scale:
  description: Test scaling an auto-scaled Kubernetes cluster by adding pods.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    min_vm_count: 1
    max_vm_count: 100
    vm_spec: *default_dual_core
    poll_for_events: true
"""

NUM_PODS = flags.DEFINE_integer(
    'kubernetes_scale_num_replicas', 5, 'Number of new instances to create'
)
REPORT_PERCENTILES = flags.DEFINE_boolean(
    'kubernetes_scale_report_latency_percentiles',
    True,
    'Whether to report percentiles of event latencies',
)
REPORT_LATENCIES = flags.DEFINE_boolean(
    'kubernetes_scale_report_individual_latencies',
    False,
    'Whether to report individual event latencies',
)
CPUS_PER_POD = flags.DEFINE_string(
    'kubernetes_scale_pod_cpus', '250m', 'CPU limit per pod'
)
MEMORY_PER_POD = flags.DEFINE_string(
    'kubernetes_scale_pod_memory', '250M', 'Memory limit per pod'
)
CONTAINER_IMAGE = flags.DEFINE_string(
    'kubernetes_scale_container_image',
    None,
    'The container image to use for the Kubernetes scale benchmark.'
    'If not specified, the default image will be used.',
)

MANIFEST_TEMPLATE = 'container/kubernetes_scale/kubernetes_scale.yaml.j2'
DEFAULT_IMAGE = 'k8s.gcr.io/pause:3.1'
NVIDIA_GPU_IMAGE = 'nvidia/cuda:11.0.3-runtime-ubuntu20.04'


def _GetImage() -> str:
  """Get the image for the scale deployment."""
  if virtual_machine.GPU_COUNT.value:
    return NVIDIA_GPU_IMAGE
  if CONTAINER_IMAGE.value:
    return CONTAINER_IMAGE.value
  return DEFAULT_IMAGE


def CheckPrerequisites(_):
  """Validate flags and config."""
  if not REPORT_PERCENTILES.value and not REPORT_LATENCIES.value:
    raise errors.Config.InvalidValue(
        'At least one of percentiles or individual latencies must be reported.'
    )


def GetConfig(user_config):
  """Loads and returns benchmark config."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec):
  """Sets additional spec attributes."""
  bm_spec.always_call_cleanup = True


def _GetRolloutCreationTime(rollout_name: str) -> int:
  """Returns the time when the rollout was created."""
  out, _, _ = container_service.RunRetryableKubectlCommand([
      'rollout',
      'history',
      rollout_name,
      '-o',
      'jsonpath={.metadata.creationTimestamp}',
  ])
  return ConvertToEpochTime(out)


def _GetScaleTimeout() -> int:
  """Returns the timeout for the scale up & teardown."""
  base_timeout = 60 * 10  # 10 minutes
  per_pod_timeout = NUM_PODS.value * 3  # 3 seconds per pod
  if virtual_machine.GPU_COUNT.value:
    base_timeout = 60 * 30  # 30 minutes
  proposed_timeout = base_timeout + per_pod_timeout
  max_timeout = 2 * 60 * 60  # 2 hours
  return min(proposed_timeout, max_timeout)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Scales a large number of pods on kubernetes."""
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  assert isinstance(cluster, container_service.KubernetesCluster)

  initial_nodes = set(cluster.GetNodeNames())

  samples, rollout_name = ScaleUpPods(cluster)
  start_time = _GetRolloutCreationTime(rollout_name)
  pod_samples = ParseStatusChanges(cluster, 'pod', start_time)
  samples += pod_samples
  _CheckForFailures(cluster, pod_samples)
  samples += ParseStatusChanges(
      cluster, 'node', start_time, resources_to_ignore=initial_nodes
  )
  metadata = {
      'pod_memory': MEMORY_PER_POD.value,
      'pod_cpu': CPUS_PER_POD.value,
      'goal_replicas': NUM_PODS.value,
      'image': _GetImage(),
  }
  if virtual_machine.GPU_COUNT.value:
    metadata['gpu_count'] = virtual_machine.GPU_COUNT.value
    metadata['gpu_type'] = virtual_machine.GPU_TYPE.value
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

  command = None
  if virtual_machine.GPU_COUNT.value:
    # Use nvidia-smi to validate NVIDIA_GPU is available.
    command = ['sh', '-c', 'nvidia-smi && sleep 3600']
  if CONTAINER_IMAGE.value:
    command = ['sh', '-c', 'sleep infinity']

  # Request X new pods via YAML apply.
  num_new_pods = NUM_PODS.value
  max_wait_time = _GetScaleTimeout()
  resource_names = cluster.ApplyManifest(
      MANIFEST_TEMPLATE,
      Name='kubernetes-scaleup',
      Replicas=num_new_pods,
      CpuRequest=CPUS_PER_POD.value,
      MemoryRequest=MEMORY_PER_POD.value,
      NvidiaGpuRequest=virtual_machine.GPU_COUNT.value,
      Image=_GetImage(),
      Command=command,
      EphemeralStorageRequest='10Mi',
      RolloutTimeout=max_wait_time,
      PodTimeout=max_wait_time + 60,
      NodeSelectors=cluster.GetNodeSelectors(
          cluster.default_nodepool.machine_type
      ),
  )

  # Arbitrarily pick the first resource (it should be the only one.)
  assert resource_names
  rollout_name = next(resource_names)

  try:
    start_polling_time = time.monotonic()
    cluster.WaitForRollout(rollout_name, timeout=max_wait_time)

    all_new_pods = set(cluster.GetPodNames()) - initial_pods
    cluster.WaitForResource(
        'pod',
        condition_name='Ready',
        timeout=max_wait_time,
        wait_for_all=True,
        namespace='default',
    )
    end_polling_time = time.monotonic()
    logging.info(
        'In %d seconds, found all %s new pods',
        end_polling_time - start_polling_time,
        len(all_new_pods),
    )
    samples.append(
        sample.Sample(
            'pod_polling_duration',
            end_polling_time - start_polling_time,
            'seconds',
        )
    )
    return samples, rollout_name
  except (
      errors.VmUtil.IssueCommandError,
      errors.VmUtil.IssueCommandTimeoutError,
      vm_util.TimeoutExceededRetryError,
  ) as e:
    logging.warning(
        'Kubernetes failed to wait for all the rollout and/or all pods to be'
        ' ready, even with retries. Full error: %s. Continuing for now. Failure'
        ' will be checked later by number of pods with ready events.',
        e,
    )
    return [], rollout_name


def _CheckForFailures(
    cluster: container_service.KubernetesCluster,
    pod_samples: list[sample.Sample],
):
  """Fails the benchmark if not enough pods were created.

  Args:
    cluster: The cluster to check for failures on.
    pod_samples: The samples from pod transition times which includes pod Ready
      count.

  Raises:
    QuotaFailure: If a quota is exceeded.
    RunError: If scale up failed for a non-quota reason.
  """
  events = cluster.GetEvents()
  ready_count_sample = next(
      (s for s in pod_samples if s.metric == 'pod_Ready_count'), None
  )
  if (
      ready_count_sample is not None
      and ready_count_sample.value >= NUM_PODS.value
  ):
    logging.info(
        'Benchmark successfully scaled up %d pods, which is equal to or more '
        'than the goal of %d pods.',
        ready_count_sample.value,
        NUM_PODS.value,
    )
    return
  for event in events:
    if event.reason == 'FailedScaleUp' and 'quota exceeded' in event.message:
      raise errors.Benchmarks.QuotaFailure(
          'Failed to scale up to %d pods, at least one pod ran into a quota'
          ' error: %s' % (NUM_PODS.value, event.message)
      )
  if ready_count_sample is None:
    raise errors.Benchmarks.RunError(
        'No pod ready events were found & we attempted to scale up to'
        f' {NUM_PODS.value} pods; this is unusual.'
    )

  raise errors.Benchmarks.RunError(
      'Benchmark attempted to scale up to  %d pods but only %d pods were'
      ' created & ready. Check above "Kubernetes failed to wait for" logs for'
      ' exact failure location.' % (NUM_PODS.value, ready_count_sample.value)
  )


@dataclasses.dataclass
class KubernetesResourceStatusCondition:
  """Stores the information of a Kubernetes resource status condition."""

  resource_type: str
  resource_name: str
  epoch_time: int
  event: str

  @classmethod
  def FromJsonPathResult(
      cls, resource_type: str, resource_name: str, condition: dict[str, Any]
  ) -> 'KubernetesResourceStatusCondition':
    """Parses the json result of kubectl get."""
    str_time = condition['lastTransitionTime']
    return cls(
        resource_type,
        resource_name,
        epoch_time=ConvertToEpochTime(str_time),
        event=condition['type'],
    )


def _GetResourceStatusConditions(
    resource_type: str, resource_name: str
) -> list[KubernetesResourceStatusCondition]:
  """Returns the status conditions for a resource.

  Args:
    resource_type: The type of the resource to get the status conditions for.
    resource_name: The name of the resource to get the status conditions for.
      Should not have a prefix (eg pods/).

  Returns:
    A list of status condition, where each condition is a dict with type &
    lastTransitionTime.
  """
  out, _, _ = container_service.RunRetryableKubectlCommand(
      [
          'get',
          resource_type,
          resource_name,
          '-o',
          'jsonpath={.status.conditions[*]}',
      ],
      timeout=60 * 2,
  )  # 2 minutes; should be a pretty fast call.
  # Turn space separated individual json objects into a single json array.
  conditions_str = '[' + out.replace('} {', '},{') + ']'
  conditions = json.loads(conditions_str)
  return [
      KubernetesResourceStatusCondition.FromJsonPathResult(
          resource_type, resource_name, condition
      )
      for condition in conditions
  ]


def ConvertToEpochTime(timestamp: str) -> int:
  """Converts a timestamp to epoch time."""
  # Example: 2024-11-08T23:44:36Z
  return parser.parse(timestamp).timestamp()


def ParseStatusChanges(
    cluster: container_service.KubernetesCluster,
    resource_type: str,
    start_time: float,
    resources_to_ignore: abc.Set[str] = frozenset(),
) -> list[sample.Sample]:
  """Parses status transitions into samples.

  Status transitions are pulled from the cluster, and include e.g. when a pod
  transitions to PodScheduled or Ready. See tests for more example input/output.

  Args:
    cluster: The cluster to parse the status changes for.
    resource_type: The type of the resource to parse the status changes for
      (node or pod).
    start_time: The start time of the scale up operation, subtracted from
      timestamps.
    resources_to_ignore: A set of resource names to ignore.

  Returns:
    A list of samples, with various percentiles for each status condition.
  """
  all_resources = set(cluster.GetAllNamesForResourceType(resource_type + 's'))
  all_resources -= resources_to_ignore
  conditions = []
  for resource in sorted(all_resources):
    conditions += _GetResourceStatusConditions(resource_type, resource)

  samples = []
  overall_times = collections.defaultdict(list)
  for condition in conditions:
    overall_times[condition.event].append(condition.epoch_time)
  for event, timestamps in overall_times.items():
    summaries = _SummarizeTimestamps(timestamps)
    prefix = f'{resource_type}_{event}_'
    if REPORT_PERCENTILES.value:
      for percentile, value in summaries.items():
        samples.append(
            sample.Sample(
                prefix + percentile,
                value - start_time,
                'seconds',
            )
        )
    # Always report counts, because it is used in failure handling
    samples.append(
        sample.Sample(
            prefix + 'count',
            len(timestamps),
            'count',
        )
    )

    if REPORT_LATENCIES.value:
      for condition in conditions:
        metadata = {
            'k8s_resource_name': condition.resource_name,
        }
        samples.append(
            sample.Sample(
                f'{resource_type}_{condition.event}',
                condition.epoch_time - start_time,
                'seconds',
                metadata,
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
  container_service.RunKubectlCommand(['get', 'deployments'])
  container_service.RunRetryableKubectlCommand(
      ['delete', 'deployment', 'kubernetes-scaleup'],
      timeout=_GetScaleTimeout(),
      raise_on_failure=False,
  )
  container_service.RunRetryableKubectlCommand(
      ['delete', '--all', 'pods', '-n', 'default'], timeout=_GetScaleTimeout()
  )
