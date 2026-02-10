"""Benchmark which scales up to a number of nodes, then down, then back up.

Similar to kubernetes_scale, but only cares about node scaling & has additional
scaling up/down steps.
"""

import json
import time
from typing import Any

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

  initial_nodes = set(kubernetes_commands.GetNodeNames())
  initial_node_count = len(initial_nodes)

  # Do one scale up, scale down, then scale up again.
  scaleup1_samples = _ScaleUpAndCollectSamples(
      phase='scaleup1',
      replicas=NUM_NODES.value,
      cluster=cluster,
      initial_nodes=initial_nodes,
      initial_pods=set(kubernetes_commands.GetPodNames()),
      pod_phase_timeout=3 * 60 * 60,
  )

  scaledown_samples, scaledown_complete = _ScaleDownAndCollectSamples(
      phase='scaledown',
      initial_nodes=initial_nodes,
      initial_node_count=initial_node_count,
      node_timeout=2 * 60 * 60,
  )

  scaleup2_samples: list[sample.Sample] = []
  if scaledown_complete:
    scaleup2_samples = _ScaleUpAndCollectSamples(
        phase='scaleup2',
        replicas=NUM_NODES.value,
        cluster=cluster,
        initial_nodes=initial_nodes,
        initial_pods=set(kubernetes_commands.GetPodNames()),
        pod_phase_timeout=3 * 60 * 60,
    )
  else:
    logging.warning(
        'Skipping final scale up; scaled nodes not deleted within timeout.'
    )

  return scaleup1_samples + scaledown_samples + scaleup2_samples


def _ScaleDeploymentReplicas(replicas: int, wait_for_rollout: bool = True) -> None:
  container_service.RunKubectlCommand([
      'scale',
      f'--replicas={replicas}',
      'deployment/app',
  ])
  if wait_for_rollout:
    kubernetes_commands.WaitForRollout(
        'deployment/app',
        timeout=GetScaleTimeout(replicas),
    )


def _ScaleUpAndCollectSamples(
    phase: str,
    replicas: int,
    cluster: container_service.KubernetesCluster,
    initial_nodes: set[str],
    initial_pods: set[str],
    pod_phase_timeout: int,
) -> list[sample.Sample]:
  start_time = time.time()
  _ScaleDeploymentReplicas(replicas, wait_for_rollout=False)

  phase_samples: list[sample.Sample] = []
  phase_samples += _LogPodPhaseCountsUntilReady(
      phase=phase,
      timeout=pod_phase_timeout,
      desired_replicas=replicas,
  )

  ready_samples = kubernetes_scale_benchmark.ParseStatusChanges(
      'pod',
      start_time,
      resources_to_ignore=initial_pods,
  )
  kubernetes_scale_benchmark.CheckForFailures(cluster, ready_samples, replicas)
  _AddPhaseMetadata(ready_samples, phase)
  phase_samples += ready_samples
  node_samples = kubernetes_scale_benchmark.ParseStatusChanges(
      'node',
      start_time,
      resources_to_ignore=initial_nodes,
  )
  _AddPhaseMetadata(node_samples, phase)
  phase_samples += node_samples
  return phase_samples


def _ScaleDownAndCollectSamples(
    phase: str,
    initial_nodes: set[str],
    initial_node_count: int,
    node_timeout: int,
) -> tuple[list[sample.Sample], bool]:
  start_time = time.time()
  _ScaleDeploymentReplicas(0, wait_for_rollout=False)
  return _LogNodeDeletionUntilGone(
      phase=phase,
      initial_nodes=initial_nodes,
      initial_node_count=initial_node_count,
      start_time=start_time,
      timeout=node_timeout,
  )


def _LogPodPhaseCountsUntilReady(
    phase: str,
    timeout: int,
    desired_replicas: int,
) -> list[sample.Sample]:
  samples: list[sample.Sample] = []
  start_monotonic = time.monotonic()
  while True:
    phase_counts, ready_count = _GetPodPhaseCounts()
    elapsed = time.monotonic() - start_monotonic
    logging.info(
        'Pod phases (%s) after %ds: %s (Ready: %d/%d)',
        phase,
        int(elapsed),
        phase_counts,
        ready_count,
        desired_replicas,
    )
    for pod_phase, count in phase_counts.items():
      samples.append(
          sample.Sample(
              'pod_phase_count',
              count,
              'count',
              metadata={
                  'phase': phase,
                  'pod_phase': pod_phase,
                  'elapsed_seconds': elapsed,
              },
          )
      )
    samples.append(
        sample.Sample(
            'pod_phase_count',
            ready_count,
            'count',
            metadata={
                'phase': phase,
                'pod_phase': 'Ready',
                'elapsed_seconds': elapsed,
            },
        )
    )

    if ready_count >= desired_replicas:
      return samples
    if elapsed >= timeout:
      logging.warning(
          'Timed out waiting for pods to be Ready (%s). Ready: %d/%d',
          phase,
          ready_count,
          desired_replicas,
      )
      return samples
    time.sleep(60)


def _GetPodPhaseCounts() -> tuple[dict[str, int], int]:
  stdout, _, _ = container_service.RunKubectlCommand(
      [
          'get',
          'pods',
          '-o',
          'jsonpath={.items[*].metadata.name}',
      ],
      suppress_logging=True,
  )
  pod_names = stdout.split()
  pod_list: list[dict[str, Any]] = []
  for pod_name in pod_names:
    pod_stdout, _, _ = container_service.RunKubectlCommand(
        [
            'get',
            'pod',
            pod_name,
            '-o',
            'json',
        ],
        suppress_logging=True,
    )
    pod_list.append(json.loads(pod_stdout))
  phase_counts: dict[str, int] = {}
  ready_count = 0
  for pod in pod_list:
    labels = pod.get('metadata', {}).get('labels', {})
    if labels.get('app') != 'app':
      continue
    phase = pod.get('status', {}).get('phase', 'Unknown')
    phase_counts[phase] = phase_counts.get(phase, 0) + 1
    if _IsPodReady(pod):
      ready_count += 1
  return phase_counts, ready_count


def _IsPodReady(pod: dict[str, Any]) -> bool:
  for condition in pod.get('status', {}).get('conditions', []):
    if condition.get('type') == 'Ready' and condition.get('status') == 'True':
      return True
  return False


def _LogNodeDeletionUntilGone(
    phase: str,
    initial_nodes: set[str],
    initial_node_count: int,
    start_time: float,
    timeout: int,
) -> tuple[list[sample.Sample], bool]:
  samples: list[sample.Sample] = []
  deletion_times: dict[str, float] = {}
  scaled_nodes = set(kubernetes_commands.GetNodeNames()) - initial_nodes
  start_monotonic = time.monotonic()

  while True:
    current_nodes = set(kubernetes_commands.GetNodeNames())
    remaining_nodes = current_nodes - initial_nodes
    elapsed = time.monotonic() - start_monotonic

    for node in list(scaled_nodes):
      if node not in current_nodes and node not in deletion_times:
        deletion_times[node] = elapsed
        scaled_nodes.discard(node)

    samples.append(
        sample.Sample(
            'node_remaining_count',
            max(len(remaining_nodes), 0),
            'count',
            metadata={
                'phase': phase,
                'elapsed_seconds': elapsed,
            },
        )
    )

    if len(current_nodes) <= initial_node_count:
      logging.info('Node count returned to initial level.')
      break
    if elapsed >= timeout:
      logging.warning(
          'Timed out waiting for scaled nodes to delete. Remaining nodes: %d',
          max(len(remaining_nodes), 0),
      )
      break

    logging.info(
        'Remaining scaled nodes: %d',
        max(len(remaining_nodes), 0),
    )
    time.sleep(60)

  delete_samples = _BuildNodeDeletionSamples(
      deletion_times,
      phase,
      start_time,
  )
  samples += delete_samples
  return samples, len(current_nodes) <= initial_node_count


def _BuildNodeDeletionSamples(
    deletion_times: dict[str, float],
    phase: str,
    start_time: float,
) -> list[sample.Sample]:
  if not deletion_times:
    return []
  summaries = kubernetes_scale_benchmark._SummarizeTimestamps(  # pylint: disable=protected-access
      list(deletion_times.values())
  )
  percentiles = {'p50', 'p90', 'p99', 'p99.9', 'p100'}
  samples: list[sample.Sample] = []
  for percentile, value in summaries.items():
    if percentile not in percentiles:
      continue
    samples.append(
        sample.Sample(
            f'node_delete_{percentile}',
            value,
            'seconds',
            metadata={
                'phase': phase,
                'start_time_epoch': start_time,
            },
        )
    )
  return samples


def GetScaleTimeout(num_nodes: int | None = None) -> int:
  """Returns the timeout for scale operations in this benchmark."""
  nodes = num_nodes if num_nodes is not None else NUM_NODES.value
  base_timeout = 60 * 10  # 10 minutes
  per_node_timeout = nodes * 3  # 3 seconds per node
  proposed_timeout = base_timeout + per_node_timeout
  max_timeout = 60 * 60  # 1 hour
  return min(proposed_timeout, max_timeout)


def _AddPhaseMetadata(samples: list[sample.Sample], phase: str) -> None:
  for s in samples:
    s.metadata['phase'] = phase


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec):
  """Cleanups scale benchmark. Runs before teardown."""
  container_service.RunRetryableKubectlCommand(
      ['delete', 'deployment', 'app'],
      timeout=GetScaleTimeout(),
      raise_on_failure=False,
  )
