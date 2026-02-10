"""Benchmark for Kubernetes node auto-scaling: scale up, down, then up again.

Deploys a Deployment with pod anti-affinity to force one pod per node, then
measures node provisioning and de-provisioning times across three phases:

  1. Scale up to NUM_NODES replicas.
  2. Scale down to 0 replicas and wait for nodes to be removed.
  3. Scale up to NUM_NODES replicas again.

Reuses ParseStatusChanges and CheckForFailures from kubernetes_scale_benchmark
for consistent metric collection.
"""

<<<<<<< HEAD
<<<<<<< HEAD
import json
import time
from typing import Any
=======
import time
>>>>>>> 9a938ee8 (Add scaling down logic and gathering metrics)
=======
import json
import time
from typing import Any
>>>>>>> 9fbbc449 (Add scaling down logic, phases and gathering metrics)

from absl import flags
from absl import logging
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.container_service import kubernetes_commands
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

# Allow this many extra nodes above the baseline when checking whether
# scale-down is complete.  The cluster autoscaler can be conservative and
# keep nodes around for system workloads (e.g. metrics-server).
_SCALE_DOWN_NODE_BUFFER = 2

_SCALE_UP_TIMEOUT_SECONDS = 3 * 60 * 60  # 3 hours
_SCALE_DOWN_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 hours
_POLL_INTERVAL_SECONDS = 60


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
  yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
      MANIFEST_TEMPLATE,
      cloud=FLAGS.cloud,
  )
  cluster.ModifyPodSpecPlacementYaml(
      yaml_docs,
      'app',
      cluster.default_nodepool.machine_type,
  )
  list(kubernetes_commands.ApplyYaml(yaml_docs))


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the scale-up, scale-down, scale-up benchmark sequence.

  Returns:
    Combined samples from all three phases, each tagged with phase metadata.
  """
  assert bm_spec.container_cluster
  cluster = bm_spec.container_cluster
  assert isinstance(cluster, container_service.KubernetesCluster)

<<<<<<< HEAD
<<<<<<< HEAD
  initial_nodes = set(kubernetes_commands.GetNodeNames())
  initial_node_count = len(initial_nodes)

  # Phase 1: Scale up.
  scaleup1_samples = _ScaleUpAndCollect(
      'scaleup1',
      NUM_NODES.value,
      cluster,
      initial_nodes,
  )

  # Phase 2: Scale down and wait for nodes to be removed.
  scaledown_samples, nodes_removed = _ScaleDownAndCollect(
      'scaledown',
      initial_nodes,
      initial_node_count,
  )

  # Phase 3: Scale up again (only if scale-down succeeded).
  scaleup2_samples: list[sample.Sample] = []
  if nodes_removed:
    scaleup2_samples = _ScaleUpAndCollect(
        'scaleup2',
        NUM_NODES.value,
        cluster,
        initial_nodes,
    )
  else:
    logging.warning(
        'Skipping second scale up: nodes did not return to baseline.',
    )

  return scaleup1_samples + scaledown_samples + scaleup2_samples


def _ScaleUpAndCollect(
    phase: str,
    replicas: int,
    cluster: container_service.KubernetesCluster,
    initial_nodes: set[str],
) -> list[sample.Sample]:
  """Scales the deployment up and collects pod/node timing samples.

  Args:
    phase: Label for this phase (e.g. 'scaleup1').
    replicas: Target replica count.
    cluster: The Kubernetes cluster.
    initial_nodes: Node names present before scaling.

  Returns:
    Samples tagged with phase metadata.
  """
  initial_pods = set(kubernetes_commands.GetPodNames())
  start_time = time.time()
  _ScaleDeployment(replicas)

  phase_log_samples = _PollPodPhasesUntilReady(
      phase,
      replicas,
      _SCALE_UP_TIMEOUT_SECONDS,
  )
  pod_samples = kubernetes_scale_benchmark.ParseStatusChanges(
      'pod',
      start_time,
      resources_to_ignore=initial_pods,
  )
  kubernetes_scale_benchmark.CheckForFailures(
      cluster,
      pod_samples,
      replicas,
  )
  node_samples = kubernetes_scale_benchmark.ParseStatusChanges(
      'node',
      start_time,
      resources_to_ignore=initial_nodes,
  )

  all_samples = phase_log_samples + pod_samples + node_samples
  _AddPhaseMetadata(all_samples, phase)
  return all_samples


def _ScaleDownAndCollect(
    phase: str,
    initial_nodes: set[str],
    initial_node_count: int,
) -> tuple[list[sample.Sample], bool]:
  """Scales deployment to 0 and waits for autoscaler to remove nodes.

  Args:
    phase: Label for this phase.
    initial_nodes: Node names present before any scaling.
    initial_node_count: Number of nodes before any scaling.

  Returns:
    A tuple of (samples, whether nodes returned to acceptable level).
  """
  _ScaleDeployment(0)
  return _PollNodeDeletionUntilDone(
      phase,
      initial_nodes,
      initial_node_count,
      _SCALE_DOWN_TIMEOUT_SECONDS,
  )


def _PollPodPhasesUntilReady(
    phase: str,
    desired_replicas: int,
    timeout: int,
) -> list[sample.Sample]:
  """Logs pod phase counts every minute until all pods are Ready or timeout.

  Args:
    phase: Label for this phase.
    desired_replicas: Number of pods expected to become Ready.
    timeout: Maximum wall-clock seconds to poll.

  Returns:
    Time-series samples of pod counts per phase.
  """
  samples: list[sample.Sample] = []
  start = time.monotonic()
  while True:
    phase_counts, ready_count = _GetPodPhaseCounts()
    elapsed = time.monotonic() - start
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
    # Always emit a Ready count for a consistent time-series.
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
    time.sleep(_POLL_INTERVAL_SECONDS)


def _GetPodPhaseCounts() -> tuple[dict[str, int], int]:
  """Returns pod phase counts and Ready count for the 'app' deployment.

  Uses a single kubectl call with label selector `app=app`.

  Returns:
    A tuple of (phase_name -> count, ready_count).
  """
  stdout, _, _ = container_service.RunKubectlCommand(
      ['get', 'pods', '-l', 'app=app', '-o', 'json'],
      suppress_logging=True,
  )
  pods = json.loads(stdout).get('items', [])
  phase_counts: dict[str, int] = {}
  ready_count = 0
  for pod in pods:
    pod_phase = pod.get('status', {}).get('phase', 'Unknown')
    phase_counts[pod_phase] = phase_counts.get(pod_phase, 0) + 1
    if _IsPodReady(pod):
      ready_count += 1
  return phase_counts, ready_count


def _IsPodReady(pod: dict[str, Any]) -> bool:
  """Returns True if the pod has a Ready=True condition."""
  for condition in pod.get('status', {}).get('conditions', []):
    if condition.get('type') == 'Ready' and condition.get('status') == 'True':
      return True
  return False


def _PollNodeDeletionUntilDone(
    phase: str,
    initial_nodes: set[str],
    initial_node_count: int,
    timeout: int,
) -> tuple[list[sample.Sample], bool]:
  """Polls node count until autoscaler removes scaled nodes or timeout.

  Allows a small buffer (_SCALE_DOWN_NODE_BUFFER) above the initial count to
  account for the cluster autoscaler being conservative (e.g. system workloads
  preventing removal).

  Args:
    phase: Label for this phase.
    initial_nodes: Node names present before any scaling.
    initial_node_count: Number of nodes before any scaling.
    timeout: Maximum time in seconds (_SCALE_DOWN_TIMEOUT_SECONDS) to poll.

  Returns:
    A tuple of (samples, whether node count reached acceptable level).
  """
  acceptable_count = initial_node_count + _SCALE_DOWN_NODE_BUFFER
  scaled_nodes = set(kubernetes_commands.GetNodeNames()) - initial_nodes
  deletion_times: dict[str, float] = {}
  samples: list[sample.Sample] = []
  start = time.monotonic()
  done = False

  while True:
    current_nodes = set(kubernetes_commands.GetNodeNames())
    elapsed = time.monotonic() - start

    # Record deletion timestamps for nodes that disappeared.
    for node in list(scaled_nodes):
      if node not in current_nodes:
        deletion_times[node] = elapsed
        scaled_nodes.discard(node)

    remaining = max(len(current_nodes) - acceptable_count, 0)
    samples.append(
        sample.Sample(
            'node_remaining_count',
            remaining,
            'count',
            metadata={'phase': phase, 'elapsed_seconds': elapsed},
        )
    )

    if len(current_nodes) <= acceptable_count:
      logging.info(
          'Node count (%d) within acceptable threshold (%d).',
          len(current_nodes),
          acceptable_count,
      )
      done = True
      break
    if elapsed >= timeout:
      logging.warning(
          'Timed out waiting for nodes to scale down.'
          ' Remaining above threshold: %d',
          remaining,
      )
      break

    logging.info('Remaining scaled nodes above threshold: %d', remaining)
    time.sleep(_POLL_INTERVAL_SECONDS)

  samples += _SummarizeNodeDeletionTimes(deletion_times, phase)
  return samples, done


def _SummarizeNodeDeletionTimes(
    deletion_times: dict[str, float],
    phase: str,
) -> list[sample.Sample]:
  """Builds percentile samples from per-node deletion durations.

  Args:
    deletion_times: Mapping of node name to seconds-until-deleted.
    phase: Label for this phase.

  Returns:
    Samples for p50, p90, p99, p99.9, p100 of deletion times.
  """
  if not deletion_times:
    return []
  summaries = kubernetes_scale_benchmark._SummarizeTimestamps(
      list(deletion_times.values())
  )
  target_percentiles = {'p50', 'p90', 'p99', 'p99.9', 'p100'}
  samples: list[sample.Sample] = []
  for name, value in summaries.items():
    if name in target_percentiles:
      samples.append(
          sample.Sample(
              f'node_delete_{name}',
              value,
              'seconds',
              metadata={'phase': phase},
          )
      )
  return samples


def _ScaleDeployment(replicas: int) -> None:
  """Scales the 'app' deployment to the given replica count."""
  container_service.RunKubectlCommand([
      'scale',
      f'--replicas={replicas}',
      'deployment/app',
  ])


def _GetScaleTimeout(num_nodes: int | None = None) -> int:
  """Returns the timeout for a scale or cleanup operation.

  Args:
    num_nodes: Number of nodes to scale to. Defaults to NUM_NODES flag.

  Returns:
    Timeout in seconds, capped at 1 hour.
  """
  nodes = num_nodes if num_nodes is not None else NUM_NODES.value
  base_timeout = 60 * 10  # 10 minutes
  per_node_timeout = nodes * 3  # 3 seconds per node
  max_timeout = 60 * 60  # 1 hour
  return min(base_timeout + per_node_timeout, max_timeout)


def _AddPhaseMetadata(
    samples: list[sample.Sample],
    phase: str,
) -> None:
  """Adds phase metadata to all samples."""
  for s in samples:
    s.metadata['phase'] = phase
=======
  initial_node_count = len(kubernetes_commands.GetNodeNames())
  start_time = time.time()
=======
  initial_nodes = set(kubernetes_commands.GetNodeNames())
  initial_node_count = len(initial_nodes)
>>>>>>> 9fbbc449 (Add scaling down logic, phases and gathering metrics)

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
<<<<<<< HEAD
  return samples
>>>>>>> 9a938ee8 (Add scaling down logic and gathering metrics)
=======

  return scaleup1_samples + scaledown_samples + scaleup2_samples
>>>>>>> 9fbbc449 (Add scaling down logic, phases and gathering metrics)


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
  """Deletes the app deployment. Runs before cluster teardown."""
  del bm_spec  # Unused.
  container_service.RunRetryableKubectlCommand(
      ['delete', 'deployment', 'app'],
<<<<<<< HEAD
      timeout=_GetScaleTimeout(),
=======
      timeout=GetScaleTimeout(),
>>>>>>> 9fbbc449 (Add scaling down logic, phases and gathering metrics)
      raise_on_failure=False,
  )
