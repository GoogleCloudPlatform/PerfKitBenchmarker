"""Tracks resource usage across a pkb run.

Currently, only tracks vm usage within k8s clusters.

Example usage:
  cluster: container_service.KubernetesCluster = SetupK8sBenchmark()
  tracker = kubernetes_tracker.KubernetesCluster(cluster)
  with tracker.TrackUsage()
    RunK8sBenchmark()
  samples = GetBenchmarkSamples()
  samples += tracker.GenerateSamples()
"""

import collections
import contextlib
import dataclasses
import logging
import re
import time
from typing import Callable, Iterable, Iterator, Optional
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import container_service
from perfkitbenchmarker import events
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages


CLUSTER_TIME_METRIC = "cluster/usage/cluster_time"
VM_TIME_METRIC = "cluster/usage/vm_time"


def Register(_):
  pass


class KubernetesResourceTracker:
  """Class that attempts to measure the resource usage of a kubernetes cluster.

  Specifically, it will calculate:
  * VM time per VM type
  * cluster time
  * Eventually: cpu requests of user pods (to compute costs on autopilot
    clusters).

  These values could then be combined with pricing data to determine the total
  "cost" of a benchmark run.

  Usage:
      if isinstance(
          spec.container_cluster,
          container_service.KubernetesCluster
      ):
        k8s_cluster: container_service.KubernetesCluster =
        spec.container_cluster
        tracker = kubernetes_tracker.KubernetesResourceTracker(k8s_cluster)
        with tracker.TrackUsage():
          samples = spec.BenchmarkRun(spec)
        samples += tracker.GenerateSamples()
  """

  def __init__(
      self,
      cluster: container_service.KubernetesCluster,
      time_fn: Optional[Callable[[], float]] = None,
  ):
    """Create a context manager that will watch a k8s cluster usage.

    Args:
      cluster: The cluster to track.
      time_fn: A function that when called will get the current time. If not
        set, will default to time.time. (Likely only useful for unit testing.)
    """
    self._time_fn: Callable[[], float] = time_fn or time.time
    self._nodes: dict[str, _NodeTracker] = {}
    self._start_time = 0.0
    self._end_time = 0.0
    self._cluster = cluster

  @contextlib.contextmanager
  def TrackUsage(self) -> Iterator[None]:
    """Watches the cluster and calculates the total usage."""
    self.StartTracking()
    yield
    self.StopTracking()

  def StartTracking(self) -> None:
    self._start_time = self._time_fn()
    self._nodes = _GetInitialNodeDetails(self._cluster, self._start_time)

  def StopTracking(self) -> None:
    self._StopWatchingForNodeChanges()
    self._end_time = self._time_fn()
    self._nodes = _FinalizeNodeDetails(self._nodes, self._end_time)

  def GenerateSamples(self) -> Iterable[sample.Sample]:
    """Generates Samples detailing the usage of each SKU.

    If a cluster has two VMs (of the same SKU) and runs for 1h, then the
    corresponding sample will have a single entry for that SKU with 2h total
    usage.

    Must only call this *after* StopTracking()

    Yields:
      Resulting samples.

    Raises:
      RuntimeError: If called before StopTracking()
    """
    sums: dict[str, float] = collections.defaultdict(float)

    for name in self._nodes:
      end_time = self._nodes[name].end_time
      if end_time is None:
        raise RuntimeError(
            "Must only call GenerateSamples() after StopTracking()"
        )
      sums[self._nodes[name].machine_type] += (
          end_time - self._nodes[name].start_time
      )

    for machine_type in sums:
      yield sample.Sample(
          metric=VM_TIME_METRIC,
          value=sums[machine_type],
          unit="seconds",
          # Note that container_cluster_machine_type and machine_type will
          # automatically be added to all samples. However, these both refer to
          # the default nodepool machine type, even if other machine types are
          # present in the cluster, so we need to add another 'machine_type'
          # metadata.
          metadata={"vm_time_machine_type": machine_type},
      )
    yield sample.Sample(
        metric=CLUSTER_TIME_METRIC,
        value=self._end_time - self._start_time,
        unit="seconds",
    )

  def _StopWatchingForNodeChanges(self):
    """Stop watching the cluster for node add/remove events."""
    # TODO(user): consider periodically fetching the events (instead of just
    # at the end).
    polled_events = self._cluster.GetEvents()

    for e in polled_events:
      if e.resource.kind != "Node":
        continue

      name = e.resource.name
      if name is None:
        continue

      # TODO(user): These events work well for GKE; validate on AKS/EKS.
      if e.reason == "RegisteredNode":
        # Multiple attempts are made to register a node, so it's expected to see
        # multiple of these. We'll ignore all but the first.
        if name in self._nodes:
          continue

        logging.info(
            "DEBUG: RegisteredNode: %s, %s", name, self._GetMachineType(name)
        )
        self._nodes[name] = _NodeTracker(
            name=name,
            machine_type=self._GetMachineType(name),
            start_time=e.timestamp,
        )
      elif e.reason == "RemovingNode":
        if name not in self._nodes:
          # Node doesn't exist...? Rather than failing the test, we'll warn, but
          # will otherwise ignore this state.
          logging.warning(
              "Detected a kubernetes event indicating that a node (%s) is"
              " to be removed, but we have no record of this node. We'll"
              " ignore this node - it won't be counted in the"
              " %s metric.", name, VM_TIME_METRIC
          )
          continue

        # In case removing of a node fails for any reason, we'll always use the
        # timestamp of the last event we see.
        end_time = e.timestamp
        if self._nodes[name].end_time is not None:
          end_time = max(end_time, self._nodes[name].end_time)
        self._nodes[name].end_time = end_time

  def _GetMachineType(self, node_name: str) -> str:
    return _GetMachineTypeFromNodeName(self._cluster, node_name)


@dataclasses.dataclass
class _NodeTracker:
  name: str
  machine_type: str
  start_time: float
  end_time: Optional[float] = None


def _GetInitialNodeDetails(
    cluster: container_service.KubernetesCluster, start_time: float
) -> dict[str, _NodeTracker]:
  result: dict[str, _NodeTracker] = {}
  for name in cluster.GetNodeNames():
    result[name] = _NodeTracker(
        name=name,
        machine_type=_GetMachineTypeFromNodeName(cluster, name),
        start_time=start_time,
    )
  return result


def _GetMachineTypeFromNodeName(
    cluster: container_service.KubernetesCluster, node_name: str
) -> str:
  """Get the machine type of the given node.

  This method assumes that the nodepool name is embedded in the node name.
  Better would be a lookup from the cloud provider.

  Args:
    cluster: The cluster that the given node belongs to.
    node_name: The name of the node.
  Returns:
    The machine type of the node, or 'unknown' if not found.
  """
  # Get the nodepool
  # eg 'gke-pkb-8ee57c86-default-pool-232fa391-34qh' => default-pool
  # eg 'gke-pkb-8ee57c86-np1-2cd25dd3-1r9l => np1
  # TODO(user): extend this for clouds other than GCP.
  m = re.match(f"gke-{cluster.name}-(.*)-[^-]*-[^-]*", node_name)
  if not m:
    return "unknown"
  node_pool = m.group(1)

  if node_pool == "default-pool":
    return cluster.default_nodepool.machine_type

  if node_pool not in cluster.nodepools:
    return "unknown"

  bnpc: container_service.BaseNodePoolConfig = cluster.nodepools[node_pool]
  return bnpc.machine_type


def _FinalizeNodeDetails(
    node_details: dict[str, _NodeTracker], end_time: float
) -> dict[str, _NodeTracker]:
  # Set end_time for all nodes that don't already have an end time
  for name in node_details:
    if node_details[name].end_time is None:
      node_details[name].end_time = end_time
  return node_details


tracker: KubernetesResourceTracker = None


@events.before_phase.connect
def _StartTrackingVMUsage(stage: str, benchmark_spec: bm_spec.BenchmarkSpec):
  """Start tracking VM usage.

  i.e. start/stop times, durations.

  Args:
    stage: stage. All stages except for stages.RUN will be ignored.
    benchmark_spec: benchmark_spec
  """
  if stage != stages.RUN:
    return

  k8s_cluster: container_service.KubernetesCluster = (
      benchmark_spec.container_cluster
  )
  if k8s_cluster is None:
    return

  global tracker
  tracker = KubernetesResourceTracker(k8s_cluster)
  tracker.StartTracking()


@events.after_phase.connect
def _StopTrackingVMUsage(stage: str, benchmark_spec: bm_spec.BenchmarkSpec):
  """Stop tracking VM usage.

  Args:
    stage: stage. All stages except for stages.RUN will be ignored.
    benchmark_spec: benchmark_spec
  """
  if stage != stages.RUN:
    return

  k8s_cluster: container_service.KubernetesCluster = (
      benchmark_spec.container_cluster
  )
  if k8s_cluster is None:
    return

  if tracker is not None:
    tracker.StopTracking()
    k8s_cluster.AddSamples(tracker.GenerateSamples())
