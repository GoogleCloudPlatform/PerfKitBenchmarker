import time
from typing import Optional
import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.container_service import kubernetes_events
from tests import pkb_common_test_case
from perfkitbenchmarker.traces import kubernetes_tracker


class UsageTrackerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(kubeconfig="kubeconfig"))
    self.enter_context(flagsaver.flagsaver(run_uri="123"))
    self.enter_context(
        mock.patch("perfkitbenchmarker.providers.LoadProvider", autospec=True)
    )
    self.mock_get_node_names = self.enter_context(
        mock.patch.object(
            kubernetes_commands,
            "GetNodeNames",
            autospec=True,
            return_value=set(),
        )
    )

  def testTrackUsageCalculatesTotalClusterTime(self):
    fake_time = time.time()
    cluster = CreateMockCluster(name="pkb-cluster", machine_type="ignored")
    tracker = kubernetes_tracker.KubernetesResourceTracker(
        cluster, time_fn=lambda: fake_time
    )

    with tracker.TrackUsage():
      fake_time += 60.0

    samples = [
        s
        for s in tracker.GenerateSamples()
        if s.metric == kubernetes_tracker.CLUSTER_TIME_METRIC
    ]
    self.assertLen(samples, 1)
    self.assertAlmostEqual(samples[0].value, 60.0)

  def testTrackUsageWithFixedNumberOfNodes(self):
    fake_time = time.time()
    cluster = CreateMockCluster(
        name="pkb-cluster", machine_type="e2-standard-8"
    )
    self.mock_get_node_names.return_value = {
        "gke-pkb-cluster-default-pool-node-1",
        "gke-pkb-cluster-default-pool-node-2",
        "gke-pkb-cluster-default-pool-node-3",
    }
    tracker = kubernetes_tracker.KubernetesResourceTracker(
        cluster, time_fn=lambda: fake_time
    )

    with tracker.TrackUsage():
      fake_time += 60.0

    samples = [
        s
        for s in tracker.GenerateSamples()
        if s.metric == kubernetes_tracker.VM_TIME_METRIC
    ]
    self.assertLen(samples, 1)
    self.assertEqual(
        samples[0].metadata["vm_time_machine_type"], "e2-standard-8"
    )
    self.assertAlmostEqual(samples[0].value, 3 * 60.0)

  def testTrackUsageWithVariableNumberOfNodes(self):
    fake_time = time.time()

    # Simulate a cluster where:
    # * Only node-1 exists at the start
    # * node-2 is added after 20s
    # * node-1 is removed after 40s
    # End result: each node runs for 40s.
    cluster = CreateMockCluster(
        name="pkb-cluster", machine_type="e2-standard-8"
    )
    self.mock_get_node_names.side_effect = [
        {"gke-pkb-cluster-default-pool-node-1"},
        {
            "gke-pkb-cluster-default-pool-node-1",
            "gke-pkb-cluster-default-pool-node-2",
        },
    ]
    # pylint: disable=invalid-name
    cluster.GetEvents = lambda: [  # pyrefly: ignore[bad-assignment]
        _CreateEvent(
            "gke-pkb-cluster-default-pool-node-2",
            "RegisteredNode",
            fake_time + 20.0,
        ),
        _CreateEvent(
            "gke-pkb-cluster-default-pool-node-1",
            "RemovingNode",
            fake_time + 40.0,
        ),
    ]
    tracker = kubernetes_tracker.KubernetesResourceTracker(
        cluster, time_fn=lambda: fake_time
    )

    with tracker.TrackUsage():
      fake_time += 60.0

    samples = [
        s
        for s in tracker.GenerateSamples()
        if s.metric == kubernetes_tracker.VM_TIME_METRIC
    ]
    self.assertLen(samples, 1)
    self.assertEqual(
        samples[0].metadata["vm_time_machine_type"], "e2-standard-8"
    )
    self.assertAlmostEqual(samples[0].value, 2 * 40.0)

  def testTrackUsageWithMultipleNodePools(self):
    fake_time = time.time()
    cluster = CreateMockCluster(
        name="pkb-cluster",
        machine_type="e2-standard-8",
        nodepools={
            "node-pool-1": "n2-standard-4",
            "node-pool-2": "n2-highcpu-96",
        },
    )
    self.mock_get_node_names.return_value = {
        "gke-pkb-cluster-node-pool-1-node-1",
        "gke-pkb-cluster-node-pool-2-node-2",
    }
    tracker = kubernetes_tracker.KubernetesResourceTracker(
        cluster, time_fn=lambda: fake_time
    )

    with tracker.TrackUsage():
      fake_time += 60.0

    samples = [
        s
        for s in tracker.GenerateSamples()
        if s.metric == kubernetes_tracker.VM_TIME_METRIC
    ]
    self.assertLen(samples, 2)
    self.assertSameElements(
        ["n2-standard-4", "n2-highcpu-96"],
        [s.metadata["vm_time_machine_type"] for s in samples],
    )
    self.assertAlmostEqual(samples[0].value, 60.0)
    self.assertAlmostEqual(samples[1].value, 60.0)

  def testTrackUsageWithEventAddingNodeBeforeBenchmarkRun(self):
    fake_time = time.time()

    # Simulate a cluster where:
    # * node-1 exists at the start
    # * An event claims that node-1 was created a t-10s (i.e. 10s before the
    #   benchmark was run.)
    # End result: event should be ignored; total node time=60s.
    cluster = CreateMockCluster(
        name="pkb-cluster", machine_type="e2-standard-8"
    )
    self.mock_get_node_names.return_value = {
        "gke-pkb-cluster-default-pool-node-1"
    }
    cluster.GetEvents = lambda: [  # pyrefly: ignore[bad-assignment]
        _CreateEvent(
            "gke-pkb-cluster-default-pool-node-1",
            "RegisteredNode",
            fake_time - 20.0,
        ),
    ]
    tracker = kubernetes_tracker.KubernetesResourceTracker(
        cluster, time_fn=lambda: fake_time
    )

    with tracker.TrackUsage():
      fake_time += 60.0

    samples = [
        s
        for s in tracker.GenerateSamples()
        if s.metric == kubernetes_tracker.VM_TIME_METRIC
    ]
    self.assertLen(samples, 1)
    self.assertAlmostEqual(samples[0].value, 60.0)

  def testCalculatesMaxConcurrentNodes(self):
    # Arrange.
    start_time = time.time()
    fake_time = start_time

    # node-1: [0, 40]
    # node-2: [20, 60]
    # node-3: [50, 60]
    # node-4: [60, 70]
    # Max concurrency: 2 at [20, 60]
    node_1 = "gke-pkb-cluster-default-pool-node-1"
    node_2 = "gke-pkb-cluster-default-pool-node-2"
    node_3 = "gke-pkb-cluster-default-pool-node-3"
    node_4 = "gke-pkb-cluster-default-pool-node-4"
    cluster = CreateMockCluster(
        name="pkb-cluster", machine_type="e2-standard-8"
    )
    self.mock_get_node_names.side_effect = [
        {node_1},
        {node_1, node_2, node_3, node_4},
    ]
    cluster.GetEvents = lambda: [  # pyrefly: ignore[bad-assignment]
        _CreateEvent(node_1, "RegisteredNode", start_time),
        _CreateEvent(node_1, "RemovingNode", start_time + 40.0),
        _CreateEvent(node_2, "RegisteredNode", start_time + 20.0),
        _CreateEvent(node_2, "RemovingNode", start_time + 60.0),
        _CreateEvent(node_3, "RegisteredNode", start_time + 50.0),
        _CreateEvent(node_3, "RemovingNode", start_time + 60.0),
        _CreateEvent(node_3, "RegisteredNode", start_time + 60.0),
        _CreateEvent(node_3, "RemovingNode", start_time + 70.0),
    ]
    tracker = kubernetes_tracker.KubernetesResourceTracker(
        cluster, time_fn=lambda: fake_time
    )
    with tracker.TrackUsage():
      fake_time += 70.0

    # Act.
    samples = tracker.GenerateSamples()

    # Assert.
    samples_by_metric = {s.metric: s for s in samples}
    max_concurrent_sample = samples_by_metric[
        kubernetes_tracker.MAX_CONCURRENT_NODES_METRIC
    ]
    self.assertEqual(max_concurrent_sample.value, 2)
    max_concurrent_type_sample = samples_by_metric[
        kubernetes_tracker.MAX_CONCURRENT_NODES_PER_MACHINE_TYPE_METRIC
    ]
    self.assertEqual(max_concurrent_type_sample.value, 2)
    self.assertEqual(
        max_concurrent_type_sample.metadata["vm_time_machine_type"],
        "e2-standard-8",
    )


_CLUSTER_CLOUD = provider_info.UNIT_TEST


class TestKubernetesCluster(kubernetes_cluster.KubernetesCluster):

  CLOUD = _CLUSTER_CLOUD

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def GetNodePoolNames(self) -> list[str]:
    return []


def _CreateEvent(
    name: str, reason: str, timestamp: float
) -> kubernetes_events.KubernetesEvent:
  return kubernetes_events.KubernetesEvent(
      resource=kubernetes_events.KubernetesEventResource(
          kind="Node", name=name
      ),
      message="ignored",
      reason=reason,
      timestamp=timestamp,
      type="Normal",
  )


def CreateMockCluster(
    name: str,
    machine_type: str,
    nodepools: Optional[dict[str, str]] = None,
) -> kubernetes_cluster.KubernetesCluster:
  nodepool_spec = {}
  if nodepools:
    for nodepool_name, machine_type in nodepools.items():
      nodepool_spec[nodepool_name] = {
          "vm_spec": {
              _CLUSTER_CLOUD: {
                  "machine_type": machine_type,
                  "zone": "us-east2-a",
              }
          },
      }
  cluster = TestKubernetesCluster(
      container_spec.ContainerClusterSpec(
          name,
          **{
              "cloud": _CLUSTER_CLOUD,
              "vm_spec": {
                  _CLUSTER_CLOUD: {
                      "machine_type": machine_type,
                      "zone": "us-east2-a",
                  },
              },
              "nodepools": nodepool_spec,
          },
      )
  )
  cluster.GetEvents = lambda: []  # pyrefly: ignore[bad-assignment]
  return cluster


if __name__ == "__main__":
  unittest.main()
