import time
from typing import Optional
import unittest
from unittest import mock
from perfkitbenchmarker import container_service
from tests import pkb_common_test_case
from perfkitbenchmarker.traces import kubernetes_tracker


class UsageTrackerTest(pkb_common_test_case.PkbCommonTestCase):

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
    cluster.GetNodeNames = lambda: [
        "gke-pkb-cluster-default-pool-node-1",
        "gke-pkb-cluster-default-pool-node-2",
        "gke-pkb-cluster-default-pool-node-3",
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
    cluster.GetNodeNames = lambda: ["gke-pkb-cluster-default-pool-node-1"]
    cluster.GetEvents = lambda: [
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind="Node", name="gke-pkb-cluster-default-pool-node-2"
            ),
            message="ignored",
            reason="RegisteredNode",
            timestamp=fake_time + 20.0,
        ),
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind="Node", name="gke-pkb-cluster-default-pool-node-1"
            ),
            message="ignored",
            reason="RemovingNode",
            timestamp=fake_time + 40.0,
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
            "node-pool-1": CreateMockNodePool(
                name="node-pool-1", machine_type="n2-standard-4"
            ),
            "node-pool-2": CreateMockNodePool(
                name="node-pool-2", machine_type="n2-highcpu-96"
            ),
        },
    )
    cluster.GetNodeNames = lambda: [
        "gke-pkb-cluster-node-pool-1-node-1",
        "gke-pkb-cluster-node-pool-2-node-2",
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
    cluster.GetNodeNames = lambda: ["gke-pkb-cluster-default-pool-node-1"]
    cluster.GetEvents = lambda: [
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind="Node", name="gke-pkb-cluster-default-pool-node-1"
            ),
            message="ignored",
            reason="RegisteredNode",
            timestamp=fake_time - 20.0,
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


def CreateMockCluster(
    name: str,
    machine_type: str,
    nodepools: Optional[dict[str, container_service.BaseNodePoolConfig]] = None,
) -> container_service.KubernetesCluster:
  cluster = mock.create_autospec(container_service.KubernetesCluster)
  cluster.name = name
  cluster.default_nodepool = mock.create_autospec(
      container_service.BaseNodePoolConfig
  )
  cluster.default_nodepool.machine_type = machine_type
  cluster.nodepools = nodepools
  return cluster


def CreateMockNodePool(
    name: str, machine_type: str
) -> container_service.BaseNodePoolConfig:
  np = mock.create_autospec(container_service.BaseNodePoolConfig)
  np.name = name
  np.machine_type = machine_type
  return np


if __name__ == "__main__":
  unittest.main()

