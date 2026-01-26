import time
from typing import Optional
import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import container_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.container_service import kubernetes_commands
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
    kubernetes_commands.GetNodeNames = lambda: [
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
    kubernetes_commands.GetNodeNames = lambda: [
        "gke-pkb-cluster-default-pool-node-1"
    ]
    # pylint: disable=invalid-name
    cluster.GetEvents = lambda: [
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind="Node", name="gke-pkb-cluster-default-pool-node-2"
            ),
            message="ignored",
            reason="RegisteredNode",
            timestamp=fake_time + 20.0,
            type="Normal",
        ),
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind="Node", name="gke-pkb-cluster-default-pool-node-1"
            ),
            message="ignored",
            reason="RemovingNode",
            timestamp=fake_time + 40.0,
            type="Normal",
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
    kubernetes_commands.GetNodeNames = lambda: [
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
    kubernetes_commands.GetNodeNames = lambda: [
        "gke-pkb-cluster-default-pool-node-1"
    ]
    cluster.GetEvents = lambda: [
        container_service.KubernetesEvent(
            resource=container_service.KubernetesEventResource(
                kind="Node", name="gke-pkb-cluster-default-pool-node-1"
            ),
            message="ignored",
            reason="RegisteredNode",
            timestamp=fake_time - 20.0,
            type="Normal",
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


_CLUSTER_CLOUD = provider_info.UNIT_TEST


class TestKubernetesCluster(container_service.KubernetesCluster):

  CLOUD = _CLUSTER_CLOUD

  def _Create(self):
    pass

  def _Delete(self):
    pass


def CreateMockCluster(
    name: str,
    machine_type: str,
    nodepools: Optional[dict[str, str]] = None,
) -> container_service.KubernetesCluster:
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
  cluster.GetEvents = lambda: []
  kubernetes_commands.GetNodeNames = lambda: []
  return cluster


if __name__ == "__main__":
  unittest.main()
