import os
import unittest
from unittest import mock
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks.provisioning_benchmarks import provision_container_cluster_benchmark
from tests import pkb_common_test_case

KubernetesEvent = container_service.KubernetesEvent
KubernetesEventResource = container_service.KubernetesEventResource


class TestKubernetesCluster(
    pkb_common_test_case.TestResource, container_service.KubernetesCluster
):
  """Test KubernetesCluster.

  Most KubernetesCluster methods do not use instance state and do not need an
  initialized instance, but rather interact directly with RunKubectlCommand.
  """

  def __init__(self):
    # Do not call KubernetesCluster.__init__, which tries and fails to
    # construct a VM class
    self.event_poller = None
    pass


_CONTAINER_START_YAML = """
- apiVersion: v1
  count: 1
  eventTime: null
  firstTimestamp: "2023-11-21T22:44:52Z"
  involvedObject:
    apiVersion: v1
    fieldPath: spec.containers{sleep}
    kind: Pod
    name: daemon-set-jntzg
    namespace: default
    resourceVersion: "1989"
    uid: bc064e02-3fc8-4f79-a2b2-6b32ddbc6f6d
  kind: Event
  lastTimestamp: "2023-11-21T22:44:52Z"
  message: Started container sleep
  metadata:
    creationTimestamp: "2023-11-21T22:44:52Z"
    name: daemon-set-jntzg.1799c4c6d1b8570e
    namespace: default
    resourceVersion: "374"
    uid: 3d409e56-c4a1-4002-81fb-44ea43fcf42e
  reason: Started
  reportingComponent: ""
  reportingInstance: ""
  source:
    component: kubelet
    host: gke-pkb-74f3ec93-default-pool-3dcae581-9dw1
  type: Normal
"""

_NODE_READY_YAML = """
- apiVersion: v1
  count: 1
  eventTime: null
  firstTimestamp: "2023-11-21T22:44:47Z"
  involvedObject:
    kind: Node
    name: gke-pkb-74f3ec93-default-pool-3dcae581-9dw1
    uid: gke-pkb-74f3ec93-default-pool-3dcae581-9dw1
  kind: Event
  lastTimestamp: "2023-11-21T22:44:47Z"
  message: 'Node gke-pkb-74f3ec93-default-pool-3dcae581-9dw1 status is now: NodeReady'
  metadata:
    creationTimestamp: "2023-11-21T22:44:47Z"
    name: gke-pkb-74f3ec93-default-pool-3dcae581-9dw1.1799c4c5af7a5bc1
    namespace: default
    resourceVersion: "349"
    uid: d8a8c61a-0176-4a3d-8bc6-44df70afdc13
  reason: NodeReady
  reportingComponent: ""
  reportingInstance: ""
  source:
    component: kubelet
    host: gke-pkb-74f3ec93-default-pool-3dcae581-9dw1
  type: Normal
"""

_YAML_START = """
apiVersion: v1
items:
"""

_YAML_EMD = """
kind: List
metadata:
  resourceVersion:
"""

_NODE_NAME = 'gke-pkb-74f3ec93-default-pool-3dcae581-9dw1'
_POD_NAME = 'daemon-set-jntzg'


class ProvisionKubernetesClusterTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(container_service, 'RunKubectlCommand')
  def test_GetKeyScalingEventTimes(self, mock_run_kubectl):
    test_path = os.path.join(
        os.path.dirname(__file__), '../data/kubectl_get_events.yaml'
    )
    with open(test_path) as f:
      events_yaml = f.read()
    mock_run_kubectl.return_value = (events_yaml, None, None)

    cluster = TestKubernetesCluster()
    events = provision_container_cluster_benchmark._GetKeyScalingEventTimes(
        cluster, _NODE_NAME, _POD_NAME
    )

    expected_event_times = {
        'starting_kubelet': 1700606686.0,
        'node_ready': 1700606687.0,
        'node_registered': 1700606691.0,
        'image_pulling': 1700606688.0,
        'image_pulled': 1700606692.0,
        'container_created': 1700606692.0,
        'container_started': 1700606692.0,
    }

    self.assertEqual(events, expected_event_times)
    mock_run_kubectl.assert_called_once_with(
        ['get', 'events', '-o', 'yaml'],
        raise_on_timeout=True, timeout=None, stack_level=3)

  @mock.patch.object(container_service, 'RunKubectlCommand')
  def test_GetMinimumKeyScalingEventTimes(self, mock_run_kubectl):
    events_yaml = (
        _YAML_START + _NODE_READY_YAML + _CONTAINER_START_YAML + _YAML_EMD
    )
    mock_run_kubectl.return_value = (events_yaml, None, None)

    cluster = TestKubernetesCluster()
    events = provision_container_cluster_benchmark._GetKeyScalingEventTimes(
        cluster, _NODE_NAME, _POD_NAME
    )

    expected_event_times = {
        'node_ready': 1700606687.0,
        'container_started': 1700606692.0,
    }

    self.assertEqual(events, expected_event_times)
    mock_run_kubectl.assert_called_once_with(
        ['get', 'events', '-o', 'yaml'],
        raise_on_timeout=True, timeout=None, stack_level=3)

  @mock.patch.object(container_service, 'RunKubectlCommand')
  def test_NoMinimumEventsThrows(self, mock_run_kubectl):
    events_yaml = _YAML_START + _CONTAINER_START_YAML + _YAML_EMD
    mock_run_kubectl.return_value = (events_yaml, None, None)

    cluster = TestKubernetesCluster()
    with self.assertRaises(errors.Benchmarks.RunError):
      provision_container_cluster_benchmark._GetKeyScalingEventTimes(
          cluster, _NODE_NAME, _POD_NAME
      )


if __name__ == '__main__':
  unittest.main()
