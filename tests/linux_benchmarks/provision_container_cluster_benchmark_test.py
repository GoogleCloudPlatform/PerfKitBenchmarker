import os
import unittest
from unittest import mock
from perfkitbenchmarker import container_service
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
    pass


class ProvisionKubernetesClusterTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(container_service, 'RunKubectlCommand')
  def test_GetKeyScalingEventTimes(self, mock_run_kubectl):
    test_path = os.path.join(
        os.path.dirname(__file__), '../data/kubectl_get_events.yaml'
    )
    with open(test_path, 'r') as f:
      events_yaml = f.read()
    mock_run_kubectl.return_value = (events_yaml, None, None)

    cluster = TestKubernetesCluster()
    new_node = 'gke-pkb-74f3ec93-default-pool-3dcae581-9dw1'
    new_pod = 'daemon-set-jntzg'
    events = provision_container_cluster_benchmark._GetKeyScalingEventTimes(
        cluster, new_node, new_pod
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
    mock_run_kubectl.assert_called_once_with(['get', 'events', '-o', 'yaml'])


if __name__ == '__main__':
  unittest.main()
