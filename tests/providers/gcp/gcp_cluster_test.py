"""Unit tests for gcp_cluster.py.
"""

import unittest

from absl import flags
import mock
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_cluster
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class TestGceClusterSpec(gcp_cluster.GceClusterSpec):
  pass


class GcpClusterTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    vm_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {'GCP': {}}
    }
    FLAGS.run_uri = 'run12345'
    self.cluster_spec = TestGceClusterSpec(
        'test_gcp_cluster', headnode=vm_config, workers=vm_config)
    self.mock_issue = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', autospec=True))
    self.mock_issue.return_value = ('fake_key', None, None)
    self.cluster = gcp_cluster.GceCluster(self.cluster_spec)
    self.cluster.name = 'test_gcp_cluster'
    self.cluster.num_workers = 2
    self.mock_vm_class = self.enter_context(
        mock.patch.object(virtual_machine, 'GetVmClass', autospec=True)
    )
    self.mock_vm_class.return_value = pkb_common_test_case.TestVirtualMachine
    self.mock_generate_ssh_config = self.enter_context(
        mock.patch.object(vm_util, 'GenerateSSHConfig', autospec=True)
    )

  def testPostCreate(self):
    # Test _PostCreate
    self.cluster._PostCreate()
    self.assertEqual(
        self.cluster.headnode_vm.name, 'test_gcp_cluster-controller')
    self.assertEqual(
        self.cluster.worker_vms[0].name, 'test_gcp_cluster-computenodeset-0')
    self.assertEqual(
        self.cluster.worker_vms[1].name, 'test_gcp_cluster-computenodeset-1')
    self.assertEqual(
        self.cluster.vms, [self.cluster.headnode_vm] + self.cluster.worker_vms)

  def testAuthenticateVM(self):
    self.cluster.vms = [mock.MagicMock()]
    self.cluster.vms[0].has_private_key = False
    self.cluster.vms[0].RemoteCommand = mock.MagicMock()
    self.cluster.AuthenticateVM()
    self.assertTrue(self.cluster.vms[0].has_private_key)
    self.cluster.vms[0].RemoteHostCopy.assert_called_once_with(
        '/tmp/perfkitbenchmarker/runs/run12345/perfkitbenchmarker_keyfile',
        '~/.ssh/id_rsa')
    self.assertSequenceEqual(
        (mock.call(
            'echo "Host *\n  StrictHostKeyChecking no\n  User=root\n" > '
            '~/.ssh/config'),
         mock.call('chmod 600 ~/.ssh/config')),
        self.cluster.vms[0].RemoteCommand.mock_calls
    )


if __name__ == '__main__':
  unittest.main()
