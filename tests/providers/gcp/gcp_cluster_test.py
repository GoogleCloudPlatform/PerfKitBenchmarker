"""Unit tests for gcp_cluster.py.
"""

import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import cluster
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_cluster
from perfkitbenchmarker.resources.container_service import kubectl
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
        self.cluster.headnode_vm.name, 'test_gcp_cluster-controller')  # pyrefly: ignore[missing-attribute]
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
    self.cluster.vms[0].RemoteHostCopy.assert_called_once_with(  # pyrefly: ignore[missing-attribute]
        '/tmp/perfkitbenchmarker/runs/run12345/perfkitbenchmarker_keyfile',
        '~/.ssh/id_rsa')
    self.assertSequenceEqual(
        (mock.call(
            'echo "Host *\n  StrictHostKeyChecking no\n  User=root\n" > '
            '~/.ssh/config'),
         mock.call('chmod 600 ~/.ssh/config')),
        self.cluster.vms[0].RemoteCommand.mock_calls
    )

  @flagsaver.flagsaver(cluster_scheduler='slurm')
  def testGetClusterClassSlurmTypes(self):
    with flagsaver.flagsaver(cluster_type='default'):
      self.assertIs(cluster.GetClusterClass('GCP'), gcp_cluster.GceCluster)
    with flagsaver.flagsaver(cluster_type='h4d'):
      self.assertIs(cluster.GetClusterClass('GCP'), gcp_cluster.H4dCluster)
    with flagsaver.flagsaver(cluster_type='a4'):
      self.assertIs(cluster.GetClusterClass('GCP'), gcp_cluster.A4Cluster)

  def testSlurmClusterDeployContainerImage(self):
    self.cluster.TYPE = 'a4'
    with mock.patch.object(
        self.cluster, 'InstallSquashImage', autospec=True
    ) as mock_install:
      image_path = self.cluster.DeployContainerImage('nccl_cluster')
      self.assertEqual(image_path, '/opt/apps/a4.sqsh')
      mock_install.assert_called_once_with(
          'nccl_cluster', 'a4.sqsh', '/opt/apps', 'nccl_cluster/a4.dockerfile'
      )

  def testSlurmClusterRunJobSuccess(self):
    with (
        mock.patch.object(
            data, 'ResourcePath', return_value='/path/to/slurm.sh.j2'
        ),
        mock.patch.object(
            vm_util,
            'ReadAndRenderJinja2Template',
            return_value='--mpi=pmix ./all_reduce_perf',
        ) as mock_render,
        mock.patch.object(
            self.cluster,
            'RemoteCommand',
            return_value=('nccl output', ''),
        ) as mock_remote,
    ):
      stdout, stderr = self.cluster.RunJob(
          'nccl_cluster',
          timeout=3600,
          operation='all_reduce',
          env_export='export NCCL_DEBUG=INFO;',
      )
      self.assertEqual(stdout, 'nccl output')
      self.assertEqual(stderr, '')
      mock_render.assert_called_once_with(
          'nccl_cluster/slurm.sh.j2',
          trim_spaces=True,
          operation='all_reduce',
          env_export='export NCCL_DEBUG=INFO;',
      )
      mock_remote.assert_called_once_with(
          '--mpi=pmix ./all_reduce_perf',
          login_shell=True,
          timeout=3600,
          env='export NCCL_DEBUG=INFO;',
      )

  def testSlurmClusterRunJobMissingTemplateRaisesUnsupportedConfigError(self):
    with mock.patch.object(
        data,
        'ResourcePath',
        side_effect=data.ResourceNotFound('missing template'),
    ):
      with self.assertRaises(errors.Benchmarks.UnsupportedConfigError):
        self.cluster.RunJob('unsupported_benchmark')

  def testK8sClusterInitAndPostCreate(self):
    headnode_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {'GCP': {'machine_type': 'e2-standard-16'}},
    }
    workers_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {'GCP': {'machine_type': 'a4-highgpu-8g'}},
    }
    k8s_spec = TestGceClusterSpec(
        'test_k8s_spec', headnode=headnode_config, workers=workers_config
    )
    k8s_cluster = cluster.K8sCluster(k8s_spec)
    k8s_cluster.TYPE = 'a4'
    self.assertEqual(k8s_cluster.headnode_machine_type, 'e2-standard-16')
    self.assertEqual(k8s_cluster.worker_machine_type, 'a4-highgpu-8g')
    with mock.patch.object(
        kubectl, 'RunKubectlCommand', autospec=True
    ) as mock_kubectl:
      k8s_cluster._PostCreate()
      mock_kubectl.assert_called_once_with([
          'apply',
          '-k',
          cluster._TRAINING_OPERATOR_MANIFEST_URL,
      ])


if __name__ == '__main__':
  unittest.main()
