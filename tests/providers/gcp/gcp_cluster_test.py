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
      self.assertFalse(hasattr(gcp_cluster.GceCluster, 'gpus_per_node'))
    with flagsaver.flagsaver(cluster_type='h4d'):
      self.assertIs(cluster.GetClusterClass('GCP'), gcp_cluster.H4dCluster)
      self.assertFalse(hasattr(gcp_cluster.H4dCluster, 'gpus_per_node'))
    with flagsaver.flagsaver(cluster_type='a4'):
      self.assertIs(cluster.GetClusterClass('GCP'), gcp_cluster.A4Cluster)
      self.assertEqual(gcp_cluster.A4Cluster.gpus_per_node, 8)

  def testSlurmClusterDeployContainerImage(self):
    self.cluster.TYPE = 'a4'
    with mock.patch.object(
        self.cluster, 'InstallSquashImage', autospec=True
    ) as mock_install:
      image_path = self.cluster.DeployContainerImage('nccl_cluster')
      self.assertEqual(image_path, '/opt/apps/a4.sqsh')
      self.assertEqual(
          self.cluster._container_images['nccl_cluster'], '/opt/apps/a4.sqsh'
      )
      mock_install.assert_called_once_with(
          'nccl_cluster', 'a4.sqsh', '/opt/apps', 'nccl_cluster/a4.dockerfile'
      )

  def testSlurmClusterRunJobSuccess(self):
    self.cluster.headnode_vm = mock.MagicMock()
    self.cluster._container_images['nccl_cluster'] = '/opt/apps/a4.sqsh'
    self.cluster.headnode_vm.RemoteCommand.side_effect = [
        ('Submitted batch job 42\n', ''),
        ('nccl output', ''),
    ]
    with (
        mock.patch.object(
            data, 'ResourceExists', return_value=False
        ) as mock_resource_exists,
        mock.patch.object(
            data, 'ResourcePath', return_value='/path/to/slurm_job.sbatch.j2'
        ) as mock_resource_path,
    ):
      stdout, stderr = self.cluster.RunJob(
          'nccl_cluster',
          command='./all_reduce_perf',
          env={'NCCL_DEBUG': 'INFO'},
          timeout=3600,
          gpus_per_node=8,
      )
      self.assertEqual(stdout, 'nccl output')
      self.assertEqual(stderr, '')
      mock_resource_exists.assert_called_once_with(
          'nccl_cluster/slurm_job.sbatch.j2'
      )
      mock_resource_path.assert_called_once_with('cluster/slurm_job.sbatch.j2')
      self.cluster.headnode_vm.RenderTemplate.assert_called_once_with(
          '/path/to/slurm_job.sbatch.j2',
          '/opt/apps/slurm_job.sbatch',
          {
              'job_name': 'slurm_job',
              'num_workers': 2,
              'output_path': '/opt/apps/slurm-%j.out',
              'env': {'NCCL_DEBUG': 'INFO'},
              'image': '/opt/apps/a4.sqsh',
              'command': './all_reduce_perf',
              'gpus_per_node': 8,
          },
          should_log_file=True,
      )
      self.cluster.headnode_vm.PullFile.assert_called_once_with(
          vm_util.GetTempDir(), '/opt/apps/slurm-42.out'
      )
      self.assertSequenceEqual(
          [
              mock.call(
                  'sbatch --wait /opt/apps/slurm_job.sbatch',
                  login_shell=True,
                  timeout=3600,
              ),
              mock.call('cat /opt/apps/slurm-42.out'),
          ],
          self.cluster.headnode_vm.RemoteCommand.mock_calls,
      )

  def testSlurmClusterRunJobCustomJobTemplate(self):
    self.cluster.headnode_vm = mock.MagicMock()
    self.cluster.headnode_vm.RemoteCommand.side_effect = [
        ('Submitted batch job 99\n', ''),
        ('job output', ''),
    ]
    with (
        mock.patch.object(
            data, 'ResourceExists', return_value=True
        ) as mock_resource_exists,
        mock.patch.object(
            data,
            'ResourcePath',
            return_value='/path/to/custom_slurm_job.sbatch.j2',
        ) as mock_resource_path,
    ):
      stdout, _ = self.cluster.RunJob('my_bench', job_name='step1')
      self.assertEqual(stdout, 'job output')
      mock_resource_exists.assert_called_once_with('my_bench/step1.sbatch.j2')
      mock_resource_path.assert_called_once_with('my_bench/step1.sbatch.j2')
      self.cluster.headnode_vm.RenderTemplate.assert_called_once_with(
          '/path/to/custom_slurm_job.sbatch.j2',
          '/opt/apps/step1.sbatch',
          {
              'job_name': 'step1',
              'num_workers': 2,
              'output_path': '/opt/apps/slurm-%j.out',
              'env': {},
              'command': None,
          },
          should_log_file=True,
      )

  def testSlurmClusterRunJobTemplateVarsImageOverridesContainerImages(self):
    self.cluster.headnode_vm = mock.MagicMock()
    self.cluster._container_images['my_bench'] = '/opt/apps/a4.sqsh'
    self.cluster.headnode_vm.RemoteCommand.side_effect = [
        ('Submitted batch job 100\n', ''),
        ('job output', ''),
    ]
    with (
        mock.patch.object(data, 'ResourceExists', return_value=False),
        mock.patch.object(
            data, 'ResourcePath', return_value='/path/to/slurm_job.sbatch.j2'
        ),
    ):
      self.cluster.RunJob(
          'my_bench',
          command='./run_step',
          image='/opt/apps/custom.sqsh',
      )
      self.cluster.headnode_vm.RenderTemplate.assert_called_once_with(
          '/path/to/slurm_job.sbatch.j2',
          '/opt/apps/slurm_job.sbatch',
          {
              'job_name': 'slurm_job',
              'num_workers': 2,
              'output_path': '/opt/apps/slurm-%j.out',
              'env': {},
              'command': './run_step',
              'image': '/opt/apps/custom.sqsh',
          },
          should_log_file=True,
      )

  def testSlurmClusterRunJobMissingCommandRaisesValueError(self):
    self.cluster.headnode_vm = mock.MagicMock()
    with self.assertRaises(ValueError):
      self.cluster.RunJob()

  def testSlurmClusterRunJobMissingJobIdRaisesRemoteCommandError(self):
    self.cluster.headnode_vm = mock.MagicMock()
    self.cluster.headnode_vm.RemoteCommand.return_value = ('bad output', '')
    with mock.patch.object(
        data, 'ResourcePath', return_value='/path/to/slurm_job.sbatch.j2'
    ):
      with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
        self.cluster.RunJob(
            command='./all_reduce_perf',
            gpus_per_node=8,
        )

  def testSlurmClusterMissingHeadnodeRaisesInvalidValue(self):
    workers_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {'GCP': {'machine_type': 'a4-highgpu-8g'}},
    }
    spec_without_headnode = TestGceClusterSpec(
        'test_slurm_no_headnode', workers=workers_config
    )
    with self.assertRaises(errors.Config.InvalidValue):
      cluster.SlurmCluster(spec_without_headnode)

  def testK8sClusterMissingDefaultPoolRaisesInvalidValue(self):
    workers_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {'GCP': {'machine_type': 'a4-highgpu-8g'}},
    }
    spec_without_default_pool = TestGceClusterSpec(
        'test_k8s_no_default_pool', workers=workers_config
    )
    with self.assertRaises(errors.Config.InvalidValue):
      cluster.K8sCluster(spec_without_default_pool)

  def testK8sClusterInit(self):
    headnode_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {'GCP': {'machine_type': 'c3-standard-88'}},
    }
    default_pool_config = {
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
        'test_k8s_spec',
        headnode=headnode_config,
        default_pool=default_pool_config,
        workers=workers_config,
    )
    k8s_cluster = cluster.K8sCluster(k8s_spec)
    k8s_cluster.TYPE = 'a4'
    self.assertFalse(hasattr(k8s_cluster, 'headnode_spec'))
    self.assertEqual(k8s_cluster.default_pool_machine_type, 'e2-standard-16')
    self.assertEqual(k8s_cluster.worker_machine_type, 'a4-highgpu-8g')


if __name__ == '__main__':
  unittest.main()
