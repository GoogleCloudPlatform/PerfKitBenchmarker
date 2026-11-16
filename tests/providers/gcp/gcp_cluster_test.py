"""Unit tests for gcp_cluster.py.
"""

import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import cluster
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_cluster
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources.container_service import container_registry
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
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
    self.enter_context(
        mock.patch.object(
            util, 'GetDefaultProject', return_value='test-project'
        )
    )
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
    with mock.patch.object(
        self.cluster, 'AuthenticateVM', autospec=True
    ) as mock_auth:
      self.cluster._PostCreate()
      mock_auth.assert_called_once_with()
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

  def _CreateA4K8sCluster(self) -> gcp_cluster.A4K8sCluster:
    default_pool_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {
            'GCP': {'machine_type': 'n2d-standard-8', 'zone': 'us-central1-a'}
        },
    }
    workers_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_count': 2,
        'vm_spec': {
            'GCP': {'machine_type': 'a4-highgpu-8g', 'zone': 'us-central1-a'}
        },
    }
    k8s_spec = TestGceClusterSpec(
        'test_a4_k8s_spec',
        default_pool=default_pool_config,
        workers=workers_config,
    )
    k8s_cluster = gcp_cluster.A4K8sCluster(k8s_spec)
    k8s_cluster.name = 'pkb12345'
    return k8s_cluster

  @flagsaver.flagsaver(cluster_scheduler='k8s')
  def testGetClusterClassK8sTypes(self):
    with flagsaver.flagsaver(cluster_type='default'):
      self.assertIs(cluster.GetClusterClass('GCP'), gcp_cluster.GkeK8sCluster)
      self.assertFalse(hasattr(gcp_cluster.GkeK8sCluster, 'gpus_per_node'))
    with flagsaver.flagsaver(cluster_type='a4'):
      self.assertIs(cluster.GetClusterClass('GCP'), gcp_cluster.A4K8sCluster)
      self.assertEqual(gcp_cluster.A4K8sCluster.gpus_per_node, 8)

  def testK8sClusterDeployContainerImagePreprovisionedRegistry(self):
    k8s_cluster = self._CreateA4K8sCluster()
    with flagsaver.flagsaver(
        preprovisioned_container_registry=(
            'us-central1-docker.pkg.dev/my-proj/my-repo'
        )
    ):
      image_uri = k8s_cluster.DeployContainerImage('nccl_cluster')
      self.assertEqual(
          image_uri,
          'us-central1-docker.pkg.dev/my-proj/my-repo/nccl_cluster_a4:latest',
      )
      self.assertEqual(
          k8s_cluster._container_images['nccl_cluster'],
          'us-central1-docker.pkg.dev/my-proj/my-repo/nccl_cluster_a4:latest',
      )

    with flagsaver.flagsaver(
        preprovisioned_container_registry=(
            'us-central1-docker.pkg.dev/my-proj/my-repo/custom:v1'
        )
    ):
      image_uri = k8s_cluster.DeployContainerImage('nccl_cluster')
      self.assertEqual(
          image_uri,
          'us-central1-docker.pkg.dev/my-proj/my-repo/custom:v1',
      )

  def testK8sClusterDeployContainerImageBuildAndPush(self):
    k8s_cluster = self._CreateA4K8sCluster()
    mock_registry = mock.MagicMock()
    mock_registry.GetOrBuild.return_value = (
        'us-central1-docker.pkg.dev/test_proj/pkbrun12345/nccl_cluster_a4'
    )
    mock_registry_cls = mock.MagicMock(return_value=mock_registry)
    with (
        mock.patch.object(
            container_registry,
            'GetContainerRegistryClass',
            return_value=mock_registry_cls,
        ),
        mock.patch.object(
            data, 'ResourcePath', return_value='/path/to/a4.dockerfile'
        ),
        mock.patch('shutil.copyfile') as mock_copyfile,
        mock.patch('os.makedirs'),
    ):
      image_uri = k8s_cluster.DeployContainerImage('nccl_cluster')
      self.assertEqual(
          image_uri,
          'us-central1-docker.pkg.dev/test_proj/pkbrun12345/nccl_cluster_a4',
      )
      mock_copyfile.assert_called_once()
      mock_registry.Create.assert_called_once_with()
      mock_registry.GetOrBuild.assert_called_once()
      k8s_cluster._DeleteDependencies()
      mock_registry.Delete.assert_called_once_with()

  def testA4K8sClusterRunJobSuccessAndCleanup(self):
    k8s_cluster = self._CreateA4K8sCluster()
    k8s_cluster._container_images['nccl_cluster'] = (
        'us-central1-docker.pkg.dev/my-proj/my-repo/nccl_cluster_a4:latest'
    )
    self.assertEqual(
        k8s_cluster.GetResourceMetadata()['container_images'],
        {
            'nccl_cluster': (
                'us-central1-docker.pkg.dev/my-proj/my-repo/'
                'nccl_cluster_a4:latest'
            )
        },
    )
    with (
        mock.patch.object(
            data, 'ResourceExists', return_value=False
        ) as mock_resource_exists,
        mock.patch.object(
            kubernetes_commands,
            'ApplyManifest',
            return_value=iter(['service/k8s-job', 'job.batch/k8s-job']),
        ) as mock_apply,
        mock.patch.object(
            kubernetes_commands,
            'WaitForResourceForMultiConditions',
            return_value='condition=complete',
        ) as mock_wait,
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('nccl k8s output', '', 0),
                ('deleted', '', 0),
            ],
        ) as mock_kubectl,
    ):
      stdout, stderr = k8s_cluster.RunJob(
          'nccl_cluster',
          command='./all_reduce_perf',
          env={'NCCL_DEBUG': 'INFO'},
          timeout=3600,
          gpus_per_node=8,
      )
      self.assertEqual(stdout, 'nccl k8s output')
      self.assertEqual(stderr, '')
      mock_resource_exists.assert_called_once_with(
          'nccl_cluster/k8s_job.yaml.j2'
      )
      mock_apply.assert_called_once_with(
          'cluster/k8s_job.yaml.j2',
          job_name='k8s-job',
          num_workers=2,
          worker_machine_type='a4-highgpu-8g',
          env={
              'LD_LIBRARY_PATH': (
                  '/usr/local/gib/lib64:/usr/local/nvidia/lib64'
              ),
              'OMPI_MCA_btl_tcp_if_include': 'eth0',
              'NCCL_SOCKET_IFNAME': 'eth0',
              'NCCL_DEBUG': 'INFO',
          },
          command='./all_reduce_perf',
          gpus_per_node=8,
          resource_claim_template='default-rdma',
          image=(
              'us-central1-docker.pkg.dev/my-proj/my-repo/'
              'nccl_cluster_a4:latest'
          ),
      )
      mock_wait.assert_called_once_with(
          'job/k8s-job',
          ['condition=complete', 'condition=failed'],
          timeout=3600,
      )
      self.assertSequenceEqual(
          [
              mock.call([
                  'logs',
                  '-l',
                  'job-name=k8s-job,batch.kubernetes.io/job-completion-index=0',
                  '--tail=-1',
              ]),
              mock.call(
                  [
                      'delete',
                      'service/k8s-job',
                      'job.batch/k8s-job',
                      '--ignore-not-found=true',
                      '--wait=true',
                  ],
                  raise_on_failure=False,
              ),
          ],
          mock_kubectl.mock_calls,
      )

  def testK8sClusterRunJobMissingImageRaisesValueError(self):
    k8s_cluster = self._CreateA4K8sCluster()
    with mock.patch.object(data, 'ResourceExists', return_value=False):
      with self.assertRaises(ValueError):
        k8s_cluster.RunJob('nccl_cluster', command='./all_reduce_perf')

  def testK8sClusterRunJobFailedConditionRaisesRemoteCommandError(self):
    k8s_cluster = self._CreateA4K8sCluster()
    k8s_cluster._container_images['nccl_cluster'] = (
        'us-central1-docker.pkg.dev/my-proj/my-repo/nccl_cluster_a4:latest'
    )
    with (
        mock.patch.object(data, 'ResourceExists', return_value=False),
        mock.patch.object(
            kubernetes_commands,
            'ApplyManifest',
            return_value=iter(['service/k8s-job', 'job.batch/k8s-job']),
        ),
        mock.patch.object(
            kubernetes_commands,
            'WaitForResourceForMultiConditions',
            return_value='condition=failed',
        ),
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            side_effect=[
                ('rank 0 error', 'fatal stderr', 0),
                ('deleted', '', 0),
            ],
        ) as mock_kubectl,
    ):
      with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
        k8s_cluster.RunJob('nccl_cluster', command='./all_reduce_perf')
      self.assertEqual(mock_kubectl.call_count, 2)

  def testK8sClusterRunJobDeletesResourcesOnWaitFailure(self):
    k8s_cluster = self._CreateA4K8sCluster()
    k8s_cluster._container_images['nccl_cluster'] = (
        'us-central1-docker.pkg.dev/my-proj/my-repo/nccl_cluster_a4:latest'
    )
    with (
        mock.patch.object(data, 'ResourceExists', return_value=False),
        mock.patch.object(
            kubernetes_commands,
            'ApplyManifest',
            return_value=iter(['service/k8s-job', 'job.batch/k8s-job']),
        ),
        mock.patch.object(
            kubernetes_commands,
            'WaitForResourceForMultiConditions',
            side_effect=errors.VmUtil.IssueCommandTimeoutError('timed out'),
        ),
        mock.patch.object(
            kubectl, 'RunKubectlCommand', return_value=('deleted', '', 0)
        ) as mock_kubectl,
    ):
      with self.assertRaises(errors.VmUtil.IssueCommandTimeoutError):
        k8s_cluster.RunJob('nccl_cluster', command='./all_reduce_perf')
      mock_kubectl.assert_called_once_with(
          [
              'delete',
              'service/k8s-job',
              'job.batch/k8s-job',
              '--ignore-not-found=true',
              '--wait=true',
          ],
          raise_on_failure=False,
      )

  def testGkeK8sClusterRegionalZoneRaisesInvalidValue(self):
    default_pool_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {
            'GCP': {'machine_type': 'n2d-standard-8', 'zone': 'us-central1'}
        },
    }
    workers_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_count': 2,
        'vm_spec': {
            'GCP': {'machine_type': 'a4-highgpu-8g', 'zone': 'us-central1'}
        },
    }
    k8s_spec = TestGceClusterSpec(
        'test_regional_k8s_spec',
        default_pool=default_pool_config,
        workers=workers_config,
    )
    with self.assertRaises(errors.Config.InvalidValue):
      gcp_cluster.A4K8sCluster(k8s_spec)

  def testGkeK8sClusterPostCreateGetsCredentials(self):
    k8s_cluster = self._CreateA4K8sCluster()
    with mock.patch.object(
        util.GcloudCommand, 'IssueRetryable'
    ) as mock_issue_retryable:
      k8s_cluster._PostCreate()
      mock_issue_retryable.assert_called_once()

  def testA4K8sClusterRenderClusterConfigSpotAndReservation(self):
    with flagsaver.flagsaver(
        gce_preemptible_vms=True,
        gcp_preemptible_status_bucket='test-bucket',
        gce_reservation_id=None,
    ):
      k8s_cluster = self._CreateA4K8sCluster()
      k8s_cluster._RenderClusterConfig()
      self.assertIn('spot: true', k8s_cluster._config)
      self.assertIn('spot: $(vars.spot)', k8s_cluster._config)
      self.assertIn('name: default-rdma', k8s_cluster._config)
      self.assertIn('deviceClassName: mrdma.google.com', k8s_cluster._config)
      self.assertNotIn('reservation_affinity:', k8s_cluster._config)

    with flagsaver.flagsaver(
        gce_preemptible_vms=False,
        gce_reservation_id='test-reservation',
    ):
      k8s_cluster = self._CreateA4K8sCluster()
      k8s_cluster._RenderClusterConfig()
      self.assertIn('spot: false', k8s_cluster._config)
      self.assertIn('spot: $(vars.spot)', k8s_cluster._config)
      self.assertIn('reservation: test-reservation', k8s_cluster._config)
      self.assertIn(
          'consume_reservation_type: SPECIFIC_RESERVATION', k8s_cluster._config
      )
      self.assertIn('- name: $(vars.reservation)', k8s_cluster._config)

    with flagsaver.flagsaver(
        gce_preemptible_vms=True,
        gcp_preemptible_status_bucket='test-bucket',
        gce_reservation_id='test-reservation',
    ):
      with self.assertRaises(errors.Config.InvalidValue):
        self._CreateA4K8sCluster()

  def testA4ClusterRenderClusterConfigSpotAndReservation(self):
    vm_config = {
        'cloud': 'GCP',
        'os_type': 'ubuntu2004',
        'vm_spec': {
            'GCP': {'machine_type': 'a4-highgpu-8g', 'zone': 'us-central1-a'}
        },
    }
    a4_spec = TestGceClusterSpec(
        'test_a4_slurm_spec', headnode=vm_config, workers=vm_config
    )
    with flagsaver.flagsaver(
        gce_preemptible_vms=True,
        gcp_preemptible_status_bucket='test-bucket',
        gce_reservation_id=None,
    ):
      slurm_cluster = gcp_cluster.A4Cluster(a4_spec)
      slurm_cluster.name = 'pkb12345'
      slurm_cluster._RenderClusterConfig()
      self.assertIn('a4h_reservation_name: ""', slurm_cluster._config)
      self.assertIn('a4h_enable_spot_vm: true', slurm_cluster._config)

    with flagsaver.flagsaver(
        gce_preemptible_vms=False,
        gce_reservation_id='test-reservation',
    ):
      slurm_cluster = gcp_cluster.A4Cluster(a4_spec)
      slurm_cluster.name = 'pkb12345'
      slurm_cluster._RenderClusterConfig()
      self.assertIn(
          'a4h_reservation_name: "test-reservation"',
          slurm_cluster._config,
      )
      self.assertIn('a4h_enable_spot_vm: false', slurm_cluster._config)

    with flagsaver.flagsaver(
        gce_preemptible_vms=True,
        gcp_preemptible_status_bucket='test-bucket',
        gce_reservation_id='test-reservation',
    ):
      with self.assertRaises(errors.Config.InvalidValue):
        gcp_cluster.A4Cluster(a4_spec)


if __name__ == '__main__':
  unittest.main()
