# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Class to represent a GCE cluster."""

from collections.abc import Mapping
import os
from typing import Any, Tuple

from absl import flags as absl_flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import cluster
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import flags
from perfkitbenchmarker.providers.gcp import util


FLAGS = absl_flags.FLAGS
_CONFIG_FILE_NAME = 'cluster.yaml'
_DEPLOY_TIMEOUT = 1800


def _DeployGcluster(config: str, config_path: str) -> None:
  """Writes rendered config and deploys a GCP cluster with Cluster Toolkit."""
  with open(config_path, 'w') as config_file:
    config_file.write(config)
  vm_util.IssueCommand(
      [
          flags.GCLUSTER_PATH.value,
          'deploy',
          config_path,
          '--force',
          '--auto-approve',
          f'--out={vm_util.GetTempDir()}',
          '-l',
          'IGNORE',
      ],
      timeout=_DEPLOY_TIMEOUT,
  )


def _DestroyGcluster(cluster_name: str) -> None:
  """Destroys a GCP cluster provisioned with Cluster Toolkit."""
  vm_util.IssueCommand(
      [
          flags.GCLUSTER_PATH.value,
          'destroy',
          f'{vm_util.GetTempDir()}/{cluster_name}',
          '--auto-approve',
      ],
      timeout=600,
  )


class GceClusterSpec(cluster.BaseClusterSpec):
  """Class to represent a GCE cluster."""

  CLOUD = provider_info.GCP


class GceCluster(cluster.SlurmCluster):
  """Class to represent a GCE cluster."""

  CLOUD = provider_info.GCP
  # TODO(yuyanting): Add cluster type as attributes.
  # We probably will need to implement different type of clusters with
  # different default templates and Render functions.
  DEFAULT_TEMPLATE = 'cluster/cluster_toolkit.yaml.j2'

  def __init__(self, cluster_spec: GceClusterSpec):
    """Initialize GceCluster class.

    Args:
      cluster_spec: cluster.GceClusterSpec object.
    """
    super().__init__(cluster_spec)
    if FLAGS.gce_preemptible_vms and flags.GCE_RESERVATION_ID.value:
      raise errors.Config.InvalidValue(
          'Cannot specify both --gce_preemptible_vms and --gce_reservation_id.'
      )
    self.project: str = cluster_spec.workers.vm_spec.project  # pyrefly: ignore[missing-attribute]
    self._config_path: str = os.path.join(
        vm_util.GetTempDir(), _CONFIG_FILE_NAME)
    self._pub_key, _, _ = vm_util.IssueCommand(
        ['cat', vm_util.GetPublicKeyPath()]
    )
    self.nfs_path: str = '/opt/apps'

  def _GetTemplateParameters(self):
    """Returns parameters that will be used to render the cluster config."""
    tags = util.GetDefaultTags(FLAGS.timeout_minutes)
    controller_tags = tags.copy()
    controller_tags['ssh-keys'] = 'perfkit:' + self._pub_key
    compute_tags = tags.copy()
    compute_tags['ssh-keys'] = 'root:' + self._pub_key

    # Disable SMT when possible (default in cluster toolkit)
    if FLAGS['disable_smt'].present and FLAGS.disable_smt:
      threads_per_core = 1
    else:
      threads_per_core = 2

    return {
        'name': self.name,
        'zone': self.zone,
        'region': util.GetRegionFromZone(self.zone),
        'num_workers': self.num_workers,
        'worker_machine_type': self.worker_machine_type,
        'headnode_machine_type': self.headnode_machine_type,
        'image_family': self.workers_spec.image_family,  # pyrefly: ignore[missing-attribute]
        'image_project': self.workers_spec.image_project,  # pyrefly: ignore[missing-attribute]
        'project': self.project,
        # boot disk of headnode is also mounted as NFS
        'nfs_size': self.headnode_spec.boot_disk_size,
        'compute_tags': compute_tags,
        'controller_tags': controller_tags,
        'enabe_spot_vm': FLAGS.gce_preemptible_vms,
        'enable_spot_vm': FLAGS.gce_preemptible_vms,
        'reservation_id': flags.GCE_RESERVATION_ID.value,
        'threads_per_core': threads_per_core,
    }

  def _Create(self):
    """Create GCP cluster with cluster toolkit."""
    _DeployGcluster(self._config, self._config_path)

  def _Delete(self):
    """Delete GCP cluster with cluster toolkit."""
    _DestroyGcluster(self.name)

  def _PostCreate(self):
    """Post create actions after GCP cluster is created.

    The method does the following:
    1. Create VM objects and backfill VM information based on VM name.
    2. Prepare VM environment.
    3. Configure SSH on both headnode and workers.
    """
    def _UpdateHeadNode(vm):
      vm.name = f'{self.name}-controller'

    self.headnode_vm = self.BackfillVm(self.headnode_spec, _UpdateHeadNode)
    self._WaitForClusterReady()

    for i in range(self.num_workers):

      def _UpdateWorker(vm):
        vm.name = f'{self.name}-computenodeset-{i}'  # pylint: disable=cell-var-from-loop
        vm.user_name = 'root'
        if self.workers_static_disk:
          vm.disks = [self.workers_static_disk]

      self.worker_vms.append(
          self.BackfillVm(
              self.workers_spec,
              _UpdateWorker,
          )
      )
    # The workers may provision as we issue this command for the first time.
    self.RemoteCommand(
        'sudo sed -i '
        '"s/PermitRootLogin no/PermitRootLogin yes/g" '
        '/etc/ssh/sshd_config',
        timeout=600,
    )
    self.RemoteCommand('sudo service sshd restart', timeout=120)
    self.headnode_vm.RemoteCommand(f'sudo chmod -R 755 {self.nfs_path}')
    self.vms = [self.headnode_vm] + self.worker_vms

    def _SetupEnvironment(vm):
      vm.RemoteCommand(
          f'sudo ln -s {self.nfs_path} {linux_packages.INSTALL_DIR}'
      )
      vm.PrepareVMEnvironment()

    background_tasks.RunThreaded(_SetupEnvironment, self.vms)
    vm_util.GenerateSSHConfig(
        self.vms,
        {
            'headnode': [self.headnode_vm],
            'worker': self.worker_vms,
        },
    )
    super()._PostCreate()

  def AuthenticateVM(self):
    """Authenticate all VMs in the cluster to access each other."""
    for vm in self.vms:
      if vm.has_private_key:
        continue
      vm.RemoteHostCopy(
          vm_util.GetPrivateKeyPath(), linux_virtual_machine.REMOTE_KEY_PATH
      )
      vm.RemoteCommand(
          'echo "Host *\n  StrictHostKeyChecking no\n  User=root\n" >'
          ' ~/.ssh/config'
      )
      vm.RemoteCommand('chmod 600 ~/.ssh/config')
      vm.has_private_key = True


class H4dCluster(GceCluster):
  """Class representing a H4D cluster."""

  DEFAULT_TEMPLATE = 'cluster/h4d.yaml.j2'
  TYPE = 'h4d'


class A4Cluster(GceCluster):
  """Class representing a A4 cluster."""

  DEFAULT_TEMPLATE = 'cluster/a4.yaml.j2'
  TYPE = 'a4'
  gpus_per_node: int = 8

  def _PostCreate(self):
    super()._PostCreate()

    def _InstallNVContainerToolkit(vm):
      vm.Install('nvidia_container_toolkit')
      vm.RemoteCommand('sudo nvidia-smi -pm 1')

    background_tasks.RunThreaded(_InstallNVContainerToolkit, self.worker_vms)


class GkeK8sCluster(cluster.K8sCluster):
  """Class representing a GKE cluster provisioned via Cluster Toolkit."""

  CLOUD = provider_info.GCP
  DEFAULT_TEMPLATE = 'cluster/gke/gke_default.yaml.j2'

  def __init__(self, cluster_spec: GceClusterSpec):
    super().__init__(cluster_spec)
    if self.zone and util.IsRegion(self.zone):
      raise errors.Config.InvalidValue(
          f'GkeK8sCluster requires a zone, got region {self.zone}.'
      )
    if FLAGS.gce_preemptible_vms and flags.GCE_RESERVATION_ID.value:
      raise errors.Config.InvalidValue(
          'Cannot specify both --gce_preemptible_vms and --gce_reservation_id.'
      )
    self.project: str = (
        cluster_spec.workers.vm_spec.project  # pyrefly: ignore[missing-attribute]
        or util.GetDefaultProject()
    )
    self._config_path: str = os.path.join(
        vm_util.GetTempDir(), _CONFIG_FILE_NAME
    )

  def _GetTemplateParameters(self) -> dict[str, Any]:
    return {
        'name': self.name,
        'zone': self.zone,
        'region': util.GetRegionFromZone(self.zone),
        'cluster_availability_type': 'ZONAL',
        'num_workers': self.num_workers,
        'worker_machine_type': self.worker_machine_type,
        'default_pool_machine_type': self.default_pool_machine_type,
        'project': self.project,
        'enable_spot_vm': FLAGS.gce_preemptible_vms,
        'reservation_id': flags.GCE_RESERVATION_ID.value,
    }

  def _Create(self):
    """Create GKE cluster with cluster toolkit."""
    _DeployGcluster(self._config, self._config_path)

  def _Delete(self):
    """Delete GKE cluster with cluster toolkit."""
    _DestroyGcluster(self.name)

  def _PostCreate(self):
    """Configures kubeconfig for the created GKE cluster."""
    super()._PostCreate()
    cmd = util.GcloudCommand(
        self, 'container', 'clusters', 'get-credentials', self.name
    )
    env = os.environ.copy()
    env['KUBECONFIG'] = self.kubeconfig
    cmd.IssueRetryable(env=env)


class A4K8sCluster(GkeK8sCluster):
  """Class representing a GKE A4 cluster."""

  DEFAULT_TEMPLATE = 'cluster/gke/a4.yaml.j2'
  TYPE = 'a4'
  gpus_per_node = 8
  # All A4 gIB NCCL tuning variables (NCCL_NET=gIB, NCCL_TUNER_CONFIG_PATH,
  # etc.) are already baked into data/nccl_cluster/a4.dockerfile, which sets
  # OMPI_MCA_btl_tcp_if_include=enp0s19 and NCCL_SOCKET_IFNAME=enp0s19,enp192s20
  # for bare-VM/Slurm host NICs. Inside GKE pods, the primary pod network
  # interface is virtualized as eth0 (while the 8 RDMA NICs attach via DRANET),
  # so we override those two socket interface variables to eth0 and include the
  # gIB and GKE NVIDIA driver library paths in LD_LIBRARY_PATH.
  DEFAULT_JOB_ENV = {
      'LD_LIBRARY_PATH': '/usr/local/gib/lib64:/usr/local/nvidia/lib64',
      'OMPI_MCA_btl_tcp_if_include': 'eth0',
      'NCCL_SOCKET_IFNAME': 'eth0',
  }

  def RunJob(
      self,
      benchmark_name: str | None = None,
      job_name: str = 'k8s_job',
      command: str | None = None,
      env: Mapping[str, str] | None = None,
      timeout: int = 7200,
      **template_vars: Any,
  ) -> Tuple[str, str]:
    template_vars.setdefault('resource_claim_template', 'default-rdma')
    return super().RunJob(
        benchmark_name=benchmark_name,
        job_name=job_name,
        command=command,
        env=env,
        timeout=timeout,
        **template_vars,
    )
