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

import os

from absl import flags as absl_flags
import jinja2
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import cluster
from perfkitbenchmarker import data
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import flags
from perfkitbenchmarker.providers.gcp import util


FLAGS = absl_flags.FLAGS
_CONFIG_FILE_NAME = 'cluster.yaml'


class GceClusterSpec(cluster.BaseClusterSpec):
  """Class to represent a GCE cluster."""

  CLOUD = provider_info.GCP


class GceCluster(cluster.BaseCluster):
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
    self.project: str = cluster_spec.workers.vm_spec.project
    self._config_path: str = os.path.join(
        vm_util.GetTempDir(), _CONFIG_FILE_NAME)
    self._pub_key, _, _ = vm_util.IssueCommand(
        ['cat', vm_util.GetPublicKeyPath()]
    )
    self.nfs_path: str = '/opt/apps'

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    self._RenderClusterConfig()

  def _RenderClusterConfig(self):
    """Render the config file that will be used to create the cluster."""
    tags = util.GetDefaultTags(FLAGS.timeout_minutes)
    controller_tags = tags.copy()
    controller_tags['ssh-keys'] = 'perfkit:' + self._pub_key
    compute_tags = tags.copy()
    compute_tags['ssh-keys'] = 'root:' + self._pub_key
    with open(data.ResourcePath(self.template)) as content:
      template = jinja2.Template(
          content.read(), undefined=jinja2.StrictUndefined
      )
      # Disable SMT when possible (default in cluster toolkit)
      threads_per_core = 0
      if FLAGS['disable_smt'].present:
        threads_per_core = 1 if FLAGS.disable_smt else 2
      self._config = template.render(
          name=self.name,
          zone=self.zone,
          region=util.GetRegionFromZone(self.zone),
          num_workers=self.num_workers,
          worker_machine_type=self.worker_machine_type,
          headnode_machine_type=self.headnode_machine_type,
          image_family=self.workers_spec.image_family,
          image_project=self.workers_spec.image_project,
          project=self.project,
          # boot disk of headnode is also mounted as NFS
          nfs_size=self.headnode_spec.boot_disk_size,
          compute_tags=compute_tags,
          controller_tags=controller_tags,
          enabe_spot_vm=FLAGS.gce_preemptible_vms,
          threads_per_core=threads_per_core,
      )

  def _Create(self):
    """Create GCP cluster with cluster toolkit."""
    with open(self._config_path, 'w') as config_file:
      config_file.write(self._config)
    vm_util.IssueCommand([
        flags.GCLUSTER_PATH.value,
        'deploy',
        self._config_path,
        '--force',
        '--auto-approve',
        f'--out={vm_util.GetTempDir()}',
        '-l',
        'IGNORE'
    ])

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

  def _Delete(self):
    """Delete GCP cluster with cluster toolkit."""
    vm_util.IssueCommand([
        flags.GCLUSTER_PATH.value,
        'destroy',
        f'{vm_util.GetTempDir()}/{self.name}',
        '--auto-approve',
    ])

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
      vm.has_private_key: bool = True


class H4dCluster(GceCluster):
  """Class representing a H4D cluster."""

  DEFAULT_TEMPLATE = 'cluster/h4d.yaml.j2'
  TYPE = 'h4d'
