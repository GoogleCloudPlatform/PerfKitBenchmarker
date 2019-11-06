# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Resource encapsulating provisioned cloud NFS services.

Defines a resource for use in other benchmarks such as SpecSFS2014 and FIO.

Example --benchmark_config_file:

nfs_10_tb: &nfs_10_tb
  AWS:
    disk_type: nfs
    mount_point: /scratch

specsfs:
  name: specsfs2014
  flags:
    specsfs2014_num_runs: 1
    specsfs2014_load: 1
  vm_groups:
    clients:
      disk_spec: *nfs_10_tb
      vm_count: 1
      os_type: rhel
    gluster_servers:
      vm_count: 0
"""

import abc
import logging
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import resource

flags.DEFINE_string('nfs_tier', None, 'NFS Mode')
flags.DEFINE_string('nfs_version', None, 'NFS Version')

FLAGS = flags.FLAGS

_MOUNT_NFS_RE = re.compile(r'.*type nfs \((.*?)\)', re.MULTILINE)

UNMANAGED = 'Unmanaged'


def GetNfsServiceClass(cloud):
  """Get the NFS service corresponding to the cloud.

  Args:
    cloud: The name of the cloud to supply the NFS service.

  Returns:
    The NFS service class for this cloud.

  Raises:
    NotImplementedError: No service found for this cloud.
  """
  return resource.GetResourceClass(BaseNfsService, CLOUD=cloud)


class BaseNfsService(resource.BaseResource):
  """Object representing an NFS Service."""

  # subclasses must override this with a list or tuple for acceptable
  # "nfs_tier" values if applicable.
  NFS_TIERS = None
  RESOURCE_TYPE = 'BaseNfsService'
  DEFAULT_NFS_VERSION = None
  DEFAULT_TIER = None

  def __init__(self, disk_spec, zone):
    super(BaseNfsService, self).__init__()
    self.disk_spec = disk_spec
    self.zone = zone
    self.server_directory = '/'
    self.nfs_tier = FLAGS.nfs_tier or self.DEFAULT_TIER
    if self.nfs_tier and self.NFS_TIERS and self.nfs_tier not in self.NFS_TIERS:
      # NFS service does not have to have a list of nfs_tiers nor does it have
      # to be implemented by a provider
      raise errors.Config.InvalidValue(
          ('nfs_tier "%s" not in acceptable list "%s" '
           'for cloud %s') % (self.nfs_tier, self.NFS_TIERS, self.CLOUD))
    logging.debug('%s NFS service with nfs_tier %s zone %s default version %s',
                  self.CLOUD, self.nfs_tier, self.zone,
                  self.DEFAULT_NFS_VERSION)

  def CreateNfsDisk(self):
    mount_point = '%s:%s' % (self.GetRemoteAddress(), self.server_directory)
    return disk.NfsDisk(self.disk_spec, mount_point, self.DEFAULT_NFS_VERSION,
                        self.nfs_tier)

  @abc.abstractmethod
  def _IsReady(self):
    """Boolean function to determine if disk is NFS mountable."""
    pass

  @abc.abstractmethod
  def GetRemoteAddress(self):
    """The NFS server's address."""
    pass


class StaticNfsService(BaseNfsService):
  """Object allowing VMs to connect to a preprovisioned NFS endpoint."""
  CLOUD = 'Static'

  def __init__(self, disk_spec):
    super(StaticNfsService, self).__init__(disk_spec, None)
    self.ip_address = disk_spec.nfs_ip_address
    self.server_directory = disk_spec.nfs_directory or '/'

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def CreateNfsDisk(self):
    mount_point = '%s:/%s' % (self.GetRemoteAddress(), self.server_directory)
    return disk.NfsDisk(self.disk_spec, mount_point, None, None)

  def _IsReady(self):
    """Boolean function to determine if disk is NFS mountable."""
    return True

  def GetRemoteAddress(self):
    """The NFS server's address."""
    return self.ip_address


class UnmanagedNfsService(BaseNfsService):
  """Object allowing VMs to connect to a local NFS disk."""
  CLOUD = UNMANAGED

  # Allows anybody to write to the NFS mount.
  _EXPORT_FS_COMMAND = ' && '.join([
      'sudo mkdir -p {export_dir}',
      'sudo chown $USER:$USER {export_dir}',
      'sudo chmod 777 {export_dir}',
      'echo "{export_dir} *(rw,sync,no_subtree_check,no_root_squash)" | '
      'sudo tee -a /etc/exports',
      'sudo exportfs -a'
  ])

  _NFS_NAME = {
      os_types.RHEL: 'nfs-server',
      os_types.DEBIAN: 'nfs-kernel-server',
  }
  _NFS_RESTART_CMD = 'sudo systemctl restart {nfs_name}'

  def __init__(self, disk_spec, server_vm):
    super(UnmanagedNfsService, self).__init__(disk_spec, None)
    self.server_vm = server_vm
    # Path on the server to export. Must be different from mount_point.
    self.server_directory = disk_spec.device_path or '/pkb-nfs-server-directory'
    logging.info('Exporting server directory %s', self.server_directory)
    assert self.server_directory != disk_spec.mount_point, \
        'export server directory must be different from mount point'

  def GetRemoteAddress(self):
    """The NFS server's address."""
    return self.server_vm.internal_ip

  def _ExportNfsDir(self, export_dir_path):
    """Export a directory on the NFS server to be shared with NFS clients.

    Args:
      export_dir_path: Path to the directory to export.
    """
    self.server_vm.RemoteCommand(
        self._EXPORT_FS_COMMAND.format(export_dir=export_dir_path))
    nfs_name = self._NFS_NAME[self.server_vm.BASE_OS_TYPE]
    self.server_vm.RemoteCommand(
        self._NFS_RESTART_CMD.format(nfs_name=nfs_name))

  def _Create(self):
    assert self.server_vm, 'NFS server VM not created.'
    self.server_vm.Install('nfs_server')
    self._ExportNfsDir(self.server_directory)

  def _Delete(self):
    pass

  def _IsReady(self):
    """Boolean function to determine if disk is NFS mountable."""
    return True
