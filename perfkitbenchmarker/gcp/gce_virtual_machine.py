# Copyright 2014 Google Inc. All rights reserved.
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
"""Class to represent a GCE Virtual Machine object.

Zones:
run 'gcutil listzones'
Machine Types:
run 'gcutil listmachinetypes'
Images:
run 'gcutil listimages'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import json
import re

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.gcp import gce_disk
from perfkitbenchmarker.gcp import util


flags.DEFINE_integer('gce_num_local_ssds', 0,
                     'The number of ssds that should be added to the VM. Note '
                     'that this is currently only supported in certain zones '
                     '(see https://cloud.google.com/compute/docs/local-ssd).')
flags.DEFINE_string('gcloud_scopes', None, 'If set, space-separated list of '
                    'scopes to apply to every created machine')

FLAGS = flags.FLAGS

SET_INTERRUPTS_SH = 'set-interrupts.sh'
BOOT_DISK_SIZE_GB = 10
BOOT_DISK_TYPE = disk.STANDARD
NVME = 'nvme'
SCSI = 'SCSI'


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  def __init__(self, vm_spec):
    """Initialize a GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(GceVirtualMachine, self).__init__(vm_spec)
    disk_spec = disk.BaseDiskSpec(BOOT_DISK_SIZE_GB, BOOT_DISK_TYPE, None)
    self.boot_disk = gce_disk.GceDisk(
        disk_spec, self.name, self.zone, self.project, self.image)
    self.max_local_disks = FLAGS.gce_num_local_ssds
    self.local_disk_counter = 0

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.boot_disk.Create()

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    self.boot_disk.Delete()

  def _Create(self):
    """Create a GCE VM instance."""
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')
    with vm_util.NamedTemporaryFile(dir=vm_util.GetTempDir(),
                                    prefix='key-metadata') as tf:
      tf.write('%s:%s\n' % (self.user_name, public_key))
      tf.close()
      create_cmd = [FLAGS.gcloud_path,
                    'compute',
                    'instances',
                    'create', self.name,
                    '--disk',
                    'name=%s' % self.boot_disk.name,
                    'boot=yes',
                    'mode=rw',
                    '--machine-type', self.machine_type,
                    '--tags=perfkitbenchmarker',
                    '--maintenance-policy', 'TERMINATE',
                    '--no-restart-on-failure',
                    '--metadata-from-file',
                    'sshKeys=%s' % tf.name,
                    '--metadata',
                    'owner=%s' % FLAGS.owner]
      ssd_interface_option = NVME if NVME in self.image else SCSI
      for _ in range(self.max_local_disks):
        create_cmd.append('--local-ssd')
        create_cmd.append('interface=%s' % ssd_interface_option)
      if FLAGS.gcloud_scopes:
        create_cmd.extend(['--scopes'] +
                          re.split(r'[,; ]', FLAGS.gcloud_scopes))
      create_cmd.extend(util.GetDefaultGcloudFlags(self))
      vm_util.IssueCommand(create_cmd)

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    getinstance_cmd = [FLAGS.gcloud_path,
                       'compute',
                       'instances',
                       'describe', self.name]
    getinstance_cmd.extend(util.GetDefaultGcloudFlags(self))
    stdout, _, _ = vm_util.IssueCommand(getinstance_cmd)
    response = json.loads(stdout)
    network_interface = response['networkInterfaces'][0]
    self.internal_ip = network_interface['networkIP']
    self.ip_address = network_interface['accessConfigs'][0]['natIP']

  def _Delete(self):
    """Delete a GCE VM instance."""
    delete_cmd = [FLAGS.gcloud_path,
                  'compute',
                  'instances',
                  'delete', self.name]
    delete_cmd.extend(util.GetDefaultGcloudFlags(self))
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the VM exists."""
    getinstance_cmd = [FLAGS.gcloud_path,
                       'compute',
                       'instances',
                       'describe', self.name]
    getinstance_cmd.extend(util.GetDefaultGcloudFlags(self))
    stdout, _, _ = vm_util.IssueCommand(getinstance_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    # Get the names for the disk(s) we are going to create.
    if disk_spec.disk_type == disk.LOCAL:
      new_count = self.local_disk_counter + disk_spec.num_striped_disks
      if new_count > self.max_local_disks:
        raise errors.Error('Not enough local disks.')
      disk_names = ['local-ssd-%d' % i
                    for i in range(self.local_disk_counter, new_count)]
      self.local_disk_counter = new_count
    else:
      disk_names = ['%s-data-%d-%d' % (self.name, len(self.scratch_disks), i)
                    for i in range(disk_spec.num_striped_disks)]

    # Instantiate the disk(s).
    disks = [gce_disk.GceDisk(disk_spec, name, self.zone, self.project)
             for name in disk_names]

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          disks on the VM (e.g. '/dev/sdb').
    """
    return ['/dev/disk/by-id/google-local-ssd-%d' % i
            for i in range(self.max_local_disks)]

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM via 'gcloud compute instances add-metadata'."""
    if not kwargs:
      return
    cmd = [FLAGS.gcloud_path, 'compute', 'instances', 'add-metadata',
           self.name, '--metadata']
    for key, value in kwargs.iteritems():
      cmd.append('{0}={1}'.format(key, value))
    cmd.extend(util.GetDefaultGcloudFlags(self))
    vm_util.IssueCommand(cmd)

class ContainerizedDebianBasedGceVirtualMachine(GceVirtualMachine,
                               linux_virtual_machine.ContainerizedDebianMixin):
  pass

class DebianBasedGceVirtualMachine(GceVirtualMachine,
                                   linux_virtual_machine.DebianMixin):
  pass


class RhelBasedGceVirtualMachine(GceVirtualMachine,
                                 linux_virtual_machine.RhelMixin):
  pass
