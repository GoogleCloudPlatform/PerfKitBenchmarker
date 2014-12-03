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

import gflags as flags

from perfkitbenchmarker import disk
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.gcp import gce_disk
from perfkitbenchmarker.gcp import util


flags.DEFINE_integer('gce_num_local_ssds', 0,
                     'The number of ssds that should be added to the VM. Note '
                     'that this is currently only supported in certain zones '
                     '(see https://cloud.google.com/compute/docs/local-ssd).')
FLAGS = flags.FLAGS

SET_INTERRUPTS_SH = 'set-interrupts.sh'
BOOT_DISK_SIZE_GB = 10
BOOT_DISK_TYPE = 'pd-standard'
DRIVE_START_LETTER = 'b'
NVME = 'nvme'
SCSI = 'SCSI'


class GceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Google Compute Engine Virtual Machine."""

  instance_counter = 0

  def __init__(self, vm_spec):
    """Initialize a GCE virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(GceVirtualMachine, self).__init__(vm_spec)
    self.name = 'perfkit-%s-%s' % (FLAGS.run_uri, self.instance_counter)
    GceVirtualMachine.instance_counter += 1
    disk_spec = disk.BaseDiskSpec(BOOT_DISK_SIZE_GB, BOOT_DISK_TYPE, None)
    self.boot_disk = gce_disk.GceDisk(
        disk_spec, self.name, self.zone, self.project, self.image)
    self.num_ssds = FLAGS.gce_num_local_ssds

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.boot_disk.Create()

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    self.boot_disk.Delete()

  def _Create(self):
    """Create a GCE VM instance."""
    super(GceVirtualMachine, self)._Create()
    with open(self.ssh_public_key) as f:
      key = f.read().rstrip('\n').replace(' ', '\\ ')
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
                    '--metadata',
                    'sshKeys=%s:%s' % (self.user_name, key),
                    'owner=%s' % FLAGS.owner]
      ssd_interface_option = NVME if NVME in self.image else SCSI
      for _ in range(self.num_ssds):
        create_cmd.append('--local-ssd')
        create_cmd.append('interface=%s' % ssd_interface_option)
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
    stdout, _, _ = vm_util.IssueCommand(getinstance_cmd)
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
    name = '%s-scratch-%s' % (self.name, len(self.scratch_disks))
    scratch_disk = gce_disk.GceDisk(disk_spec, name, self.zone, self.project)
    self.scratch_disks.append(scratch_disk)

    scratch_disk.Create()
    scratch_disk.Attach(self)

    self.FormatDisk(scratch_disk.GetDevicePath())
    self.MountDisk(scratch_disk.GetDevicePath(), disk_spec.mount_point)


  def GetName(self):
    """Get a GCE VM's unique name."""
    return self.name

  def GetLocalDrives(self):
    """Returns a list of local drives on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          drives on the VM (e.g. '/dev/sdb').
    """
    return ['/dev/disk/by-id/google-local-ssd-%d' % i
            for i in range(self.num_ssds)]

  def SetupLocalDrives(self, mount_path=virtual_machine.LOCAL_MOUNT_PATH):
    """Set up any local drives that exist.

    Sets up drives as usual, then performs GCE specific setup
    (runs set-interrupts.sh).

    Args:
      mount_path: The path where the local drives should be mounted. If this
          is None, then the device won't be formatted or mounted.

    Returns:
      A boolean indicating whether the setup occured.
    """
    ret = super(GceVirtualMachine, self).SetupLocalDrives(mount_path=mount_path)
    if ret:
      self.PushDataFile(SET_INTERRUPTS_SH)
      self.RemoteCommand(
          'chmod +rx set-interrupts.sh; sudo ./set-interrupts.sh')
    return ret
