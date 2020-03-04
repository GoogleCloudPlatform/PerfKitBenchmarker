# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing classes related to YC disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
Use 'yc compute disk-types list' to determine valid disk types.
"""

import time
import json
import random
import string

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker.providers import YC
from perfkitbenchmarker.providers.yandexcloud import util

FLAGS = flags.FLAGS

NET_SSD = 'network-ssd'
NET_HDD = 'network-hdd'

DISK_TYPE = {disk.STANDARD: NET_HDD, disk.REMOTE_SSD: NET_SSD}

DISK_METADATA = {
    NET_HDD: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
    },
    NET_SSD: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
}

disk.RegisterDiskTypeMap(YC, DISK_TYPE)


class YcDisk(disk.BaseDisk):
  """Object representing an YC Disk."""

  def __init__(self, disk_spec, name, zone, folder_id=None,
               image_name=None, image_folder_id=None):
    super(YcDisk, self).__init__(disk_spec)
    self.attached_vm_name = None
    self.image_name = image_name
    self.image_folder_id = image_folder_id
    self.name = name
    self.zone = zone
    self.folder_id = folder_id
    self.device_name = ''.join(random.sample(string.ascii_lowercase, 12))
    self.metadata.update(DISK_METADATA[disk_spec.disk_type])

  def _Create(self):
    """Creates the disk."""
    cmd = util.YcCommand(self, 'compute', 'disks', 'create')
    cmd.flags['name'] = self.name
    cmd.flags['size'] = self.disk_size
    cmd.flags['type'] = self.disk_type
    cmd.flags['zone'] = self.zone
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    if self.image_name:
      cmd.flags['image-name'] = self.image_name
    if self.image_folder_id:
      cmd.flags['source-image-folder-id'] = self.image_folder_id
    _, stderr, retcode = cmd.Issue()
    util.CheckYcResponseKnownFailures(stderr, retcode)

  def _Delete(self):
    """Deletes the disk."""
    cmd = util.YcCommand(self, 'compute', 'disks', 'delete', self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns true if the disk exists."""
    cmd = util.YcCommand(self, 'compute', 'disks', 'describe', self.name)
    stdout, _, _ = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The YcVirtualMachine instance to which the disk will be attached.
    """
    self.attached_vm_name = vm.name
    cmd = util.YcCommand(self, 'compute', 'instances', 'attach-disk',
                         self.attached_vm_name)

    cmd.flags['device-name'] = self.device_name
    cmd.flags['disk-name'] = self.name
    cmd.IssueRetryable()

    time.sleep(1)

    vm.RemoteCommand('sudo udevadm trigger')

  def Detach(self):
    """Detaches the disk from a VM."""
    cmd = util.YcCommand(self, 'compute', 'instances', 'detach-disk',
                         self.attached_vm_name)
    cmd.flags['device-name'] = self.device_name
    cmd.IssueRetryable()
    self.attached_vm_name = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    return '/dev/disk/by-id/virtio-%s' % self.device_name
