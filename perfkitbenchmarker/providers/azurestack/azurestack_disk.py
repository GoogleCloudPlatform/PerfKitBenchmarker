# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing classes related to AzureStack disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
At this time, AzureStack only supports one disk type, so the disk spec's disk type
is ignored.
See http://msdn.microsoft.com/en-us/library/azurestack/dn790303.aspx for more
information about azurestack disks.
"""

import json
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azurestack
from perfkitbenchmarker.providers.azurestack import azurestack_network
from perfkitbenchmarker.providers.azurestack import flags as azurestack_flags

FLAGS = flags.FLAGS

DRIVE_START_LETTER = 'c'

PREMIUM_STORAGE = 'Premium_LRS'
STANDARD_DISK = 'Standard_LRS'

DISK_TYPE = {disk.STANDARD: STANDARD_DISK,
             disk.REMOTE_SSD: PREMIUM_STORAGE}

HOST_CACHING = 'host_caching'

AZURESTACK = 'AzureStack'
disk.RegisterDiskTypeMap(AZURESTACK, DISK_TYPE)

AZURESTACK_REPLICATION_MAP = {
    azurestack_flags.LRS: disk.ZONE,
    azurestack_flags.ZRS: disk.REGION,
    # Deliberately omitting PLRS, because that is set explicty in __init__,
    # and (RA)GRS, because those are asynchronously replicated.
}

LOCAL_SSD_PREFIXES = {
    'Standard_D',
    'Standard_G'
}


def LocalDiskIsSSD(machine_type):
  """Check whether the local disk is an SSD drive."""

  return any((machine_type.startswith(prefix)
              for prefix in LOCAL_SSD_PREFIXES))


class AzureStackDisk(disk.BaseDisk):
  """Object representing an AzureStack Disk."""

  _lock = threading.Lock()

  def __init__(self, disk_spec, vm_name, machine_type,
               storage_account, lun, is_image=False):
    super(AzureStackDisk, self).__init__(disk_spec)
    self.host_caching = FLAGS.azurestack_host_caching
    self.name = vm_name + str(lun)
    self.vm_name = vm_name
    self.resource_group = azurestack_network.GetResourceGroup()
    self.storage_account = storage_account
    # lun is AzureStack's abbreviation for "logical unit number"
    self.lun = lun
    self.is_image = is_image
    self._deleted = False

    if self.disk_type == PREMIUM_STORAGE:
      self.metadata.update({
          disk.MEDIA: disk.SSD,
          disk.REPLICATION: disk.ZONE,
          HOST_CACHING: self.host_caching,
      })
    elif self.disk_type == STANDARD_DISK:
      self.metadata.update({
          disk.MEDIA: disk.HDD,
          disk.REPLICATION: AZURESTACK_REPLICATION_MAP[FLAGS.azurestack_storage_type],
          HOST_CACHING: self.host_caching,
      })
    elif self.disk_type == disk.LOCAL:
      media = disk.SSD if LocalDiskIsSSD(machine_type) else disk.HDD

      self.metadata.update({
          disk.MEDIA: media,
          disk.REPLICATION: disk.NONE,
      })

  def _Create(self):
    """Creates the disk."""
    assert not self.is_image

    with self._lock:
      _, _, retcode = vm_util.IssueCommand(
          [azurestack.AZURESTACK_PATH, 'vm', 'unmanaged-disk', 'attach', '--new',
           '--caching', self.host_caching, '--name', self.name,
           '--lun', str(self.lun),
           '--vm-name', self.vm_name, '--size-gb', str(self.disk_size)] +
          self.resource_group.args)

      if retcode:
        raise errors.Resource.RetryableCreationError(
            'Error creating AzureStack disk.')

  def _Delete(self):
    """Deletes the disk."""
    assert not self.is_image
    self._deleted = True

  def _Exists(self):
    """Returns true if the disk exists."""
    assert not self.is_image
    if self._deleted:
      return False

    stdout, _, _ = vm_util.IssueCommand(
        [azurestack.AZURESTACK_PATH, 'vm', 'unmanaged-disk', 'list',
         '--output', 'json',
         '--name', self.vm_name] + self.resource_group.args)
    try:
      json.loads(stdout)
      return True
    except:
      return False

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AzureStackVirtualMachine instance to which the disk will be attached.
    """
    pass  # TODO(user): Implement Attach()
    # (not critical because disks are attached to VMs when created)

  def Detach(self):
    """Detaches the disk from a VM."""
    # Not needed since the resource group can be deleted
    # without detaching disks.
    pass

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.disk_type == disk.LOCAL:
      return '/dev/sdb'
    else:
      return '/dev/sd%s' % chr(ord(DRIVE_START_LETTER) + self.lun)
