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

"""Module containing classes related to Azure disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
At this time, Azure only supports one disk type, so the disk spec's disk type
is ignored.
See http://msdn.microsoft.com/en-us/library/azure/dn790303.aspx for more
information about azure disks.
"""

import json
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import flags as azure_flags

FLAGS = flags.FLAGS

DRIVE_START_LETTER = 'c'

PREMIUM_STORAGE = 'premium-storage'
STANDARD_DISK = 'standard-disk'

DISK_TYPE = {disk.STANDARD: STANDARD_DISK,
             disk.REMOTE_SSD: PREMIUM_STORAGE}

AZURE = 'Azure'
disk.RegisterDiskTypeMap(AZURE, DISK_TYPE)

PREMIUM_STORAGE_METADATA = {
    disk.MEDIA: disk.SSD,
    disk.REPLICATION: disk.ZONE,
    disk.LEGACY_DISK_TYPE: disk.REMOTE_SSD
}

AZURE_REPLICATION_MAP = {
    azure_flags.LRS: disk.ZONE,
    azure_flags.ZRS: disk.REGION,
    # Deliberately omitting PLRS, because that is handled by
    # PREMIUM_STORAGE_METADATA, and (RA)GRS, because those are
    # asynchronously replicated.
}

LOCAL_SSD_PREFIXES = {
    'Standard_D',
    'Standard_G'
}


def LocalDiskIsSSD(machine_type):
  """Check whether the local disk is an SSD drive."""

  return any((machine_type.startswith(prefix)
              for prefix in LOCAL_SSD_PREFIXES))


class AzureDisk(disk.BaseDisk):
  """Object representing an Azure Disk."""

  _lock = threading.Lock()

  def __init__(self, disk_spec, vm_name, machine_type,
               storage_account, lun, is_image=False):
    super(AzureDisk, self).__init__(disk_spec)
    self.host_caching = FLAGS.azure_host_caching
    self.name = None
    self.vm_name = vm_name
    self.resource_group = azure_network.GetResourceGroup()
    self.storage_account = storage_account
    # lun is Azure's abbreviation for "logical unit number"
    self.lun = lun
    self.is_image = is_image
    self._deleted = False

    if self.disk_type == PREMIUM_STORAGE:
      self.metadata = PREMIUM_STORAGE_METADATA
    elif self.disk_type == STANDARD_DISK:
      self.metadata = {
          disk.MEDIA: disk.HDD,
          disk.REPLICATION: AZURE_REPLICATION_MAP[FLAGS.azure_storage_type],
          disk.LEGACY_DISK_TYPE: disk.STANDARD
      }
    elif self.disk_type == disk.LOCAL:
      media = disk.SSD if LocalDiskIsSSD(machine_type) else disk.HDD

      self.metadata = {
          disk.MEDIA: media,
          disk.REPLICATION: disk.NONE,
          disk.LEGACY_DISK_TYPE: disk.LOCAL
      }

  def _Create(self):
    """Creates the disk."""
    assert not self.is_image

    if self.disk_type == PREMIUM_STORAGE:
      assert FLAGS.azure_storage_type == azure_flags.PLRS
    else:
      assert FLAGS.azure_storage_type != azure_flags.PLRS

    with self._lock:
      _, _, retcode = vm_util.IssueCommand(
          [azure.AZURE_PATH, 'vm', 'disk', 'attach-new',
           '--host-caching', self.host_caching,
           '--lun', str(self.lun),
           self.vm_name, str(self.disk_size)] +
          self.resource_group.args)

      if retcode:
        raise errors.Resource.RetryableCreationError(
            'Error creating Azure disk.')

      disk = self._GetDiskJSON()
      self.name = disk['name']

  def _Delete(self):
    """Deletes the disk."""
    assert not self.is_image
    self._deleted = True

  def _GetDiskJSON(self):
    """Get Azure's JSON representation of the disk."""
    stdout, _ = vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'disk', 'list',
         '--json',
         self.vm_name] + self.resource_group.args)
    response = json.loads(stdout)

    # data disks are ordered *alphabetically* by lun, so once we hit
    # lun 10 the ordering doesn't match numerical order. We could
    # compute the right offset for the alphabetical ordering, but it's
    # simpler and more resilient to just search the array, and the
    # size is limited to 15 so it will never take too long.
    data_disk = (disk for disk in response
                 if int(disk.get('lun', 0)) == self.lun).next()

    return data_disk

  def _Exists(self):
    """Returns true if the disk exists."""
    if self._deleted:
      return False

    # Since we don't do .Create() or .Delete() on disks with
    # is_image=True, we never call _Exists() on them either. The code
    # below is not correct for image disks, but that's okay.
    assert not self.is_image

    _, _, retcode = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'storage', 'blob', 'show',
         '--container', 'vhds',
         '--json',
         '%s.vhd' % self.name] + self.storage_account.connection_args)

    return retcode == 0

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AzureVirtualMachine instance to which the disk will be attached.
    """
    pass  # TODO(user): Implement Attach()
    # (not critical because disks are attached to VMs when created)

  def Detach(self):
    """Detaches the disk from a VM."""

    vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'disk', 'detach',
         '--vm-name', self.vm_name,
         '--lun', self.lun] + self.resource_group.args)

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.disk_type == disk.LOCAL:
      return '/dev/sdb'
    else:
      return '/dev/sd%s' % chr(ord(DRIVE_START_LETTER) + self.lun)
