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
"""Module containing classes related to Azure disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
At this time, Azure only supports one disk type, so the disk spec's disk type
is ignored.
See http://msdn.microsoft.com/en-us/library/azure/dn790303.aspx for more
information about azure disks.
"""


import itertools
import json
import re
import threading

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import flags as azure_flags
from perfkitbenchmarker.providers.azure import util
from six.moves import range

FLAGS = flags.FLAGS

MAX_DRIVE_SUFFIX_LENGTH = 2  # Last allowable device is /dev/sdzz.

PREMIUM_STORAGE = 'Premium_LRS'
STANDARD_DISK = 'Standard_LRS'
ULTRA_STORAGE = 'UltraSSD_LRS'

DISK_TYPE = {disk.STANDARD: STANDARD_DISK, disk.REMOTE_SSD: PREMIUM_STORAGE}

HOST_CACHING = 'host_caching'

AZURE = 'Azure'
disk.RegisterDiskTypeMap(AZURE, DISK_TYPE)

AZURE_REPLICATION_MAP = {
    azure_flags.LRS: disk.ZONE,
    azure_flags.ZRS: disk.REGION,
    # Deliberately omitting PLRS, because that is set explicty in __init__,
    # and (RA)GRS, because those are asynchronously replicated.
}

LOCAL_SSD_PREFIXES = {'Standard_D', 'Standard_G', 'Standard_L'}

AZURE_NVME_TYPES = [
    r'(Standard_L[0-9]+s_v2)',
]

# https://docs.microsoft.com/en-us/azure/virtual-machines/azure-vms-no-temp-disk
# D/Ev4 and D/E(i)sv4 VMs do not have tmp/OS disk; Dv3, Dsv3, and Ddv4 VMs do.
# Same for *v5, including Milan machines.
AZURE_NO_TMP_DISK_TYPES = [
    r'(Standard_D[0-9]+s?_v4)',
    r'(Standard_E[0-9]+i?s?_v4)',
    r'(Standard_D[0-9]+s?_v5)',
    r'(Standard_E[0-9]+i?s?_v5)',
    r'(Standard_D[0-9]+as?_v5)',
    r'(Standard_E[0-9]+as?_v5)',
]


def _ProductWithIncreasingLength(iterable, max_length):
  """Yields increasing length cartesian products of iterable."""
  for length in range(1, max_length + 1):
    for p in itertools.product(iterable, repeat=length):
      yield p


def _GenerateDrivePathSuffixes():
  """Yields drive path suffix strings.

  Drive path suffixes in the form 'a', 'b', 'c', 'd', ..., 'z', 'aa', 'ab', etc.
  Note: the os-disk will be /dev/sda, and the temporary disk will be /dev/sdb:
  https://docs.microsoft.com/en-us/azure/virtual-machines/linux/faq#can-i-use-the-temporary-disk-devsdb1-to-store-data
  Some newer VMs (e.g. Dsv4 VMs) do not have temporary disks.

  The linux kernel code that determines this naming can be found here:
  https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/scsi/sd.c?h=v2.6.37#n2262

  Quoting the link from above:
  SCSI disk names starts at sda. The 26th device is sdz and the 27th is sdaa.
  The last one for two lettered suffix is sdzz which is followed by sdaaa.
  """
  character_range = range(ord('a'), ord('z') + 1)
  products = _ProductWithIncreasingLength(
      character_range, MAX_DRIVE_SUFFIX_LENGTH)

  for p in products:
    yield ''.join(chr(c) for c in p)


REMOTE_DRIVE_PATH_SUFFIXES = list(_GenerateDrivePathSuffixes())


class TooManyAzureDisksError(Exception):
  """Exception raised when too many disks are attached."""
  pass


def LocalDiskIsSSD(machine_type):
  """Check whether the local disk is an SSD drive."""

  return any((machine_type.startswith(prefix) for prefix in LOCAL_SSD_PREFIXES))


def LocalDriveIsNvme(machine_type):
  """Check if the machine type uses NVMe driver."""
  return any(
      re.search(machine_series, machine_type)
      for machine_series in AZURE_NVME_TYPES)


def HasTempDrive(machine_type):
  """Check if the machine type has the temp drive (sdb)."""
  return not any(
      re.search(machine_series, machine_type)
      for machine_series in AZURE_NO_TMP_DISK_TYPES)


class AzureDisk(disk.BaseDisk):
  """Object representing an Azure Disk."""

  _lock = threading.Lock()

  def __init__(self,
               disk_spec,
               vm,
               lun,
               is_image=False):
    super(AzureDisk, self).__init__(disk_spec)
    self.host_caching = FLAGS.azure_host_caching
    self.vm = vm
    self.vm_name = vm.name
    self.name = self.vm_name + str(lun)
    self.resource_group = azure_network.GetResourceGroup()
    self.storage_account = vm.storage_account
    # lun is Azure's abbreviation for "logical unit number"
    self.lun = lun
    self.is_image = is_image
    self._deleted = False
    self.machine_type = vm.machine_type
    if self.disk_type == PREMIUM_STORAGE:
      self.metadata.update({
          disk.MEDIA: disk.SSD,
          disk.REPLICATION: disk.ZONE,
          HOST_CACHING: self.host_caching,
      })
    elif self.disk_type == STANDARD_DISK:
      self.metadata.update({
          disk.MEDIA: disk.HDD,
          disk.REPLICATION: AZURE_REPLICATION_MAP[FLAGS.azure_storage_type],
          HOST_CACHING: self.host_caching,
      })
    elif self.disk_type == disk.LOCAL:
      media = disk.SSD if LocalDiskIsSSD(self.machine_type) else disk.HDD

      self.metadata.update({
          disk.MEDIA: media,
          disk.REPLICATION: disk.NONE,
      })

  def _Create(self):
    """Creates the disk."""
    assert not self.is_image

    if self.disk_type == ULTRA_STORAGE and not self.vm.availability_zone:
      raise Exception(f'Azure Ultradisk is being created in zone "{self.zone}"'
                      'which was not specified to have an availability zone. '
                      'Availability zones are specified with zone-\\d  e.g. '
                      'eastus1-2 for availability zone 2 in zone eastus1')
    with self._lock:
      _, _, retcode = vm_util.IssueCommand([
          azure.AZURE_PATH, 'vm', 'disk', 'attach', '--new', '--caching',
          self.host_caching, '--name', self.name, '--lun',
          str(self.lun), '--sku', self.disk_type, '--vm-name', self.vm_name,
          '--size-gb',
          str(self.disk_size)
      ] + self.resource_group.args, raise_on_failure=False)

      if retcode:
        raise errors.Resource.RetryableCreationError(
            'Error creating Azure disk.')

      _, _, retcode = vm_util.IssueCommand([
          azure.AZURE_PATH, 'disk', 'update', '--name', self.name, '--set',
          util.GetTagsJson(self.resource_group.timeout_minutes)
      ] + self.resource_group.args, raise_on_failure=False)

      if retcode:
        raise errors.Resource.RetryableCreationError(
            'Error tagging Azure disk.')

      if (self.disk_type == ULTRA_STORAGE and (
          FLAGS.azure_provisioned_iops or FLAGS.azure_provisioned_throughput)):
        args = ([azure.AZURE_PATH, 'disk', 'update', '--name', self.name] +
                self.resource_group.args)

        if FLAGS.azure_provisioned_iops:
          args = args + ['--disk-iops-read-write',
                         str(FLAGS.azure_provisioned_iops)]
        if FLAGS.azure_provisioned_throughput:
          args = args + ['--disk-mbps-read-write',
                         str(FLAGS.azure_provisioned_throughput)]

        _, _, _ = vm_util.IssueCommand(args, raise_on_failure=True)

  def _Delete(self):
    """Deletes the disk."""
    assert not self.is_image
    self._deleted = True

  def _Exists(self):
    """Returns true if the disk exists."""
    assert not self.is_image
    if self._deleted:
      return False

    stdout, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH, 'disk', 'show', '--output', 'json', '--name',
        self.name
    ] + self.resource_group.args, raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except:
      return False

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AzureVirtualMachine instance to which the disk will be attached.
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
      if LocalDriveIsNvme(self.machine_type):
        return '/dev/nvme%sn1' % str(self.lun)
      # Temp disk naming isn't always /dev/sdb:
      # https://github.com/MicrosoftDocs/azure-docs/issues/54055
      return '/dev/disk/cloud/azure_resource'
    else:
      try:
        start_index = 1  # the os drive is always at index 0; skip the OS drive.
        if HasTempDrive(self.machine_type):
          start_index += 1
        return '/dev/sd%s' % REMOTE_DRIVE_PATH_SUFFIXES[start_index + self.lun]
      except IndexError:
        raise TooManyAzureDisksError()
