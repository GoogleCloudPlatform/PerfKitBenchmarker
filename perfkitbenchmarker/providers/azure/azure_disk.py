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
import time
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import flags as azure_flags
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS

MAX_DRIVE_SUFFIX_LENGTH = 2  # Last allowable device is /dev/sdzz.

PREMIUM_STORAGE_V2 = 'PremiumV2_LRS'
PREMIUM_STORAGE = 'Premium_LRS'
PREMIUM_ZRS = 'Premium_ZRS'
STANDARD_SSD_LRS = 'StandardSSD_LRS'
STANDARD_SSD_ZRS = 'StandardSSD_ZRS'
STANDARD_DISK = 'Standard_LRS'
ULTRA_STORAGE = 'UltraSSD_LRS'


# https://learn.microsoft.com/en-us/rest/api/compute/disks/list?view=rest-compute-2023-10-02&tabs=HTTP#diskstorageaccounttypes
AZURE_REMOTE_DISK_TYPES = [
    PREMIUM_STORAGE,
    PREMIUM_STORAGE_V2,
    STANDARD_SSD_LRS,
    STANDARD_SSD_ZRS,
    STANDARD_DISK,
    ULTRA_STORAGE,
    PREMIUM_ZRS,
]

HOST_CACHING = 'host_caching'

AZURE = 'Azure'

AZURE_REPLICATION_MAP = {
    azure_flags.LRS: disk.ZONE,
    azure_flags.ZRS: disk.REGION,
    # Deliberately omitting PLRS, because that is set explicty in __init__,
    # and (RA)GRS, because those are asynchronously replicated.
}

LOCAL_SSD_PREFIXES = {'Standard_D', 'Standard_G', 'Standard_L'}

AZURE_NVME_TYPES = [
    r'(Standard_L[0-9]+s_v2)',
    r'(Standard_L[0-9]+a?s_v3)',
]

# https://docs.microsoft.com/en-us/azure/virtual-machines/azure-vms-no-temp-disk
# D/Ev4 and D/E(i)sv4 VMs do not have tmp/OS disk; Dv3, Dsv3, and Ddv4 VMs do.
AZURE_NO_TMP_DISK_TYPES = [
    r'(Standard_D[0-9]+s?_v4)',
    r'(Standard_E[0-9]+i?s?_v4)',
]


def _ProductWithIncreasingLength(iterable, max_length):
  """Yields increasing length cartesian products of iterable."""
  for length in range(1, max_length + 1):
    yield from itertools.product(iterable, repeat=length)


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
      character_range, MAX_DRIVE_SUFFIX_LENGTH
  )

  for p in products:
    yield ''.join(chr(c) for c in p)


REMOTE_DRIVE_PATH_SUFFIXES = list(_GenerateDrivePathSuffixes())


class TooManyAzureDisksError(Exception):
  """Exception raised when too many disks are attached."""

  pass


def LocalDiskIsSSD(machine_type):
  """Check whether the local disk is an SSD drive."""

  return any(machine_type.startswith(prefix) for prefix in LOCAL_SSD_PREFIXES)


def LocalDriveIsNvme(machine_type):
  """Check if the machine type uses NVMe driver."""
  return any(
      re.search(machine_series, machine_type)
      for machine_series in AZURE_NVME_TYPES
  )


def HasTempDrive(machine_type):
  """Check if the machine type has the temp drive (sdb)."""
  # Only applies to some gen 4 VMs.
  series_number = util.GetMachineSeriesNumber(machine_type)
  if series_number > 5:
    # This method is assuming it's a SCSI drive.
    # TODO(pclay): Support NVMe temp drives in v6 VMs.
    return False
  if series_number == 5:
    return machine_type.endswith('ds_v5')
  return not any(
      re.search(machine_series, machine_type)
      for machine_series in AZURE_NO_TMP_DISK_TYPES
  )


class AzureDisk(disk.BaseDisk):
  """Object representing an Azure Disk."""

  _lock = threading.Lock()

  def __init__(self, disk_spec, vm, lun, is_image=False):
    super().__init__(disk_spec)
    self.host_caching = FLAGS.azure_host_caching
    self.vm = vm
    self.vm_name = vm.name
    self.name = self.vm_name + str(lun)
    self.zone = vm.zone
    self.availability_zone = vm.availability_zone
    self.region = util.GetRegionFromZone(self.zone)
    self.resource_group = azure_network.GetResourceGroup()
    self.storage_account = vm.storage_account
    # lun is Azure's abbreviation for "logical unit number"
    self.lun = lun
    self.is_image = is_image
    self._deleted = False
    self.machine_type = vm.machine_type
    self.provisioned_iops = disk_spec.provisioned_iops
    self.provisioned_throughput = disk_spec.provisioned_throughput
    if (
        self.disk_type == PREMIUM_STORAGE
        or self.disk_type == PREMIUM_STORAGE_V2
        or self.disk_type == ULTRA_STORAGE
    ):
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
    elif self.disk_type == PREMIUM_ZRS:
      self.metadata.update({
          disk.MEDIA: disk.SSD,
          disk.REPLICATION: disk.REGION,
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
      raise Exception(
          f'Azure Ultradisk is being created in zone "{self.zone}"'
          'which was not specified to have an availability zone. '
          'Availability zones are specified with zone-\\d  e.g. '
          'eastus1-2 for availability zone 2 in zone eastus1'
      )
    with self._lock:
      if FLAGS.azure_attach_disk_with_create:
        cmd = [
            azure.AZURE_PATH,
            'vm',
            'disk',
            'attach',
            '--new',
            '--caching',
            self.host_caching,
            '--name',
            self.name,
            '--lun',
            str(self.lun),
            '--sku',
            self.disk_type,
            '--vm-name',
            self.vm_name,
            '--size-gb',
            str(self.disk_size),
        ] + self.resource_group.args
      else:
        cmd = [
            azure.AZURE_PATH,
            'disk',
            'create',
            '--name',
            self.name,
            '--size-gb',
            str(self.disk_size),
            '--sku',
            self.disk_type,
            '--location',
            self.region,
            '--zone',
            self.availability_zone,
        ] + self.resource_group.args
      self.create_disk_start_time = time.time()
      _, stderr, retcode = vm_util.IssueCommand(
          cmd, raise_on_failure=False, timeout=600
      )
      self.create_disk_end_time = time.time()
      if retcode:
        raise errors.Resource.RetryableCreationError(
            f'Error creating Azure disk:\n{stderr}'
        )

      _, _, retcode = vm_util.IssueCommand(
          [
              azure.AZURE_PATH,
              'disk',
              'update',
              '--name',
              self.name,
              '--set',
              util.GetTagsJson(self.resource_group.timeout_minutes),
          ]
          + self.resource_group.args,
          raise_on_failure=False,
      )

      if retcode:
        raise errors.Resource.RetryableCreationError(
            'Error tagging Azure disk.'
        )

      if self.disk_type in [ULTRA_STORAGE, PREMIUM_STORAGE_V2] and (
          self.provisioned_iops or self.provisioned_throughput
      ):
        args = [
            azure.AZURE_PATH,
            'disk',
            'update',
            '--name',
            self.name,
        ] + self.resource_group.args

        if self.provisioned_iops:
          args = args + [
              '--disk-iops-read-write',
              str(self.provisioned_iops),
          ]
        if self.provisioned_throughput:
          args = args + [
              '--disk-mbps-read-write',
              str(self.provisioned_throughput),
          ]

        _, _, _ = vm_util.IssueCommand(args, raise_on_failure=True)
        # create disk end time includes disk update command as well

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
        [
            azure.AZURE_PATH,
            'disk',
            'show',
            '--output',
            'json',
            '--name',
            self.name,
        ]
        + self.resource_group.args,
        raise_on_failure=False,
    )
    try:
      json.loads(stdout)
      return True
    except:
      return False

  def _Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AzureVirtualMachine instance to which the disk will be attached.
    """
    if FLAGS.azure_attach_disk_with_create:
      return
    self.attach_start_time = time.time()
    _, _, retcode = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'vm',
            'disk',
            'attach',
            '--vm-name',
            vm.name,
            '--name',
            self.name,
        ]
        + self.resource_group.args,
        raise_on_failure=False,
        timeout=600,
    )
    self.attach_end_time = time.time()
    if retcode:
      raise errors.Resource.RetryableCreationError(
          'Error attaching Azure disk to VM.'
      )

  def _Detach(self):
    """Detaches the disk from a VM."""
    _, _, retcode = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'vm',
            'disk',
            'detach',
            '--vm-name',
            self.vm.name,
            '--name',
            self.name,
        ]
        + self.resource_group.args,
        raise_on_failure=False,
        timeout=600,
    )
    if retcode:
      raise errors.Resource.RetryableCreationError(
          'Error detaching Azure disk from VM.'
      )

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
        if self.vm.SupportsNVMe():
          # boot disk is nvme0n1. temp drive, if exists, uses scsi.
          return '/dev/nvme0n%s' % str(1 + start_index + self.lun)
        if HasTempDrive(self.machine_type):
          start_index += 1
        return '/dev/sd%s' % REMOTE_DRIVE_PATH_SUFFIXES[start_index + self.lun]
      except IndexError:
        raise TooManyAzureDisksError()

  def IsNvme(self):
    if self.disk_type == disk.LOCAL:
      return LocalDriveIsNvme(self.machine_type)
    elif self.disk_type in AZURE_REMOTE_DISK_TYPES:
      return self.vm.SupportsNVMe()
    else:
      return False


class AzureStripedDisk(disk.StripedDisk):
  """Object representing multiple azure disks striped together."""

  def _Create(self):
    create_tasks = []
    for disk_details in self.disks:
      create_tasks.append((disk_details.Create, (), {}))
    background_tasks.RunParallelThreads(create_tasks, max_concurrency=200)

  def _Attach(self, vm):
    if FLAGS.azure_attach_disk_with_create:
      return
    disk_names = [disk_details.name for disk_details in self.disks]
    self.attach_start_time = time.time()
    _, _, retcode = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'vm',
            'disk',
            'attach',
            '--vm-name',
            vm.name,
            '--disks',
        ]
        + disk_names
        + vm.resource_group.args,
        raise_on_failure=False,
        timeout=600,
    )
    if retcode:
      raise errors.Resource.RetryableCreationError(
          'Error attaching Multiple Azure disks to VM.'
      )
    self.attach_end_time = time.time()

  def _Detach(self):
    for disk_details in self.disks:
      disk_details.Detach()

  def GetAttachTime(self):
    if self.attach_start_time and self.attach_end_time:
      return self.attach_end_time - self.attach_start_time
