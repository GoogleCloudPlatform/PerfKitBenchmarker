# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing strategies to prepare disks.

This module abstract out the disk strategies of a virtual machine so code can
be resuse and abstracted across clouds.
There are two types of diks strategies.
1. DiskCreationStrategy - This strategy governs how a
                          virtual machine should create the disk resource.
2. SetUpDiskStrategy - This strategy controls how a disk are set up.
"""
import copy
import json
import logging
import time
from typing import Any, Union

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import context as pkb_context
from perfkitbenchmarker import disk
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

virtual_machine = Any  # virtual_machine.py imports this module.


class CreateDiskStrategy:
  """Handles disk related logic to create GCE Resource disk.

  This strategy class handles creation of a disk.

  Attributes:
    vm: The virutal machine.
    disk_spec: Spec of the disk to be created.
    disk_count: The number of disk.
    disk_specs: Duplicated disk_spec by disk_count
  """

  def __init__(
      self,
      vm: 'virtual_machine. BaseVirtualMachine',
      disk_spec: disk.BaseDiskSpec,
      disk_count: int,
  ):
    self.vm = vm
    self.disk_spec = disk_spec
    self.disk_count = disk_count
    if disk_spec.disk_type == disk.LOCAL and disk_count is None:
      disk_count = self.vm.max_local_disks
    self.disk_specs = [copy.copy(disk_spec) for _ in range(disk_count)]
    # In the event that we need to create multiple disks from the same
    # DiskSpec, we need to ensure that they have different mount points.
    if disk_count > 1 and disk_spec.mount_point:
      for i, vm_disk_spec in enumerate(self.disk_specs):
        vm_disk_spec.mount_point += str(i)

  def AddMetadataToDiskResource(self):
    """Add metadata to the disk resource for tagging purpose."""
    pass

  def GetCreationCommand(self) -> dict[str, Any]:
    """Returns the command to create the disk resource with the VM."""
    return {}

  def CreateAndAttachDisk(self) -> None:
    """Calls Create and attach disk if needed."""
    if not self.DiskCreatedOnVMCreation():
      return
    self._CreateAndAttachDisk()

  def _CreateAndAttachDisk(self) -> None:
    """Creates and attaches the disk resource to the VM."""
    raise NotImplementedError()

  def GetResourceMetadata(self):
    """Returns the metadata of the disk resource."""
    return {}

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on the VM."""
    raise NotImplementedError()

  def GetSetupDiskStrategy(self) -> 'SetUpDiskStrategy':
    """Returns the SetUpDiskStrategy for the disk."""
    return EmptySetupDiskStrategy(self.vm, self.disk_spec)


class EmptyCreateDiskStrategy(CreateDiskStrategy):
  """Strategies to create nothing. Useful when there is no resource disk."""

  # pylint: disable=super-init-not-called
  def __init__(self, vm: Any, disk_spec: disk.BaseDiskSpec, disk_count: int):
    self.disk_spec = disk_spec
    self.disk_count = disk_count
    self.vm = vm

  def _CreateAndAttachDisk(self) -> None:
    """Does nothing."""
    return

  def DiskCreatedOnVMCreation(self) -> bool:
    return True


class DisksAreNotVisibleError(Exception):
  pass


class DisksAreNotDetachedError(Exception):
  pass


def GetNonBootDiskCount(vm) -> int:
  """Returns the number of non boot disks attached to the VM."""
  stdout, stderr = vm.RemoteCommand('lsblk --json', ignore_failure=True)
  if not stdout:
    logging.error(stderr)
  disks_json = json.loads(stdout)
  block_devices = disks_json['blockdevices']
  # This logic won't work for VMs that come with local ssd because
  # the type is 'disk' for local ssd as well.
  block_devices_disks = [d for d in block_devices if d['type'] == 'disk']
  non_boot_disks_attached = (
      len(block_devices_disks) - 1
  )  # subtracting boot disk
  return non_boot_disks_attached


class SetUpDiskStrategy:
  """Strategies to set up ram disks."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_spec: disk.BaseDiskSpec,
  ):
    self.vm = vm
    self.disk_spec = disk_spec
    self.scratch_disks = []

  def SetUpDisk(self) -> None:
    if self.vm.OS_TYPE in os_types.LINUX_OS_TYPES:
      self.SetUpDiskOnLinux()
    else:
      self.SetUpDiskOnWindows()

  def SetUpDiskOnWindows(self):
    """Performs Windows specific setup of ram disk."""
    raise NotImplementedError(
        f'{self.disk_spec.disk_type} is not supported on Windows.'
    )

  def SetUpDiskOnLinux(self):
    """Performs Linux specific setup of ram disk."""
    raise NotImplementedError(
        f'{self.disk_spec.disk_type} is not supported on linux.'
    )

  def GetTotalDiskCount(self) -> int:
    total_disks = 0
    for scratch_disk in self.scratch_disks:
      if isinstance(scratch_disk, disk.StripedDisk):
        total_disks += len(scratch_disk.disks)
      else:
        total_disks += 1
    return total_disks

  def CheckDisksVisibility(self):
    """Checks for all the disks to be visible from the VM.

    Returns:
      True : if all the disks are visible to the VM
    """
    non_boot_disks_attached = GetNonBootDiskCount(self.vm)
    if non_boot_disks_attached == self.GetTotalDiskCount():
      return True
    else:
      return False

  @vm_util.Retry(
      poll_interval=1,
      timeout=200,
      max_retries=200,
      retryable_exceptions=(DisksAreNotVisibleError,),
  )
  def WaitForDisksToVisibleFromVm(self, start_time) -> float:
    """Waits for the disks to be visible from the Guest.

    Args:
      start_time: time when the attach operation started.

    Returns:
      time taken for all the disks to be visible from the Guest.

    Raises:
      DisksAreNotVisibleError: if the disks are not visible.
    """
    # not implemented for Windows
    if self.vm.OS_TYPE not in os_types.LINUX_OS_TYPES:
      return -1
    self.CheckDisksVisibility()
    if not self.CheckDisksVisibility():
      raise DisksAreNotVisibleError('Disks not visible')
    return time.time() - start_time


class EmptySetupDiskStrategy(SetUpDiskStrategy):
  """Strategies to set up nothing. This is useful when there is no disk."""

  def SetUpDisk(self) -> None:
    pass


class SetUpRamDiskStrategy(SetUpDiskStrategy):
  """Strategies to set up ram disks."""

  def SetUpDiskOnLinux(self):
    """Performs Linux specific setup of ram disk."""
    scratch_disk = disk.BaseDisk(self.disk_spec)
    logging.info(
        'Mounting and creating Ram Disk %s, %s',
        scratch_disk.mount_point,
        scratch_disk.disk_size,
    )
    mnt_cmd = (
        'sudo mkdir -p {0};sudo mount -t tmpfs -o size={1}g tmpfs {0};'
        'sudo chown -R $USER:$USER {0};'
    ).format(scratch_disk.mount_point, scratch_disk.disk_size)
    self.vm.RemoteHostCommand(mnt_cmd)
    self.vm.scratch_disks.append(disk.BaseDisk(self.disk_spec))


class SetUpNFSDiskStrategy(SetUpDiskStrategy):
  """Strategies to set up NFS disks."""

  def __init__(
      self,
      vm,
      disk_spec: disk.BaseDiskSpec,
      unmanaged_nfs_service: nfs_service.BaseNfsService | None = None,
  ):
    super().__init__(vm, disk_spec)
    if unmanaged_nfs_service:
      self.nfs_service = unmanaged_nfs_service
    else:
      self.nfs_service = getattr(
          pkb_context.GetThreadBenchmarkSpec(), 'nfs_service'
      )

  def SetUpDiskOnLinux(self):
    """Performs Linux specific setup of ram disk."""
    self.vm.Install('nfs_utils')
    nfs_disk = self.nfs_service.CreateNfsDisk()
    self.vm.MountDisk(
        nfs_disk.GetDevicePath(),
        self.disk_spec.mount_point,
        self.disk_spec.disk_type,
        nfs_disk.mount_options,
        nfs_disk.fstab_options,
    )

    self.vm.scratch_disks.append(nfs_disk)


class SetUpSMBDiskStrategy(SetUpDiskStrategy):
  """Strategies to set up NFS disks."""

  def SetUpDiskOnLinux(self):
    """Performs Linux specific setup of ram disk."""
    smb_service = getattr(pkb_context.GetThreadBenchmarkSpec(), 'smb_service')
    smb_disk = smb_service.CreateSmbDisk()
    self.vm.MountDisk(
        smb_disk.GetDevicePath(),
        self.disk_spec.mount_point,
        self.disk_spec.disk_type,
        smb_disk.mount_options,
        smb_disk.fstab_options,
    )

    self.vm.scratch_disks.append(smb_disk)


class PrepareScratchDiskStrategy:
  """Strategies to prepare scratch disks."""

  TEMPDB_DISK_LETTER = 'T'

  def PrepareScratchDisk(
      self,
      vm,
      scratch_disk: Union[disk.BaseDisk, disk.StripedDisk],
      disk_spec: disk.BaseDiskSpec,
  ) -> None:
    if vm.OS_TYPE in os_types.LINUX_OS_TYPES:
      self.PrepareLinuxScratchDisk(vm, scratch_disk, disk_spec)
    else:
      self.PrepareWindowsScratchDisk(vm, scratch_disk, disk_spec)

  def PrepareLinuxScratchDisk(
      self,
      vm,
      scratch_disk: Union[disk.BaseDisk, disk.StripedDisk],
      disk_spec: disk.BaseDiskSpec,
  ) -> None:
    """Format and mount scratch disk.

    Args:
      vm: Linux Virtual Machine to create scratch disk on.
      scratch_disk: Scratch disk to be formatted and mounted.
      disk_spec: The BaseDiskSpec object corresponding to the disk.
    """
    if isinstance(scratch_disk, disk.StripedDisk) and scratch_disk.is_striped:
      # the scratch disk is a logical device stripped together from raw disks
      # scratch disk device path == disk_spec device path
      # scratch disk device path != raw disks device path
      scratch_disk_device_path = '/dev/md%d' % len(vm.scratch_disks)
      scratch_disk.device_path = scratch_disk_device_path
      disk_spec.device_path = scratch_disk_device_path
      raw_device_paths = [d.GetDevicePath() for d in scratch_disk.disks]
      vm.StripeDisks(raw_device_paths, scratch_disk.GetDevicePath())

    if disk_spec.mount_point and not vm.IsMounted(disk_spec.mount_point):
      vm.FormatDisk(scratch_disk.GetDevicePath(), disk_spec.disk_type)
      vm.MountDisk(
          scratch_disk.GetDevicePath(),
          disk_spec.mount_point,
          disk_spec.disk_type,
          scratch_disk.mount_options,
          scratch_disk.fstab_options,
      )

    vm.scratch_disks.append(scratch_disk)

  def PrepareWindowsScratchDisk(
      self,
      vm,
      scratch_disk: Union[disk.BaseDisk, disk.StripedDisk],
      disk_spec: disk.BaseDiskSpec,
  ) -> None:
    """Format and mount scratch disk.

    Args:
      vm: Windows Virtual Machine to create scratch disk on.
      scratch_disk: Scratch disk to be formatted and mounted.
      disk_spec: The BaseDiskSpec object corresponding to the disk.
    """
    # Create and then run a Diskpart script that will initialize the disks,
    # create a volume, and then format and mount the volume.
    script = ''

    # Get DeviceId and Model (FriendlyNam) for all disks
    # attached to the VM except boot disk.
    # Using Get-Disk has option to query for disks with no partition
    # (partitionstyle -eq 'raw'). Returned Number and FriendlyName represent
    # DeviceID and model of the disk. Device ID is used for Diskpart cleanup.
    # https://learn.microsoft.com/en-us/powershell/module/
    # storage/get-disk?view=windowsserver2022-ps
    stdout, _ = vm.RemoteCommand(
        "Get-Disk | Where partitionstyle -eq 'raw' | "
        'Select  Number,FriendlyName'
    )
    query_disk_numbers = []
    lines = stdout.splitlines()
    for line in lines:
      if line:
        device, model = line.strip().split(' ', 1)
        if 'Number' in device or '-----' in device:
          continue

        if disk_spec.disk_type == 'local':
          if vm.DiskDriveIsLocal(device, model):
            query_disk_numbers.append(device)
        else:
          if not vm.DiskDriveIsLocal(device, model):
            query_disk_numbers.append(device)

    if scratch_disk.is_striped:
      disk_numbers = query_disk_numbers
    else:
      disk_numbers = query_disk_numbers[0]

    for disk_number in disk_numbers:
      # For each disk, set the status to online (if it is not already),
      # remove any formatting or partitioning on the disks, and convert
      # it to a dynamic disk so it can be used to create a volume.
      # TODO(user): Fix on Azure machines with temp disk, e.g.
      # Ebdsv5 which have 1 local disk with partitions. Perhaps this means
      # removing the clean and convert gpt lines.
      script += (
          'select disk %s\n'
          'online disk noerr\n'
          'attributes disk clear readonly\n'
          'clean\n'
          'convert gpt\n'
          'convert dynamic\n' % disk_number
      )

    # Create a volume out of the disk(s).
    if scratch_disk.is_striped:
      script += 'create volume stripe disk=%s\n' % ','.join(disk_numbers)
    else:
      script += 'create volume simple\n'

    # If a mount point has been specified, create the directory where it will be
    # mounted and assign the mount point to the volume.
    mount_command = ''
    if disk_spec.mount_point:
      vm.RemoteCommand('mkdir %s' % disk_spec.mount_point)
      mount_command = 'assign mount=%s\n' % disk_spec.mount_point
    # Format the volume, based on OS type.
    if vm.OS_TYPE in os_types.WINDOWS_SQLSERVER_OS_TYPES:
      format_command = 'format fs=ntfs quick unit=64k'
    else:
      format_command = 'format quick'

    # Add format type, drive letter and mount point for the diskpart script.
    script += '%s\nassign letter=%s\n%s' % (
        format_command,
        vm.assigned_disk_letter.lower(),
        mount_command,
    )
    # No-op, useful for understanding the state of the disks
    vm.RunDiskpartScript('list disk')
    vm.RunDiskpartScript(script)

    # Grant user permissions on the drive
    vm.RemoteCommand(
        'icacls {}: /grant Users:F /L'.format(vm.assigned_disk_letter)
    )
    vm.RemoteCommand(
        'icacls {}: --% /grant Users:(OI)(CI)F /L'.format(
            vm.assigned_disk_letter
        )
    )

    vm.scratch_disks.append(scratch_disk)

    if (
        vm.OS_TYPE in os_types.WINDOWS_SQLSERVER_OS_TYPES
        and disk_spec.disk_type != 'local'
    ):
      self.PrepareTempDbDisk(vm)

  def GetLocalSSDNames(self) -> list[str]:
    """Names of local ssd device when running Get-PhysicalDisk."""
    return []

  def GetLocalSSDDeviceIDs(self, vm) -> list[str]:
    """Get all local ssd device ids."""
    names = self.GetLocalSSDNames()

    if not names:
      # This function is not implemented in this cloud provider.
      logging.info('Temp DB is not supported on this cloud')
      return []

    clause = ' -or '.join([f'($_.FriendlyName -eq "{name}")' for name in names])
    clause = '{' + clause + '}'

    # Select the DeviceID from the output
    # It will be one integer per line.
    # i.e
    # 3
    # 4
    stdout, _ = vm.RemoteCommand(
        f'Get-PhysicalDisk | where-object {clause} | Select -exp DeviceID'
    )

    # int casting will clean up the spacing and make sure we are getting
    # correct device id.
    return [
        str(int(device_id)) for device_id in stdout.split('\n') if device_id
    ]

  def PrepareTempDbDisk(self, vm: 'virtual_machine.BaseVirtualMachine'):
    """Format and mount temp db disk for sql server."""
    # Create and then run a Diskpart script that will initialize the disks,
    # create a volume, and then format and mount the volume.
    # This would trigger when we detect local ssds on the VM
    # and the disk type is not set to local ssd.
    script = ''
    local_ssd_disks = self.GetLocalSSDDeviceIDs(vm)
    if not local_ssd_disks:
      # No local ssd detected, will not move temp db.
      return

    for disk_number in local_ssd_disks:
      # For local SSD disk, set the status to online (if it is not already),
      # remove any formatting or partitioning on the disks, and convert
      # it to a dynamic disk so it can be used to create a volume.
      script += (
          'select disk %s\n'
          'online disk noerr\n'
          'attributes disk clear readonly\n'
          'clean\n'
          'convert gpt\n'
          'convert dynamic\n' % disk_number
      )
    if len(local_ssd_disks) > 1:
      script += 'create volume stripe disk=%s\n' % ','.join(local_ssd_disks)
    else:
      script += 'create volume simple\n'

    script += 'format fs=ntfs quick unit=64k\nassign letter={}\n'.format(
        self.TEMPDB_DISK_LETTER.lower()
    )
    vm.RunDiskpartScript(script)

    # Grant user permissions on the drive
    vm.RemoteCommand(
        'icacls {}: /grant Users:F /L'.format(self.TEMPDB_DISK_LETTER)
    )
    vm.RemoteCommand(
        'icacls {}: --% /grant Users:(OI)(CI)F /L'.format(
            self.TEMPDB_DISK_LETTER
        )
    )
    vm.RemoteCommand('mkdir {}:\\TEMPDB'.format(self.TEMPDB_DISK_LETTER))


class DetachDiskStrategy:
  """Strategies to detach disks from VM."""

  def __init__(self, vm: Any):
    self.vm = vm

  def CheckDisksDetach(self):
    """Checks for all the disks to be detached from the VM.

    Returns:
      True : if all the disks are detached from the VM
    """
    non_boot_disks_attached = GetNonBootDiskCount(self.vm)
    if non_boot_disks_attached == 0:
      return True
    else:
      return False

  @vm_util.Retry(
      poll_interval=1,
      timeout=200,
      max_retries=200,
      retryable_exceptions=(DisksAreNotDetachedError,),
  )
  def WaitForDisksToDetachFromVm(self) -> float:
    """Waits for the disks to detach from the Guest.

    Returns:
      time taken for all the disks to detach from the Guest.

    Raises:
      DisksAreNotDetachedError: if any disk is visible.
    """
    self.CheckDisksDetach()
    if not self.CheckDisksDetach():
      raise DisksAreNotDetachedError('Disks not visible')
    return time.time()

  def DetachDisks(self) -> float:
    detach_tasks = []
    for scratch_disk in self.vm.scratch_disks:
      detach_tasks.append((scratch_disk.Detach, (), {}))
    detach_tasks.append((self.WaitForDisksToDetachFromVm, (), {}))
    time_returned = background_tasks.RunParallelThreads(
        detach_tasks, max_concurrency=200
    )
    return time_returned[1]
