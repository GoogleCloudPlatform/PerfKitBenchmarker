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

This module abstract out the disk algorithm for formatting and creating
scratch disks.
"""
import logging
from typing import Union

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import os_types

FLAGS = flags.FLAGS


class SetUpRamDiskStrategy:
  """Strategies to set up ram disks."""

  def SetUpDisk(
      self,
      vm,
      disk_spec: disk.BaseDiskSpec,
  ) -> None:
    if vm.OS_TYPE in os_types.LINUX_OS_TYPES:
      self.SetUpDiskOnLinux(vm, disk_spec)
    else:
      raise NotImplementedError('Ram disk is only supported on linux.')

  def SetUpDiskOnLinux(self, vm, disk_spec: disk.BaseDiskSpec) -> None:
    """Performs Linux specific setup of ram disk."""
    scratch_disk = disk.BaseDisk(disk_spec)
    logging.info(
        'Mounting and creating Ram Disk %s, %s',
        scratch_disk.mount_point,
        scratch_disk.disk_size,
    )
    mnt_cmd = (
        'sudo mkdir -p {0};sudo mount -t tmpfs -o size={1}g tmpfs {0};'
        'sudo chown -R $USER:$USER {0};'
    ).format(scratch_disk.mount_point, scratch_disk.disk_size)
    vm.RemoteHostCommand(mnt_cmd)
    vm.scratch_disks.append(disk.BaseDisk(disk_spec))


class PrepareScratchDiskStrategy:
  """Strategies to prepare scratch disks."""

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
    """Helper method to format and mount scratch disk.

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

    if disk_spec.mount_point:
      if isinstance(scratch_disk, disk.MountableDisk):
        scratch_disk.Mount(vm)
      else:
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
    """Helper method to format and mount scratch disk.

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
    # mounted, format the volume, and assign the mount point to the volume.
    if disk_spec.mount_point:
      vm.RemoteCommand('mkdir %s' % disk_spec.mount_point)
      format_command = 'format quick'

      if vm.OS_TYPE in os_types.WINDOWS_SQLSERVER_OS_TYPES:
        format_command = 'format fs=ntfs quick unit=64k'

      script += '%s\nassign letter=%s\nassign mount=%s\n' % (
          format_command,
          vm.assigned_disk_letter.lower(),
          disk_spec.mount_point,
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

    if FLAGS.gce_num_local_ssds > 0 and FLAGS.db_disk_type != 'local':
      vm.PrepareTempDbDisk()
