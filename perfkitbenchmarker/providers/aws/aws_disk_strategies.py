# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
import collections
import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker.providers.aws import aws_disk


FLAGS = flags.FLAGS
virtual_machine = Any  # pylint: disable=invalid-name


def GetCreateDiskStrategy(
    vm: 'virtual_machine.BaseVirtualMachine',
    disk_spec: aws_disk.AwsDiskSpec,
    disk_count: int,
) -> disk_strategies.CreateDiskStrategy:
  """Returns the strategies to create disks for the disk type."""
  if disk_spec and disk_count > 0:
    if disk_spec.disk_type == disk.LOCAL:
      return CreateLocalDiskStrategy(vm, disk_spec, disk_count)
    elif disk_spec.disk_type in aws_disk.AWS_REMOTE_DISK_TYPES:
      return CreateRemoteDiskStrategy(vm, disk_spec, disk_count)
  return CreateNonResourceDiskStrategy(vm, disk_spec, disk_count)


class AWSCreateDiskStrategy(disk_strategies.CreateDiskStrategy):
  """Same as CreateDiskStrategy, but with Base Disk spec."""

  disk_spec: aws_disk.AwsDiskSpec
  LOCAL_DRIVE_START_LETTER = 'b'

  def _GetNvmeBootIndex(self):
    if aws_disk.LocalDriveIsNvme(
        self.vm.machine_type
    ) and aws_disk.EbsDriveIsNvme(self.vm.machine_type):
      # identify boot drive
      # If this command ever fails consider 'findmnt -nM / -o source'
      cmd = (
          'realpath /dev/disk/by-label/cloudimg-rootfs '
          '| grep --only-matching "nvme[0-9]*"'
      )
      boot_drive = self.vm.RemoteCommand(cmd, ignore_failure=True)[0].strip()
      if boot_drive:
        # get the boot drive index by dropping the nvme prefix
        boot_idx = int(boot_drive[4:])
        logging.info('found boot drive at nvme index %d', boot_idx)
        return boot_idx
      else:
        logging.warning('Failed to identify NVME boot drive index. Assuming 0.')
        return 0

  def BuildDiskSpecId(self, spec_index, strip_index):
    """Generates an unique string to represent a disk_spec position."""
    return f'{spec_index}_{strip_index}'


class CreateLocalDiskStrategy(AWSCreateDiskStrategy):
  """Creates a local disk on AWS."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_spec: aws_disk.AwsDiskSpec,
      disk_count: int,
  ):
    super().__init__(vm, disk_spec, disk_count)
    self.local_disk_groups = []
    nvme_boot_drive_index = self._GetNvmeBootIndex()
    for spec_id, disk_spec in enumerate(self.disk_specs):
      disks = []
      for i in range(disk_spec.num_striped_disks):
        disk_spec_id = self.BuildDiskSpecId(spec_id, i)
        data_disk = aws_disk.AwsDisk(
            disk_spec, self.vm.zone, self.vm.machine_type, disk_spec_id
        )
        device_letter = chr(
            ord(self.LOCAL_DRIVE_START_LETTER) + self.vm.local_disk_counter
        )
        # Assign device letter to the disk.
        data_disk.AssignDeviceLetter(device_letter, nvme_boot_drive_index)  # pytype: disable=wrong-arg-types
        # Local disk numbers start at 1 (0 is the system disk).
        data_disk.disk_number = self.vm.local_disk_counter + 1
        self.vm.local_disk_counter += 1
        if self.vm.local_disk_counter > self.vm.max_local_disks:
          raise errors.Error('Not enough local disks.')
        disks.append(data_disk)
      self.local_disk_groups.append(disks)

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    return SetUpLocalDiskStrategy(self.vm, self.disk_specs)

  def GetBlockDeviceMap(self) -> list[dict[str, str]]:
    mappings = []
    if not self.DiskCreatedOnVMCreation():
      return mappings
    for spec_index, disk_spec in enumerate(self.disk_specs):
      if not self.vm.DiskTypeCreatedOnVMCreation(disk_spec.disk_type):
        continue
      for i in range(disk_spec.num_striped_disks):
        mapping = collections.OrderedDict()
        device_letter = aws_disk.AwsDisk.GenerateDeviceLetter(self.vm.name)
        device_name_prefix = aws_disk.AwsDisk.GenerateDeviceNamePrefix(
            disk_spec.disk_type
        )
        device_name = device_name_prefix + device_letter
        mapping['DeviceName'] = device_name
        mapping['VirtualName'] = 'ephemeral%s' % i
        if not aws_disk.LocalDriveIsNvme(self.vm.machine_type):
          # non-nvme local disks just gets its name as device path,
          # logging it now since it does not show up in block-device-mapping
          # of DescribeInstances call.
          self.vm.LogDeviceByName(device_name, None, device_name)
        mappings.append(mapping)
        disk_spec_id = self.BuildDiskSpecId(spec_index, i)
        self.vm.LogDeviceByDiskSpecId(disk_spec_id, device_name)
    return mappings

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    return self.disk_spec.create_with_vm


class CreateRemoteDiskStrategy(AWSCreateDiskStrategy):
  """Creates a remote disk on AWS."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_spec: aws_disk.AwsDiskSpec,
      disk_count: int,
  ):
    super().__init__(vm, disk_spec, disk_count)
    self.remote_disk_groups = []
    for spec_id, disk_spec in enumerate(self.disk_specs):
      disks = []
      for i in range(disk_spec.num_striped_disks):
        disk_spec_id = self.BuildDiskSpecId(spec_id, i)
        data_disk = aws_disk.AwsDisk(
            disk_spec, self.vm.zone, self.vm.machine_type, disk_spec_id
        )
        data_disk.disk_number = (
            self.vm.remote_disk_counter + 1 + self.vm.max_local_disks
        )
        self.vm.remote_disk_counter += 1
        disks.append(data_disk)
      self.remote_disk_groups.append(disks)

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    return self.disk_spec.create_with_vm

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    return SetUpRemoteDiskStrategy(self.vm, self.disk_specs)

  def GetBlockDeviceMap(
      self,
  ):
    mappings = []
    if not self.DiskCreatedOnVMCreation():
      return mappings
    for spec_index, disk_spec in enumerate(self.disk_specs):
      if not self.vm.DiskTypeCreatedOnVMCreation(disk_spec.disk_type):
        continue
      for i in range(disk_spec.num_striped_disks):
        mapping = collections.OrderedDict()
        device_letter = aws_disk.AwsDisk.GenerateDeviceLetter(self.vm.name)
        device_name_prefix = aws_disk.AwsDisk.GenerateDeviceNamePrefix(
            disk_spec.disk_type
        )
        device_name = device_name_prefix + device_letter
        mapping['DeviceName'] = device_name
        ebs_block = collections.OrderedDict()
        ebs_block['VolumeType'] = disk_spec.disk_type
        ebs_block['VolumeSize'] = disk_spec.disk_size
        ebs_block['DeleteOnTermination'] = True
        if disk_spec.provisioned_iops and disk_spec.disk_type in [
            aws_disk.IO1,
            aws_disk.IO2,
            aws_disk.GP3,
        ]:
          ebs_block['Iops'] = disk_spec.provisioned_iops
        if disk_spec.throughput and disk_spec.disk_type in [aws_disk.GP3]:
          ebs_block['Throughput'] = disk_spec.throughput
        mapping['Ebs'] = ebs_block
        mappings.append(mapping)
        disk_spec_id = self.BuildDiskSpecId(spec_index, i)
        self.vm.LogDeviceByDiskSpecId(disk_spec_id, device_name)
    return mappings


class CreateNonResourceDiskStrategy(disk_strategies.EmptyCreateDiskStrategy):
  """Strategies to create non remote and non local disks like RAM, SMB."""

  def DiskCreatedOnVMCreation(self) -> bool:
    # This have to be set to False due to _CreateScratchDiskFromDisks in
    # virtual_machine.py.
    return False

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    if not self.disk_spec:
      return disk_strategies.EmptySetupDiskStrategy(self.vm, self.disk_spec)

    if self.disk_spec.disk_type == disk.RAM:
      return disk_strategies.SetUpRamDiskStrategy(self.vm, self.disk_spec)
    elif self.disk_spec.disk_type == disk.SMB:
      return disk_strategies.SetUpSMBDiskStrategy(self.vm, self.disk_spec)
    elif self.disk_spec.disk_type == disk.NFS:
      return disk_strategies.SetUpNFSDiskStrategy(self.vm, self.disk_spec)

    return disk_strategies.EmptySetupDiskStrategy(self.vm, self.disk_spec)

  def GetBlockDeviceMap(
      self,
  ):
    return []


class AWSSetupDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Strategies to setup remote disk."""

  def PopulateNVMEDevicePath(self, scratch_disk, nvme_devices):
    local_devices = []
    ebs_devices = {}
    for device in nvme_devices:
      device_path = device['DevicePath']
      model_number = device['ModelNumber']
      volume_id = device['SerialNumber'].replace('vol', 'vol-')
      if model_number == 'Amazon Elastic Block Store':
        ebs_devices[volume_id] = device_path
      elif model_number == 'Amazon EC2 NVMe Instance Storage':
        local_devices.append(device_path)
      else:
        raise errors.Benchmarks.UnsupportedConfigError(
            f'{model_number} NVME devices is not supported.'
        )

    # because local devices are assigned in a round robin manner,
    # some local devices might already have been assigned (previous spec_index).
    # remove local devices that have been already been assigned.
    for _, aws_id in self.vm.disk_identifiers_by_device.items():
      if aws_id.path in local_devices:
        local_devices.remove(aws_id.path)

    if not local_devices and not ebs_devices:
      return

    disks = scratch_disk.disks if scratch_disk.is_striped else [scratch_disk]
    for d in disks:
      if d.disk_type == disk.NFS:
        continue
      elif d.disk_type == disk.LOCAL:
        if aws_disk.LocalDriveIsNvme(self.vm.machine_type):
          # assign in a round robin manner. Since NVME local disks
          # are created ignoring all pkb naming conventions and assigned
          # random names on the fly.
          # pytype: disable=attribute-error
          disk_name = self.GetDeviceByDiskSpecId(d.disk_spec_id)
          self.vm.LogDeviceByName(disk_name, None, local_devices.pop())
      elif aws_disk.EbsDriveIsNvme(self.vm.machine_type):
        # EBS NVME volumes have disk_name assigned
        # looks up disk_name by disk_spec_id
        # populate the aws identifier information
        # pytype: disable=attribute-error
        disk_name = self.GetDeviceByDiskSpecId(d.disk_spec_id)
        volume_id = self.GetVolumeIdByDevice(disk_name)
        if volume_id in ebs_devices:
          self.UpdateDeviceByName(disk_name, volume_id, ebs_devices[volume_id])

  def UpdateDevicePath(self, scratch_disk):
    """Updates the paths for all raw devices inside the VM."""
    disks = scratch_disk.disks if scratch_disk.is_striped else [scratch_disk]
    for d in disks:
      if d.disk_type == disk.NFS:
        continue
      device_name = self.GetDeviceByDiskSpecId(d.disk_spec_id)
      d.device_path = self.GetPathByDevice(device_name)

  def GetPathByDevice(self, disk_name):
    """Gets the logged device path."""
    assert disk_name is not None
    aws_ids = self.vm.disk_identifiers_by_device.get(disk_name, None)
    assert aws_ids is not None
    return aws_ids.path

  def GetDeviceByDiskSpecId(self, disk_spec_id):
    """Gets the logged device name by disk_spec_id."""
    assert disk_spec_id is not None
    return self.vm.device_by_disk_spec_id.get(disk_spec_id, None)

  def GetVolumeIdByDevice(self, disk_name):
    """Gets the logged volume id by disk_name."""
    assert disk_name is not None
    aws_ids = self.vm.disk_identifiers_by_device.get(disk_name, None)
    assert aws_ids is not None
    return aws_ids.volume_id

  def UpdateDeviceByName(self, device_name, volume_id=None, path=None):
    """Gets the volume id by disk_name."""
    assert device_name is not None
    aws_ids = self.vm.disk_identifiers_by_device.get(device_name, None)
    assert aws_ids is not None
    if aws_ids.volume_id and volume_id:
      assert aws_ids.volume_id == volume_id
    # not asserting path similarity because we need to overwrite the path of
    # ebs nvme devices (set to /dev/xvdb? on creation and re-named to nvme?n?)
    self.vm.LogDeviceByName(device_name, volume_id, path)


class SetUpLocalDiskStrategy(AWSSetupDiskStrategy):
  """Strategies to setup remote disk."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_specs: list[aws_disk.AwsDiskSpec],
  ):
    super().__init__(vm, disk_specs[0])
    self.vm = vm
    self.disk_specs = disk_specs

  def SetUpDisk(self):
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disk_group = self.vm.create_disk_strategy.local_disk_groups[disk_spec_id]
      if len(disk_group) > 1:
        # If the disk_spec called for a striped disk, create one.
        scratch_disk = disk.StripedDisk(disk_spec, disk_group)
      else:
        scratch_disk = disk_group[0]
      if self.vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        # here, all disks are created (either at vm creation or in line above).
        # But we don't have all the raw device paths,
        # which are necessary for striping the scratch disk on Linux,
        # but not on Windows.
        # The path is not updated for Windows machines.
        nvme_devices = self.vm.GetNVMEDeviceInfo()
        self.PopulateNVMEDevicePath(scratch_disk, nvme_devices)
        self.UpdateDevicePath(scratch_disk)
      AWSPrepareScratchDiskStrategy().PrepareScratchDisk(
          self.vm, scratch_disk, disk_spec
      )


class SetUpRemoteDiskStrategy(AWSSetupDiskStrategy):
  """Strategies to setup remote disk."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_specs: list[aws_disk.AwsDiskSpec],
  ):
    super().__init__(vm, disk_specs[0])
    self.vm = vm
    self.disk_specs = disk_specs

  def SetUpDisk(self):
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disk_group = self.vm.create_disk_strategy.remote_disk_groups[disk_spec_id]
      if len(disk_group) > 1:
        # If the disk_spec called for a striped disk, create one.
        scratch_disk = disk.StripedDisk(disk_spec, disk_group)
      else:
        scratch_disk = disk_group[0]
      if not self.vm.create_disk_strategy.DiskCreatedOnVMCreation():
        scratch_disk.Create()
        scratch_disk.Attach(self.vm)
      if self.vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        # here, all disks are created (either at vm creation or in line above).
        # But we don't have all the raw device paths,
        # which are necessary for striping the scratch disk on Linux,
        # but not on Windows.
        # The path is not updated for Windows machines.
        nvme_devices = self.vm.GetNVMEDeviceInfo()
        self.PopulateNVMEDevicePath(scratch_disk, nvme_devices)
        self.UpdateDevicePath(scratch_disk)
      AWSPrepareScratchDiskStrategy().PrepareScratchDisk(
          self.vm, scratch_disk, disk_spec
      )


class AWSPrepareScratchDiskStrategy(disk_strategies.PrepareScratchDiskStrategy):
  """Strategies to prepare scratch disk on AWS."""

  def GetLocalSSDNames(self) -> list[str]:
    return ['NVMe Amazon EC2 NVMe']
