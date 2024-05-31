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

import time
from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.azure import azure_disk

FLAGS = flags.FLAGS
virtual_machine = Any  # pylint: disable=invalid-name


def GetCreateDiskStrategy(
    vm: 'virtual_machine.BaseVirtualMachine',
    disk_spec: disk.BaseDiskSpec,
    disk_count: int,
) -> disk_strategies.CreateDiskStrategy:
  """Returns the strategies to create disks for the disk type."""
  if disk_spec and disk_count > 0:
    if disk_spec.disk_type == disk.LOCAL:
      return AzureCreateLocalSSDDiskStrategy(vm, disk_spec, disk_count)
    elif disk_spec.disk_type in azure_disk.AZURE_REMOTE_DISK_TYPES:
      return AzureCreateRemoteDiskStrategy(vm, disk_spec, disk_count)
  return AzureCreateNonResourceDiskStrategy(vm, disk_spec, disk_count)


class AzureCreateDiskStrategy(disk_strategies.CreateDiskStrategy):
  """Same as CreateDiskStrategy, but with Base Disk spec."""

  disk_spec: disk.BaseDiskSpec


class AzureCreateOSDiskStrategy(AzureCreateDiskStrategy):
  """Strategies to create OS disk."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_spec: disk.BaseDiskSpec,
      disk_count: int,
  ):
    super().__init__(vm, disk_spec, disk_count)
    self.vm = vm
    self.disk_count = disk_count
    if disk_spec is None:
      disk_spec = disk.BaseDiskSpec('azure_os_disk')
    disk_spec.disk_type = vm.boot_disk_type or vm.storage_account.storage_type
    if vm.boot_disk_size:
      disk_spec.disk_size = vm.boot_disk_size
    self.disk = azure_disk.AzureDisk(
        self.disk_spec, self.vm, None, is_image=True
    )
    self.disk_spec = disk_spec

  def GetCreationCommand(self) -> dict[str, Any]:
    dic = {}
    disk_type_cmd = []
    disk_size_cmd = []
    if self.disk_spec.disk_type:
      disk_type_cmd = ['--storage-sku', f'os={self.disk.disk_type}']
    if self.disk and self.disk.disk_size:
      disk_size_cmd = ['--os-disk-size-gb', str(self.disk.disk_size)]
    dic['create-disk'] = disk_type_cmd + disk_size_cmd
    return dic


class AzureCreateRemoteDiskStrategy(AzureCreateDiskStrategy):
  """Strategies to create remote disks."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_spec: disk.BaseDiskSpec,
      disk_count: int,
  ):
    super().__init__(vm, disk_spec, disk_count)
    self.remote_disk_groups = []
    self.vm = vm
    self.setup_disk_strategy = None
    for _, disk_spec in enumerate(self.disk_specs):
      disks = []
      for _ in range(disk_spec.num_striped_disks):
        disk_number = self.vm.remote_disk_counter + 1 + self.vm.max_local_disks
        self.vm.remote_disk_counter += 1
        lun = next(self.vm.lun_counter)
        data_disk = azure_disk.AzureDisk(disk_spec, self.vm, lun)
        # TODO(arushigaur) Clean code to avoid using
        # disk_number as it is used for windows only
        data_disk.disk_number = disk_number
        disks.append(data_disk)
      self.remote_disk_groups.append(disks)

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    return False

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    if self.setup_disk_strategy is None:
      self.setup_disk_strategy = AzureSetUpRemoteDiskStrategy(
          self.vm, self.disk_specs
      )
    return self.setup_disk_strategy


class AzureCreateLocalSSDDiskStrategy(AzureCreateDiskStrategy):
  """Strategies to create local disks."""

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    return True

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    return AzureSetUpLocalSSDDiskStrategy(self.vm, self.disk_spec)


class AzureCreateNonResourceDiskStrategy(
    disk_strategies.EmptyCreateDiskStrategy
):
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

    return disk_strategies.EmptySetupDiskStrategy(self.vm, self.disk_spec)


class AzureSetUpLocalSSDDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Strategies to setup local disk."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_spec: disk.BaseDiskSpec,
  ):
    super().__init__(vm, disk_spec)
    self.vm = vm
    self.disk_spec = disk_spec

  def SetUpDisk(self):
    disks = []

    for _ in range(self.disk_spec.num_striped_disks):
      data_disk = self._CreateLocalDisk()
      disks.append(data_disk)
    if len(disks) > 1:
      # If the disk_spec called for a striped disk, create one.
      scratch_disk = disk.StripedDisk(self.disk_spec, disks)
    else:
      scratch_disk = disks[0]
    AzurePrepareScratchDiskStrategy().PrepareScratchDisk(
        self.vm, scratch_disk, self.disk_spec
    )

    if self.disk_spec.num_striped_disks < self.vm.max_local_disks:
      for _ in range(
          self.vm.max_local_disks - self.disk_spec.num_striped_disks):
        data_disk = self._CreateLocalDisk()
        disks.append(data_disk)
        AzurePrepareScratchDiskStrategy().PrepareScratchDisk(
            self.vm, data_disk, self.disk_spec)
    self.vm.SetupLocalDisks()

  def _CreateLocalDisk(self):
    disk_number = self.vm.local_disk_counter + 1
    self.vm.local_disk_counter += 1
    if self.vm.local_disk_counter > self.vm.max_local_disks:
      raise errors.Error('Not enough local disks.')
    lun = next(self.vm.lun_counter)
    data_disk = azure_disk.AzureDisk(self.disk_spec, self.vm, lun)
    data_disk.disk_number = disk_number
    return data_disk


class AzureSetUpRemoteDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Strategies to setup remote disk."""

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_specs: list[disk.BaseDiskSpec],
  ):
    super().__init__(vm, disk_specs[0])
    self.vm = vm
    self.disk_specs = disk_specs
    self.scratch_disks = []

  def SetUpDisk(self):
    create_tasks = []
    scratch_disks = []
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disk_group = self.vm.create_disk_strategy.remote_disk_groups[disk_spec_id]
      if len(disk_group) > 1:
        # If the disk_spec called for a striped disk, create one.
        scratch_disk = azure_disk.AzureStripedDisk(disk_spec, disk_group)
      else:
        scratch_disk = disk_group[0]
      scratch_disks.append((scratch_disk, disk_spec))
      if not self.vm.create_disk_strategy.DiskCreatedOnVMCreation():
        create_tasks.append((scratch_disk.Create, (), {}))
    background_tasks.RunParallelThreads(create_tasks, max_concurrency=200)
    self.scratch_disks = [scratch_disk for scratch_disk, _ in scratch_disks]
    self.AttachDisks()
    for scratch_disk, disk_spec in scratch_disks:
      AzurePrepareScratchDiskStrategy().PrepareScratchDisk(
          self.vm, scratch_disk, disk_spec
      )

  def AttachDisks(self):
    attach_tasks = []
    start_time = time.time()
    if not self.scratch_disks or FLAGS.azure_attach_disk_with_create:
      return
    attach_tasks.append((self.WaitForDisksToVisibleFromVm, [start_time], {}))
    attach_tasks.append((self.AttachAzureDisks, (), {}))
    return_from_threads = background_tasks.RunParallelThreads(
        attach_tasks, max_concurrency=200
    )
    self.time_to_visible = return_from_threads[0]

  def AttachAzureDisks(self) -> None:
    for scratch_disk in self.scratch_disks:
      # Not doing parallel attach of azure disks because Azure
      # throws conflict due to concurrent request
      scratch_disk.Attach(self.vm)


class AzurePrepareScratchDiskStrategy(
    disk_strategies.PrepareScratchDiskStrategy
):
  """Strategies to prepare scratch disk on Azure."""

  def GetLocalSSDNames(self) -> list[str]:
    # This is only for Ls-Series local ssd
    # Temp disk is automatically setup on Azure and default to disk D.
    return ['Microsoft NVMe Direct Disk']

  def PrepareTempDbDisk(self, vm: 'virtual_machine.BaseVirtualMachine'):
    if azure_disk.HasTempDrive(vm.machine_type):
      vm.RemoteCommand('mkdir D:\\TEMPDB')
