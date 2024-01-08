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
from typing import Any
from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


virtual_machine = Any  # pylint: disable=invalid-name


def GetCreateDiskStrategy(
    vm: 'virtual_machine.BaseVirtualMachine',
    disk_spec: gce_disk.GceDiskSpec,
    disk_count: int,
) -> disk_strategies.CreateDiskStrategy:
  if disk_spec:
    if disk_spec.disk_type in gce_disk.GCE_REMOTE_DISK_TYPES:
      return CreatePDDiskStrategy(vm, disk_spec, disk_count)
    elif disk_spec.disk_type == disk.LOCAL:
      return CreateLSSDDiskStrategy(vm, disk_spec, disk_count)
  return GCEEmptyCreateDiskStrategy(vm, disk_spec, disk_count)


class GCPCreateDiskStrategy(disk_strategies.CreateDiskStrategy):
  """Same as CreateDiskStrategy, but with GCP Disk spec."""

  disk_spec: gce_disk.GceDiskSpec


class CreatePDDiskStrategy(GCPCreateDiskStrategy):
  """Contains logic to create persistence disk on GCE."""

  def __init__(self, vm: Any, disk_spec: disk.BaseDiskSpec, disk_count: int):
    super().__init__(vm, disk_spec, disk_count)
    self.pd_disk_groups = []
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disks = []
      for i in range(disk_spec.num_striped_disks):
        name = _GenerateDiskNamePrefix(vm, disk_spec_id, i)
        data_disk = gce_disk.GceDisk(
            disk_spec,
            name,
            vm.zone,
            vm.project,
            replica_zones=disk_spec.replica_zones,
        )
        if gce_disk.PdDriveIsNvme(vm):
          data_disk.interface = gce_disk.NVME
        else:
          data_disk.interface = gce_disk.SCSI
        vm.remote_disk_counter += 1
        disks.append(data_disk)
      self.pd_disk_groups.append(disks)

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    if self.disk_spec.replica_zones:
      # GCE regional disks cannot use create-on-create.
      return False
    return self.disk_spec.create_with_vm

  def AddMetadataToDiskResource(self):
    if not self.DiskCreatedOnVMCreation():
      return
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      for i in range(disk_spec.num_striped_disks):
        name = _GenerateDiskNamePrefix(self.vm, disk_spec_id, i)
        cmd = util.GcloudCommand(
            self.vm, 'compute', 'disks', 'add-labels', name
        )
        cmd.flags['labels'] = util.MakeFormattedDefaultTags()
        cmd.Issue()

  def GetCreationCommand(self) -> dict[str, Any]:
    if not self.DiskCreatedOnVMCreation():
      return {}

    create_disks = []
    dic = {}
    for disk_group in self.pd_disk_groups:
      for pd_disk in disk_group:
        create_disks.append(pd_disk.GetCreateFlags())
    if create_disks:
      dic['create-disk'] = create_disks
    return dic


class CreateLSSDDiskStrategy(GCPCreateDiskStrategy):
  """Contains logic to create LSSD disk on VM."""

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    return True


class GCEEmptyCreateDiskStrategy(disk_strategies.EmptyCreateDiskStrategy):
  """Empty CreateDiskStrategy for GCE when disks are not needed."""

  def DiskCreatedOnVMCreation(self) -> bool:
    # This have to be set to False due to _CreateScratchDiskFromDisks in
    # virtual_machine.py.
    return False


class SetUpGCEResourceDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Base Strategy class to set up local ssd and pd-ssd."""

  def __init__(self, disk_specs: list[disk.BaseDiskSpec]):
    self.disk_specs = disk_specs

  def FindRemoteNVMEDevices(self, nvme_devices):
    """Find the paths for all remote NVME devices inside the VM."""
    remote_nvme_devices = [
        device['DevicePath']
        for device in nvme_devices
        if device['ModelNumber'] == 'nvme_card-pd'
    ]

    return sorted(remote_nvme_devices)

  def UpdateDevicePath(self, scratch_disk, remote_nvme_devices):
    """Updates the paths for all remote NVME devices inside the VM."""
    disks = []
    if isinstance(scratch_disk, disk.StripedDisk):
      disks = scratch_disk.disks
    else:
      disks = [scratch_disk]

    # round robin assignment since we cannot tell the disks apart.
    for d in disks:
      if (
          d.disk_type in gce_disk.GCE_REMOTE_DISK_TYPES
          and d.interface == gce_disk.NVME
      ):
        d.name = remote_nvme_devices.pop()


class SetUpGceLocalDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to set up local disks."""

  def SetUpDisk(self, vm: Any, disk_spec: disk.BaseDiskSpec):
    # disk spec is not used here.
    vm.SetupLocalDisks()
    disks = []
    for _ in range(disk_spec.num_striped_disks):
      if vm.ssd_interface == gce_disk.SCSI:
        name = 'local-ssd-%d' % vm.local_disk_counter
      elif vm.ssd_interface == gce_disk.NVME:
        name = f'local-nvme-ssd-{vm.local_disk_counter}'
      else:
        raise errors.Error('Unknown Local SSD Interface.')

      data_disk = gce_disk.GceLocalDisk(disk_spec, name)
      vm.local_disk_counter += 1
      if vm.local_disk_counter > vm.max_local_disks:
        raise errors.Error('Not enough local disks.')
      disks.append(data_disk)

    if len(disks) == 1:
      scratch_disk = disks[0]
    else:
      scratch_disk = disk.StripedDisk(disk_spec, disks)
    # Device path is needed to stripe disks on Linux, but not on Windows.
    # The path is not updated for Windows machines.
    if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
      nvme_devices = vm.GetNVMEDeviceInfo()
      remote_nvme_devices = self.FindRemoteNVMEDevices(nvme_devices)
      self.UpdateDevicePath(scratch_disk, remote_nvme_devices)
    GCEPrepareScratchDiskStrategy().PrepareScratchDisk(
        vm, scratch_disk, disk_spec
    )


class SetUpPDDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to Persistance disk on GCE."""

  def __init__(self, disk_specs: list[gce_disk.GceDiskSpec]):
    super().__init__(disk_specs)
    self.disk_specs = disk_specs

  def SetUpDisk(self, vm: Any, _: disk.BaseDiskSpec):
    # disk spec is not used here.
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disk_group = vm.create_disk_strategy.pd_disk_groups[disk_spec_id]
      # Create the disk if it is not created on create
      if not vm.create_disk_strategy.DiskCreatedOnVMCreation():
        for pd_disk in disk_group:
          pd_disk.Create()
          pd_disk.Attach(vm)

      if len(disk_group) > 1:
        # If the disk_spec called for a striped disk, create one.
        scratch_disk = disk.StripedDisk(disk_spec, disk_group)
      else:
        scratch_disk = disk_group[0]

      # Device path is needed to stripe disks on Linux, but not on Windows.
      # The path is not updated for Windows machines.
      if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        nvme_devices = vm.GetNVMEDeviceInfo()
        remote_nvme_devices = self.FindRemoteNVMEDevices(nvme_devices)
        self.UpdateDevicePath(scratch_disk, remote_nvme_devices)
      GCEPrepareScratchDiskStrategy().PrepareScratchDisk(
          vm, scratch_disk, disk_spec
      )


class SetUpGcsFuseDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Strategies to set up ram disks."""

  DEFAULT_MOUNT_OPTIONS = [
      'allow_other',
      'dir_mode=755',
      'file_mode=755',
      'implicit_dirs',
  ]

  def SetUpDiskOnLinux(self, vm, disk_spec):
    """Performs Linux specific setup of ram disk."""
    scratch_disk = disk.BaseDisk(disk_spec)
    vm.Install('gcsfuse')
    vm.RemoteCommand(
        f'sudo mkdir -p {disk_spec.mount_point} && sudo chmod a+w'
        f' {disk_spec.mount_point}'
    )

    opts = ','.join(self.DEFAULT_MOUNT_OPTIONS + FLAGS.mount_options)
    bucket = FLAGS.gcsfuse_bucket
    target = disk_spec.mount_point
    vm.RemoteCommand(f'sudo mount -t gcsfuse -o {opts} {bucket} {target}')
    vm.scratch_disks.append(scratch_disk)


class GCEPrepareScratchDiskStrategy(disk_strategies.PrepareScratchDiskStrategy):
  """Strategies to prepare scratch disk on GCE."""

  def GetLocalSSDNames(self):
    return ['Google EphemeralDisk', 'nvme_card']


def _GenerateDiskNamePrefix(
    vm: 'virtual_machine. BaseVirtualMachine', disk_spec_id: int, index: int
) -> str:
  """Generates a deterministic disk name given disk_spec_id and index."""
  return f'{vm.name}-data-{disk_spec_id}-{index}'
