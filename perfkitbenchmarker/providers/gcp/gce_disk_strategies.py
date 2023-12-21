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
    vm: 'virtual_machine. BaseVirtualMachine',
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
    create_disks = []
    dic = {}
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      if not self.DiskCreatedOnVMCreation():
        continue
      for i in range(disk_spec.num_striped_disks):
        name = _GenerateDiskNamePrefix(self.vm, disk_spec_id, i)
        pd_args = [
            f'name={name}',
            f'device-name={name}',
            f'size={disk_spec.disk_size}',
            f'type={disk_spec.disk_type}',
            'auto-delete=yes',
            'boot=no',
            'mode=rw',
        ]
        if (
            disk_spec.provisioned_iops
            and disk_spec.disk_type in gce_disk.GCE_DYNAMIC_IOPS_DISK_TYPES
        ):
          pd_args += [f'provisioned-iops={disk_spec.provisioned_iops}']
        if (
            disk_spec.provisioned_throughput
            and disk_spec.disk_type
            in gce_disk.GCE_DYNAMIC_THROUGHPUT_DISK_TYPES
        ):
          pd_args += [
              f'provisioned-throughput={disk_spec.provisioned_throughput}'
          ]
        create_disks.append(','.join(pd_args))
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

  def _CreateScratchDiskFromDisks(self, vm, disk_spec, disks):
    """Helper method to create scratch data disks."""
    # This will soon be moved to disk class. As the disk class should
    # be able to handle how it's created and attached
    if len(disks) > 1:
      # If the disk_spec called for a striped disk, create one.
      scratch_disk = disk.StripedDisk(disk_spec, disks)
    else:
      scratch_disk = disks[0]

    if not vm.create_disk_strategy.DiskCreatedOnVMCreation():
      scratch_disk.Create()
      scratch_disk.Attach(vm)

    return scratch_disk


class SetUpGceLocalDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to set up local disks."""

  def SetUpDisk(self, vm: Any, _: disk.BaseDiskSpec):
    # disk spec is not used here.
    vm.SetupLocalDisks()
    for disk_spec in self.disk_specs:
      disks = []
      for _ in range(disk_spec.num_striped_disks):
        if vm.ssd_interface == gce_disk.SCSI:
          name = 'local-ssd-%d' % vm.local_disk_counter
          disk_number = vm.local_disk_counter + 1
        elif vm.ssd_interface == gce_disk.NVME:
          name = f'local-nvme-ssd-{vm.local_disk_counter}'
          # TODO(user): Apply for new Gen 3 VMs (barring M3)
          if vm.machine_type.startswith('c3'):
            # TODO(user): Also consider removing disk_number
            disk_number = vm.local_disk_counter
          else:  # includes N2D CVM
            disk_number = vm.local_disk_counter + vm.NVME_START_INDEX
        else:
          raise errors.Error('Unknown Local SSD Interface.')

        data_disk = gce_disk.GceLocalDisk(disk_spec, name)
        data_disk.disk_number = disk_number
        vm.local_disk_counter += 1
        if vm.local_disk_counter > vm.max_local_disks:
          raise errors.Error('Not enough local disks.')
        disks.append(data_disk)

      scratch_disk = self._CreateScratchDiskFromDisks(vm, disk_spec, disks)
      # Device path is needed to stripe disks on Linux, but not on Windows.
      # The path is not updated for Windows machines.
      if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        nvme_devices = vm.GetNVMEDeviceInfo()
        remote_nvme_devices = vm.FindRemoteNVMEDevices(
            scratch_disk, nvme_devices
        )
        vm.UpdateDevicePath(scratch_disk, remote_nvme_devices)
      disk_strategies.PrepareScratchDiskStrategy().PrepareScratchDisk(
          vm, scratch_disk, disk_spec
      )
      if disk_spec.num_striped_disks > 1:
        break


class SetUpPDDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to Persistance disk on GCE."""

  def __init__(self, disk_specs: list[gce_disk.GceDiskSpec]):
    super().__init__(disk_specs)
    self.disk_specs = disk_specs

  def SetUpDisk(self, vm: Any, _: disk.BaseDiskSpec):
    # disk spec is not used here.
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
        # Remote disk numbers start at 1+max_local_disks (0 is the system disk
        # and local disks occupy 1-max_local_disks).
        data_disk.disk_number = vm.remote_disk_counter + 1 + vm.max_local_disks
        vm.remote_disk_counter += 1
        disks.append(data_disk)

      scratch_disk = self._CreateScratchDiskFromDisks(vm, disk_spec, disks)
      # Device path is needed to stripe disks on Linux, but not on Windows.
      # The path is not updated for Windows machines.
      if vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        nvme_devices = vm.GetNVMEDeviceInfo()
        remote_nvme_devices = vm.FindRemoteNVMEDevices(
            scratch_disk, nvme_devices
        )
        vm.UpdateDevicePath(scratch_disk, remote_nvme_devices)
      disk_strategies.PrepareScratchDiskStrategy().PrepareScratchDisk(
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


def _GenerateDiskNamePrefix(
    vm: 'virtual_machine. BaseVirtualMachine', disk_spec_id: int, index: int
) -> str:
  """Generates a deterministic disk name given disk_spec_id and index."""
  return f'{vm.name}-data-{disk_spec_id}-{index}'
