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
import threading
import time
from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
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
  if disk_spec and disk_count > 0:
    if disk_spec.disk_type in gce_disk.GCE_REMOTE_DISK_TYPES:
      return CreatePDDiskStrategy(vm, disk_spec, disk_count)
    elif disk_spec.disk_type == disk.LOCAL:
      return CreateLSSDDiskStrategy(vm, disk_spec, disk_count)
  return GCECreateNonResourceDiskStrategy(vm, disk_spec, disk_count)


class GCPCreateDiskStrategy(disk_strategies.CreateDiskStrategy):
  """Same as CreateDiskStrategy, but with GCP Disk spec."""

  disk_spec: gce_disk.GceDiskSpec


class CreatePDDiskStrategy(GCPCreateDiskStrategy):
  """Contains logic to create persistence disk on GCE."""

  def __init__(self, vm: Any, disk_spec: disk.BaseDiskSpec, disk_count: int):
    super().__init__(vm, disk_spec, disk_count)
    self.remote_disk_groups = []
    self.setup_disk_strategy = None
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disks = []
      for i in range(disk_spec.num_striped_disks):
        name = _GenerateDiskNamePrefix(
            vm,
            disk_spec.multi_writer_group_name,
            disk_spec_id,
            disk_spec.multi_writer_mode,
            i,
        )
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
      self.remote_disk_groups.append(disks)

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    if self.disk_spec.replica_zones or self.disk_spec.multi_writer_mode:
      # GCE regional disks cannot use create-on-create.
      return False
    return self.disk_spec.create_with_vm

  def AddMetadataToDiskResource(self):
    if not self.DiskCreatedOnVMCreation():
      return
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      for i in range(disk_spec.num_striped_disks):
        name = _GenerateDiskNamePrefix(
            self.vm,
            disk_spec.multi_writer_group_name,
            disk_spec_id,
            disk_spec.multi_writer_mode,
            i,
        )
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
    for disk_group in self.remote_disk_groups:
      for pd_disk in disk_group:
        create_disks.append(pd_disk.GetCreateFlags())
    if create_disks:
      dic['create-disk'] = create_disks
    return dic

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    if self.disk_spec.multi_writer_mode:
      self.setup_disk_strategy = SetUpMultiWriterPDDiskStrategy(
          self.vm, self.disk_specs
      )
    elif self.setup_disk_strategy is None:
      self.setup_disk_strategy = SetUpPDDiskStrategy(self.vm, self.disk_specs)
    return self.setup_disk_strategy


class CreateLSSDDiskStrategy(GCPCreateDiskStrategy):
  """Contains logic to create LSSD disk on VM."""

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    return True

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    if self.disk_spec.num_partitions and self.disk_spec.partition_size:
      return SetUpPartitionedGceLocalDiskStrategy(self.vm, self.disk_spec)
    else:
      return SetUpGceLocalDiskStrategy(self.vm, self.disk_spec)


class GCECreateNonResourceDiskStrategy(disk_strategies.EmptyCreateDiskStrategy):
  """CreateDiskStrategy when there is no pd disks."""

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

    elif self.disk_spec.disk_type == disk.OBJECT_STORAGE:
      return SetUpGcsFuseDiskStrategy(self.vm, self.disk_spec)

    elif self.disk_spec.disk_type == disk.NFS:
      return disk_strategies.SetUpNFSDiskStrategy(self.vm, self.disk_spec)

    return disk_strategies.EmptySetupDiskStrategy(self.vm, self.disk_spec)


class SetUpGCEResourceDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Base Strategy class to set up local ssd and pd-ssd."""

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

  def SetupGCELocalDisks(self):
    """Performs Linux specific setup of local disks."""
    pass


class SetUpGceLocalDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to set up local disks."""

  def SetUpDisk(self):
    # disk spec is not used here.
    self.SetupGCELocalDisks()
    disks = []
    for _ in range(self.disk_spec.num_striped_disks):
      data_disk = self._CreateLocalDisk()
      disks.append(data_disk)
    if len(disks) == 1:
      scratch_disk = disks[0]
    else:
      scratch_disk = disk.StripedDisk(self.disk_spec, disks)
    GCEPrepareScratchDiskStrategy().PrepareScratchDisk(
        self.vm, scratch_disk, self.disk_spec
    )

    # In the event local disks are not striped together and kept as raw disks
    if self.disk_spec.num_striped_disks < self.vm.max_local_disks:
      for _ in range(
          self.vm.max_local_disks - self.disk_spec.num_striped_disks
      ):
        data_disk = self._CreateLocalDisk()
        disks.append(data_disk)
        GCEPrepareScratchDiskStrategy().PrepareScratchDisk(
            self.vm, data_disk, self.disk_spec
        )
    self.vm.SetupLocalDisks()

  def _CreateLocalDisk(self):
    if self.vm.ssd_interface == gce_disk.SCSI:
      name = 'local-ssd-%d' % self.vm.local_disk_counter
    elif self.vm.ssd_interface == gce_disk.NVME:
      name = f'local-nvme-ssd-{self.vm.local_disk_counter}'
    else:
      raise errors.Error('Unknown Local SSD Interface.')
    self.vm.local_disk_counter += 1
    if self.vm.local_disk_counter > self.vm.max_local_disks:
      raise errors.Error('Not enough local disks.')
    return gce_disk.GceLocalDisk(self.disk_spec, name)


class SetUpPartitionedGceLocalDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to set up local disks with custom partitions."""

  def SetUpDisk(self):
    self.SetupGCELocalDisks()
    while self.vm.local_disk_counter < self.vm.max_local_disks:
      if self.vm.ssd_interface == gce_disk.SCSI:
        name = 'local-ssd-%d' % self.vm.local_disk_counter
      elif self.vm.ssd_interface == gce_disk.NVME:
        name = f'local-nvme-ssd-{self.vm.local_disk_counter}'
      else:
        raise errors.Error('Unknown Local SSD Interface.')
      raw_disk = gce_disk.GceLocalDisk(self.disk_spec, name)
      self.vm.local_disk_counter += 1
      if self.vm.IsDiskFormatted(name, self.disk_spec.num_partitions):
        # Skip if already partitioned/formatted.
        continue
      partitions = self.vm.PartitionDisk(
          name,
          raw_disk.GetDevicePath(),
          raw_disk.num_partitions,
          raw_disk.partition_size,
      )
      for partition_name in partitions:
        scratch_disk = gce_disk.GceLocalDisk(self.disk_spec, partition_name)
        GCEPrepareScratchDiskStrategy().PrepareScratchDisk(
            self.vm, scratch_disk, self.disk_spec
        )


class SetUpMultiWriterPDDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to Persistence disk on GCE."""

  _MULTIWRITER_DISKS = {}
  _GLOBAL_LOCK = threading.Lock()

  def __init__(self, vm, disk_specs: list[gce_disk.GceDiskSpec]):
    if vm.OS_TYPE in os_types.LINUX_OS_TYPES:
      raise errors.Error(
          'MultiWriter disk(s) configuration is currently not supported for'
          ' Linux vm.'
      )

    super().__init__(vm, disk_specs[0])
    self.disk_specs = disk_specs
    self.scratch_disks = []

  def SetUpDisk(self):
    self._GLOBAL_LOCK.acquire()
    create_and_setup_disk = False
    self.scratch_disks = self.vm.create_disk_strategy.remote_disk_groups[0]
    for scratch_disk in self.scratch_disks:
      if scratch_disk.name not in self._MULTIWRITER_DISKS:
        create_and_setup_disk = True
        scratch_disk.Create()
        self._MULTIWRITER_DISKS[scratch_disk.name] = True

      scratch_disk.Attach(self.vm)
      # Device path is needed to stripe disks on Linux, but not on Windows.
      # The path is not updated for Windows machines.
      if self.vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        nvme_devices = self.vm.GetNVMEDeviceInfo()
        remote_nvme_devices = self.FindRemoteNVMEDevices(nvme_devices)
        self.UpdateDevicePath(scratch_disk, remote_nvme_devices)
    if create_and_setup_disk:
      GCEPrepareScratchDiskStrategy().PrepareScratchDisk(
          self.vm, self.scratch_disks[0], self.disk_specs[0]
      )

    # Add scratch disk to the vm.scratch_disks list
    for scratch_disk in self.scratch_disks:
      # Add scratch_disk to vm.scratch_disks list
      if (scratch_disk not in self.vm.scratch_disks):
        self.vm.scratch_disks.append(scratch_disk)

    self._GLOBAL_LOCK.release()


class SetUpPDDiskStrategy(SetUpGCEResourceDiskStrategy):
  """Strategies to Persistance disk on GCE."""

  def __init__(self, vm, disk_specs: list[gce_disk.GceDiskSpec]):
    super().__init__(vm, disk_specs[0])
    self.disk_specs = disk_specs
    self.scratch_disks = []
    self.time_to_visible = None

  def SetUpDisk(self):
    # disk spec is not used here.
    create_tasks = []
    scratch_disks = []
    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disk_group = self.vm.create_disk_strategy.remote_disk_groups[disk_spec_id]
      if len(disk_group) > 1:
        # If the disk_spec called for a striped disk, create one.
        scratch_disk = gce_disk.GceStripedDisk(disk_spec, disk_group)
      else:
        scratch_disk = disk_group[0]
      scratch_disks.append((scratch_disk, disk_spec))
      if not self.vm.create_disk_strategy.DiskCreatedOnVMCreation():
        create_tasks.append((scratch_disk.Create, (), {}))
    background_tasks.RunParallelThreads(create_tasks, max_concurrency=200)
    self.scratch_disks = [scratch_disk for scratch_disk, _ in scratch_disks]
    self.AttachDisks()
    for scratch_disk, disk_spec in scratch_disks:
      # Device path is needed to stripe disks on Linux, but not on Windows.
      # The path is not updated for Windows machines.
      if self.vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        nvme_devices = self.vm.GetNVMEDeviceInfo()
        remote_nvme_devices = self.FindRemoteNVMEDevices(nvme_devices)
        self.UpdateDevicePath(scratch_disk, remote_nvme_devices)
      GCEPrepareScratchDiskStrategy().PrepareScratchDisk(
          self.vm, scratch_disk, disk_spec
      )

  def AttachDisks(self):
    attach_tasks = []
    start_time = time.time()
    if (
        self.vm.create_disk_strategy.DiskCreatedOnVMCreation()
        or not self.scratch_disks
    ):
      return
    attach_tasks.append((self.WaitForDisksToVisibleFromVm, [start_time], {}))
    for scratch_disk in self.scratch_disks:
      attach_tasks.append((scratch_disk.Attach, [self.vm], {}))
    return_from_threads = background_tasks.RunParallelThreads(
        attach_tasks, max_concurrency=200
    )
    self.time_to_visible = return_from_threads[0]


class SetUpGcsFuseDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Strategies to set up ram disks."""

  DEFAULT_MOUNT_OPTIONS = [
      'allow_other',
      'dir_mode=755',
      'file_mode=755',
      'implicit_dirs',
  ]

  def SetUpDiskOnLinux(self):
    """Performs Linux specific setup of ram disk."""
    scratch_disk = disk.BaseDisk(self.disk_spec)
    self.vm.Install('gcsfuse')
    self.vm.RemoteCommand(
        f'sudo mkdir -p {self.disk_spec.mount_point} && sudo chmod a+w'
        f' {self.disk_spec.mount_point}'
    )

    opts = ','.join(self.DEFAULT_MOUNT_OPTIONS + FLAGS.mount_options)
    bucket = FLAGS.gcsfuse_bucket
    target = self.disk_spec.mount_point
    self.vm.RemoteCommand(f'sudo mount -t gcsfuse -o {opts} {bucket} {target}')
    self.vm.scratch_disks.append(scratch_disk)


class GCEPrepareScratchDiskStrategy(disk_strategies.PrepareScratchDiskStrategy):
  """Strategies to prepare scratch disk on GCE."""

  def GetLocalSSDNames(self):
    return ['Google EphemeralDisk', 'nvme_card']


def _GenerateDiskNamePrefix(
    vm: 'virtual_machine. BaseVirtualMachine',
    group_name: str,
    disk_spec_id: int,
    is_multiwriter: bool,
    index: int,
) -> str:
  """Generates a deterministic disk name given disk_spec_id and index."""
  if is_multiwriter:
    # if group_name is not specified, use the vm_group name
    if not group_name:
      group_name = vm.vm_group
    return f'pkb-{FLAGS.run_uri}-multiwriter-{group_name}-{index}'
  return f'{vm.name}-data-{disk_spec_id}-{index}'
