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
import time
from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS
virtual_machine = Any  # pylint: disable=invalid-name


def GetCreateDiskStrategy(
    vm: 'virtual_machine.BaseVirtualMachine',
    disk_spec: aws_disk.AwsDiskSpec,
    disk_count: int,
) -> 'AWSCreateDiskStrategy':
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

  def BuildDiskSpecId(self, spec_index, strip_index):
    """Generates an unique string to represent a disk_spec position."""
    return f'{spec_index}_{strip_index}'

  def GetBlockDeviceMap(self) -> list[dict[str, str]]:
    # Implemented by subclasses.
    raise NotImplementedError()


class CreateLocalDiskStrategy(AWSCreateDiskStrategy):
  """Creates a local disk on AWS."""

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    return SetUpLocalDiskStrategy(self.vm, self.disk_specs)  # pytype: disable=wrong-arg-types

  def GetBlockDeviceMap(self) -> list[dict[str, str]]:
    mappings = []
    if not self.DiskCreatedOnVMCreation():
      return mappings
    for spec_index, disk_spec in enumerate(self.disk_specs):
      if not self.vm.DiskTypeCreatedOnVMCreation(disk_spec.disk_type):
        continue
      for i in range(self.vm.max_local_disks):
        mapping = collections.OrderedDict()
        device_letter = aws_disk.AwsDisk.GenerateDeviceLetter(self.vm.name)
        device_name_prefix = aws_disk.AwsDisk.GenerateDeviceNamePrefix()
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
    self.setup_disk_strategy = None

  def DiskCreatedOnVMCreation(self) -> bool:
    """Returns whether the disk is created on VM creation."""
    return self.disk_spec.create_with_vm

  def GetSetupDiskStrategy(self) -> disk_strategies.SetUpDiskStrategy:
    """Returns the SetUpDiskStrategy for the disk."""
    if self.setup_disk_strategy is None:
      self.setup_disk_strategy = SetUpRemoteDiskStrategy(  # pytype: disable=wrong-arg-types
          self.vm, self.disk_specs
      )
    return self.setup_disk_strategy

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
        device_name_prefix = aws_disk.AwsDisk.GenerateDeviceNamePrefix()
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
        if disk_spec.provisioned_throughput and disk_spec.disk_type in [
            aws_disk.GP3
        ]:
          ebs_block['Throughput'] = disk_spec.provisioned_throughput
        mapping['Ebs'] = ebs_block
        mappings.append(mapping)
        disk_spec_id = self.BuildDiskSpecId(spec_index, i)
        self.vm.LogDeviceByDiskSpecId(disk_spec_id, device_name)
    return mappings


class CreateNonResourceDiskStrategy(
    disk_strategies.EmptyCreateDiskStrategy, AWSCreateDiskStrategy
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
    elif self.disk_spec.disk_type == disk.NFS:
      return disk_strategies.SetUpNFSDiskStrategy(self.vm, self.disk_spec)
    elif self.disk_spec.disk_type == disk.OBJECT_STORAGE:
      return SetUpS3MountPointDiskStrategy(self.vm, self.disk_spec)
    elif self.disk_spec.disk_type == disk.LUSTRE:
      return AwsLustreSetupDiskStrategy(self.vm, self.disk_spec)

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
    # pytype: disable=attribute-error
    disks = scratch_disk.disks if scratch_disk.is_striped else [scratch_disk]
    for d in disks:
      if d.disk_type == disk.NFS:
        continue
      elif d.disk_type == disk.LOCAL:
        if aws_disk.LocalDriveIsNvme(self.vm.machine_type):
          # assign in a round robin manner. Since NVME local disks
          # are created ignoring all pkb naming conventions and assigned
          # random names on the fly.
          disk_name = self.GetDeviceByDiskSpecId(d.disk_spec_id)
          self.vm.LogDeviceByName(disk_name, None, local_devices.pop())
      elif aws_disk.EbsDriveIsNvme(self.vm.machine_type):
        # EBS NVME volumes have disk_name assigned
        # looks up disk_name by disk_spec_id
        # populate the aws identifier information
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
      # pytype: enable=attribute-error
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

  LOCAL_DRIVE_START_LETTER = 'b'

  def __init__(
      self,
      vm: 'virtual_machine.BaseVirtualMachine',
      disk_specs: list[aws_disk.AwsDiskSpec],
  ):
    super().__init__(vm, disk_specs[0])
    self.vm = vm
    self.disk_specs = disk_specs

  def SetUpDisk(self):
    for spec_id, disk_spec in enumerate(self.disk_specs):
      disks = []
      for i in range(disk_spec.num_striped_disks):
        data_disk = self._CreateLocalDisk(disk_spec, spec_id, i)
        disks.append(data_disk)
      if len(disks) > 1:
        # If the disk_spec called for a striped disk, create one.
        scratch_disk = disk.StripedDisk(disk_spec, disks)
      else:
        scratch_disk = disks[0]
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

      # in the event that local disks are not striped and kept as raw disks.
      if self.vm.local_disk_counter < self.vm.max_local_disks:
        for i in range(self.vm.local_disk_counter, self.vm.max_local_disks):
          data_disk = self._CreateLocalDisk(disk_spec, spec_id, i)
          disks.append(data_disk)

          if self.vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
            nvme_devices = self.vm.GetNVMEDeviceInfo()
            self.PopulateNVMEDevicePath(data_disk, nvme_devices)
            self.UpdateDevicePath(data_disk)
            AWSPrepareScratchDiskStrategy().PrepareScratchDisk(
                self.vm, data_disk, disk_spec
            )
      self.vm.SetupLocalDisks()

  def _CreateLocalDisk(self, disk_spec, spec_id, i):
    disk_spec_id = self.vm.create_disk_strategy.BuildDiskSpecId(spec_id, i)
    data_disk = aws_disk.AwsDisk(
        disk_spec, self.vm.zone, self.vm.machine_type, disk_spec_id
    )
    device_letter = chr(
        ord(self.LOCAL_DRIVE_START_LETTER) + self.vm.local_disk_counter
    )
    nvme_boot_drive_index = self._GetNvmeBootIndex()
    data_disk.AssignDeviceLetter(device_letter, nvme_boot_drive_index)  # pytype: disable=wrong-arg-types
    data_disk.disk_number = self.vm.local_disk_counter + 1
    self.vm.local_disk_counter += 1
    if self.vm.local_disk_counter > self.vm.max_local_disks:
      raise errors.Error('Not enough local disks.')
    return data_disk

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
    self.scratch_disks = []
    self.time_to_visible = None

  def SetUpDisk(self):
    create_tasks = []
    scratch_disks = []

    for disk_spec_id, disk_spec in enumerate(self.disk_specs):
      disk_group = self.vm.create_disk_strategy.remote_disk_groups[disk_spec_id]
      if len(disk_group) > 1:
        # If the disk_spec called for a striped disk, create one.
        scratch_disk = aws_disk.AwsStripedDisk(disk_spec, disk_group)
      else:
        scratch_disk = disk_group[0]
      scratch_disks.append((scratch_disk, disk_spec))
      if not self.vm.create_disk_strategy.DiskCreatedOnVMCreation():
        create_tasks.append((scratch_disk.Create, (), {}))
    self.scratch_disks = [scratch_disk for scratch_disk, _ in scratch_disks]
    background_tasks.RunParallelThreads(create_tasks, max_concurrency=200)
    self.AttachDisks()
    for scratch_disk, disk_spec in scratch_disks:
      if self.vm.OS_TYPE not in os_types.WINDOWS_OS_TYPES:
        # here, all disks are created (either at vm creation or in
        # line above). But we don't have all the raw device paths,
        # which are necessary for striping the scratch disk on Linux,
        # but not on Windows.
        # The path is not updated for Windows machines.
        nvme_devices = self.vm.GetNVMEDeviceInfo()
        self.PopulateNVMEDevicePath(scratch_disk, nvme_devices)
        self.UpdateDevicePath(scratch_disk)
      AWSPrepareScratchDiskStrategy().PrepareScratchDisk(
          self.vm, scratch_disk, disk_spec
      )
    for scratch_disk in self.scratch_disks:
      if hasattr(scratch_disk, 'disk_spec_id'):
        scratch_disk.disk_name = self.vm.device_by_disk_spec_id.get(
            scratch_disk.disk_spec_id, None
        )
        if scratch_disk.disk_name in self.vm.disk_identifiers_by_device:
          scratch_disk.id = self.vm.disk_identifiers_by_device[
              scratch_disk.disk_name
          ].volume_id

  def AttachDisks(self):
    attach_tasks = []
    start_time = time.time()
    if (
        self.vm.create_disk_strategy.DiskCreatedOnVMCreation()
        or not self.scratch_disks
    ):
      return
    attach_tasks.append(
        (self.WaitForRemoteDisksToVisibleFromVm, [start_time], {})
    )
    for scratch_disk in self.scratch_disks:
      attach_tasks.append((scratch_disk.Attach, [self.vm], {}))
    return_from_threads = background_tasks.RunParallelThreads(
        attach_tasks, max_concurrency=200
    )
    self.time_to_visible = return_from_threads[0]


class SetUpS3MountPointDiskStrategy(AWSSetupDiskStrategy):
  """Strategies to set up S3 buckets."""

  DEFAULT_MOUNT_OPTIONS = [
      '--allow-other',
      '--dir-mode=755',
      '--file-mode=755',
      # Sets part size to 64MiB. Max upload 64000MiB.
      '--write-part-size=67108864',
      '--incremental-upload',
      '--allow-overwrite',
      '--allow-delete',
  ]

  def SetUpDisk(self):
    """Performs setup of S3 buckets."""
    self.vm.Install('mountpoint')
    target = self.disk_spec.mount_point
    self.vm.RemoteCommand(f'sudo mkdir -p {target} && sudo chmod a+w {target}')

    all_mount_options = self.DEFAULT_MOUNT_OPTIONS + FLAGS.mount_options
    if not FLAGS.aws_s3_mount_enable_metadata_cache:
      all_mount_options += ['--metadata-ttl=0']

    opts = ' '.join(all_mount_options)
    zone_id = util.GetZoneId(self.vm.zone)
    s3_express_zonal_suffix = f'--{zone_id}--x-s3'
    bucket_name = (
        FLAGS.object_storage_fuse_bucket_name
        or f'mountpoint-{FLAGS.run_uri.lower()}{s3_express_zonal_suffix}'
    )
    s3_spec = s3.S3BucketSpec(
        mount_point=target,
        bucket_name=bucket_name,
        region=util.GetRegionFromZone(self.vm.zone),
        zone=self.vm.zone,
        is_s3_express=True,
    )
    s3_client = s3.S3Bucket(s3_spec)
    if not FLAGS.object_storage_fuse_bucket_name:
      s3_client.Create()

    self.vm.RemoteCommand(f'sudo mount-s3 {bucket_name} {target} {opts}')
    self.vm.scratch_disks.append(s3_client)


class AWSPrepareScratchDiskStrategy(disk_strategies.PrepareScratchDiskStrategy):
  """Strategies to prepare scratch disk on AWS."""

  def GetLocalSSDNames(self) -> list[str]:
    return ['NVMe Amazon EC2 NVMe']


class AwsLustreSetupDiskStrategy(disk_strategies.SetUpLustreDiskStrategy):
  """Strategies to prepare Lustre based disks on AWS."""

  def SetUpDiskOnLinux(self):
    """Performs Linux specific setup of Lustre disk."""
    vm = self.vm
    vm.InstallPackages('lustre-client')
    if FLAGS.aws_efa:
      # https://docs.aws.amazon.com/fsx/latest/LustreGuide/configure-efa-clients.html
      stdout, _ = vm.RemoteCommand('ip -br -4 a sh | grep $(hostname -i)')
      eth = stdout.split()[0]
      vm.RemoteCommand(
          'sudo modprobe lnet; '
          f'sudo /sbin/modprobe kefalnd ipif_name={eth}; '
          'sudo modprobe ksocklnd; sudo lnetctl lnet configure'
      )
      vm.RemoteCommand(
          f'sudo lnet del --net tcp; sudo lnetctl net add --net tcp --if {eth}'
      )
      stdout, _ = vm.RemoteCommand('ls -1 /sys/class/infiniband | wc -l')
      num_efa = int(stdout)
      vm.RemoteCommand(
          'sudo lnetctl net add --net efa --if '
          '$(ls -1 /sys/class/infiniband | head -n1) --peer-credits 32'
      )
      if num_efa > 1:
        vm.RemoteCommand(
            'sudo lnetctl net add --net efa --if '
            '$(ls -1 /sys/class/infiniband | tail -n1) --peer-credits 32'
        )
      vm.RemoteCommand('sudo lnetctl set discovery 1')
      vm.RemoteCommand('sudo lnetctl udsp add --src efa --priority 0')
      vm.RemoteCommand('sudo lnetctl set discovery 1')
      vm.RemoteCommand('sudo modprobe lustre;  sudo lnetctl net show')
    super().SetUpDiskOnLinux()
    # Apply best practices
    # https://docs.aws.amazon.com/fsx/latest/LustreGuide/performance-tips.html
    vm.RemoteCommand('sudo lctl set_param ldlm.namespaces.*.lru_max_age=600000')
    vm.RemoteCommand('sudo lctl set_param ldlm.namespaces.*.lru_size=18000')
    vm.RemoteCommand(
        'sudo touch /etc/modprobe.d/modprobe.conf; '
        'sudo chmod 766 /etc/modprobe.d/modprobe.conf; '
        'echo "options ptlrpc ptlrpcd_per_cpt_max=32" >> '
        '/etc/modprobe.d/modprobe.conf'
    )
    vm.RemoteCommand(
        'echo "options ksocklnd credits=2560" >> /etc/modprobe.d/modprobe.conf'
    )
    vm.RemoteCommand('sudo lctl set_param osc.*OST*.max_rpcs_in_flight=32')
    vm.RemoteCommand('sudo lctl set_param mdc.*.max_rpcs_in_flight=64')
