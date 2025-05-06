# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing classes related to GCE disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
Use 'gcloud compute disk-types list' to determine valid disk types.
"""

import json
import logging
import time
from typing import Any

from absl import flags
import dateutil.parser
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import boot_disk
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import util
import requests
from requests import adapters


FLAGS = flags.FLAGS

PD_STANDARD = 'pd-standard'
PD_SSD = 'pd-ssd'
PD_BALANCED = 'pd-balanced'
PD_EXTREME = 'pd-extreme'
HYPERDISK_THROUGHPUT = 'hyperdisk-throughput'
HYPERDISK_EXTREME = 'hyperdisk-extreme'
HYPERDISK_BALANCED = 'hyperdisk-balanced'
HYPERDISK_BALANCED_HA = 'hyperdisk-balanced-high-availability'
GCE_REMOTE_DISK_TYPES = [
    PD_STANDARD,
    PD_SSD,
    PD_BALANCED,
    PD_EXTREME,
    HYPERDISK_THROUGHPUT,
    HYPERDISK_EXTREME,
    HYPERDISK_BALANCED,
    HYPERDISK_BALANCED_HA,
]
# Defaults picked to align with console.
GCE_DYNAMIC_IOPS_DISK_TYPE_DEFAULTS = {
    PD_EXTREME: 100000,
    HYPERDISK_EXTREME: 100000,
    HYPERDISK_BALANCED: 3600,
    HYPERDISK_BALANCED_HA: 3600,
}
# Defaults picked to align with console.
GCE_DYNAMIC_THROUGHPUT_DISK_TYPE_DEFAULTS = {
    HYPERDISK_BALANCED: 290,
    HYPERDISK_THROUGHPUT: 180,
    HYPERDISK_BALANCED_HA: 290,
}

REGIONAL_DISK_SCOPE = 'regional'

DISK_METADATA = {
    PD_STANDARD: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
    },
    PD_BALANCED: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    PD_SSD: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    PD_EXTREME: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    HYPERDISK_THROUGHPUT: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
    },
    HYPERDISK_EXTREME: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    HYPERDISK_BALANCED: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    HYPERDISK_BALANCED_HA: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.REGION,
    },
    disk.LOCAL: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.NONE,
    },
}

SCSI = 'SCSI'
NVME = 'NVME'

# Latest GCE families have a preset
# number of SSDs, so we set those attributes directly from the machine type.
FIXED_SSD_MACHINE_TYPES = {
    'z3-highmem-176': 12,
    'z3-highmem-88': 12,
    'z3-highmem-88-standardlssd': 6,
    'c3-standard-4-lssd': 1,
    'c3-standard-8-lssd': 2,
    'c3-standard-22-lssd': 4,
    'c3-standard-44-lssd': 8,
    'c3-standard-88-lssd': 16,
    'c3d-standard-8-lssd': 1,
    'c3d-standard-16-lssd': 1,
    'c3d-standard-30-lssd': 2,
    'c3d-standard-60-lssd': 4,
    'c3d-standard-90-lssd': 8,
    'c3d-standard-180-lssd': 16,
    'c3d-standard-360-lssd': 32,
    'c3d-highmem-8-lssd': 1,
    'c3d-highmem-16-lssd': 1,
    'c3d-highmem-30-lssd': 2,
    'c3d-highmem-60-lssd': 4,
    'c3d-highmem-90-lssd': 8,
    'c3d-highmem-180-lssd': 16,
    'c3d-highmem-360-lssd': 32,
}

NVME_PD_MACHINE_FAMILIES = ['m3']


class GceServiceUnavailableError(Exception):
  """Error for retrying _Attach when the describe output indicates that 'The service is currently unavailable'."""


def PdDriveIsNvme(vm):
  """Check if the machine uses NVMe for PD."""
  machine_type = vm.machine_type
  family = machine_type.split('-')[0].lower()
  if family in NVME_PD_MACHINE_FAMILIES:
    return True
  # In other cases, only a sub-category of a family uses nvme,
  # such as confidential VMs on Milan.
  # this is not robust, but can get refactored when
  # there is more clarity on what groups of VMs are NVMe.
  if gcp_flags.GCE_CONFIDENTIAL_COMPUTE.value:
    return True
  return False


# Add labels fails sometimes with a timeout - consider moving to Create().
@vm_util.Retry(
    max_retries=3,
    retryable_exceptions=(
        errors.VmUtil.IssueCommandTimeoutError,
    ),
)
def AddLabels(gcp_resource: resource.BaseResource, disk_name: str):
  """Add labels to a disk created by a service that fails to label a disk.

  Disks created by PKB (and most services) are labeled at create time.

  Args:
    gcp_resource: a resource with the project and zone of the disk.
    disk_name: the name of the disk
  """
  cmd = util.GcloudCommand(
      gcp_resource, 'compute', 'disks', 'add-labels', disk_name
  )
  cmd.flags['labels'] = util.MakeFormattedDefaultTags()
  cmd.Issue()


class GceDiskSpec(disk.BaseDiskSpec):
  """Object holding the information needed to create a GCPDisk."""

  interface: str
  num_local_ssds: int
  create_with_vm: bool
  replica_zones: list[str]

  CLOUD = provider_info.GCP

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Overrides config values with flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. Is
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.

    Returns:
      dict mapping config option names to values derived from the config
      values or flag values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['gce_ssd_interface'].present:
      config_values['interface'] = flag_values.gce_ssd_interface
    if flag_values['gce_num_local_ssds'].present:
      config_values['num_local_ssds'] = flag_values.gce_num_local_ssds
    if flag_values['gcp_create_disks_with_vm'].present:
      config_values['create_with_vm'] = flag_values.gcp_create_disks_with_vm
    if flag_values['data_disk_zones'].present:
      config_values['replica_zones'] = flag_values.data_disk_zones

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'interface': (option_decoders.StringDecoder, {'default': 'NVME'}),
        'num_local_ssds': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0},
        ),
        'create_with_vm': (
            option_decoders.BooleanDecoder,
            {'default': True},
        ),
        'replica_zones': (
            option_decoders.ListDecoder,
            {
                'item_decoder': option_decoders.StringDecoder(),
                'default': None,
            },
        ),
    })
    return result


class GceBootDisk(boot_disk.BootDisk):
  """Object representing a GCE Boot Disk."""

  def __init__(self, vm, boot_disk_spec):
    """Initialize a Boot Diks."""
    super().__init__(boot_disk_spec)
    self.name = vm.name
    self.image = None
    self.vm = vm
    self.boot_disk_size = boot_disk_spec.boot_disk_size
    self.boot_disk_type = boot_disk_spec.boot_disk_type
    self.boot_disk_iops = None
    self.boot_disk_throughput = None
    if self.boot_disk_type in GCE_DYNAMIC_IOPS_DISK_TYPE_DEFAULTS.keys():
      self.boot_disk_iops = boot_disk_spec.boot_disk_iops
    if self.boot_disk_type in GCE_DYNAMIC_THROUGHPUT_DISK_TYPE_DEFAULTS.keys():
      self.boot_disk_throughput = boot_disk_spec.boot_disk_throughput

  def GetCreationCommand(self):
    dic = {'boot-disk-auto-delete': True}
    if self.boot_disk_size:
      dic['boot-disk-size'] = self.boot_disk_size
    if self.boot_disk_type:
      dic['boot-disk-type'] = self.boot_disk_type
    if self.boot_disk_iops:
      dic['boot-disk-provisioned-iops'] = self.boot_disk_iops
    if self.boot_disk_throughput:
      dic['boot-disk-provisioned-throughput'] = self.boot_disk_throughput
    return dic

  @vm_util.Retry()
  def PostCreate(self):
    getdisk_cmd = util.GcloudCommand(
        self.vm, 'compute', 'disks', 'describe', self.name
    )
    stdout, _, _ = getdisk_cmd.Issue()
    response = json.loads(stdout)
    # Updates all the attrubutes to make sure it's up to date
    self.image = response['sourceImage'].split('/')[-1]
    self.boot_disk_size = response['sizeGb']
    self.boot_disk_type = response['type'].split('/')[-1]

  def GetResourceMetadata(self):
    result = super().GetResourceMetadata()
    result['boot_disk_type'] = self.boot_disk_type
    result['boot_disk_size'] = self.boot_disk_size
    if self.boot_disk_iops:
      result['boot_disk_provisioned_iops'] = self.boot_disk_iops
    if self.boot_disk_throughput:
      result['boot_disk_provisioned_throughput'] = self.boot_disk_throughput
    return result


class GceLocalDisk(disk.BaseDisk):
  """Object representing a GCE Local Disk."""

  def __init__(self, disk_spec, name):
    super().__init__(disk_spec)
    self.interface = disk_spec.interface
    self.metadata['interface'] = disk_spec.interface
    self.metadata.update(DISK_METADATA[disk_spec.disk_type])
    self.name = name

  def GetDevicePath(self) -> str:
    return f'/dev/disk/by-id/google-{self.name}'

  def IsNvme(self):
    return self.interface == NVME


class GceDisk(disk.BaseDisk):
  """Object representing a GCE Disk."""

  def __init__(
      self,
      disk_spec,
      name,
      zone,
      project,
      image=None,
      image_project=None,
      replica_zones=None,
  ):
    super().__init__(disk_spec)
    self.spec = disk_spec
    self.interface = disk_spec.interface
    self.attached_vm_name = None
    self.image = image
    self.image_project = image_project
    self.name = name
    self.zone = zone
    self.project = project
    self.replica_zones = replica_zones
    self.region = util.GetRegionFromZone(self.zone)
    self.provisioned_iops = None
    self.provisioned_throughput = None
    if self.disk_type in GCE_DYNAMIC_IOPS_DISK_TYPE_DEFAULTS.keys():
      self.provisioned_iops = disk_spec.provisioned_iops
      if not self.provisioned_iops:
        self.provisioned_iops = GCE_DYNAMIC_IOPS_DISK_TYPE_DEFAULTS[
            self.disk_type
        ]
      self.metadata['iops'] = self.provisioned_iops
    if self.disk_type in GCE_DYNAMIC_THROUGHPUT_DISK_TYPE_DEFAULTS.keys():
      self.provisioned_throughput = disk_spec.provisioned_throughput
      if not self.provisioned_throughput:
        self.provisioned_throughput = GCE_DYNAMIC_THROUGHPUT_DISK_TYPE_DEFAULTS[
            self.disk_type
        ]
      self.metadata['throughput'] = self.provisioned_throughput

    disk_metadata = DISK_METADATA[disk_spec.disk_type]
    if self.replica_zones:
      disk_metadata[disk.REPLICATION] = disk.REGION
      self.metadata['replica_zones'] = replica_zones
    self.metadata.update(DISK_METADATA[disk_spec.disk_type])

  def _Create(self):
    """Creates the disk."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'create', self.name)
    cmd.flags['size'] = self.disk_size
    cmd.flags['type'] = self.disk_type
    if self.provisioned_iops:
      cmd.flags['provisioned-iops'] = self.provisioned_iops
    if self.provisioned_throughput:
      cmd.flags['provisioned-throughput'] = self.provisioned_throughput
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    if self.image:
      cmd.flags['image'] = self.image
    if self.image_project:
      cmd.flags['image-project'] = self.image_project

    if self.replica_zones:
      cmd.flags['region'] = self.region
      cmd.flags['replica-zones'] = ','.join(self.replica_zones)
      del cmd.flags['zone']

    if self.multi_writer_disk:
      cmd.flags['access-mode'] = 'READ_WRITE_MANY'
    if self.spec.snapshot_name:
      cmd.flags['source-snapshot'] = self.spec.snapshot_name
    self.create_disk_start_time = time.time()
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    util.CheckGcloudResponseKnownFailures(stderr, retcode)
    self.create_disk_end_time = self._GetEndTime(stdout)

  def _Delete(self):
    """Deletes the disk, as well as all associated snapshot and restore disks."""
    if self.snapshots:
      for snapshot in self.snapshots:
        snapshot.Delete()
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'delete', self.name)
    if self.replica_zones:
      cmd.flags['region'] = self.region
      del cmd.flags['zone']
    cmd.Issue(raise_on_failure=False)

  def _Describe(self):
    """Returns json describing the disk or None on failure."""
    cmd = util.GcloudCommand(self, 'compute', 'disks', 'describe', self.name)

    if self.replica_zones:
      cmd.flags['region'] = self.region
      del cmd.flags['zone']

    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    try:
      result = json.loads(stdout)
    except ValueError:
      return None
    return result

  def _IsReady(self):
    """Returns true if the disk is ready."""
    result = self._Describe()
    if not result:
      return False
    return result.get('status') == 'READY'

  def _Exists(self):
    """Returns true if the disk exists."""
    result = self._Describe()
    return bool(result)

  def Exists(self):
    return self._Exists()

  def _GetEndTime(self, cmd_issue_response: str):
    """Returns the end time of the attach operation."""
    end_time = time.time()
    return end_time

  @vm_util.Retry(
      poll_interval=30,
      max_retries=10,
      retryable_exceptions=(
          GceServiceUnavailableError,
          errors.VmUtil.IssueCommandTimeoutError,
      ),
  )
  def _Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The GceVirtualMachine instance to which the disk will be attached.

    Raises:
      GceServiceUnavailableError: when the service is not available
    """
    self.attached_vm_name = vm.name
    cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'attach-disk', self.attached_vm_name
    )
    cmd.flags['device-name'] = self.name
    cmd.flags['disk'] = self.name

    if self.replica_zones:
      cmd.flags['disk-scope'] = REGIONAL_DISK_SCOPE
      cmd.flags['region'] = self.region

    self.attach_start_time = time.time()
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    self.attach_end_time = self._GetEndTime(stdout)

    # Gcloud attach-disk commands may still attach disks despite being rate
    # limited.
    if retcode:
      if (
          'The service is currently unavailable' in stderr
          or 'Please try again in 30 seconds.' in stderr
      ):
        logging.info('disk attach command failed, retrying.')
        raise GceServiceUnavailableError()
      if (
          cmd.rate_limited
          and 'is already being used' in stderr
          and FLAGS.retry_on_rate_limited
      ):
        return
      debug_text = 'Ran: {%s}\nReturnCode:%s\nSTDOUT: %s\nSTDERR: %s' % (
          ' '.join(cmd.GetCommand()),
          retcode,
          stdout,
          stderr,
      )
      raise errors.VmUtil.CalledProcessException(
          'Command returned a non-zero exit code:\n{}'.format(debug_text)
      )

  def GetCreateFlags(self):
    name = self.name
    pd_args = [
        f'name={name}',
        f'device-name={name}',
        f'size={self.spec.disk_size}',
        f'type={self.spec.disk_type}',
        'auto-delete=yes',
        'boot=no',
        'mode=rw',
    ]
    if self.provisioned_iops:
      pd_args += [f'provisioned-iops={self.provisioned_iops}']
    if self.provisioned_throughput:
      pd_args += [f'provisioned-throughput={self.provisioned_throughput}']
    return ','.join(pd_args)

  def _Detach(self):
    """Detaches the disk from a VM."""
    cmd = util.GcloudCommand(
        self, 'compute', 'instances', 'detach-disk', self.attached_vm_name
    )
    cmd.flags['device-name'] = self.name

    if self.replica_zones:
      cmd.flags['disk-scope'] = REGIONAL_DISK_SCOPE
    stdout, _ = cmd.IssueRetryable()
    self.attached_vm_name = None
    self.detach_end_time = self._GetEndTime(stdout)

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.interface == NVME:
      return self.name
    # by default, returns this name id.
    return f'/dev/disk/by-id/google-{self.name}'

  def IsNvme(self):
    return self.interface == NVME

  def CreateSnapshot(self):
    """Creates a snapshot of the disk."""
    snapshot = GceDiskSnapshot(self, len(self.snapshots) + 1)
    snapshot.Create()
    self.snapshots.append(snapshot)


class GceDiskSnapshot(disk.DiskSnapshot):
  """Object representing a GCE Disk Snapshot.

  Attributes:
    source_disk: The GceDisk object that the snapshot is created from.
    disk_spec: The disk spec of the source disk.
    source_disk_name: The name of the source disk.
    source_disk_size: The size of the source disk.
    source_disk_type: The type of the source disk.
    name: The name of the snapshot.
    zone: The zone of the source disk.
    region: The region of the source disk.
    project: The project of the source disk.
    storage_gb: The storage used by the snapshot in GB.
    restore_disks: A list of GceDisk objects created from the snapshot.
    num_restore_disks: The number of disks restored from this snapshot.
  """

  def __init__(self, source_disk, snapshot_num):
    super().__init__()
    self.source_disk = source_disk
    self.disk_spec = source_disk.spec
    self.source_disk_name = source_disk.name
    self.source_disk_size = source_disk.disk_size
    self.source_disk_type = source_disk.disk_type
    self.snapshot_num = snapshot_num
    self.name = f'disk-snapshot-{self.source_disk_name}-{self.snapshot_num}'
    self.zone = source_disk.zone
    self.region = util.GetRegionFromZone(self.zone)
    self.project = source_disk.project

  def _Create(self):
    """Creates a snapshot of the disk.

    Raises:
      errors.VmUtil.CalledProcessException: When the command returns a non-zero
      exit code.
    """
    cmd = util.GcloudCommand(self, 'compute', 'snapshots', 'create', self.name)
    del cmd.flags['zone']
    cmd.flags['source-disk'] = self.source_disk_name
    cmd.flags['source-disk-zone'] = self.zone
    cmd.flags['storage-location'] = self.region
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    self.creation_start_time = time.time()
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    self.creation_end_time = self._Describe()

    if retcode:
      debug_text = 'Ran: {%s}\nReturnCode:%s\nSTDOUT: %s\nSTDERR: %s' % (
          ' '.join(cmd.GetCommand()),
          retcode,
          stdout,
          stderr,
      )
      raise errors.VmUtil.CalledProcessException(
          'Command returned a non-zero exit code:\n{}'.format(debug_text)
      )

  @vm_util.Retry(
      poll_interval=0.5,
      max_retries=10,
      log_errors=True,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def _Describe(self):
    """Describe the snapshot, storing the storage size in GB.

    Returns:
        float: The time when the snapshot was created.

    Raises:
        errors.Resource.RetryableCreationError: If the snapshot is not created
        and ready.
    """
    cmd = util.GcloudCommand(
        self, 'compute', 'snapshots', 'describe', self.name
    )
    del cmd.flags['zone']
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    result = json.loads(stdout)
    self.storage_gb = int(result['storageBytes']) // (2**30)
    snapshot_status = result['status']
    if snapshot_status == 'READY':
      return time.time()
    logging.info(
        'Disk %s snapshot %s has status %s.',
        self.source_disk_name,
        self.name,
        snapshot_status,
    )
    raise errors.Resource.RetryableCreationError()

  def Restore(self):
    """Creates a disk from the snapshot."""
    self.restore_disk_name = f'{self.name}-restore-{self.num_restore_disks}'
    self.disk_spec.snapshot_name = self.name
    restore_disk = GceDisk(
        self.disk_spec, self.restore_disk_name, self.zone, self.project
    )
    restore_disk.Create()
    self.restore_disks.append(restore_disk)
    self.num_restore_disks += 1

    return self.restore_disks[-1]

  def Delete(self):
    """Deletes the snapshot."""
    if self.restore_disks:
      for restore_disk in self.restore_disks:
        restore_disk.Delete()
        self.num_restore_disks -= 1
    cmd = util.GcloudCommand(self, 'compute', 'snapshots', 'delete', self.name)
    cmd.Issue(raise_on_failure=False)


class GceStripedDisk(disk.StripedDisk):
  """Object representing multiple GCP disks striped together."""

  def __init__(self, disk_spec, disks):
    super().__init__(disk_spec, disks)
    if len(disks) <= 1:
      raise ValueError(
          f'{len(disks)} disks found for GceStripedDisk'
      )
    self.disks = disks
    self.spec = disk_spec
    self.interface = disk_spec.interface
    data_disk = disks[0]
    self.attached_vm_name = None
    self.image = data_disk.image
    self.image_project = data_disk.image_project
    self.zone = data_disk.zone
    self.project = data_disk.project
    self.replica_zones = data_disk.replica_zones
    self.region = util.GetRegionFromZone(self.zone)
    self.provisioned_iops = data_disk.provisioned_iops
    self.provisioned_throughput = data_disk.provisioned_throughput
    self.metadata['iops'] = self.provisioned_iops
    self.metadata['throughput'] = self.provisioned_throughput

  def _Create(self):
    """Creates the disk."""
    create_tasks = []
    for disk_details in self.disks:
      create_tasks.append((disk_details.Create, (), {}))
    background_tasks.RunParallelThreads(create_tasks, max_concurrency=200)

  def _PostCreate(self):
    """Called after _CreateResource() is called."""
    for disk_details in self.disks:
      disk_details.created = True

  def _GetDiskNames(self):
    return [d.name for d in self.disks]

  def _Attach(self, vm):
    attach_tasks = []
    for disk_details in self.disks:
      attach_tasks.append((disk_details.Attach, [vm], {}))
    background_tasks.RunParallelThreads(attach_tasks, max_concurrency=200)

  def _Exists(self):
    for disk_details in self.disks:
      if not disk_details.Exists():
        return False
    return True

  def _Detach(self):
    detach_tasks = []
    for disk_details in self.disks:
      detach_tasks.append((disk_details.Detach, (), {}))
    background_tasks.RunParallelThreads(detach_tasks, max_concurrency=200)
