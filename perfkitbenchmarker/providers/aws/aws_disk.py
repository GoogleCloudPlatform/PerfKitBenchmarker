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

"""Module containing classes related to AWS disks.

Disks can be created, deleted, attached to VMs, and detached from VMs.
See http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html to
determine valid disk types.
See http://aws.amazon.com/ebs/details/ for more information about AWS (EBS)
disks.
"""

import dataclasses
import json
import logging
import string
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import util


class AwsStateRetryableError(Exception):
  """Error for retrying when an AWS disk is in a transitional state."""

VOLUME_EXISTS_STATUSES = frozenset(['creating', 'available', 'in-use', 'error'])
VOLUME_DELETED_STATUSES = frozenset(['deleting', 'deleted'])
VOLUME_KNOWN_STATUSES = VOLUME_EXISTS_STATUSES | VOLUME_DELETED_STATUSES

STANDARD = 'standard'
GP2 = 'gp2'
GP3 = 'gp3'
IO1 = 'io1'
IO2 = 'io2'
ST1 = 'st1'
SC1 = 'sc1'

AWS_REMOTE_DISK_TYPES = [STANDARD, SC1, ST1, GP2, GP3, IO1, IO2]

DISK_TYPE = {
    disk.STANDARD: STANDARD,
    disk.REMOTE_SSD: GP2,
    disk.PIOPS: IO1
}

# any disk types here, consider adding them to AWS_REMOTE_DISK_TYPES as well.
DISK_METADATA = {
    STANDARD: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
    },
    GP2: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    GP3: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    IO1: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    IO2: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    ST1: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE
    },
    SC1: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE
    }
}

LOCAL_SSD_METADATA = {
    disk.MEDIA: disk.SSD,
    disk.REPLICATION: disk.NONE,
}

LOCAL_HDD_METADATA = {
    disk.MEDIA: disk.HDD,
    disk.REPLICATION: disk.NONE,
}

LOCAL_HDD_PREFIXES = ['d2', 'hs1', 'h1', 'c1', 'cc2', 'm1', 'm2']
# Following lists based on
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html
NON_EBS_NVME_TYPES = [
    'c4',
    'd2',
    'f1',
    'g3',
    'h1',
    'i3',
    'm4',
    'p2',
    'p3',
    'r4',
    't2',
    'x1',
    'x1e',
    'm1',
    'm3',
    'c1',
    'cc2',
    'c3',
    'm2',
    'cr1',
    'r3',
    'hs1',
    'i2',
    'g2',
    't1',
]
NON_LOCAL_NVME_TYPES = LOCAL_HDD_PREFIXES + [
    'c3', 'cr1', 'g2', 'i2', 'm3', 'r3', 'x1', 'x1e']

# Following dictionary based on
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
NUM_LOCAL_VOLUMES = {
    'c1.medium': 1,
    'c1.xlarge': 4,
    'c3.large': 2,
    'c3.xlarge': 2,
    'c3.2xlarge': 2,
    'c3.4xlarge': 2,
    'c3.8xlarge': 2,
    'cc2.8xlarge': 4,
    'cg1.4xlarge': 2,
    'cr1.8xlarge': 2,
    'g2.2xlarge': 1,
    'hi1.4xlarge': 2,
    'hs1.8xlarge': 24,
    'i2.xlarge': 1,
    'i2.2xlarge': 2,
    'i2.4xlarge': 4,
    'i2.8xlarge': 8,
    'm1.small': 1,
    'm1.medium': 1,
    'm1.large': 2,
    'm1.xlarge': 4,
    'm2.xlarge': 1,
    'm2.2xlarge': 1,
    'm2.4xlarge': 2,
    'm3.medium': 1,
    'm3.large': 1,
    'm3.xlarge': 2,
    'm3.2xlarge': 2,
    'r3.large': 1,
    'r3.xlarge': 1,
    'r3.2xlarge': 1,
    'r3.4xlarge': 1,
    'r3.8xlarge': 2,
    'd2.xlarge': 3,
    'd2.2xlarge': 6,
    'd2.4xlarge': 12,
    'd2.8xlarge': 24,
    'd3.xlarge': 3,
    'd3.2xlarge': 6,
    'd3.4xlarge': 12,
    'd3.8xlarge': 24,
    'd3en.large': 1,
    'd3en.xlarge': 2,
    'd3en.2xlarge': 4,
    'd3en.4xlarge': 8,
    'd3en.6xlarge': 12,
    'd3en.8xlarge': 16,
    'd3en.12xlarge': 24,
    'i3.large': 1,
    'i3.xlarge': 1,
    'i3.2xlarge': 1,
    'i3.4xlarge': 2,
    'i3.8xlarge': 4,
    'i3.16xlarge': 8,
    'i3.metal': 8,
    'i4i.large': 1,
    'i4i.xlarge': 1,
    'i4i.2xlarge': 1,
    'i4i.4xlarge': 1,
    'i4i.8xlarge': 2,
    'i4i.16xlarge': 4,
    'i4i.32xlarge': 8,
    'is4gen.medium': 1,
    'is4gen.large': 1,
    'is4gen.xlarge': 1,
    'is4gen.2xlarge': 1,
    'is4gen.4xlarge': 2,
    'is4gen.8xlarge': 4,
    'im4gn.large': 1,
    'im4gn.xlarge': 1,
    'im4gn.2xlarge': 1,
    'im4gn.4xlarge': 1,
    'im4gn.8xlarge': 2,
    'im4gn.16xlarge': 4,
    'i3en.large': 1,
    'i3en.xlarge': 1,
    'i3en.2xlarge': 2,
    'i3en.3xlarge': 1,
    'i3en.6xlarge': 2,
    'i3en.12xlarge': 4,
    'i3en.24xlarge': 8,
    'i3en.metal': 8,
    'c5ad.large': 1,
    'c5ad.xlarge': 1,
    'c5ad.2xlarge': 1,
    'c5ad.4xlarge': 2,
    'c5ad.8xlarge': 2,
    'c5ad.12xlarge': 2,
    'c5ad.16xlarge': 2,
    'c5ad.24xlarge': 2,
    'c5d.large': 1,
    'c5d.xlarge': 1,
    'c5d.2xlarge': 1,
    'c5d.4xlarge': 1,
    'c5d.9xlarge': 1,
    'c5d.18xlarge': 2,
    'c5d.24xlarge': 4,
    'c5d.metal': 4,
    'c6gd.large': 1,
    'c6gd.xlarge': 1,
    'c6gd.2xlarge': 1,
    'c6gd.4xlarge': 1,
    'c6gd.8xlarge': 1,
    'c6gd.12xlarge': 2,
    'c6gd.16xlarge': 2,
    'c6gd.metal': 2,
    'm5d.large': 1,
    'm5d.xlarge': 1,
    'm5d.2xlarge': 1,
    'm5d.4xlarge': 2,
    'm5d.8xlarge': 2,
    'm5d.12xlarge': 2,
    'm5d.24xlarge': 4,
    'm5d.metal': 4,
    'm6gd.large': 1,
    'm6gd.xlarge': 1,
    'm6gd.2xlarge': 1,
    'm6gd.4xlarge': 1,
    'm6gd.8xlarge': 1,
    'm6gd.12xlarge': 2,
    'm6gd.16xlarge': 2,
    'm6gd.metal': 2,
    'm6id.large': 1,
    'm6id.xlarge': 1,
    'm6id.2xlarge': 1,
    'm6id.4xlarge': 1,
    'm6id.8xlarge': 1,
    'm6id.12xlarge': 2,
    'm6id.16xlarge': 2,
    'm6id.24xlarge': 4,
    'm6id.32xlarge': 4,
    'm6id.metal': 4,
    'r5d.large': 1,
    'r5d.xlarge': 1,
    'r5d.2xlarge': 1,
    'r5d.4xlarge': 2,
    'r5d.8xlarge': 2,
    'r5d.12xlarge': 2,
    'r5d.16xlarge': 4,
    'r5d.24xlarge': 4,
    'r6id.large': 1,
    'r6id.xlarge': 1,
    'r6id.2xlarge': 1,
    'r6id.4xlarge': 1,
    'r6id.8xlarge': 1,
    'r6id.12xlarge': 2,
    'r6id.16xlarge': 2,
    'r6id.24xlarge': 4,
    'r6id.32xlarge': 4,
    'r6gd.large': 1,
    'r6gd.xlarge': 1,
    'r6gd.2xlarge': 1,
    'r6gd.4xlarge': 1,
    'r6gd.8xlarge': 1,
    'r6gd.12xlarge': 2,
    'r6gd.16xlarge': 2,
    'z1d.large': 1,
    'z1d.xlarge': 1,
    'z1d.2xlarge': 1,
    'z1d.3xlarge': 2,
    'z1d.6xlarge': 1,
    'z1d.12xlarge': 2,
    'x1.16xlarge': 1,
    'x1.32xlarge': 2,
    'x1e.xlarge': 1,
    'x1e.2xlarge': 1,
    'x1e.4xlarge': 1,
    'x1e.8xlarge': 1,
    'x1e.16xlarge': 1,
    'x1e.32xlarge': 2,
    'f1.2xlarge': 1,
    'f1.4xlarge': 1,
    'f1.16xlarge': 4,
    'p3dn.24xlarge': 2,
    'p4d.24xlarge': 8,
    'g4dn.xlarge': 1,
    'g4dn.2xlarge': 1,
    'g4dn.4xlarge': 1,
    'g4dn.8xlarge': 1,
    'g4dn.12xlarge': 1,
    'g4dn.16xlarge': 1,
    'g4dn.metal': 2,
    'g4ad.xlarge': 1,
    'g4ad.2xlarge': 1,
    'g4ad.4xlarge': 1,
    'g4ad.8xlarge': 1,
    'g4ad.16xlarge': 2,
}


def LocalDiskIsHDD(machine_type):
  """Check whether the local disks use spinning magnetic storage."""
  return machine_type.split('.')[0].lower() in LOCAL_HDD_PREFIXES


def LocalDriveIsNvme(machine_type):
  """Check if the machine type uses NVMe driver."""
  return machine_type.split('.')[0].lower() not in NON_LOCAL_NVME_TYPES


def EbsDriveIsNvme(machine_type):
  """Check if the machine type uses NVMe driver."""
  instance_family = machine_type.split('.')[0].lower()
  return (instance_family not in NON_EBS_NVME_TYPES or
          'metal' in machine_type)


AWS = 'AWS'
disk.RegisterDiskTypeMap(AWS, DISK_TYPE)


class AwsDiskSpec(disk.BaseDiskSpec):
  """Object holding the information needed to create an AwsDisk.

  Attributes:
    iops: None or int. IOPS for Provisioned IOPS (SSD) volumes in AWS.
    throughput: None or int. Throughput for (SSD) volumes in AWS.
  """

  CLOUD = provider_info.AWS

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(AwsDiskSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['aws_provisioned_iops'].present:
      config_values['iops'] = flag_values.aws_provisioned_iops
    if flag_values['aws_provisioned_throughput'].present:
      config_values['throughput'] = flag_values.aws_provisioned_throughput

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(AwsDiskSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'iops': (option_decoders.IntDecoder, {
            'default': None,
            'none_ok': True
        })
    })
    result.update({
        'throughput': (option_decoders.IntDecoder, {
            'default': None,
            'none_ok': True
        })
    })
    return result


@dataclasses.dataclass
class AWSDiskIdentifiers:
  """Identifiers of an AWS disk assigned by AWS at creation time."""
  volume_id: str
  path: str


class AwsDisk(disk.BaseDisk):
  """Object representing an Aws Disk."""

  _lock = threading.Lock()
  # this is a mapping of vm_id to unused alphabetical device letters.
  available_device_letters_by_vm = {}

  def __init__(self, disk_spec, zone, machine_type, disk_spec_id=None):
    super(AwsDisk, self).__init__(disk_spec)
    self.iops = disk_spec.iops
    self.throughput = disk_spec.throughput
    self.id = None
    self.zone = zone
    self.region = util.GetRegionFromZone(zone)
    self.device_letter = None
    self.attached_vm_id = None
    self.machine_type = machine_type
    if self.disk_type != disk.LOCAL:
      self.metadata.update(DISK_METADATA.get(self.disk_type, {}))
    else:
      self.metadata.update((LOCAL_HDD_METADATA
                            if LocalDiskIsHDD(machine_type)
                            else LOCAL_SSD_METADATA))
    if self.iops:
      self.metadata['iops'] = self.iops
    if self.throughput:
      self.metadata['throughput'] = self.throughput
    self.disk_spec_id = disk_spec_id

  def AssignDeviceLetter(self, letter_suggestion, nvme_boot_drive_index):
    if (LocalDriveIsNvme(self.machine_type) and
        EbsDriveIsNvme(self.machine_type)):
      first_device_letter = 'b'
      local_drive_number = ord(letter_suggestion) - ord(first_device_letter)
      logging.info('local drive number is: %d', local_drive_number)
      if local_drive_number < nvme_boot_drive_index:
        self.device_letter = letter_suggestion
      else:
        # skip the boot drive
        self.device_letter = chr(ord(letter_suggestion) + 1)
    else:
      self.device_letter = letter_suggestion

  def _Create(self):
    """Creates the disk."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-volume',
        '--region=%s' % self.region,
        '--size=%s' % self.disk_size,
        '--volume-type=%s' % self.disk_type]
    if not util.IsRegion(self.zone):
      create_cmd.append('--availability-zone=%s' % self.zone)
    if self.disk_type in [IO1, IO2]:
      create_cmd.append('--iops=%s' % self.iops)
    if self.disk_type == GP3 and self.iops:
      create_cmd.append('--iops=%s' % self.iops)
    if self.disk_type == GP3 and self.throughput:
      create_cmd.append('--throughput=%s' % self.throughput)

    try:
      stdout, _, _ = vm_util.IssueCommand(create_cmd)
    except errors.VmUtil.IssueCommandError as error:
      error_message = str(error)
      is_quota_error = 'MaxIOPSLimitExceeded' in error_message
      if is_quota_error:
        raise errors.Benchmarks.QuotaFailure(error_message)
      raise error

    response = json.loads(stdout)
    self.id = response['VolumeId']
    util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the disk."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-volume',
        '--region=%s' % self.region,
        '--volume-id=%s' % self.id]
    logging.info('Deleting AWS volume %s. This may fail if the disk is not '
                 'yet detached, but will be retried.', self.id)
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def _Exists(self):
    """Returns true if the disk exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-volumes',
        '--region=%s' % self.region,
        '--filter=Name=volume-id,Values=%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    volumes = response['Volumes']
    assert len(volumes) < 2, 'Too many volumes.'
    if not volumes:
      return False
    status = volumes[0]['State']
    assert status in VOLUME_KNOWN_STATUSES, status
    return status in VOLUME_EXISTS_STATUSES

  @vm_util.Retry(
      poll_interval=0.5,
      log_errors=True,
      retryable_exceptions=(AwsStateRetryableError,))
  def _WaitForAttachedState(self):
    """Returns if the state of the disk is attached.

    Returns:
      Whether the disk is in an attached state. If not, raises an
      error.

    Raises:
      AwsUnknownStatusError: If an unknown status is returned from AWS.
      AwsStateRetryableError: If the disk attach is pending. This is retried.
    """
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-volumes',
        '--region=%s' % self.region,
        '--volume-ids=%s' % self.id,
    ]

    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    status = response['Volumes'][0]['Attachments'][0]['State']
    if status.lower() != 'attached':
      logging.info('Disk (id:%s) attaching to '
                   'VM (id:%s) has status %s.',
                   self.id, self.attached_vm_id, status)

      raise AwsStateRetryableError()
    volume_id = response['Volumes'][0]['Attachments'][0]['VolumeId']
    device_name = response['Volumes'][0]['Attachments'][0]['Device']
    return volume_id, device_name

  @classmethod
  def GenerateDeviceNamePrefix(cls, disk_type):
    """Generates the device name prefix depending on the device type."""
    if disk_type == disk.LOCAL:
      return '/dev/xvd'
    else:
      return '/dev/xvdb'

  @classmethod
  def GenerateDeviceLetter(cls, vm_name):
    """Generates the next available device letter for a given VM."""
    with cls._lock:
      if vm_name not in cls.available_device_letters_by_vm:
        all_available_letters = set(string.ascii_lowercase)
        # local ssds cannot use 'a' to allow for boot disk naming.
        # remove 'a' as an available device letter,
        # so that both local ssds and remote disks can share this naming
        # convention.
        all_available_letters.remove('a')
        cls.available_device_letters_by_vm[vm_name] = all_available_letters
      device_letter = min(cls.available_device_letters_by_vm[vm_name])
      cls.available_device_letters_by_vm[vm_name].remove(device_letter)
    return device_letter

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AwsVirtualMachine instance to which the disk will be attached.
    """
    self.device_letter = AwsDisk.GenerateDeviceLetter(vm.name)

    device_name = (
        self.GenerateDeviceNamePrefix(self.disk_type) + self.device_letter)
    attach_cmd = util.AWS_PREFIX + [
        'ec2',
        'attach-volume',
        '--region=%s' % self.region,
        '--instance-id=%s' % vm.id,
        '--volume-id=%s' % self.id,
        '--device=%s' % device_name]
    logging.info('Attaching AWS volume %s. This may fail if the disk is not '
                 'ready, but will be retried.', self.id)
    util.IssueRetryableCommand(attach_cmd)
    volume_id, device_name = self._WaitForAttachedState()
    vm.LogDeviceByName(device_name, volume_id)
    if self.disk_spec_id:
      vm.LogDeviceByDiskSpecId(self.disk_spec_id, device_name)

  def Detach(self):
    """Detaches the disk from a VM."""
    detach_cmd = util.AWS_PREFIX + [
        'ec2',
        'detach-volume',
        '--region=%s' % self.region,
        '--instance-id=%s' % self.attached_vm_id,
        '--volume-id=%s' % self.id]
    util.IssueRetryableCommand(detach_cmd)

    with self._lock:
      assert self.attached_vm_id in AwsDisk.available_device_letters_by_vm
      AwsDisk.available_device_letters_by_vm[self.attached_vm_id].add(
          self.device_letter)
      self.attached_vm_id = None
      self.device_letter = None
