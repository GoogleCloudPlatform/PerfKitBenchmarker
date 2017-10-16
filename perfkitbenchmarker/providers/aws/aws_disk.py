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

import json
import logging
import string
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import util

VOLUME_EXISTS_STATUSES = frozenset(['creating', 'available', 'in-use', 'error'])
VOLUME_DELETED_STATUSES = frozenset(['deleting', 'deleted'])
VOLUME_KNOWN_STATUSES = VOLUME_EXISTS_STATUSES | VOLUME_DELETED_STATUSES

STANDARD = 'standard'
GP2 = 'gp2'
IO1 = 'io1'
ST1 = 'st1'
SC1 = 'sc1'

DISK_TYPE = {
    disk.STANDARD: STANDARD,
    disk.REMOTE_SSD: GP2,
    disk.PIOPS: IO1
}

DISK_METADATA = {
    STANDARD: {
        disk.MEDIA: disk.HDD,
        disk.REPLICATION: disk.ZONE,
    },
    GP2: {
        disk.MEDIA: disk.SSD,
        disk.REPLICATION: disk.ZONE,
    },
    IO1: {
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

LOCAL_HDD_PREFIXES = ['d2', 'hs']


def LocalDiskIsHDD(machine_type):
  """Check whether the local disks use spinning magnetic storage."""

  return machine_type[:2].lower() in LOCAL_HDD_PREFIXES


def LocalDriveIsNvme(machine_type):
  """Check if the machine type uses NVMe driver."""
  return machine_type[:2].lower() == 'i3'


AWS = 'AWS'
disk.RegisterDiskTypeMap(AWS, DISK_TYPE)


class AwsDiskSpec(disk.BaseDiskSpec):
  """Object holding the information needed to create an AwsDisk.

  Attributes:
    iops: None or int. IOPS for Provisioned IOPS (SSD) volumes in AWS.
  """

  CLOUD = providers.AWS

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

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(AwsDiskSpec, cls)._GetOptionDecoderConstructions()
    result.update({'iops': (option_decoders.IntDecoder, {'default': None,
                                                         'none_ok': True})})
    return result


class AwsDisk(disk.BaseDisk):
  """Object representing an Aws Disk."""

  _lock = threading.Lock()
  vm_devices = {}

  def __init__(self, disk_spec, zone, machine_type):
    super(AwsDisk, self).__init__(disk_spec)
    self.iops = disk_spec.iops
    self.id = None
    self.zone = zone
    self.region = util.GetRegionFromZone(zone)
    self.device_letter = None
    self.attached_vm_id = None
    self.machine_type = machine_type
    if self.disk_type != disk.LOCAL:
      self.metadata.update(DISK_METADATA[self.disk_type])
    else:
      self.metadata.update((LOCAL_HDD_METADATA
                            if LocalDiskIsHDD(machine_type)
                            else LOCAL_SSD_METADATA))
    if self.iops:
      self.metadata['iops'] = self.iops

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
    if self.disk_type == IO1:
      create_cmd.append('--iops=%s' % self.iops)
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
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
    vm_util.IssueCommand(delete_cmd)

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

  def Attach(self, vm):
    """Attaches the disk to a VM.

    Args:
      vm: The AwsVirtualMachine instance to which the disk will be attached.
    """
    with self._lock:
      self.attached_vm_id = vm.id
      if self.attached_vm_id not in AwsDisk.vm_devices:
        AwsDisk.vm_devices[self.attached_vm_id] = set(
            string.ascii_lowercase)
      self.device_letter = min(AwsDisk.vm_devices[self.attached_vm_id])
      AwsDisk.vm_devices[self.attached_vm_id].remove(self.device_letter)

    attach_cmd = util.AWS_PREFIX + [
        'ec2',
        'attach-volume',
        '--region=%s' % self.region,
        '--instance-id=%s' % self.attached_vm_id,
        '--volume-id=%s' % self.id,
        '--device=%s' % self.GetDevicePath()]
    logging.info('Attaching AWS volume %s. This may fail if the disk is not '
                 'ready, but will be retried.', self.id)
    util.IssueRetryableCommand(attach_cmd)

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
      assert self.attached_vm_id in AwsDisk.vm_devices
      AwsDisk.vm_devices[self.attached_vm_id].add(self.device_letter)
      self.attached_vm_id = None
      self.device_letter = None

  def GetDevicePath(self):
    """Returns the path to the device inside the VM."""
    if self.disk_type == disk.LOCAL:
      if LocalDriveIsNvme(self.machine_type):
        return '/dev/nvme%sn1' % str(ord(self.device_letter) - ord('b'))
      return '/dev/xvd%s' % self.device_letter
    else:
      return '/dev/xvdb%s' % self.device_letter
