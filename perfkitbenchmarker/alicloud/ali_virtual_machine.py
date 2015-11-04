# Copyright 2015 Google Inc. All rights reserved.
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

"""Class to represent an Ali Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import json
import threading
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import disk
from perfkitbenchmarker.alicloud import ali_disk
from perfkitbenchmarker.alicloud import util

FLAGS = flags.FLAGS
NON_HVM_PREFIXES = ['t1', 's1', 's2', 's3', 'm1']

flags.DEFINE_string('ali_user_name', 'ubuntu',
                    'This determines the user name that Perfkit will '
                    'attempt to use. This must be changed in order to '
                    'use any image other than ubuntu.')
flags.DEFINE_integer('ali_bandwidth_in', 100, 'Inbound Bandwidth')
flags.DEFINE_integer('ali_bandwidth_out', 100, 'Outbound Bandwidth')
flags.DEFINE_string('io_optimized', 'none',
                    'IO optimized for disk in AliCloud. The default is '
                    'none which means no IO optimized'
                    '"optimized" means use IO optimized.')


DRIVE_START_LETTER = 'b'
DEFAULT_DISK_SIZE = 500
INSTANCE = 'instance'
IMAGE = 'image'
SNAPSHOT = 'snapshot'
DISK = 'disk'
NONE = 'none'
IO_OPTIMIZED = 'io_optimized'
RESOURCE_TYPE = {
    INSTANCE: 'instance',
    IMAGE: 'image',
    SNAPSHOT: 'snapshot',
    DISK: 'disk',
}
IO_STRATAGE = {
    NONE: 'none',
    IO_OPTIMIZED: 'optimized',
}

NUM_LOCAL_VOLUMES = {
    'ecs.t1.small': 4,
    'ecs.s1.small': 4,
    'ecs.s1.medium': 4,
    'ecs.s2.small': 4,
    'ecs.s2.large': 4,
    'ecs.s2.xlarge': 4,
    'ecs.s3.medium': 4,
    'ecs.s3.large': 4,
    'ecs.m1.medium': 4,
}
INSTANCE_EXISTS_STATUSES = frozenset(
    ['Starting', 'Running', 'Stopping', 'Stopped'])
INSTANCE_DELETED_STATUSES = frozenset([])
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES

DEFAULT_IMAGE = "ubuntu1404_64_20G_aliaegis_20150325.vhd",


class AliVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an AliCloud Virtual Machine."""

  DEFAULT_ZONE = 'cn-hangzhou-d'
  DEFAULT_MACHINE_TYPE = 'ecs.s3.large'
  IMAGE_NAME_FILTER = 'ubuntu1404_64_20G_aliaegis*'

  _lock = threading.Lock()
  imported_keyfile_set = set()
  deleted_keyfile_set = set()

  def __init__(self, vm_spec, network, firewall):
    """Initialize a AliCloud virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the VM.
      network: network.BaseNetwork object corresponding to the VM.
      firewall: network.BaseFirewall object corresponding to the VM.
    """
    super(AliVirtualMachine, self).__init__(vm_spec, network, firewall)
    self.image = self.image or DEFAULT_IMAGE
    self.user_name = FLAGS.ali_user_name
    self.region = util.GetRegionByZone(self.zone)
    self.bandwidth_in = FLAGS.ali_bandwidth_in
    self.bandwidth_out = FLAGS.ali_bandwidth_out
    self.scratch_disk_size = FLAGS.scratch_disk_size or DEFAULT_DISK_SIZE


  @classmethod
  def _GetDefaultImage(cls, region):
    """Returns the default image given the machine type and region.

    If no default is configured, this will return None.
    """
    if cls.IMAGE_NAME_FILTER is None:
      return None

    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeImage',
        '--RegionId %s' % region,
        '--ImageName \'%s\'' % cls.IMAGE_NAME_FILTER]
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = util.IssueRetryableCommand(describe_cmd)

    if not stdout:
      return None

    images = json.loads(stdout)['Images']['Image']
    # We want to return the latest version of the image, and since the wildcard
    # portion of the image name is the image's creation date, we can just take
    # the image with the 'largest' name.
    return max(images, key=lambda image: image['ImageName'])['ImageId']

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data and tag it."""
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeInstances',
        '--RegionId %s' % self.region,
        '--InstanceIds \'["%s"]\'' % self.id]
    logging.info('Getting instance %s public IP. This will fail until '
                 'a public IP is available, but will be retried.', self.id)
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    instance = response['Instances']['Instance'][0]
    assert instance['PublicIpAddress']['IpAddress'][0] == self.ip_address
    self.internal_ip = instance['InnerIpAddress']['IpAddress'][0]
    self.group_id = instance['SecurityGroupIds']['SecurityGroupId'][0]

    key_file = vm_util.GetPublicKeyPath()
    util.AddPubKeyToHost(self.ip_address,
                         self.password,
                         key_file,
                         self.user_name)
    util.AddDefaultTags(self.id, RESOURCE_TYPE[INSTANCE], self.region)

  def _CreateDependencies(self):
    """Create VM dependencies."""
    pass

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    pass

  def _Create(self):
    """Create a VM instance."""

    if self.image is None:
      # This is here and not in the __init__ method bceauese _GetDefaultImage
      # does a nontrivial amount of work (it calls the aliyuncli).
      self.image = self._GetDefaultImage(self.region)

    self.password = util.GeneratePassword()

    create_cmd = util.ALI_PREFIX + [
        'ecs',
        'CreateInstance',
        '--InstanceName perfkit-%s' % FLAGS.run_uri,
        '--RegionId %s' % self.region,
        '--ZoneId %s' % self.zone,
        '--ImageId %s' % self.image,
        '--InstanceType %s' % self.machine_type,
        '--InternetChargeType PayByTraffic',
        '--InternetMaxBandwidthIn %s' % self.bandwidth_in,
        '--InternetMaxBandwidthOut %s' % self.bandwidth_out,
        '--SecurityGroupId %s' % self.network.security_group.group_id,
        '--Password %s' % self.password]

    if FLAGS.scratch_disk_type == disk.LOCAL:
      disk_cmd = [
          '--SystemDiskCategory ephemeral_ssd',
          '--DataDisk1Category ephemeral_ssd',
          '--DataDisk1Size %s' % self.scratch_disk_size,
          '--DataDisk1Device /dev/xvd%s' % DRIVE_START_LETTER]
      create_cmd += disk_cmd

    if FLAGS.io_optimized == IO_STRATAGE[IO_OPTIMIZED]:
      io_opt_cmd = ['--IoOptimized optimized']
      create_cmd += io_opt_cmd

    create_cmd = util.GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['InstanceId']

    allocateip_cmd = util.ALI_PREFIX + [
        'ecs',
        'AllocatePublicIpAddress',
        '--RegionId %s' % self.region,
        '--InstanceId %s' % self.id]
    allocateip_cmd = util.GetEncodedCmd(allocateip_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(allocateip_cmd)
    response = json.loads(stdout)
    self.ip_address = response['IpAddress']

    start_cmd = util.ALI_PREFIX + [
        'ecs',
        'StartInstance',
        '--RegionId %s' % self.region,
        '--InstanceId %s' % self.id]
    start_cmd = util.GetEncodedCmd(start_cmd)
    vm_util.IssueRetryableCommand(start_cmd)

  def _Delete(self):
    """Delete a VM instance."""
    stop_cmd = util.ALI_PREFIX + [
        'ecs',
        'StopInstance',
        '--RegionId %s' % self.region,
        '--InstanceId %s' % self.id]
    stop_cmd = util.GetEncodedCmd(stop_cmd)
    vm_util.IssueRetryableCommand(stop_cmd)

    delete_cmd = util.ALI_PREFIX + [
        'ecs',
        'DeleteInstance',
        '--RegionId %s' % self.region,
        '--InstanceId %s' % self.id]
    delete_cmd = util.GetEncodedCmd(delete_cmd)
    vm_util.IssueRetryableCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the VM exists."""
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeInstances',
        '--RegionId %s' % self.region,
        '--InstanceIds \'["%s"]\'' % str(self.id)]
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    instances = response['Instances']['Instance']
    assert len(instances) < 2, 'Too many instances.'
    if not instances:
      return False
    assert len(instances) == 1, 'Wrong number of instances.'
    status = instances[0]['Status']
    assert status in INSTANCE_KNOWN_STATUSES, status
    return status in INSTANCE_EXISTS_STATUSES

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    data_disk = ali_disk.AliDisk(disk_spec, self.zone)
    self.scratch_disks.append(data_disk)

    if disk_spec.disk_type != disk.LOCAL:
      data_disk.Create()
      data_disk.Attach(self)
    else:
      data_disk.device_letter = DRIVE_START_LETTER

    self.FormatDisk(data_disk.GetDevicePath())
    self.MountDisk(data_disk.GetDevicePath(), disk_spec.mount_point)

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          disks on the VM (e.g. '/dev/xvdb').
    """
    return ['/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)
            for i in xrange(NUM_LOCAL_VOLUMES[self.machine_type])]

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    util.AddTags(self.id, RESOURCE_TYPE[INSTANCE], self.region, **kwargs)


class DebianBasedAliVirtualMachine(AliVirtualMachine,
                                   linux_virtual_machine.DebianMixin):
  IMAGE_NAME_FILTER = 'ubuntu1404_64*aliaegis*.vhd'


class RhelBasedAliVirtualMachine(AliVirtualMachine,
                                 linux_virtual_machine.RhelMixin):
  pass


class WindowsAliVirtualMachine(AliVirtualMachine,
                               windows_virtual_machine.WindowsMixin):
  pass
