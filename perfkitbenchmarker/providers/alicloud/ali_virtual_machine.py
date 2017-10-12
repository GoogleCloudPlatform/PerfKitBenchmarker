# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
from perfkitbenchmarker.providers.alicloud import ali_disk
from perfkitbenchmarker.providers.alicloud import ali_network
from perfkitbenchmarker.providers.alicloud import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS
NON_HVM_PREFIXES = ['t1', 's1', 's2', 's3', 'm1']

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
SSH_PORT = 22


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

DEFAULT_IMAGE = "ubuntu_14_0405_64_40G_alibase_20170711.vhd",


class AliVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an AliCloud Virtual Machine."""

  CLOUD = providers.ALICLOUD
  DEFAULT_ZONE = 'cn-hangzhou-d'
  DEFAULT_MACHINE_TYPE = 'ecs.s3.large'
  IMAGE_NAME_FILTER = 'ubuntu_14_0405_64_40G_alibase*'

  _lock = threading.Lock()
  imported_keyfile_set = set()
  deleted_keyfile_set = set()

  def __init__(self, vm_spec):
    """Initialize a AliCloud virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the VM.
    """
    super(AliVirtualMachine, self).__init__(vm_spec)
    self.image = self.image or DEFAULT_IMAGE
    self.user_name = FLAGS.ali_user_name
    self.region = util.GetRegionByZone(self.zone)
    self.bandwidth_in = FLAGS.ali_bandwidth_in
    self.bandwidth_out = FLAGS.ali_bandwidth_out
    self.scratch_disk_size = FLAGS.scratch_disk_size or DEFAULT_DISK_SIZE
    self.system_disk_type = FLAGS.ali_system_disk_type
    self.eip_address_bandwidth = FLAGS.ali_eip_address_bandwidth
    self.network = ali_network.AliNetwork.GetNetwork(self)
    self.firewall = ali_network.AliFirewall.GetFirewall()

  @vm_util.Retry(poll_interval=1, log_errors=False)
  def _WaitForInstanceStatus(self, status_list):
    """Waits until the instance's status is in status_list"""
    logging.info('Waits until the instance\'s status is one of statuses: %s',
                 status_list)
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeInstances',
        '--RegionId %s' % self.region,
        '--InstanceIds \'["%s"]\'' % self.id]
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    instances = response['Instances']['Instance']
    assert len(instances) == 1
    status = instances[0]['Status']
    assert status in status_list

  @vm_util.Retry(poll_interval=5, max_retries=30, log_errors=False)
  def _WaitForEipStatus(self, status_list):
    """Waits until the instance's status is in status_list"""
    logging.info('Waits until the eip\'s status is one of statuses: %s',
                 status_list)
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeEipAddresses',
        '--RegionId %s' % self.region,
        '--AllocationId %s' % self.eip_id]
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    EipAddresses = response['EipAddresses']['EipAddress']
    assert len(EipAddresses) == 1
    status = EipAddresses[0]['Status']
    assert status in status_list

  def _AllocatePubIp(self, region, instance_id):
    """Allocate a public ip address and associate it to the instance"""
    if FLAGS.ali_use_vpc:
      allocatip_cmd = util.ALI_PREFIX + [
          'ecs',
          'AllocateEipAddress',
          '--RegionId %s' % region,
          '--InternetChargeType PayByTraffic',
          '--Bandwidth %s' % self.eip_address_bandwidth]
      allocatip_cmd = util.GetEncodedCmd(allocatip_cmd)
      stdout, _ = vm_util.IssueRetryableCommand(allocatip_cmd)
      response = json.loads(stdout)
      self.ip_address = response['EipAddress']
      self.eip_id = response['AllocationId']

      self._WaitForInstanceStatus(['Stopped', 'Running'])

      associate_cmd = util.ALI_PREFIX + [
          'ecs',
          'AssociateEipAddress',
          '--RegionId %s' % region,
          '--AllocationId  %s' % self.eip_id,
          '--InstanceId %s' % instance_id,
          '--InstanceType EcsInstance']
      associate_cmd = util.GetEncodedCmd(associate_cmd)
      vm_util.IssueRetryableCommand(associate_cmd)

    else:
      allocatip_cmd = util.ALI_PREFIX + [
          'ecs',
          'AllocatePublicIpAddress',
          '--RegionId %s' % region,
          '--InstanceId %s' % instance_id]
      allocatip_cmd = util.GetEncodedCmd(allocatip_cmd)
      stdout, _ = vm_util.IssueRetryableCommand(allocatip_cmd)
      response = json.loads(stdout)
      self.ip_address = response['IpAddress']


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
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)

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
    if self.network.use_vpc:
      pub_ip_address = instance['EipAddress']['IpAddress']
      self.internal_ip = \
          instance['VpcAttributes']['PrivateIpAddress']['IpAddress'][0]
    else:
      pub_ip_address = instance['PublicIpAddress']['IpAddress'][0]
      self.internal_ip = instance['InnerIpAddress']['IpAddress'][0]
    assert self.ip_address == pub_ip_address
    self.group_id = instance['SecurityGroupIds']['SecurityGroupId'][0]

    self._WaitForInstanceStatus(['Running'])

    self.firewall.AllowPort(self, SSH_PORT)
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
        '--SecurityGroupId %s' % self.network.security_group.group_id,
        '--Password %s' % self.password]

    if FLAGS.scratch_disk_type == disk.LOCAL:
      disk_cmd = [
          '--SystemDiskCategory ephemeral_ssd',
          '--DataDisk1Category ephemeral_ssd',
          '--DataDisk1Size %s' % self.scratch_disk_size,
          '--DataDisk1Device %s%s' % (util.GetDrivePathPrefix(),
                                      DRIVE_START_LETTER)]
      create_cmd.extend(disk_cmd)

    if FLAGS.ali_io_optimized is not None:
      create_cmd.extend(['--IoOptimized optimized',
                         '--SystemDiskCategory %s' % self.system_disk_type])

    if FLAGS.ali_use_vpc:
      create_cmd.extend(['--VpcId %s' % self.network.vpc.id,
                        '--VSwitchId %s' % self.network.vswitch.id])
    else:
      create_cmd.extend([
          '--InternetChargeType PayByTraffic',
          '--InternetMaxBandwidthIn %s' % self.bandwidth_in,
          '--InternetMaxBandwidthOut %s' % self.bandwidth_out])

    create_cmd = util.GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['InstanceId']

    self._AllocatePubIp(self.region, self.id)

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

    self._WaitForInstanceStatus(['Stopped'])

    delete_cmd = util.ALI_PREFIX + [
        'ecs',
        'DeleteInstance',
        '--RegionId %s' % self.region,
        '--InstanceId %s' % self.id]
    delete_cmd = util.GetEncodedCmd(delete_cmd)
    vm_util.IssueRetryableCommand(delete_cmd)

    if FLAGS.ali_use_vpc:
      self._WaitForEipStatus(['Available'])
      release_eip_cmd = util.ALI_PREFIX + [
          'ecs',
          'ReleaseEipAddress',
          '--RegionId %s' % self.region,
          '--AllocationId %s' % self.eip_id]
      release_eip_cmd = util.GetEncodedCmd(release_eip_cmd)
      vm_util.IssueRetryableCommand(release_eip_cmd)

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

    data_disk.WaitForDiskStatus(['In_use'])

    self.FormatDisk(data_disk.GetDevicePath())
    self.MountDisk(data_disk.GetDevicePath(), disk_spec.mount_point)

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    util.AddTags(self.id, RESOURCE_TYPE[INSTANCE], self.region, **kwargs)


class DebianBasedAliVirtualMachine(AliVirtualMachine,
                                   linux_virtual_machine.DebianMixin):
  IMAGE_NAME_FILTER = 'ubuntu_14_0405_64*alibase*.vhd'


class RhelBasedAliVirtualMachine(AliVirtualMachine,
                                 linux_virtual_machine.RhelMixin):
  pass


class WindowsAliVirtualMachine(AliVirtualMachine,
                               windows_virtual_machine.WindowsMixin):
  pass
