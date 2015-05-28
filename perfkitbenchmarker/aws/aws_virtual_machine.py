# Copyright 2014 Google Inc. All rights reserved.
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

"""Class to represent an AWS Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import json
import logging
import threading

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import package_managers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.aws import aws_disk
from perfkitbenchmarker.aws import util

FLAGS = flags.FLAGS

flags.DEFINE_string('aws_user_name', 'ubuntu',
                    'This determines the user name that Perfkit will '
                    'attempt to use. This must be changed in order to '
                    'use any image other than ubuntu.')

HVM = 'HVM'
PV = 'PV'
NON_HVM_PREFIXES = ['m1', 'c1', 't1', 'm2']
US_EAST_1 = 'us-east-1'
US_WEST_1 = 'us-west-1'
US_WEST_2 = 'us-west-2'
EU_WEST_1 = 'eu-west-1'
AP_NORTHEAST_1 = 'ap-northeast-1'
AP_SOUTHEAST_1 = 'ap-southeast-1'
AP_SOUTHEAST_2 = 'ap-southeast-2'
SA_EAST_1 = 'sa-east-1'
AMIS = {
    HVM: {
        US_EAST_1: 'ami-acff23c4',
        US_WEST_1: 'ami-05717d40',
        US_WEST_2: 'ami-fbce8bcb',
        EU_WEST_1: 'ami-30b46b47',
        AP_NORTHEAST_1: 'ami-d186dcd0',
        AP_SOUTHEAST_1: 'ami-9afca7c8',
        AP_SOUTHEAST_2: 'ami-956706af',
        SA_EAST_1: 'ami-9970d884',
    },
    PV: {
        US_EAST_1: 'ami-d2ff23ba',
        US_WEST_1: 'ami-73717d36',
        US_WEST_2: 'ami-f1ce8bc1',
        EU_WEST_1: 'ami-4ab46b3d',
        AP_NORTHEAST_1: 'ami-c786dcc6',
        AP_SOUTHEAST_1: 'ami-eefca7bc',
        AP_SOUTHEAST_2: 'ami-996706a3',
        SA_EAST_1: 'ami-6770d87a',
    }
}
PLACEMENT_GROUP_PREFIXES = frozenset(
    ['c3', 'c4', 'cc2', 'cg1', 'g2', 'cr1', 'r3', 'hi1', 'i2'])
NUM_LOCAL_VOLUMES = {
    'c1.medium': 1, 'c1.xlarge': 4,
    'c3.large': 2, 'c3.xlarge': 2, 'c3.2xlarge': 2, 'c3.4xlarge': 2,
    'c3.8xlarge': 2, 'cc2.8xlarge': 4,
    'cg1.4xlarge': 2, 'cr1.8xlarge': 2, 'g2.2xlarge': 1,
    'hi1.4xlarge': 2, 'hs1.8xlarge': 24,
    'i2.xlarge': 1, 'i2.2xlarge': 2, 'i2.4xlarge': 4, 'i2.8xlarge': 8,
    'm1.small': 1, 'm1.medium': 1, 'm1.large': 2, 'm1.xlarge': 4,
    'm2.xlarge': 1, 'm2.2xlarge': 1, 'm2.4xlarge': 2,
    'm3.medium': 1, 'm3.large': 1, 'm3.xlarge': 2, 'm3.2xlarge': 2,
    'r3.large': 1, 'r3.xlarge': 1, 'r3.2xlarge': 1, 'r3.4xlarge': 1,
    'r3.8xlarge': 2,
}
DRIVE_START_LETTER = 'b'
INSTANCE_EXISTS_STATUSES = frozenset(
    ['pending', 'running', 'stopping', 'stopped'])
INSTANCE_DELETED_STATUSES = frozenset(['shutting-down', 'terminated'])
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES


def GetBlockDeviceMap(machine_type):
  """Returns the block device map to expose all devices for a given machine.

  Args:
    machine_type: The machine type to create a block device map for.

  Returns:
    The json representation of the block device map for a machine compatible
    with the AWS CLI, or if the machine type has no local disks, it will
    return None.
  """
  if machine_type in NUM_LOCAL_VOLUMES:
    mappings = [{'VirtualName': 'ephemeral%s' % i,
                 'DeviceName': '/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)}
                for i in xrange(NUM_LOCAL_VOLUMES[machine_type])]
    return json.dumps(mappings)
  else:
    return None


def GetImage(machine_type, region):
  """Gets an ami compatible with the machine type and zone."""
  prefix = machine_type.split('.')[0]
  if prefix in NON_HVM_PREFIXES:
    return AMIS[PV][region]
  else:
    return AMIS[HVM][region]


def IsPlacementGroupCompatible(machine_type):
  """Returns True if VMs of 'machine_type' can be put in a placement group."""
  prefix = machine_type.split('.')[0]
  return prefix in PLACEMENT_GROUP_PREFIXES


class AwsVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an AWS Virtual Machine."""

  _lock = threading.Lock()
  imported_keyfile_set = set()
  deleted_keyfile_set = set()

  def __init__(self, vm_spec):
    """Initialize a AWS virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(AwsVirtualMachine, self).__init__(vm_spec)
    self.region = self.zone[:-1]
    self.image = self.image or GetImage(self.machine_type, self.region)
    self.user_name = FLAGS.aws_user_name
    if self.machine_type in NUM_LOCAL_VOLUMES:
      self.max_local_disks = NUM_LOCAL_VOLUMES[self.machine_type]
    self.local_disk_counter = 0

  def ImportKeyfile(self):
    """Imports the public keyfile to AWS."""
    with self._lock:
      if self.region in self.imported_keyfile_set:
        return
      cat_cmd = ['cat',
                 vm_util.GetPublicKeyPath()]
      keyfile, _ = vm_util.IssueRetryableCommand(cat_cmd)
      import_cmd = util.AWS_PREFIX + [
          'ec2', '--region=%s' % self.region,
          'import-key-pair',
          '--key-name=%s' % 'perfkit-key-%s' % FLAGS.run_uri,
          '--public-key-material=%s' % keyfile]
      vm_util.IssueRetryableCommand(import_cmd)
      self.imported_keyfile_set.add(self.region)
      if self.region in self.deleted_keyfile_set:
        self.deleted_keyfile_set.remove(self.region)

  def DeleteKeyfile(self):
    """Deletes the imported keyfile for a region."""
    with self._lock:
      if self.region in self.deleted_keyfile_set:
        return
      delete_cmd = util.AWS_PREFIX + [
          'ec2', '--region=%s' % self.region,
          'delete-key-pair',
          '--key-name=%s' % 'perfkit-key-%s' % FLAGS.run_uri]
      vm_util.IssueRetryableCommand(delete_cmd)
      self.deleted_keyfile_set.add(self.region)
      if self.region in self.imported_keyfile_set:
        self.imported_keyfile_set.remove(self.region)

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data and tag it."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-instances',
        '--region=%s' % self.region,
        '--instance-ids=%s' % self.id]
    logging.info('Getting instance %s public IP. This will fail until '
                 'a public IP is available, but will be retried.', self.id)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    instance = response['Reservations'][0]['Instances'][0]
    self.ip_address = instance['PublicIpAddress']
    self.internal_ip = instance['PrivateIpAddress']
    self.group_id = instance['SecurityGroups'][0]['GroupId']
    util.AddDefaultTags(self.id, self.region)

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.ImportKeyfile()

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    self.DeleteKeyfile()

  def _Create(self):
    """Create a VM instance."""
    super(AwsVirtualMachine, self)._Create()

    placement = 'AvailabilityZone=%s' % self.zone
    if IsPlacementGroupCompatible(self.machine_type):
      placement += ',GroupName=%s' % self.network.placement_group.name
    block_device_map = GetBlockDeviceMap(self.machine_type)

    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'run-instances',
        '--region=%s' % self.region,
        '--subnet-id=%s' % self.network.subnet.id,
        '--associate-public-ip-address',
        '--image-id=%s' % self.image,
        '--instance-type=%s' % self.machine_type,
        '--placement=%s' % placement,
        '--key-name=%s' % 'perfkit-key-%s' % FLAGS.run_uri]
    if block_device_map:
      create_cmd.append('--block-device-mappings=%s' % block_device_map)
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['Instances'][0]['InstanceId']

  def _Delete(self):
    """Delete a VM instance."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'terminate-instances',
        '--region=%s' % self.region,
        '--instance-ids=%s' % self.id]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the VM exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-instances',
        '--region=%s' % self.region,
        '--filter=Name=instance-id,Values=%s' % self.id]
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    reservations = response['Reservations']
    assert len(reservations) < 2, 'Too many reservations.'
    if not reservations:
      return False
    instances = reservations[0]['Instances']
    assert len(instances) == 1, 'Wrong number of instances.'
    status = instances[0]['State']['Name']
    assert status in INSTANCE_KNOWN_STATUSES, status
    return status in INSTANCE_EXISTS_STATUSES

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    # Instantiate the disk(s) that we want to create.
    if disk_spec.disk_type == disk.LOCAL:
      disks = []
      for _ in range(disk_spec.num_striped_disks):
        local_disk = aws_disk.AwsDisk(disk_spec, self.zone)
        local_disk.device_letter = chr(ord(DRIVE_START_LETTER) +
                                       self.local_disk_counter)
        self.local_disk_counter += 1
        disks.append(local_disk)
      if self.local_disk_counter > self.max_local_disks:
        raise errors.Error('Not enough local disks.')
    else:
      disks = [aws_disk.AwsDisk(disk_spec, self.zone)
               for _ in range(disk_spec.num_striped_disks)]

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          disks on the VM (e.g. '/dev/sdb').
    """
    return ['/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)
            for i in xrange(NUM_LOCAL_VOLUMES[self.machine_type])]

  def SetupLocalDisks(self):
    """Performs AWS specific setup of local disks."""
    # Some images may automount one local disk, but we don't
    # want to fail if this wasn't the case.
    self.RemoteCommand('sudo umount /mnt', ignore_failure=True)

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    util.AddTags(self.id, self.region, **kwargs)


class DebianBasedAwsVirtualMachine(AwsVirtualMachine,
                                   package_managers.AptMixin):
  pass


class RhelBasedAwsVirtualMachine(AwsVirtualMachine,
                                 package_managers.YumMixin):
  pass
