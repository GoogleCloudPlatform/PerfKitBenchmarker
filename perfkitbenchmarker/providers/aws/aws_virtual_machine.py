# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
from collections import OrderedDict

"""Class to represent an AWS Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import base64
import collections
import json
import logging
import uuid
import threading
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

HVM = 'HVM'
PV = 'PV'
NON_HVM_PREFIXES = ['m1', 'c1', 't1', 'm2']
NON_PLACEMENT_GROUP_PREFIXES = frozenset(['t2', 'm3'])
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
    'r3.8xlarge': 2, 'd2.xlarge': 3, 'd2.2xlarge': 6, 'd2.4xlarge': 12,
    'd2.8xlarge': 24, 'x1.32xlarge': 2, 'i3.large': 1, 'i3.xlarge': 1,
    'i3.2xlarge': 1, 'i3.4xlarge': 2, 'i3.8xlarge': 4, 'i3.16xlarge': 8
}
DRIVE_START_LETTER = 'b'
INSTANCE_EXISTS_STATUSES = frozenset(
    ['pending', 'running', 'stopping', 'stopped'])
INSTANCE_DELETED_STATUSES = frozenset(['shutting-down', 'terminated'])
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES
HOST_EXISTS_STATES = frozenset(
    ['available', 'under-assessment', 'permanent-failure'])
HOST_RELEASED_STATES = frozenset(['released', 'released-permanent-failure'])
KNOWN_HOST_STATES = HOST_EXISTS_STATES | HOST_RELEASED_STATES

# See http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-bid-status.html
SPOT_INSTANCE_REQUEST_HOLDING_STATUSES = frozenset(
    ['capacity-not-available', 'capacity-oversubscribed', 'price-too-low',
     'not-scheduled-yet', 'launch-group-constraint', 'az-group-constraint',
     'placement-group-constraint', 'constraint-not-fulfillable'])
SPOT_INSTANCE_REQUEST_TERMINAL_STATUSES = frozenset(
    ['schedule-expired', 'canceled-before-fulfillment', 'bad-parameters',
     'system-error', 'request-canceled-and-instance-running',
     'marked-for-termination', 'instance-terminated-by-price',
     'instance-terminated-by-user', 'instance-terminated-no-capacity',
     'instance-terminated-capacity-oversubscribed',
     'instance-terminated-launch-group-constraint'])


def GetRootBlockDeviceSpecForImage(image_id, region):
  """ Queries the CLI and returns the root block device specification as a dict.

  Args:
    image_id: The EC2 image id to query
    region: The EC2 region in which the image resides

  Returns:
    The root block device specification as returned by the AWS cli,
    as a Python dict. If the image is not found, or if the response
    is malformed, an exception will be raised.
  """
  command = util.AWS_PREFIX + [
      'ec2',
      'describe-images',
      '--region=%s' % region,
      '--image-ids=%s' % image_id,
      '--query', 'Images[]']
  stdout, _ = util.IssueRetryableCommand(command)
  images = json.loads(stdout)
  assert images
  assert len(images) == 1, \
      'Expected to receive only one image description for %s' % image_id
  image_spec = images[0]
  root_device_name = image_spec['RootDeviceName']
  block_device_mappings = image_spec['BlockDeviceMappings']
  root_block_device_dict = next((x for x in block_device_mappings if
                                 x['DeviceName'] == root_device_name))
  return root_block_device_dict


def GetBlockDeviceMap(machine_type, root_volume_size_gb=None,
                      image_id=None, region=None):
  """Returns the block device map to expose all devices for a given machine.

  Args:
    machine_type: The machine type to create a block device map for.
    root_volume_size: The desired size of the root volume, in GiB,
      or None to the default provided by AWS.
    image: The image id (AMI) to use in order to lookup the default
      root device specs. This is only required if root_volume_size
      is specified.
    region: The region which contains the specified image. This is only
      required if image_id is specified.

  Returns:
    The json representation of the block device map for a machine compatible
    with the AWS CLI, or if the machine type has no local disks, it will
    return None. If root_volume_size_gb and image_id are provided, the block
    device map will include the specification for the root volume.
  """
  mappings = []
  if root_volume_size_gb is not None:
    if image_id is None:
      raise ValueError(
          "image_id must be provided if root_volume_size_gb is specified")
    if region is None:
      raise ValueError(
          "region must be provided if image_id is specified")
    root_block_device = GetRootBlockDeviceSpecForImage(image_id, region)
    root_block_device['Ebs']['VolumeSize'] = root_volume_size_gb
    # The 'Encrypted' key must be removed or the CLI will complain
    root_block_device['Ebs'].pop('Encrypted')
    mappings.append(root_block_device)

  if (machine_type in NUM_LOCAL_VOLUMES and
      not aws_disk.LocalDriveIsNvme(machine_type)):
    for i in xrange(NUM_LOCAL_VOLUMES[machine_type]):
      od = OrderedDict()
      od['VirtualName'] = 'ephemeral%s' % i
      od['DeviceName'] = '/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)
      mappings.append(od)
  if len(mappings):
    return json.dumps(mappings)
  return None


def IsPlacementGroupCompatible(machine_type):
  """Returns True if VMs of 'machine_type' can be put in a placement group."""
  prefix = machine_type.split('.')[0]
  return prefix not in NON_PLACEMENT_GROUP_PREFIXES


class AwsDedicatedHost(resource.BaseResource):
  """Object representing an AWS host.

  Attributes:
    region: The AWS region of the host.
    zone: The AWS availability zone of the host.
    machine_type: The machine type of VMs that may be created on the host.
    client_token: A uuid that makes the creation request idempotent.
    id: The host_id of the host.
  """

  def __init__(self, machine_type, zone):
    super(AwsDedicatedHost, self).__init__()
    self.machine_type = machine_type
    self.zone = zone
    self.region = util.GetRegionFromZone(self.zone)
    self.client_token = str(uuid.uuid4())
    self.id = None

  def _Create(self):
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'allocate-hosts',
        '--region=%s' % self.region,
        '--client-token=%s' % self.client_token,
        '--instance-type=%s' % self.machine_type,
        '--availability-zone=%s' % self.zone,
        '--auto-placement=off',
        '--quantity=1']
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    if self.id:
      delete_cmd = util.AWS_PREFIX + [
          'ec2',
          'release-hosts',
          '--region=%s' % self.region,
          '--host-ids=%s' % self.id]
      vm_util.IssueCommand(delete_cmd)

  @vm_util.Retry()
  def _Exists(self):
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-hosts',
        '--region=%s' % self.region,
        '--filter=Name=client-token,Values=%s' % self.client_token]
    stdout, _, _ = vm_util.IssueCommand(describe_cmd)
    response = json.loads(stdout)
    hosts = response['Hosts']
    assert len(hosts) < 2, 'Too many hosts.'
    if not hosts:
      return False
    host = hosts[0]
    self.id = host['HostId']
    state = host['State']
    assert state in KNOWN_HOST_STATES, state
    return state in HOST_EXISTS_STATES


class AwsVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create an AwsVirtualMachine.

  Attributes:
      use_dedicated_host: bool. Whether to create this VM on a dedicated host.
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
    super(AwsVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['aws_boot_disk_size'].present:
      config_values['boot_disk_size'] = flag_values.aws_boot_disk_size
    if flag_values['aws_spot_instances'].present:
      config_values['use_spot_instance'] = flag_values.aws_spot_instances
    if flag_values['aws_spot_price'].present:
      config_values['spot_price'] = flag_values.aws_spot_price

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(AwsVmSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'use_spot_instance': (option_decoders.BooleanDecoder,
                              {'default': False}),
        'spot_price': (option_decoders.FloatDecoder, {'default': 0.0}),
        'boot_disk_size': (option_decoders.IntDecoder, {'default': None})})

    return result


class AwsVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an AWS Virtual Machine."""

  CLOUD = providers.AWS
  IMAGE_NAME_FILTER = None
  DEFAULT_ROOT_DISK_TYPE = 'gp2'

  _lock = threading.Lock()
  imported_keyfile_set = set()
  deleted_keyfile_set = set()
  deleted_hosts = set()
  host_map = collections.defaultdict(list)

  def __init__(self, vm_spec):
    """Initialize a AWS virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(AwsVirtualMachine, self).__init__(vm_spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.user_name = FLAGS.aws_user_name
    if self.machine_type in NUM_LOCAL_VOLUMES:
      self.max_local_disks = NUM_LOCAL_VOLUMES[self.machine_type]
    self.user_data = None
    self.network = aws_network.AwsNetwork.GetNetwork(self)
    self.firewall = aws_network.AwsFirewall.GetFirewall()
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.use_spot_instance = vm_spec.use_spot_instance
    self.spot_price = vm_spec.spot_price
    self.boot_disk_size = vm_spec.boot_disk_size
    self.client_token = str(uuid.uuid4())
    self.host = None
    self.id = None
    self.metadata.update({
        'spot_instance': self.use_spot_instance,
        'spot_price': self.spot_price,
    })

    if self.use_dedicated_host and util.IsRegion(self.zone):
      raise ValueError(
          'In order to use dedicated hosts, you must specify an availability '
          'zone, not a region ("zone" was %s).' % self.zone)
    if self.machine_type[:2].lower() == 'i3' and not self.image:
      # TODO(user): Remove this check when pkb defaults to ubuntu-1604.
      raise ValueError(
          'In order to use i3 instances, you must specify --image.')

    if self.use_spot_instance and self.spot_price <= 0.0:
      raise ValueError(
          'In order to use spot instances you must specify a spot price '
          'greater than 0.0.')

  @property
  def host_list(self):
    """Returns the list of hosts that are compatible with this VM."""
    return self.host_map[(self.machine_type, self.zone)]

  @property
  def group_id(self):
    """Returns the security group ID of this VM."""
    return self.network.regional_network.vpc.default_security_group_id

  @classmethod
  def _GetDefaultImage(cls, machine_type, region):
    """Returns the default image given the machine type and region.

    If no default is configured, this will return None.
    """
    if cls.IMAGE_NAME_FILTER is None:
      return None

    prefix = machine_type.split('.')[0]
    virt_type = 'paravirtual' if prefix in NON_HVM_PREFIXES else 'hvm'

    describe_cmd = util.AWS_PREFIX + [
        '--region=%s' % region,
        'ec2',
        'describe-images',
        '--query', 'Images[*].{Name:Name,ImageId:ImageId}',
        '--filters',
        'Name=name,Values=%s' % cls.IMAGE_NAME_FILTER,
        'Name=block-device-mapping.volume-type,Values=%s' %
        cls.DEFAULT_ROOT_DISK_TYPE,
        'Name=virtualization-type,Values=%s' % virt_type]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)

    if not stdout:
      return None

    images = json.loads(stdout)
    # We want to return the latest version of the image, and since the wildcard
    # portion of the image name is the image's creation date, we can just take
    # the image with the 'largest' name.
    return max(images, key=lambda image: image['Name'])['ImageId']

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
      util.IssueRetryableCommand(import_cmd)
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
      util.IssueRetryableCommand(delete_cmd)
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
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    instance = response['Reservations'][0]['Instances'][0]
    self.ip_address = instance['PublicIpAddress']
    self.internal_ip = instance['PrivateIpAddress']
    if util.IsRegion(self.zone):
      self.zone = str(instance['Placement']['AvailabilityZone'])
    util.AddDefaultTags(self.id, self.region)

    assert self.group_id == instance['SecurityGroups'][0]['GroupId'], (
        self.group_id, instance['SecurityGroups'][0]['GroupId'])

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.ImportKeyfile()
    # _GetDefaultImage calls the AWS CLI.
    self.image = self.image or self._GetDefaultImage(self.machine_type,
                                                     self.region)
    self.AllowRemoteAccessPorts()

    if self.use_dedicated_host:
      with self._lock:
        if not self.host_list:
          host = AwsDedicatedHost(self.machine_type, self.zone)
          self.host_list.append(host)
          host.Create()
        self.host = self.host_list[-1]

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    self.DeleteKeyfile()
    if self.host:
      with self._lock:
        if self.host in self.host_list:
          self.host_list.remove(self.host)
        if self.host not in self.deleted_hosts:
          self.host.Delete()
          self.deleted_hosts.add(self.host)

  def _Create(self):
    """Create a VM instance."""
    if self.use_spot_instance:
      self._CreateSpot()
    else:
      self._CreateOnDemand()

  def _CreateOnDemand(self):
    """Create an OnDemand VM instance."""
    placement = []
    if not util.IsRegion(self.zone):
      placement.append('AvailabilityZone=%s' % self.zone)
    if self.use_dedicated_host:
      placement.append('Tenancy=host,HostId=%s' % self.host.id)
      num_hosts = len(self.host_list)
    elif IsPlacementGroupCompatible(self.machine_type):
      placement.append('GroupName=%s' % self.network.placement_group.name)
    placement = ','.join(placement)
    block_device_map = GetBlockDeviceMap(self.machine_type,
                                         self.boot_disk_size,
                                         self.image,
                                         self.region)
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'run-instances',
        '--region=%s' % self.region,
        '--subnet-id=%s' % self.network.subnet.id,
        '--associate-public-ip-address',
        '--client-token=%s' % self.client_token,
        '--image-id=%s' % self.image,
        '--instance-type=%s' % self.machine_type,
        '--key-name=%s' % 'perfkit-key-%s' % FLAGS.run_uri]
    if block_device_map:
      create_cmd.append('--block-device-mappings=%s' % block_device_map)
    if placement:
      create_cmd.append('--placement=%s' % placement)
    if self.user_data:
      create_cmd.append('--user-data=%s' % self.user_data)
    _, stderr, _ = vm_util.IssueCommand(create_cmd)
    if self.use_dedicated_host and 'InsufficientCapacityOnHost' in stderr:
      logging.warning(
          'Creation failed due to insufficient host capacity. A new host will '
          'be created and instance creation will be retried.')
      with self._lock:
        if num_hosts == len(self.host_list):
          host = AwsDedicatedHost(self.machine_type, self.zone)
          self.host_list.append(host)
          host.Create()
        self.host = self.host_list[-1]
      self.client_token = str(uuid.uuid4())
      raise errors.Resource.RetryableCreationError()

  def _CreateSpot(self):
    """Create a Spot VM instance."""
    placement = OrderedDict()
    if not util.IsRegion(self.zone):
      placement['AvailabilityZone'] = self.zone
    if self.use_dedicated_host:
      raise errors.Resource.CreationError(
          'Tenancy=host is not supported for Spot Instances')
    elif IsPlacementGroupCompatible(self.machine_type):
      placement['GroupName'] = self.network.placement_group.name
    block_device_map = GetBlockDeviceMap(self.machine_type,
                                         self.boot_disk_size,
                                         self.image,
                                         self.region)
    network_interface = [OrderedDict([
        ('DeviceIndex', 0),
        ('AssociatePublicIpAddress', True),
        ('SubnetId', self.network.subnet.id)])]
    launch_specification = OrderedDict([
        ('ImageId', self.image),
        ('InstanceType', self.machine_type),
        ('KeyName', 'perfkit-key-%s' % FLAGS.run_uri),
        ('Placement', placement)])
    if block_device_map:
      launch_specification['BlockDeviceMappings'] = json.loads(
          block_device_map, object_pairs_hook=collections.OrderedDict)
    launch_specification['NetworkInterfaces'] = network_interface
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'request-spot-instances',
        '--region=%s' % self.region,
        '--spot-price=%s' % self.spot_price,
        '--client-token=%s' % self.client_token,
        '--launch-specification=%s' % json.dumps(launch_specification,
                                                 separators=(',', ':'))]
    stdout, stderr, _ = vm_util.IssueCommand(create_cmd)
    create_response = json.loads(stdout)
    self.spot_instance_request_id =\
        create_response['SpotInstanceRequests'][0]['SpotInstanceRequestId']

    util.AddDefaultTags(self.spot_instance_request_id, self.region)

    while True:
      describe_sir_cmd = util.AWS_PREFIX + [
          '--region=%s' % self.region,
          'ec2',
          'describe-spot-instance-requests',
          '--spot-instance-request-ids=%s' % self.spot_instance_request_id]
      stdout, stderr, _ = vm_util.IssueCommand(describe_sir_cmd)

      sir_response = json.loads(stdout)['SpotInstanceRequests']
      assert len(sir_response) == 1, 'Expected exactly 1 SpotInstanceRequest'

      status_code = sir_response[0]['Status']['Code']

      if status_code in SPOT_INSTANCE_REQUEST_HOLDING_STATUSES or \
         status_code in SPOT_INSTANCE_REQUEST_TERMINAL_STATUSES:
        message = sir_response[0]['Status']['Message']
        raise errors.Resource.CreationError(message)
      elif status_code == "fulfilled":
        self.id = sir_response[0]['InstanceId']
        break

      time.sleep(2)

  def _Delete(self):
    """Delete a VM instance."""
    if self.id:
      delete_cmd = util.AWS_PREFIX + [
          'ec2',
          'terminate-instances',
          '--region=%s' % self.region,
          '--instance-ids=%s' % self.id]
      vm_util.IssueCommand(delete_cmd)
    if hasattr(self, 'spot_instance_request_id'):
      cancel_cmd = util.AWS_PREFIX + [
          '--region=%s' % self.region,
          'ec2',
          'cancel-spot-instance-requests',
          '--spot-instance-request-ids=%s' % self.spot_instance_request_id]
      vm_util.IssueCommand(cancel_cmd)


  def _Exists(self):
    """Returns true if the VM exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-instances',
        '--region=%s' % self.region]

    if self.use_spot_instance:
      if self.id:
        describe_cmd.append('--instance-id=%s' % self.id)
      else:
        return False
    else:
      describe_cmd.append(
          '--filter=Name=client-token,Values=%s' % self.client_token)

    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    reservations = response['Reservations']
    assert len(reservations) < 2, 'Too many reservations.'
    if not reservations:
      return False
    instances = reservations[0]['Instances']
    assert len(instances) == 1, 'Wrong number of instances.'
    status = instances[0]['State']['Name']
    self.id = instances[0]['InstanceId']
    assert status in INSTANCE_KNOWN_STATUSES, status
    return status in INSTANCE_EXISTS_STATUSES

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    # Instantiate the disk(s) that we want to create.
    disks = []
    for _ in range(disk_spec.num_striped_disks):
      data_disk = aws_disk.AwsDisk(disk_spec, self.zone, self.machine_type)
      if disk_spec.disk_type == disk.LOCAL:
        data_disk.device_letter = chr(ord(DRIVE_START_LETTER) +
                                      self.local_disk_counter)
        # Local disk numbers start at 1 (0 is the system disk).
        data_disk.disk_number = self.local_disk_counter + 1
        self.local_disk_counter += 1
        if self.local_disk_counter > self.max_local_disks:
          raise errors.Error('Not enough local disks.')
      else:
        # Remote disk numbers start at 1 + max_local disks (0 is the system disk
        # and local disks occupy [1, max_local_disks]).
        data_disk.disk_number = (self.remote_disk_counter +
                                 1 + self.max_local_disks)
        self.remote_disk_counter += 1
      disks.append(data_disk)

    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    util.AddTags(self.id, self.region, **kwargs)


class DebianBasedAwsVirtualMachine(AwsVirtualMachine,
                                   linux_virtual_machine.DebianMixin):
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-trusty-14.04-amd64-*'


class JujuBasedAwsVirtualMachine(AwsVirtualMachine,
                                 linux_virtual_machine.JujuMixin):
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-trusty-14.04-amd64-*'


class RhelBasedAwsVirtualMachine(AwsVirtualMachine,
                                 linux_virtual_machine.RhelMixin):
  IMAGE_NAME_FILTER = 'amzn-ami-*-x86_64-*'

  def __init__(self, vm_spec):
    super(RhelBasedAwsVirtualMachine, self).__init__(vm_spec)
    user_name_set = FLAGS['aws_user_name'].present
    self.user_name = FLAGS.aws_user_name if user_name_set else 'ec2-user'


class WindowsAwsVirtualMachine(AwsVirtualMachine,
                               windows_virtual_machine.WindowsMixin):

  IMAGE_NAME_FILTER = 'Windows_Server-2012-R2_RTM-English-64Bit-Core-*'

  def __init__(self, vm_spec):
    super(WindowsAwsVirtualMachine, self).__init__(vm_spec)
    self.user_name = 'Administrator'
    self.user_data = ('<powershell>%s</powershell>' %
                      windows_virtual_machine.STARTUP_SCRIPT)

  @vm_util.Retry()
  def _GetDecodedPasswordData(self):
    # Retreive a base64 encoded, encrypted password for the VM.
    get_password_cmd = util.AWS_PREFIX + [
        'ec2',
        'get-password-data',
        '--region=%s' % self.region,
        '--instance-id=%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(get_password_cmd)
    response = json.loads(stdout)
    password_data = response['PasswordData']

    # AWS may not populate the password data until some time after
    # the VM shows as running. Simply retry until the data shows up.
    if not password_data:
      raise ValueError('No PasswordData in response.')

    # Decode the password data.
    return base64.b64decode(password_data)


  def _PostCreate(self):
    """Retrieve generic VM info and then retrieve the VM's password."""
    super(WindowsAwsVirtualMachine, self)._PostCreate()

    # Get the decoded password data.
    decoded_password_data = self._GetDecodedPasswordData()

    # Write the encrypted data to a file, and use openssl to
    # decrypt the password.
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(decoded_password_data)
      tf.close()
      decrypt_cmd = ['openssl',
                     'rsautl',
                     '-decrypt',
                     '-in',
                     tf.name,
                     '-inkey',
                     vm_util.GetPrivateKeyPath()]
      password, _ = vm_util.IssueRetryableCommand(decrypt_cmd)
      self.password = password
