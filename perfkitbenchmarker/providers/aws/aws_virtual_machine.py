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

"""Class to represent an AWS Virtual Machine object.

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import collections
import json
import logging
import posixpath
import re
import threading
import uuid

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util
from six.moves import range

FLAGS = flags.FLAGS
flags.DEFINE_enum('aws_credit_specification', None,
                  ['CpuCredits=unlimited', 'CpuCredits=standard'],
                  'Credit specification for burstable vms.')

HVM = 'hvm'
PV = 'paravirtual'
NON_HVM_PREFIXES = ['m1', 'c1', 't1', 'm2']
NON_PLACEMENT_GROUP_PREFIXES = frozenset(['t2', 'm3', 't3'])
# Following dictionary based on
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
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
    'd2.8xlarge': 24, 'i3.large': 1, 'i3.xlarge': 1,
    'i3.2xlarge': 1, 'i3.4xlarge': 2, 'i3.8xlarge': 4, 'i3.16xlarge': 8,
    'i3.metal': 8,
    'c5d.large': 1, 'c5d.xlarge': 1, 'c5d.2xlarge': 1, 'c5d.4xlarge': 1,
    'c5d.9xlarge': 1, 'c5d.18xlarge': 2,
    'm5d.large': 1, 'm5d.xlarge': 1, 'm5d.2xlarge': 1, 'm5d.4xlarge': 2,
    'm5d.12xlarge': 2, 'm5d.24xlarge': 4,
    'r5d.large': 1, 'r5d.xlarge': 1, 'r5d.2xlarge': 1, 'r5d.4xlarge': 2,
    'r5d.12xlarge': 2, 'r5d.24xlarge': 4,
    'z1d.large': 1, 'z1d.xlarge': 1, 'z1d.2xlarge': 1, 'z1d.3xlarge': 2,
    'z1d.6xlarge': 1, 'z1d.12xlarge': 2,
    'x1.16xlarge': 1, 'x1.32xlarge': 2,
    'x1e.xlarge': 1, 'x1e.2xlarge': 1, 'x1e.4xlarge': 1, 'x1e.8xlarge': 1,
    'x1e.16xlarge': 1, 'x1e.32xlarge': 2,
    'f1.2xlarge': 1, 'f1.4xlarge': 1, 'f1.16xlarge': 4,
    'p3dn.24xlarge': 2
}
DRIVE_START_LETTER = 'b'
TERMINATED = 'terminated'
SHUTTING_DOWN = 'shutting-down'
INSTANCE_EXISTS_STATUSES = frozenset(['running', 'stopping', 'stopped'])
INSTANCE_DELETED_STATUSES = frozenset([SHUTTING_DOWN, TERMINATED])
INSTANCE_TRANSITIONAL_STATUSES = frozenset(['pending'])
INSTANCE_KNOWN_STATUSES = (INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES
                           | INSTANCE_TRANSITIONAL_STATUSES)
HOST_EXISTS_STATES = frozenset(
    ['available', 'under-assessment', 'permanent-failure'])
HOST_RELEASED_STATES = frozenset(['released', 'released-permanent-failure'])
KNOWN_HOST_STATES = HOST_EXISTS_STATES | HOST_RELEASED_STATES

AWS_INITIATED_SPOT_TERMINATING_TRANSITION_STATUSES = frozenset(
    ['marked-for-termination', 'marked-for-stop'])

AWS_INITIATED_SPOT_TERMINAL_STATUSES = frozenset(
    ['instance-terminated-by-price', 'instance-terminated-by-service',
     'instance-terminated-no-capacity',
     'instance-terminated-capacity-oversubscribed',
     'instance-terminated-launch-group-constraint'])

USER_INITIATED_SPOT_TERMINAL_STATUSES = frozenset(
    ['request-canceled-and-instance-running', 'instance-terminated-by-user'])

ARM_PROCESSOR_PREFIXES = ['a1']

# Processor architectures
ARM = 'arm64'
X86 = 'x86_64'

# These are the project numbers of projects owning common images.
# Some numbers have corresponding owner aliases, but they are not used here.
AMAZON_LINUX_IMAGE_PROJECT = '137112412989'  # alias amazon
# https://coreos.com/os/docs/latest/booting-on-ec2.html
COREOS_IMAGE_PROJECT = '595879546273'
# From https://wiki.debian.org/Cloud/AmazonEC2Image/Buster
# TODO(pclay): replace with Marketplace AMI when available
DEBIAN_IMAGE_PROJECT = '136693071363'
MARKETPLACE_IMAGE_PROJECT = '679593333241'  # alias aws-marketplace
# https://access.redhat.com/articles/2962171
RHEL_IMAGE_PROJECT = '309956199498'
# https://help.ubuntu.com/community/EC2StartersGuide#Official_Ubuntu_Cloud_Guest_Amazon_Machine_Images_.28AMIs.29
UBUNTU_IMAGE_PROJECT = '099720109477'  # Owned by canonical
# Some Windows images are also available in marketplace project, but this is the
# one selected by the AWS console.
WINDOWS_IMAGE_PROJECT = '801119661308'  # alias amazon

# Machine type to host architecture.
_MACHINE_TYPE_PREFIX_TO_HOST_ARCH = {
    'a1': 'cortex-a72',
}

# Parameters for use with Elastic Fiber Adapter
_EFA_PARAMS = {
    'InterfaceType': 'efa',
    'DeviceIndex': 0,
    'AssociatePublicIpAddress': True
}
# Location of EFA installer
_EFA_URL = ('https://s3-us-west-2.amazonaws.com/aws-efa-installer/'
            'aws-efa-installer-{version}.tar.gz')
# Command to install Libfabric and OpenMPI
_EFA_INSTALL_CMD = ';'.join([
    'curl -O {url}', 'tar -xvzf {tarfile}', 'cd aws-efa-installer',
    'sudo ./efa_installer.sh -y', 'rm -rf {tarfile} aws-efa-installer'
])


class AwsTransitionalVmRetryableError(Exception):
  """Error for retrying _Exists when an AWS VM is in a transitional state."""


class AwsDriverDoesntSupportFeatureError(Exception):
  """Raised if there is an attempt to set a feature not supported."""


class AwsUnexpectedWindowsAdapterOutputError(Exception):
  """Raised when querying the status of a windows adapter failed."""


class AwsUnknownStatusError(Exception):
  """Error indicating an unknown status was encountered."""


class AwsImageNotFoundError(Exception):
  """Error indicating no appropriate AMI could be found."""


def GetRootBlockDeviceSpecForImage(image_id, region):
  """Queries the CLI and returns the root block device specification as a dict.

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
  assert len(images) == 1, (
      'Expected to receive only one image description for %s' % image_id)
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
    root_volume_size_gb: The desired size of the root volume, in GiB,
      or None to the default provided by AWS.
    image_id: The image id (AMI) to use in order to lookup the default
      root device specs. This is only required if root_volume_size
      is specified.
    region: The region which contains the specified image. This is only
      required if image_id is specified.

  Returns:
    The json representation of the block device map for a machine compatible
    with the AWS CLI, or if the machine type has no local disks, it will
    return None. If root_volume_size_gb and image_id are provided, the block
    device map will include the specification for the root volume.

  Raises:
    ValueError: If required parameters are not passed.
  """
  mappings = []
  if root_volume_size_gb is not None:
    if image_id is None:
      raise ValueError(
          'image_id must be provided if root_volume_size_gb is specified')
    if region is None:
      raise ValueError(
          'region must be provided if image_id is specified')
    root_block_device = GetRootBlockDeviceSpecForImage(image_id, region)
    root_block_device['Ebs']['VolumeSize'] = root_volume_size_gb
    # The 'Encrypted' key must be removed or the CLI will complain
    root_block_device['Ebs'].pop('Encrypted')
    mappings.append(root_block_device)

  if (machine_type in NUM_LOCAL_VOLUMES and
      not aws_disk.LocalDriveIsNvme(machine_type)):
    for i in range(NUM_LOCAL_VOLUMES[machine_type]):
      od = collections.OrderedDict()
      od['VirtualName'] = 'ephemeral%s' % i
      od['DeviceName'] = '/dev/xvd%s' % chr(ord(DRIVE_START_LETTER) + i)
      mappings.append(od)
  if mappings:
    return json.dumps(mappings)
  return None


def IsPlacementGroupCompatible(machine_type):
  """Returns True if VMs of 'machine_type' can be put in a placement group."""
  prefix = machine_type.split('.')[0]
  return prefix not in NON_PLACEMENT_GROUP_PREFIXES


def GetProcessorArchitecture(machine_type):
  """Returns the processor architecture of the VM."""
  prefix = machine_type.split('.')[0]
  if prefix in ARM_PROCESSOR_PREFIXES:
    return ARM
  else:
    return X86


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
    self.fill_fraction = 0.0

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
      vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

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
        'use_spot_instance': (option_decoders.BooleanDecoder, {
            'default': False
        }),
        'spot_price': (option_decoders.FloatDecoder, {
            'default': None
        }),
        'boot_disk_size': (option_decoders.IntDecoder, {
            'default': None
        })
    })

    return result


def _GetKeyfileSetKey(region):
  """Returns a key to use for the keyfile set.

  This prevents other runs in the same process from reusing the key.

  Args:
    region: The region the keyfile is in.
  """
  return (region, FLAGS.run_uri)


class AwsKeyFileManager(object):
  """Object for managing AWS Keyfiles."""
  _lock = threading.Lock()
  imported_keyfile_set = set()
  deleted_keyfile_set = set()

  @classmethod
  def ImportKeyfile(cls, region):
    """Imports the public keyfile to AWS."""
    with cls._lock:
      if _GetKeyfileSetKey(region) in cls.imported_keyfile_set:
        return
      cat_cmd = ['cat',
                 vm_util.GetPublicKeyPath()]
      keyfile, _ = vm_util.IssueRetryableCommand(cat_cmd)
      import_cmd = util.AWS_PREFIX + [
          'ec2', '--region=%s' % region,
          'import-key-pair',
          '--key-name=%s' % cls.GetKeyNameForRun(),
          '--public-key-material=%s' % keyfile]
      _, stderr, retcode = vm_util.IssueCommand(
          import_cmd, raise_on_failure=False)
      if retcode:
        if 'KeyPairLimitExceeded' in stderr:
          raise errors.Benchmarks.QuotaFailure(
              'KeyPairLimitExceeded in %s: %s' % (region, stderr))
        else:
          raise errors.Benchmarks.PrepareException(stderr)

      cls.imported_keyfile_set.add(_GetKeyfileSetKey(region))
      if _GetKeyfileSetKey(region) in cls.deleted_keyfile_set:
        cls.deleted_keyfile_set.remove(_GetKeyfileSetKey(region))

  @classmethod
  def DeleteKeyfile(cls, region):
    """Deletes the imported keyfile for a region."""
    with cls._lock:
      if _GetKeyfileSetKey(region) in cls.deleted_keyfile_set:
        return
      delete_cmd = util.AWS_PREFIX + [
          'ec2', '--region=%s' % region,
          'delete-key-pair',
          '--key-name=%s' % cls.GetKeyNameForRun()]
      util.IssueRetryableCommand(delete_cmd)
      cls.deleted_keyfile_set.add(_GetKeyfileSetKey(region))
      if _GetKeyfileSetKey(region) in cls.imported_keyfile_set:
        cls.imported_keyfile_set.remove(_GetKeyfileSetKey(region))

  @classmethod
  def GetKeyNameForRun(cls):
    return 'perfkit-key-{0}'.format(FLAGS.run_uri)


class AwsVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an AWS Virtual Machine."""

  CLOUD = providers.AWS

  # The IMAGE_NAME_FILTER is passed to the AWS CLI describe-images command to
  # filter images by name. This must be set by subclasses, but may be overridden
  # by the aws_image_name_filter flag.
  IMAGE_NAME_FILTER = None

  # The IMAGE_NAME_REGEX can be used to further filter images by name. It
  # applies after the IMAGE_NAME_FILTER above. Note that before this regex is
  # applied, Python's string formatting is used to replace {virt_type} and
  # {disk_type} by the respective virtualization type and root disk type of the
  # VM, allowing the regex to contain these strings. This regex supports
  # arbitrary Python regular expressions to further narrow down the set of
  # images considered.
  IMAGE_NAME_REGEX = None

  # Project that owns the AMIs of this OS type. Default to AWS Marketplace
  # official image project.
  IMAGE_OWNER = MARKETPLACE_IMAGE_PROJECT

  # Some AMIs use a project code to find the latest (in addition to owner, and
  # filter)
  IMAGE_PRODUCT_CODE_FILTER = None

  DEFAULT_ROOT_DISK_TYPE = 'gp2'
  DEFAULT_USER_NAME = 'ec2-user'

  _lock = threading.Lock()
  deleted_hosts = set()
  host_map = collections.defaultdict(list)

  def __init__(self, vm_spec):
    """Initialize a AWS virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.

    Raises:
      ValueError: If an incompatible vm_spec is passed.
    """
    super(AwsVirtualMachine, self).__init__(vm_spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.user_name = FLAGS.aws_user_name or self.DEFAULT_USER_NAME
    if self.machine_type in NUM_LOCAL_VOLUMES:
      self.max_local_disks = NUM_LOCAL_VOLUMES[self.machine_type]
    self.user_data = None
    self.network = aws_network.AwsNetwork.GetNetwork(self)
    self.placement_group = getattr(vm_spec, 'placement_group',
                                   self.network.placement_group)
    self.firewall = aws_network.AwsFirewall.GetFirewall()
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.use_spot_instance = vm_spec.use_spot_instance
    self.spot_price = vm_spec.spot_price
    self.boot_disk_size = vm_spec.boot_disk_size
    self.client_token = str(uuid.uuid4())
    self.host = None
    self.id = None
    self.metadata.update({
        'spot_instance':
            self.use_spot_instance,
        'spot_price':
            self.spot_price,
        'placement_group_strategy':
            self.placement_group.strategy if self.placement_group else 'none',
        'aws_credit_specification':
            FLAGS.aws_credit_specification
            if FLAGS.aws_credit_specification else 'none'
    })
    self.early_termination = False
    self.spot_status_code = None

    if self.use_dedicated_host and util.IsRegion(self.zone):
      raise ValueError(
          'In order to use dedicated hosts, you must specify an availability '
          'zone, not a region ("zone" was %s).' % self.zone)

    if self.use_dedicated_host and self.use_spot_instance:
      raise ValueError(
          'Tenancy=host is not supported for Spot Instances')

  @property
  def host_list(self):
    """Returns the list of hosts that are compatible with this VM."""
    return self.host_map[(self.machine_type, self.zone)]

  @property
  def group_id(self):
    """Returns the security group ID of this VM."""
    return self.network.regional_network.vpc.default_security_group_id

  @classmethod
  def GetDefaultImage(cls, machine_type, region):
    """Returns the default image given the machine type and region.

    If specified, the aws_image_name_filter and aws_image_name_regex flags will
    override os_type defaults.

    Args:
      machine_type: The machine_type of the VM, used to determine virtualization
        type.
      region: The region of the VM, as images are region specific.

    Raises:
      AwsImageNotFoundError: If a default image cannot be found.

    Returns:
      The ID of the latest image, or None if no default image is configured or
      none can be found.
    """

    # These cannot be REQUIRED_ATTRS, because nesting REQUIRED_ATTRS breaks.
    if not cls.IMAGE_OWNER:
      raise NotImplementedError('AWS OSMixins require IMAGE_OWNER')
    if not cls.IMAGE_NAME_FILTER:
      raise NotImplementedError('AWS OSMixins require IMAGE_NAME_FILTER')

    if FLAGS.aws_image_name_filter:
      cls.IMAGE_NAME_FILTER = FLAGS.aws_image_name_filter

    if FLAGS.aws_image_name_regex:
      cls.IMAGE_NAME_REGEX = FLAGS.aws_image_name_regex

    prefix = machine_type.split('.')[0]
    virt_type = PV if prefix in NON_HVM_PREFIXES else HVM
    processor_architecture = GetProcessorArchitecture(machine_type)

    describe_cmd = util.AWS_PREFIX + [
        '--region=%s' % region,
        'ec2',
        'describe-images',
        '--query', 'Images[*].{Name:Name,ImageId:ImageId}',
        '--filters',
        'Name=name,Values=%s' % cls.IMAGE_NAME_FILTER,
        'Name=block-device-mapping.volume-type,Values=%s' %
        cls.DEFAULT_ROOT_DISK_TYPE,
        'Name=virtualization-type,Values=%s' % virt_type,
        'Name=architecture,Values=%s' % processor_architecture]
    if cls.IMAGE_PRODUCT_CODE_FILTER:
      describe_cmd.extend(['Name=product-code,Values=%s' %
                           cls.IMAGE_PRODUCT_CODE_FILTER])
    describe_cmd.extend(['--owners', cls.IMAGE_OWNER])
    stdout, _ = util.IssueRetryableCommand(describe_cmd)

    if not stdout:
      raise AwsImageNotFoundError('aws describe-images did not produce valid '
                                  'output.')

    if cls.IMAGE_NAME_REGEX:
      # Further filter images by the IMAGE_NAME_REGEX filter.
      image_name_regex = cls.IMAGE_NAME_REGEX.format(
          virt_type=virt_type, disk_type=cls.DEFAULT_ROOT_DISK_TYPE,
          architecture=processor_architecture)
      images = []
      excluded_images = []
      for image in json.loads(stdout):
        if re.search(image_name_regex, image['Name']):
          images.append(image)
        else:
          excluded_images.append(image)

      if excluded_images:
        logging.debug('Excluded the following images with regex "%s": %s',
                      image_name_regex,
                      sorted(image['Name'] for image in excluded_images))
    else:
      images = json.loads(stdout)

    if not images:
      raise AwsImageNotFoundError('No AMIs with given filters found.')

    # We want to return the latest version of the image, and since the wildcard
    # portion of the image name is the image's creation date, we can just take
    # the image with the 'largest' name.
    # TODO(deitz): Investigate sorting by CreationDate instead of by Name.
    return max(images, key=lambda image: image['Name'])['ImageId']

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

    assert self.group_id == instance['SecurityGroups'][0]['GroupId'], (
        self.group_id, instance['SecurityGroups'][0]['GroupId'])
    if FLAGS.aws_efa:
      self.InstallPackages('curl')
      url = _EFA_URL.format(version=FLAGS.aws_efa_version)
      self.RemoteCommand(
          _EFA_INSTALL_CMD.format(url=url, tarfile=posixpath.basename(url)))

  def _CreateDependencies(self):
    """Create VM dependencies."""
    AwsKeyFileManager.ImportKeyfile(self.region)
    # GetDefaultImage calls the AWS CLI.
    self.image = self.image or self.GetDefaultImage(self.machine_type,
                                                    self.region)
    self.AllowRemoteAccessPorts()

    if self.use_dedicated_host:
      with self._lock:
        if (not self.host_list or (self.num_vms_per_host and
                                   self.host_list[-1].fill_fraction +
                                   1.0 / self.num_vms_per_host > 1.0)):
          host = AwsDedicatedHost(self.machine_type, self.zone)
          self.host_list.append(host)
          host.Create()
        self.host = self.host_list[-1]
        if self.num_vms_per_host:
          self.host.fill_fraction += 1.0 / self.num_vms_per_host

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    AwsKeyFileManager.DeleteKeyfile(self.region)
    if self.host:
      with self._lock:
        if self.host in self.host_list:
          self.host_list.remove(self.host)
        if self.host not in self.deleted_hosts:
          self.host.Delete()
          self.deleted_hosts.add(self.host)

  def _Create(self):
    """Create a VM instance."""
    placement = []
    if not util.IsRegion(self.zone):
      placement.append('AvailabilityZone=%s' % self.zone)
    if self.use_dedicated_host:
      placement.append('Tenancy=host,HostId=%s' % self.host.id)
      num_hosts = len(self.host_list)
    elif self.placement_group:
      if IsPlacementGroupCompatible(self.machine_type):
        placement.append('GroupName=%s' % self.placement_group.name)
      else:
        logging.warn(
            'VM not placed in Placement Group. VM Type %s not supported',
            self.machine_type)
    placement = ','.join(placement)
    block_device_map = GetBlockDeviceMap(self.machine_type,
                                         self.boot_disk_size,
                                         self.image,
                                         self.region)
    tags = {}
    tags.update(self.vm_metadata)
    tags.update(util.MakeDefaultTags())
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'run-instances',
        '--region=%s' % self.region,
        '--subnet-id=%s' % self.network.subnet.id,
        '--client-token=%s' % self.client_token,
        '--image-id=%s' % self.image,
        '--instance-type=%s' % self.machine_type,
        '--key-name=%s' % AwsKeyFileManager.GetKeyNameForRun(),
        '--tag-specifications=%s' %
        util.FormatTagSpecifications('instance', tags)]
    if FLAGS.aws_efa:
      create_cmd.extend([
          '--network-interfaces',
          ','.join(['%s=%s' % item for item in sorted(_EFA_PARAMS.items())])
      ])
    else:
      create_cmd.append('--associate-public-ip-address')
    if block_device_map:
      create_cmd.append('--block-device-mappings=%s' % block_device_map)
    if placement:
      create_cmd.append('--placement=%s' % placement)
    if FLAGS.aws_credit_specification:
      create_cmd.append('--credit-specification=%s' %
                        FLAGS.aws_credit_specification)
    if self.user_data:
      create_cmd.append('--user-data=%s' % self.user_data)
    if self.capacity_reservation_id:
      create_cmd.append(
          '--capacity-reservation-specification=CapacityReservationTarget='
          '{CapacityReservationId=%s}' % self.capacity_reservation_id)
    if self.use_spot_instance:
      instance_market_options = collections.OrderedDict()
      spot_options = collections.OrderedDict()
      spot_options['SpotInstanceType'] = 'one-time'
      spot_options['InstanceInterruptionBehavior'] = 'terminate'
      if self.spot_price:
        spot_options['MaxPrice'] = str(self.spot_price)
      instance_market_options['MarketType'] = 'spot'
      instance_market_options['SpotOptions'] = spot_options
      create_cmd.append(
          '--instance-market-options=%s' % json.dumps(instance_market_options))
    _, stderr, retcode = vm_util.IssueCommand(create_cmd,
                                              raise_on_failure=False)

    machine_type_prefix = self.machine_type.split('.')[0]
    host_arch = _MACHINE_TYPE_PREFIX_TO_HOST_ARCH.get(machine_type_prefix)
    if host_arch:
      self.host_arch = host_arch

    if self.use_dedicated_host and 'InsufficientCapacityOnHost' in stderr:
      if self.num_vms_per_host:
        raise errors.Resource.CreationError(
            'Failed to create host: %d vms of type %s per host exceeds '
            'memory capacity limits of the host' %
            (self.num_vms_per_host, self.machine_type))
      else:
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
    if 'InsufficientInstanceCapacity' in stderr:
      if self.use_spot_instance:
        self.spot_status_code = 'InsufficientSpotInstanceCapacity'
        self.early_termination = True
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
    if 'SpotMaxPriceTooLow' in stderr:
      self.spot_status_code = 'SpotMaxPriceTooLow'
      self.early_termination = True
      raise errors.Resource.CreationError(stderr)
    if 'InstanceLimitExceeded' in stderr:
      raise errors.Benchmarks.QuotaFailure(stderr)
    if retcode:
      raise errors.Resource.CreationError(
          'Failed to create VM: %s return code: %s' % (retcode, stderr))

  def _Delete(self):
    """Delete a VM instance."""
    if self.id:
      delete_cmd = util.AWS_PREFIX + [
          'ec2',
          'terminate-instances',
          '--region=%s' % self.region,
          '--instance-ids=%s' % self.id]
      vm_util.IssueCommand(delete_cmd, raise_on_failure=False)
    if hasattr(self, 'spot_instance_request_id'):
      cancel_cmd = util.AWS_PREFIX + [
          '--region=%s' % self.region,
          'ec2',
          'cancel-spot-instance-requests',
          '--spot-instance-request-ids=%s' % self.spot_instance_request_id]
      vm_util.IssueCommand(cancel_cmd, raise_on_failure=False)

  @vm_util.Retry(max_retries=5)
  def UpdateInterruptibleVmStatus(self):
    if hasattr(self, 'spot_instance_request_id'):
      describe_cmd = util.AWS_PREFIX + [
          '--region=%s' % self.region,
          'ec2',
          'describe-spot-instance-requests',
          '--spot-instance-request-ids=%s' % self.spot_instance_request_id]
      stdout, _, _ = vm_util.IssueCommand(describe_cmd)
      sir_response = json.loads(stdout)['SpotInstanceRequests']
      self.spot_status_code = sir_response[0]['Status']['Code']
      self.early_termination = (self.spot_status_code in
                                AWS_INITIATED_SPOT_TERMINAL_STATUSES)

  @vm_util.Retry(poll_interval=1, log_errors=False,
                 retryable_exceptions=(AwsTransitionalVmRetryableError,))
  def _Exists(self):
    """Returns whether the VM exists.

    This method waits until the VM is no longer pending.

    Returns:
      Whether the VM exists.

    Raises:
      AwsUnknownStatusError: If an unknown status is returned from AWS.
      AwsTransitionalVmRetryableError: If the VM is pending. This is retried.
    """
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-instances',
        '--region=%s' % self.region,
        '--filter=Name=client-token,Values=%s' % self.client_token]

    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    reservations = response['Reservations']
    assert len(reservations) < 2, 'Too many reservations.'
    if not reservations:
      if not self.create_start_time:
        return False
      logging.info('No reservation returned by describe-instances. This '
                   'sometimes shows up immediately after a successful '
                   'run-instances command. Retrying describe-instances '
                   'command.')
      raise AwsTransitionalVmRetryableError()
    instances = reservations[0]['Instances']
    assert len(instances) == 1, 'Wrong number of instances.'
    status = instances[0]['State']['Name']
    self.id = instances[0]['InstanceId']
    if self.use_spot_instance:
      self.spot_instance_request_id = instances[0]['SpotInstanceRequestId']

    if status not in INSTANCE_KNOWN_STATUSES:
      raise AwsUnknownStatusError('Unknown status %s' % status)
    if status in INSTANCE_TRANSITIONAL_STATUSES:
      logging.info('VM has status %s; retrying describe-instances command.',
                   status)
      raise AwsTransitionalVmRetryableError()
    # In this path run-instances succeeded, a pending instance was created, but
    # not fulfilled so it moved to terminated.
    if (status == TERMINATED and
        instances[0]['StateReason']['Code'] ==
        'Server.InsufficientInstanceCapacity'):
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(
          instances[0]['StateReason']['Message'])
    # In this path run-instances succeeded, a pending instance was created, but
    # instance is shutting down due to internal server error. This is a
    # retryable command for run-instance.
    # Client token needs to be refreshed for idempotency.
    if (status == SHUTTING_DOWN and
        instances[0]['StateReason']['Code'] == 'Server.InternalError'):
      self.client_token = str(uuid.uuid4())
    return status in INSTANCE_EXISTS_STATUSES

  def _GetNvmeBootIndex(self):
    if aws_disk.LocalDriveIsNvme(self.machine_type) and \
       aws_disk.EbsDriveIsNvme(self.machine_type):
      # identify boot drive
      cmd = 'lsblk | grep "part /$" | grep -o "nvme[0-9]*"'
      boot_drive = self.RemoteCommand(cmd, ignore_failure=True)[0].strip()
      if len(boot_drive) > 0:
        # get the boot drive index by dropping the nvme prefix
        boot_idx = int(boot_drive[4:])
        logging.info('found boot drive at nvme index %d', boot_idx)
        return boot_idx
      else:
        # boot drive is not nvme
        return 0

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.

    Raises:
      CreationError: If an NFS disk is listed but the NFS service not created.
    """
    # Instantiate the disk(s) that we want to create.
    disks = []
    nvme_boot_drive_index = self._GetNvmeBootIndex()
    for _ in range(disk_spec.num_striped_disks):
      if disk_spec.disk_type == disk.NFS:
        data_disk = self._GetNfsService().CreateNfsDisk()
      else:
        data_disk = aws_disk.AwsDisk(disk_spec, self.zone, self.machine_type)
      if disk_spec.disk_type == disk.LOCAL:
        device_letter = chr(ord(DRIVE_START_LETTER) + self.local_disk_counter)
        data_disk.AssignDeviceLetter(device_letter, nvme_boot_drive_index)
        # Local disk numbers start at 1 (0 is the system disk).
        data_disk.disk_number = self.local_disk_counter + 1
        self.local_disk_counter += 1
        if self.local_disk_counter > self.max_local_disks:
          raise errors.Error('Not enough local disks.')
      elif disk_spec.disk_type == disk.NFS:
        pass
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
    if self.use_spot_instance:
      util.AddDefaultTags(self.spot_instance_request_id, self.region)

  def InstallCli(self):
    """Installs the AWS cli and credentials on this AWS vm."""
    self.Install('awscli')
    self.Install('aws_credentials')

  def DownloadPreprovisionedData(self, install_path, module_name, filename):
    """Downloads a data file from an AWS S3 bucket with pre-provisioned data.

    Use --aws_preprovisioned_data_bucket to specify the name of the bucket.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
    """
    self.InstallCli()
    # TODO(deitz): Add retry logic.
    self.RemoteCommand(GenerateDownloadPreprovisionedDataCommand(
        install_path, module_name, filename))

  def ShouldDownloadPreprovisionedData(self, module_name, filename):
    """Returns whether or not preprovisioned data is available."""
    self.Install('aws_credentials')
    self.Install('awscli')
    return FLAGS.aws_preprovisioned_data_bucket and self.TryRemoteCommand(
        GenerateStatPreprovisionedDataCommand(module_name, filename))

  def IsInterruptible(self):
    """Returns whether this vm is an interruptible vm (spot vm).

    Returns: True if this vm is an interruptible vm (spot vm).
    """
    return self.use_spot_instance

  def WasInterrupted(self):
    """Returns whether this spot vm was terminated early by AWS.

    Returns: True if this vm was terminated early by AWS.
    """
    return self.early_termination

  def GetVmStatusCode(self):
    """Returns the early termination code if any.

    Returns: Early termination code.
    """
    return self.spot_status_code

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping string property key to value.
    """
    result = super(AwsVirtualMachine, self).GetResourceMetadata()
    result['boot_disk_type'] = self.DEFAULT_ROOT_DISK_TYPE
    result['boot_disk_size'] = self.boot_disk_size or 'default'
    if self.use_dedicated_host:
      result['num_vms_per_host'] = self.num_vms_per_host
    result['efa'] = FLAGS.aws_efa
    if FLAGS.aws_efa:
      result['efa_version'] = FLAGS.aws_efa_version
    return result


class ClearBasedAwsVirtualMachine(AwsVirtualMachine,
                                  linux_virtual_machine.ClearMixin):
  IMAGE_NAME_FILTER = 'clear/images/*/clear-*'
  DEFAULT_USER_NAME = 'clear'


class CoreOsBasedAwsVirtualMachine(AwsVirtualMachine,
                                   linux_virtual_machine.CoreOsMixin):
  IMAGE_NAME_FILTER = 'CoreOS-stable-*-hvm*'
  IMAGE_OWNER = COREOS_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'core'


class Debian9BasedAwsVirtualMachine(AwsVirtualMachine,
                                    linux_virtual_machine.Debian9Mixin):
  IMAGE_NAME_FILTER = 'debian-stretch-*64-*'
  DEFAULT_USER_NAME = 'admin'


class Debian10BasedAwsVirtualMachine(AwsVirtualMachine,
                                     linux_virtual_machine.Debian10Mixin):
  # From https://wiki.debian.org/Cloud/AmazonEC2Image/Buster
  # TODO(pclay): replace with Marketplace AMI when available
  IMAGE_NAME_FILTER = 'debian-10-*64*'
  IMAGE_OWNER = DEBIAN_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'admin'


class Ubuntu1604BasedAwsVirtualMachine(AwsVirtualMachine,
                                       linux_virtual_machine.Ubuntu1604Mixin):
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-xenial-16.04-*64-server-20*'
  IMAGE_OWNER = UBUNTU_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'ubuntu'


class Ubuntu1710BasedAwsVirtualMachine(AwsVirtualMachine,
                                       linux_virtual_machine.Ubuntu1710Mixin):
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-artful-17.10-*64-server-20*'
  IMAGE_OWNER = UBUNTU_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'ubuntu'


class Ubuntu1804BasedAwsVirtualMachine(AwsVirtualMachine,
                                       linux_virtual_machine.Ubuntu1804Mixin):
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-bionic-18.04-*64-server-20*'
  IMAGE_OWNER = UBUNTU_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'ubuntu'


class JujuBasedAwsVirtualMachine(AwsVirtualMachine,
                                 linux_virtual_machine.JujuMixin):
  """Class with configuration for AWS Juju virtual machines."""
  IMAGE_NAME_FILTER = 'ubuntu/images/*/ubuntu-trusty-14.04-*64-server-20*'
  IMAGE_OWNER = UBUNTU_IMAGE_PROJECT
  PYTHON_PIP_PACKAGE_VERSION = '9.0.3'
  DEFAULT_USER_NAME = 'ubuntu'


class AmazonLinux2BasedAwsVirtualMachine(
    AwsVirtualMachine, linux_virtual_machine.AmazonLinux2Mixin):
  """Class with configuration for AWS Amazon Linux 2 virtual machines."""
  IMAGE_NAME_FILTER = 'amzn2-ami-*-*-*'
  IMAGE_OWNER = AMAZON_LINUX_IMAGE_PROJECT


class AmazonLinux1BasedAwsVirtualMachine(
    AwsVirtualMachine, linux_virtual_machine.AmazonLinux1Mixin):
  """Class with configuration for AWS Amazon Linux 1 virtual machines."""
  IMAGE_NAME_FILTER = 'amzn-ami-*-*-*'
  IMAGE_OWNER = AMAZON_LINUX_IMAGE_PROJECT
  # IMAGE_NAME_REGEX tightens up the image filter for Amazon Linux to avoid
  # non-standard Amazon Linux images. This fixes a bug in which we were
  # selecting "amzn-ami-hvm-BAD1.No.NO.DONOTUSE-x86_64-gp2" as the latest image.
  IMAGE_NAME_REGEX = (
      r'^amzn-ami-{virt_type}-\d+\.\d+\.\d+.\d+-{architecture}-{disk_type}$')


class VersionlessRhelBaseAwsVirtualMachine(
    linux_virtual_machine.VersionlessRhelMixin,
    AmazonLinux1BasedAwsVirtualMachine):
  ALTERNATIVE_OS = AmazonLinux1BasedAwsVirtualMachine.OS_TYPE


class Rhel7BasedAwsVirtualMachine(AwsVirtualMachine,
                                  linux_virtual_machine.Rhel7Mixin):
  """Class with configuration for AWS RHEL 7 virtual machines."""
  # Documentation on finding RHEL images:
  # https://access.redhat.com/articles/2962171
  IMAGE_NAME_FILTER = 'RHEL-7*_GA*'
  IMAGE_OWNER = RHEL_IMAGE_PROJECT


class Rhel8BasedAwsVirtualMachine(AwsVirtualMachine,
                                  linux_virtual_machine.Rhel8Mixin):
  """Class with configuration for AWS RHEL 8 virtual machines."""
  # Documentation on finding RHEL images:
  # https://access.redhat.com/articles/2962181
  # All RHEL AMIs are HVM. HVM- blocks HVM_BETA.
  IMAGE_NAME_FILTER = 'RHEL-8*_HVM-*'
  IMAGE_OWNER = RHEL_IMAGE_PROJECT


class CentOs7BasedAwsVirtualMachine(AwsVirtualMachine,
                                    linux_virtual_machine.CentOs7Mixin):
  """Class with configuration for AWS CentOS 7 virtual machines."""
  # Documentation on finding the CentOS 7 image:
  # https://wiki.centos.org/Cloud/AWS#head-cc841c2a7d874025ae24d427776e05c7447024b2
  IMAGE_NAME_FILTER = 'CentOS*Linux*7*ENA*'
  IMAGE_PRODUCT_CODE_FILTER = 'aw0evgkw8e5c1q413zgy5pjce'
  DEFAULT_USER_NAME = 'centos'


class BaseWindowsAwsVirtualMachine(AwsVirtualMachine,
                                   windows_virtual_machine.BaseWindowsMixin):
  """Support for Windows machines on AWS."""
  DEFAULT_USER_NAME = 'Administrator'
  IMAGE_OWNER = WINDOWS_IMAGE_PROJECT

  def __init__(self, vm_spec):
    super(BaseWindowsAwsVirtualMachine, self).__init__(vm_spec)
    self.user_data = ('<powershell>%s</powershell>' %
                      windows_virtual_machine.STARTUP_SCRIPT)

  @vm_util.Retry()
  def _GetDecodedPasswordData(self):
    # Retrieve a base64 encoded, encrypted password for the VM.
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
    super(BaseWindowsAwsVirtualMachine, self)._PostCreate()

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

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the VM.

    Returns:
      dict mapping metadata key to value.
    """
    result = super(BaseWindowsAwsVirtualMachine, self).GetResourceMetadata()
    result['disable_interrupt_moderation'] = self.disable_interrupt_moderation
    return result

  @vm_util.Retry(
      max_retries=10,
      retryable_exceptions=(AwsUnexpectedWindowsAdapterOutputError,
                            errors.VirtualMachine.RemoteCommandError))
  def DisableInterruptModeration(self):
    """Disable the networking feature 'Interrupt Moderation'."""

    # First ensure that the driver supports interrupt moderation
    net_adapters, _ = self.RemoteCommand('Get-NetAdapter')
    if 'Intel(R) 82599 Virtual Function' not in net_adapters:
      raise AwsDriverDoesntSupportFeatureError(
          'Driver not tested with Interrupt Moderation in PKB.')
    aws_int_dis_path = ('HKLM\\SYSTEM\\ControlSet001\\Control\\Class\\'
                        '{4d36e972-e325-11ce-bfc1-08002be10318}\\0011')
    command = 'reg add "%s" /v *InterruptModeration /d 0 /f' % aws_int_dis_path
    self.RemoteCommand(command)
    try:
      self.RemoteCommand('Restart-NetAdapter -Name "Ethernet 2"')
    except IOError:
      # Restarting the network adapter will always fail because
      # the winrm connection used to issue the command will be
      # broken.
      pass
    int_dis_value, _ = self.RemoteCommand(
        'reg query "%s" /v *InterruptModeration' % aws_int_dis_path)
    # The second line should look like:
    #     *InterruptModeration    REG_SZ    0
    registry_query_lines = int_dis_value.splitlines()
    if len(registry_query_lines) < 3:
      raise AwsUnexpectedWindowsAdapterOutputError(
          'registry query failed: %s ' % int_dis_value)
    registry_query_result = registry_query_lines[2].split()
    if len(registry_query_result) < 3:
      raise AwsUnexpectedWindowsAdapterOutputError(
          'unexpected registry query response: %s' % int_dis_value)
    if registry_query_result[2] != '0':
      raise AwsUnexpectedWindowsAdapterOutputError(
          'InterruptModeration failed to disable')


class VersionlessWindowsAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.VersionlessWindowsMixin):
  IMAGE_NAME_FILTER = 'Windows_Server-2012-R2_RTM-English-64Bit-Core-*'


class Windows2012CoreAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine, windows_virtual_machine.Windows2012CoreMixin):
  IMAGE_NAME_FILTER = 'Windows_Server-2012-R2_RTM-English-64Bit-Core-*'


class Windows2016CoreAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine, windows_virtual_machine.Windows2016CoreMixin):
  IMAGE_NAME_FILTER = 'Windows_Server-2016-English-Core-Base-*'


class Windows2019CoreAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine, windows_virtual_machine.Windows2019CoreMixin):
  IMAGE_NAME_FILTER = 'Windows_Server-2019-English-Core-Base-*'


class Windows2012DesktopAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2012DesktopMixin):
  IMAGE_NAME_FILTER = 'Windows_Server-2012-R2_RTM-English-64Bit-Base-*'


class Windows2016DesktopAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2016DesktopMixin):
  IMAGE_NAME_FILTER = 'Windows_Server-2016-English-Full-Base-*'


class Windows2019DesktopAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2019DesktopMixin):
  IMAGE_NAME_FILTER = 'Windows_Server-2019-English-Full-Base-*'


def GenerateDownloadPreprovisionedDataCommand(install_path, module_name,
                                              filename):
  """Returns a string used to download preprovisioned data."""
  return 'aws s3 cp --only-show-errors s3://%s/%s/%s %s' % (
      FLAGS.aws_preprovisioned_data_bucket, module_name, filename,
      posixpath.join(install_path, filename))


def GenerateStatPreprovisionedDataCommand(module_name, filename):
  """Returns a string used to download preprovisioned data."""
  return 'aws s3api head-object --bucket %s --key %s/%s' % (
      FLAGS.aws_preprovisioned_data_bucket, module_name, filename)
