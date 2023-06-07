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

Images: aws ec2 describe-images --owners self amazon
All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""


import base64
import collections
import json
import logging
import posixpath
import re
import threading
import time
import uuid

from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags as pkb_flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
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

HVM = 'hvm'
PV = 'paravirtual'
NON_HVM_PREFIXES = ['m1', 'c1', 't1', 'm2']
DRIVE_START_LETTER = 'b'

# AWS EC2 Instance life cycle:
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-lifecycle.html
RUNNING = 'running'
TERMINATED = 'terminated'
SHUTTING_DOWN = 'shutting-down'
INSTANCE_TRANSITIONAL_STATUSES = frozenset(['pending'])
INSTANCE_EXISTS_STATUSES = (INSTANCE_TRANSITIONAL_STATUSES |
                            frozenset([RUNNING, 'stopping', 'stopped']))
INSTANCE_DELETED_STATUSES = frozenset([SHUTTING_DOWN, TERMINATED])
INSTANCE_KNOWN_STATUSES = (INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES)

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

# From https://wiki.debian.org/Cloud/AmazonEC2Image/Stretch
# Marketplace AMI exists, but not in all regions
DEBIAN_9_IMAGE_PROJECT = ['379101102735']
# From https://wiki.debian.org/Cloud/AmazonEC2Image/Buster
# From https://wiki.debian.org/Cloud/AmazonEC2Image/Bullseye
DEBIAN_IMAGE_PROJECT = ['136693071363']
# Owns AMIs lists here:
# https://wiki.centos.org/Cloud/AWS#Official_CentOS_Linux_:_Public_Images
# Also owns the AMIS listed in
# https://builds.coreos.fedoraproject.org/streams/stable.json
CENTOS_IMAGE_PROJECT = ['125523088429']
# Owns AMIS listed here:
# https://rockylinux.org/cloud-images/
ROCKY_LINUX_IMAGE_PROJECT = ['792107900819']
MARKETPLACE_IMAGE_PROJECT = ['679593333241']  # alias aws-marketplace
# https://access.redhat.com/articles/2962171
RHEL_IMAGE_PROJECT = ['309956199498']
# https://help.ubuntu.com/community/EC2StartersGuide#Official_Ubuntu_Cloud_Guest_Amazon_Machine_Images_.28AMIs.29
UBUNTU_IMAGE_PROJECT = ['099720109477']  # Owned by canonical
UBUNTU_EFA_IMAGE_PROJECT = ['898082745236']

# Processor architectures
ARM = 'arm64'
X86 = 'x86_64'
# Alternative name of X86 favored by some AMI names.
AMD64 = 'amd64'

# Machine type to ARM architecture.
_MACHINE_TYPE_PREFIX_TO_ARM_ARCH = {
    'a1': 'cortex-a72',
    'c6g': 'graviton2',
    'c7g': 'graviton3',
    'g5g': 'graviton2',
    'm6g': 'graviton2',
    'm7g': 'graviton3',
    'r6g': 'graviton2',
    'r7g': 'graviton3',
    't4g': 'graviton2',
    'im4g': 'graviton2',
    'is4ge': 'graviton2',
    'x2g': 'graviton2',
}

# Parameters for use with Elastic Network Interface
_ENI_PARAMS = {
    'InterfaceType': 'interface',
    'DeviceIndex': 0,
    'NetworkCardIndex': 0,
    'Groups': '',
    'SubnetId': ''
}

# Parameters for use with Elastic Fiber Adapter
_EFA_PARAMS = _ENI_PARAMS.copy()
_EFA_PARAMS['InterfaceType'] = 'efa'

# Location of EFA installer
_EFA_URL = ('https://s3-us-west-2.amazonaws.com/aws-efa-installer/'
            'aws-efa-installer-{version}.tar.gz')

FIVE_MINUTE_TIMEOUT = 300


class AwsTransitionalVmRetryableError(Exception):
  """Error for retrying _Exists when an AWS VM is in a transitional state."""


class AwsDriverDoesntSupportFeatureError(Exception):
  """Raised if there is an attempt to set a feature not supported."""


class AwsUnexpectedWindowsAdapterOutputError(Exception):
  """Raised when querying the status of a windows adapter failed."""


class AwsUnknownStatusError(Exception):
  """Error indicating an unknown status was encountered."""


class AwsVmNotCreatedError(Exception):
  """Error indicating that VM does not have a create_start_time."""


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


def GetBlockDeviceMap(vm):
  """Returns the block device map to expose all devices for a given machine.

  Args:
    vm: vm to build the block device map for.
  Returns:
    The json representation of the block device map.
    If self.boot_disk_size and self.image are provided, the block device map
    will include the specification for the root volume.
    If the machine type has non-NVME local disks, the block device map will
    also include the specification for these local ssd(s).
    NVME local disks do not need block device mappings.
    If the disk spec specifies remote disks, the block device map will also
    include specification for the remote disks.

  Raises:
    ValueError: If required parameters are not passed.
  """
  mappings = []
  if vm.boot_disk_size is not None:
    if vm.image is None:
      raise ValueError(
          'Image must be provided if boot_disk_size is specified')
    if vm.region is None:
      raise ValueError(
          'Region must be provided if image is specified')
    root_block_device = GetRootBlockDeviceSpecForImage(vm.image, vm.region)
    root_block_device['Ebs']['VolumeSize'] = vm.boot_disk_size
    # The 'Encrypted' key must be removed or the CLI will complain
    if not FLAGS.aws_vm_hibernate:
      root_block_device['Ebs'].pop('Encrypted')
    else:
      root_block_device['Ebs']['Encrypted'] = True
    mappings.append(root_block_device)

  for spec_index, disk_spec in enumerate(vm.disk_specs):
    if not vm.DiskTypeCreatedOnVMCreation(disk_spec.disk_type):
      continue

    for i in range(disk_spec.num_striped_disks):
      mapping = collections.OrderedDict()
      device_letter = aws_disk.AwsDisk.GenerateDeviceLetter(vm.name)
      device_name_prefix = aws_disk.AwsDisk.GenerateDeviceNamePrefix(
          disk_spec.disk_type)
      device_name = device_name_prefix + device_letter
      mapping['DeviceName'] = device_name
      if disk_spec.disk_type == disk.LOCAL:
        mapping['VirtualName'] = 'ephemeral%s' % i
        if not aws_disk.LocalDriveIsNvme(vm.machine_type):
          # non-nvme local disks just gets its name as device path,
          # logging it now since it does not show up in block-device-mapping
          # of DescribeInstances call.
          vm.LogDeviceByName(device_name, None, device_name)
      else:
        ebs_block = collections.OrderedDict()
        ebs_block['VolumeType'] = disk_spec.disk_type
        ebs_block['VolumeSize'] = disk_spec.disk_size
        ebs_block['DeleteOnTermination'] = True
        if disk_spec.iops and disk_spec.disk_type in [
            aws_disk.IO1, aws_disk.IO2, aws_disk.GP3
        ]:
          ebs_block['Iops'] = disk_spec.iops
        if disk_spec.throughput and disk_spec.disk_type in [aws_disk.GP3]:
          ebs_block['Throughput'] = disk_spec.throughput
        mapping['Ebs'] = ebs_block
      mappings.append(mapping)
      disk_spec_id = BuildDiskSpecId(spec_index, i)
      vm.LogDeviceByDiskSpecId(disk_spec_id, device_name)

  if mappings:
    return json.dumps(mappings)
  return None


def BuildDiskSpecId(spec_index, strip_index):
  """Generates an unique string to represent a disk_spec position."""
  return f'{spec_index}_{strip_index}'


def GetArmArchitecture(machine_type):
  """Returns the specific ARM processor architecture of the VM."""
  # c6g.medium -> c6g, m6gd.large -> m6g, c5n.18xlarge -> c5
  prefix = re.split(r'[dn]?\.', machine_type)[0]
  return _MACHINE_TYPE_PREFIX_TO_ARM_ARCH.get(prefix)


def GetProcessorArchitecture(machine_type):
  """Returns the processor architecture of the VM."""
  if GetArmArchitecture(machine_type):
    return ARM
  else:
    return X86


def GetAlternateProcessorArchitecture(machine_type):
  """Uses amd64 instead of x86_64."""
  arch = GetProcessorArchitecture(machine_type)
  if arch == X86:
    return AMD64
  return arch


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
    super(AwsVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['aws_boot_disk_size'].present:
      config_values['boot_disk_size'] = flag_values.aws_boot_disk_size
    if flag_values['aws_spot_instances'].present:
      config_values['use_spot_instance'] = flag_values.aws_spot_instances
    if flag_values['aws_spot_price'].present:
      config_values['spot_price'] = flag_values.aws_spot_price
    if flag_values['aws_spot_block_duration_minutes'].present:
      config_values['spot_block_duration_minutes'] = int(
          flag_values.aws_spot_block_duration_minutes)

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
        'spot_block_duration_minutes': (option_decoders.IntDecoder, {
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
      formatted_tags = util.FormatTagSpecifications('key-pair',
                                                    util.MakeDefaultTags())
      import_cmd = util.AWS_PREFIX + [
          'ec2', '--region=%s' % region,
          'import-key-pair',
          '--key-name=%s' % cls.GetKeyNameForRun(),
          '--public-key-material=%s' % keyfile,
          '--tag-specifications=%s' % formatted_tags,
      ]
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

  CLOUD = provider_info.AWS

  # IMAGE_SSM_PATTERN IMAGE_NAME_FILTER_PATTERN and IMAGE_NAME_REGEX support
  # the following python format strings:
  # {virt_type}: hvm or pv
  # {architecture}: x86_64 or arm64
  # {alternate_architecture}: amd64 or arm64
  # {disk_type}: gp2

  # EC2 provides AWS Systems Manager parameters for AMIs maintained by AWS
  # If specified, images will not be listed.
  # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/finding-an-ami.html#finding-an-ami-parameter-store
  IMAGE_SSM_PATTERN = None

  # The IMAGE_NAME_FILTER_PATTERN is passed to the AWS CLI describe-images
  # command to filter images by name. This must be set by subclasses, but may
  # be overridden by the aws_image_name_filter flag.
  IMAGE_NAME_FILTER_PATTERN = None

  # The IMAGE_NAME_REGEX can be used to further filter images by name. It
  # applies after the IMAGE_NAME_FILTER_PATTERN above. This regex supports
  # arbitrary Python regular expressions to further narrow down the set of
  # images considered.
  IMAGE_NAME_REGEX = None

  # List of projects that own the AMIs of this OS type. Default to
  # AWS Marketplace official image project.  Note that opt-in regions may have a
  # different image owner than default regions.
  IMAGE_OWNER = MARKETPLACE_IMAGE_PROJECT

  # Some AMIs use a project code to find the latest (in addition to owner, and
  # filter)
  IMAGE_PRODUCT_CODE_FILTER = None

  # CoreOS only distinguishes between stable and testing images in the
  # description
  IMAGE_DESCRIPTION_FILTER = None

  # Whether to include deprecated images in the list.
  ALLOW_DEPRECATED_IMAGE = False

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
    if self.machine_type in aws_disk.NUM_LOCAL_VOLUMES:
      self.max_local_disks = aws_disk.NUM_LOCAL_VOLUMES[self.machine_type]
    if self.boot_startup_script:
      self.user_data = f'file://{self.boot_startup_script}'
    else:
      self.user_data = None
    self.network = aws_network.AwsNetwork.GetNetwork(self)
    self.placement_group = self.network.placement_group
    self.firewall = aws_network.AwsFirewall.GetFirewall()
    self.use_dedicated_host = vm_spec.use_dedicated_host
    self.num_vms_per_host = vm_spec.num_vms_per_host
    self.use_spot_instance = vm_spec.use_spot_instance
    self.spot_price = vm_spec.spot_price
    self.spot_block_duration_minutes = vm_spec.spot_block_duration_minutes
    self.boot_disk_size = vm_spec.boot_disk_size
    self.client_token = str(uuid.uuid4())
    self.host = None
    self.id = None
    self.metadata.update({
        'spot_instance':
            self.use_spot_instance,
        'spot_price':
            self.spot_price,
        'spot_block_duration_minutes':
            self.spot_block_duration_minutes,
        'placement_group_strategy':
            self.placement_group.strategy
            if self.placement_group else placement_group.PLACEMENT_GROUP_NONE,
        'partition_count':
            self.placement_group.partition_count
            if self.placement_group else 0,
        'aws_credit_specification':
            FLAGS.aws_credit_specification
            if FLAGS.aws_credit_specification else 'none'
    })
    self.spot_early_termination = False
    self.spot_status_code = None
    # See:
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/enhanced-networking-os.html
    self._smp_affinity_script = 'smp_affinity.sh'

    arm_arch = GetArmArchitecture(self.machine_type)
    if arm_arch:
      self.host_arch = arm_arch
      self.is_aarch64 = True

    if self.use_dedicated_host and util.IsRegion(self.zone):
      raise ValueError(
          'In order to use dedicated hosts, you must specify an availability '
          'zone, not a region ("zone" was %s).' % self.zone)

    if self.use_dedicated_host and self.use_spot_instance:
      raise ValueError(
          'Tenancy=host is not supported for Spot Instances')
    self.allocation_id = None
    self.association_id = None
    self.aws_tags = {}
    # this maps the deterministic order
    # of this device in the VM's disk_spec to the device name
    # both disk_spec order and device name are assigned by pkb.
    self.device_by_disk_spec_id = {}
    # this is a mapping of known device name to AWS-side identifiers.
    # AWS-side identifiers are assigned by AWS at creation time.
    self.disk_identifiers_by_device = {}

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
    prefix = machine_type.split('.')[0]
    virt_type = PV if prefix in NON_HVM_PREFIXES else HVM
    processor_architecture = GetProcessorArchitecture(machine_type)
    alternate_architecture = GetAlternateProcessorArchitecture(machine_type)
    format_dict = {
        'virt_type': virt_type,
        'disk_type': cls.DEFAULT_ROOT_DISK_TYPE,
        'architecture': processor_architecture,
        'alternate_architecture': alternate_architecture,
    }
    if cls.IMAGE_SSM_PATTERN:
      if FLAGS.aws_image_name_filter:
        raise ValueError(
            '--aws_image_name_filter is not supported for AWS OS Mixins that '
            'use SSM to select AMIs. You can still pass --image.')
      stdout, _, _ = vm_util.IssueCommand(util.AWS_PREFIX + [
          'ssm', 'get-parameters',
          '--region', region,
          '--names', cls.IMAGE_SSM_PATTERN.format(**format_dict)])
      response = json.loads(stdout)
      if response['InvalidParameters']:
        raise AwsImageNotFoundError('Invalid SSM parameters:\n' + stdout)
      parameters = response['Parameters']
      assert len(parameters) == 1
      return parameters[0]['Value']

    # These cannot be REQUIRED_ATTRS, because nesting REQUIRED_ATTRS breaks.
    if not cls.IMAGE_OWNER:
      raise NotImplementedError('AWS OSMixins require IMAGE_OWNER')
    if not cls.IMAGE_NAME_FILTER_PATTERN:
      raise NotImplementedError(
          'AWS OSMixins require IMAGE_NAME_FILTER_PATTERN')

    if FLAGS.aws_image_name_filter:
      cls.IMAGE_NAME_FILTER_PATTERN = FLAGS.aws_image_name_filter

    if FLAGS.aws_image_name_regex:
      cls.IMAGE_NAME_REGEX = FLAGS.aws_image_name_regex

    image_name_filter = cls.IMAGE_NAME_FILTER_PATTERN.format(**format_dict)

    describe_cmd = util.AWS_PREFIX + [
        '--region=%s' % region,
        'ec2',
        'describe-images',
        '--query', ('Images[*].{Name:Name,ImageId:ImageId,'
                    'CreationDate:CreationDate}'),
        '--filters',
        'Name=name,Values=' + image_name_filter,
        'Name=block-device-mapping.volume-type,Values=%s' %
        cls.DEFAULT_ROOT_DISK_TYPE,
        'Name=virtualization-type,Values=%s' % virt_type,
        'Name=architecture,Values=%s' % processor_architecture]
    if cls.IMAGE_PRODUCT_CODE_FILTER:
      describe_cmd.extend(['Name=product-code,Values=%s' %
                           cls.IMAGE_PRODUCT_CODE_FILTER])
    if cls.IMAGE_DESCRIPTION_FILTER:
      describe_cmd.extend(['Name=description,Values=%s' %
                           cls.IMAGE_DESCRIPTION_FILTER])
    if cls.ALLOW_DEPRECATED_IMAGE:
      describe_cmd.append('--include-deprecated')
    else:
      # This is the default, but be explicit.
      describe_cmd.append('--no-include-deprecated')
    describe_cmd.extend(['--owners'] + cls.IMAGE_OWNER)
    stdout, _ = util.IssueRetryableCommand(describe_cmd)

    if not stdout:
      raise AwsImageNotFoundError('aws describe-images did not produce valid '
                                  'output.')

    if cls.IMAGE_NAME_REGEX:
      # Further filter images by the IMAGE_NAME_REGEX filter.
      image_name_regex = cls.IMAGE_NAME_REGEX.format(**format_dict)
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

    return max(images, key=lambda image: image['CreationDate'])['ImageId']

  @vm_util.Retry(max_retries=2)
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
    self.internal_ip = instance['PrivateIpAddress']
    for network_interface in instance.get('NetworkInterfaces', []):
      # Ensures primary NIC is first
      if network_interface['Attachment']['DeviceIndex'] == 0:
        self.internal_ips = ([network_interface['PrivateIpAddress']] +
                             self.internal_ips)
      else:
        self.internal_ips.append(network_interface['PrivateIpAddress'])
    if util.IsRegion(self.zone):
      self.zone = str(instance['Placement']['AvailabilityZone'])

    assert self.group_id == instance['SecurityGroups'][0]['GroupId'], (
        self.group_id, instance['SecurityGroups'][0]['GroupId'])
    if FLAGS.aws_efa:
      self._ConfigureEfa(instance)
    elif aws_network.AWS_ENI_COUNT.value > 1:
      self._ConfigureElasticIp(instance)
    elif 'PublicIpAddress' in instance:
      self.ip_address = instance['PublicIpAddress']
    else:
      raise errors.Resource.RetryableCreationError('Public IP not ready.')
    if 'BlockDeviceMappings' in instance:
      block_devices = instance['BlockDeviceMappings']
      for device in block_devices:
        if 'Ebs' not in device and 'VolumeId' not in device['Ebs']:
          continue
        volume_id = device['Ebs']['VolumeId']
        device_name = device['DeviceName']
        self.LogDeviceByName(device_name, volume_id, device_name)
        util.AddDefaultTags(volume_id, self.region)

  def _ConfigureEfa(self, instance):
    """Configuare EFA and associate Elastic IP.

    Args:
      instance: dict which contains instance info.
    """
    if FLAGS.aws_efa_count > 1:
      self._ConfigureElasticIp(instance)
    else:
      self.ip_address = instance['PublicIpAddress']
    if FLAGS.aws_efa_version:
      # Download EFA then call InstallEfa method so that subclass can override
      self.InstallPackages('curl')
      url = _EFA_URL.format(version=FLAGS.aws_efa_version)
      tarfile = posixpath.basename(url)
      self.RemoteCommand(f'curl -O {url}; tar -xzf {tarfile}')
      self._InstallEfa()
      # Run test program to confirm EFA working
      self.RemoteCommand('cd aws-efa-installer; '
                         'PATH=${PATH}:/opt/amazon/efa/bin ./efa_test.sh')

  def _ConfigureElasticIp(self, instance):
    """Create and associate Elastic IP.

    Args:
      instance: dict which contains instance info.
    """
    network_interface_id = None
    for network_interface in instance['NetworkInterfaces']:
      # The primary network interface (eth0) for the instance.
      if network_interface['Attachment']['DeviceIndex'] == 0:
        network_interface_id = network_interface['NetworkInterfaceId']
        break
    assert network_interface_id is not None

    stdout, _, _ = vm_util.IssueCommand(util.AWS_PREFIX +
                                        ['ec2', 'allocate-address',
                                         f'--region={self.region}',
                                         '--domain=vpc'])
    response = json.loads(stdout)
    self.ip_address = response['PublicIp']
    self.allocation_id = response['AllocationId']

    util.AddDefaultTags(self.allocation_id, self.region)

    stdout, _, _ = vm_util.IssueCommand(
        util.AWS_PREFIX + ['ec2', 'associate-address',
                           f'--region={self.region}',
                           f'--allocation-id={self.allocation_id}',
                           f'--network-interface-id={network_interface_id}'])
    response = json.loads(stdout)
    self.association_id = response['AssociationId']

  def _InstallEfa(self):
    """Installs AWS EFA packages.

    See https://aws.amazon.com/hpc/efa/
    """
    if not self.TryRemoteCommand('ulimit -l | grep unlimited'):
      self.RemoteCommand(f'echo "{self.user_name} - memlock unlimited" | '
                         'sudo tee -a /etc/security/limits.conf')
    self.RemoteCommand('cd aws-efa-installer; sudo ./efa_installer.sh -y')
    if not self.TryRemoteCommand('ulimit -l | grep unlimited'):
      # efa_installer.sh should reboot enabling this change, reboot if necessary
      self.Reboot()

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
      placement.append('GroupName=%s' % self.placement_group.name)

    placement = ','.join(placement)
    block_device_map = GetBlockDeviceMap(self)
    if not self.aws_tags:
      # Set tags for the AWS VM. If we are retrying the create, we have to use
      # the same tags from the previous call.
      self.aws_tags.update(self.vm_metadata)
      self.aws_tags.update(util.MakeDefaultTags())
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'run-instances',
        '--region=%s' % self.region,
        '--client-token=%s' % self.client_token,
        '--image-id=%s' % self.image,
        '--instance-type=%s' % self.machine_type,
        '--key-name=%s' % AwsKeyFileManager.GetKeyNameForRun(),
        '--tag-specifications=%s' %
        util.FormatTagSpecifications('instance', self.aws_tags)]

    if FLAGS.aws_vm_hibernate:
      create_cmd.extend([
          '--hibernation-options=Configured=true',
      ])

    # query fails on hpc6a.48xlarge which already disables smt.
    if FLAGS.disable_smt and self.machine_type != 'hpc6a.48xlarge':
      query_cmd = util.AWS_PREFIX + [
          'ec2',
          'describe-instance-types',
          '--instance-types',
          self.machine_type,
          '--query',
          'InstanceTypes[0].VCpuInfo.DefaultCores'
      ]
      stdout, _, retcode = vm_util.IssueCommand(query_cmd)
      cores = int(json.loads(stdout))
      create_cmd.append(f'--cpu-options=CoreCount={cores},ThreadsPerCore=1')
    if FLAGS.aws_efa:
      efas = ['--network-interfaces']
      for device_index in range(FLAGS.aws_efa_count):
        # You can assign up to one EFA per network card.
        efa_params = _EFA_PARAMS.copy()
        efa_params.update({
            'NetworkCardIndex': device_index,
            'DeviceIndex': device_index,
            'Groups': self.group_id,
            'SubnetId': self.network.subnet.id
        })
        if FLAGS.aws_efa_count == 1 and self.assign_external_ip:
          efa_params['AssociatePublicIpAddress'] = True
        efas.append(','.join(f'{key}={value}' for key, value in
                             sorted(efa_params.items())))
      create_cmd.extend(efas)
    elif aws_network.AWS_ENI_COUNT.value > 1:
      enis = ['--network-interfaces']
      enis_per_network_card = (
          aws_network.AWS_ENI_COUNT.value //
          aws_network.AWS_NETWORK_CARD_COUNT.value
          )
      for device_index in range(aws_network.AWS_ENI_COUNT.value):
        eni_params = _ENI_PARAMS.copy()
        eni_params.update({
            'NetworkCardIndex': device_index // enis_per_network_card,
            'DeviceIndex': device_index,
            'Groups': self.group_id,
            'SubnetId': self.network.subnets[device_index].id,
        })
        enis.append(','.join(f'{key}={value}' for key, value in
                             sorted(eni_params.items())))
      create_cmd.extend(enis)
    else:
      if self.assign_external_ip:
        create_cmd.append('--associate-public-ip-address')
      create_cmd.append(f'--subnet-id={self.network.subnet.id}')
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
      if self.spot_block_duration_minutes:
        spot_options['BlockDurationMinutes'] = self.spot_block_duration_minutes
      instance_market_options['MarketType'] = 'spot'
      instance_market_options['SpotOptions'] = spot_options
      create_cmd.append(
          '--instance-market-options=%s' % json.dumps(instance_market_options))
    _, stderr, retcode = vm_util.IssueCommand(create_cmd,
                                              raise_on_failure=False)

    if self.use_dedicated_host and 'InsufficientCapacityOnHost' in stderr:
      if self.num_vms_per_host:
        raise errors.Resource.CreationError(
            'Failed to create host: %d vms of type %s per host exceeds '
            'memory capacity limits of the host' %
            (self.num_vms_per_host, self.machine_type))
      else:
        logging.warning(
            'Creation failed due to insufficient host capacity. A new host '
            'will be created and instance creation will be retried.'
        )
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
        self.spot_early_termination = True
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(stderr)
    if 'SpotMaxPriceTooLow' in stderr:
      self.spot_status_code = 'SpotMaxPriceTooLow'
      self.spot_early_termination = True
      raise errors.Resource.CreationError(stderr)
    if 'InstanceLimitExceeded' in stderr or 'VcpuLimitExceeded' in stderr:
      raise errors.Benchmarks.QuotaFailure(stderr)
    if 'RequestLimitExceeded' in stderr:
      if FLAGS.retry_on_rate_limited:
        raise errors.Resource.RetryableCreationError(stderr)
      else:
        raise errors.Benchmarks.QuotaFailure(stderr)
    if 'InternalError' in stderr:
      raise errors.Resource.CreationInternalError(stderr)

    # When launching more than 1 VM into the same placement group, there is an
    # occasional error that the placement group has already been used in a
    # separate zone. Retrying fixes this error.
    if 'InvalidPlacementGroup.InUse' in stderr:
      raise errors.Resource.RetryableCreationError(stderr)
    if 'Unsupported' in stderr:
      raise errors.Benchmarks.UnsupportedConfigError(stderr)
    if retcode:
      raise errors.Resource.CreationError(
          'Failed to create VM: %s return code: %s' % (retcode, stderr))
    if not self.create_return_time:
      self.create_return_time = time.time()

  @vm_util.Retry(
      poll_interval=0.5,
      log_errors=True,
      retryable_exceptions=(AwsTransitionalVmRetryableError,))
  def _WaitForStoppedStatus(self):
    """Returns the status of the VM.

    Returns:
      Whether the VM is suspended i.e. in a stopped status. If not, raises an
      error

    Raises:
      AwsUnknownStatusError: If an unknown status is returned from AWS.
      AwsTransitionalVmRetryableError: If the VM is pending. This is retried.
    """
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-instance-status',
        '--region=%s' % self.region,
        '--instance-ids=%s' % self.id,
        '--include-all-instances',
    ]

    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    status = response['InstanceStatuses'][0]['InstanceState']['Name']
    if status.lower() != 'stopped':
      logging.info('VM has status %s.', status)

      raise AwsTransitionalVmRetryableError()

  def _BeforeSuspend(self):
    """Prepares the instance for suspend by having the VM sleep for a given duration.

    This ensures the VM is ready for hibernation
    """
    # Add a timer that waits for a given duration after vm instance is
    # created before calling suspend on the vm to ensure that the vm is
    # ready for hibernation in aws.
    time.sleep(600)

  def _PostSuspend(self):
    self._WaitForStoppedStatus()

  def _Suspend(self):
    """Suspends a VM instance."""
    suspend_cmd = util.AWS_PREFIX + [
        'ec2',
        'stop-instances',
        '--region=%s' % self.region,
        '--instance-ids=%s' % self.id,
        '--hibernate',
    ]
    try:
      vm_util.IssueCommand(suspend_cmd)
    except:
      raise errors.Benchmarks.KnownIntermittentError(
          'Instance is still not ready to hibernate')

    self._PostSuspend()

  @vm_util.Retry(
      poll_interval=0.5,
      retryable_exceptions=(AwsTransitionalVmRetryableError,))
  def _WaitForNewIP(self):
    """Checks for a new IP address, waiting if the VM is still pending.

    Raises:
      AwsTransitionalVmRetryableError: If VM is pending. This is retried.
    """
    status_cmd = util.AWS_PREFIX + [
        'ec2', 'describe-instances', f'--region={self.region}',
        f'--instance-ids={self.id}'
    ]
    stdout, _, _ = vm_util.IssueCommand(status_cmd)
    response = json.loads(stdout)
    instance = response['Reservations'][0]['Instances'][0]
    if 'PublicIpAddress' in instance:
      self.ip_address = instance['PublicIpAddress']
    else:
      logging.info('VM is pending.')
      raise AwsTransitionalVmRetryableError()

  def _PostResume(self):
    self._WaitForNewIP()

  def _Resume(self):
    """Resumes a VM instance."""
    resume_cmd = util.AWS_PREFIX + [
        'ec2',
        'start-instances',
        '--region=%s' % self.region,
        '--instance-ids=%s' % self.id,
    ]
    vm_util.IssueCommand(resume_cmd)
    self._PostResume()

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

    if FLAGS.aws_efa or aws_network.AWS_ENI_COUNT.value > 1:
      if self.association_id:
        vm_util.IssueCommand(util.AWS_PREFIX +
                             ['ec2', 'disassociate-address',
                              f'--region={self.region}',
                              f'--association-id={self.association_id}'])

      if self.allocation_id:
        vm_util.IssueCommand(util.AWS_PREFIX +
                             ['ec2', 'release-address',
                              f'--region={self.region}',
                              f'--allocation-id={self.allocation_id}'])

  #  _Start or _Stop not yet implemented for AWS
  def _Start(self):
    """Starts the VM."""
    if not self.id:
      raise errors.Benchmarks.RunError(
          'Expected VM id to be non-null. Please make sure the VM exists.')
    start_cmd = util.AWS_PREFIX + [
        'ec2', 'start-instances',
        f'--region={self.region}',
        f'--instance-ids={self.id}'
    ]
    vm_util.IssueCommand(start_cmd)

  def _PostStart(self):
    self._WaitForNewIP()

  def _Stop(self):
    """Stops the VM."""
    if not self.id:
      raise errors.Benchmarks.RunError(
          'Expected VM id to be non-null. Please make sure the VM exists.')
    stop_cmd = util.AWS_PREFIX + [
        'ec2', 'stop-instances',
        f'--region={self.region}',
        f'--instance-ids={self.id}'
    ]
    vm_util.IssueCommand(stop_cmd)

  def _PostStop(self):
    self._WaitForStoppedStatus()

  def _UpdateInterruptibleVmStatusThroughApi(self):
    if hasattr(self, 'spot_instance_request_id'):
      describe_cmd = util.AWS_PREFIX + [
          '--region=%s' % self.region,
          'ec2',
          'describe-spot-instance-requests',
          '--spot-instance-request-ids=%s' % self.spot_instance_request_id]
      stdout, _, _ = vm_util.IssueCommand(describe_cmd)
      sir_response = json.loads(stdout)['SpotInstanceRequests']
      self.spot_status_code = sir_response[0]['Status']['Code']
      self.spot_early_termination = (
          self.spot_status_code in AWS_INITIATED_SPOT_TERMINAL_STATUSES)

  @vm_util.Retry(poll_interval=1,
                 log_errors=False,
                 retryable_exceptions=(AwsTransitionalVmRetryableError,
                                       LookupError))
  def _Exists(self):
    """Returns whether the VM exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-instances',
        '--region=%s' % self.region,
        '--filter=Name=client-token,Values=%s' % self.client_token]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    reservations = response['Reservations']
    if not reservations or len(reservations) != 1:
      logging.info('describe-instances did not return exactly one reservation. '
                   'This sometimes shows up immediately after a successful '
                   'run-instances command. Retrying describe-instances '
                   'command.')
      raise AwsTransitionalVmRetryableError()
    instances = reservations[0]['Instances']
    if instances:
      status = instances[0]['State']['Name']
      # In this path run-instances succeeded, a pending instance was created,
      # but not fulfilled so it moved to terminated.
      if (status == TERMINATED and
          instances[0]['StateReason']['Code'] ==
          'Server.InsufficientInstanceCapacity'):
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(
            instances[0]['StateReason']['Message'])
      # In this path run-instances succeeded, a pending instance was created,
      # but instance is shutting down due to internal server error. This is a
      # retryable command for run-instance.
      # Client token needs to be refreshed for idempotency.
      elif (status == SHUTTING_DOWN and
            instances[0]['StateReason']['Code'] == 'Server.InternalError'):
        self.client_token = str(uuid.uuid4())
      # When measuring time-to-delete, we want Exists() to retry until the
      # VM is in the TERMINATED state. Otherwise, it is fine to say that
      # the VM no longer exists when it is SHUTTING_DOWN.
      elif pkb_flags.MEASURE_DELETE.value and status == SHUTTING_DOWN:
        logging.info('VM has status %s, retrying describe-instances command',
                     status)
        raise AwsTransitionalVmRetryableError()
      return status in INSTANCE_EXISTS_STATUSES
    else:
      return False

  @vm_util.Retry(
      poll_interval=1,
      log_errors=False,
      retryable_exceptions=(AwsUnknownStatusError,
                            AwsTransitionalVmRetryableError,))
  def _WaitUntilRunning(self):
    """Waits until the VM is running.

    This method waits until the VM is no longer pending.

    Raises:
      AwsUnknownStatusError: If an unknown status is returned from AWS.
      AwsTransitionalVmRetryableError: If the VM is pending. This is retried.
      AwsVmNotCreatedError: If the VM does not have a create_start_time by the
        time it reaches this phase of provisioning.
    """
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-instances',
        '--region=%s' % self.region,
        '--filter=Name=client-token,Values=%s' % self.client_token]

    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    reservations = response['Reservations']
    if not reservations or len(reservations) != 1:
      if not self.create_start_time:
        logging.info('VM does not have a create_start_time; '
                     'provisioning failed.')
        raise AwsVmNotCreatedError()
      logging.info('describe-instances did not return exactly one reservation. '
                   'This sometimes shows up immediately after a successful '
                   'run-instances command. Retrying describe-instances '
                   'command.')
      raise AwsTransitionalVmRetryableError()
    instances = reservations[0]['Instances']
    if not instances or len(instances) != 1:
      logging.info('describe-instances did not return exactly one instance. '
                   'Retrying describe-instances command.')
      raise AwsTransitionalVmRetryableError()
    status = instances[0]['State']['Name']
    self.id = instances[0]['InstanceId']
    if self.use_spot_instance:
      self.spot_instance_request_id = instances[0]['SpotInstanceRequestId']

    if status in INSTANCE_TRANSITIONAL_STATUSES:
      logging.info('VM has status %s; retrying describe-instances command.',
                   status)
      raise AwsTransitionalVmRetryableError()
    # In this path run-instances succeeded, a pending instance was created, but
    # not fulfilled so it moved to terminated.
    elif (status == TERMINATED and
          instances[0]['StateReason']['Code'] ==
          'Server.InsufficientInstanceCapacity'):
      raise errors.Benchmarks.InsufficientCapacityCloudFailure(
          instances[0]['StateReason']['Message'])
    # In this path run-instances succeeded, a pending instance was created, but
    # instance is shutting down due to internal server error. This is a
    # retryable command for run-instance.
    # Client token needs to be refreshed for idempotency.
    elif (status == SHUTTING_DOWN and
          instances[0]['StateReason']['Code'] == 'Server.InternalError'):
      self.client_token = str(uuid.uuid4())
    # Set the running time
    elif status == RUNNING:
      if not self.is_running_time:
        self.is_running_time = time.time()
    # TODO(user): Adjust this function to properly handle ALL potential
    #  VM statuses. In the meantime, just retry if the VM is not yet running.
    else:
      raise AwsUnknownStatusError(f'Unknown status: {status}; '
                                  'retrying describe-instances command')

  def _GetNvmeBootIndex(self):
    if (aws_disk.LocalDriveIsNvme(self.machine_type) and
        aws_disk.EbsDriveIsNvme(self.machine_type)):
      # identify boot drive
      # If this command ever fails consider 'findmnt -nM / -o source'
      cmd = ('realpath /dev/disk/by-label/cloudimg-rootfs '
             '| grep --only-matching "nvme[0-9]*"')
      boot_drive = self.RemoteCommand(cmd, ignore_failure=True)[0].strip()
      if boot_drive:
        # get the boot drive index by dropping the nvme prefix
        boot_idx = int(boot_drive[4:])
        logging.info('found boot drive at nvme index %d', boot_idx)
        return boot_idx
      else:
        logging.warning('Failed to identify NVME boot drive index. Assuming 0.')
        return 0

  def CreateScratchDisk(self, spec_index, disk_spec):
    """Create a VM's scratch disk.

    Args:
      spec_index: Deterministic order of this disk_spec in the VM's list of
        disk_specs.
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.

    Raises:
      CreationError: If an NFS disk is listed but the NFS service not created.
    """
    # Instantiate the disk(s) that we want to create.
    disks = []
    nvme_boot_drive_index = self._GetNvmeBootIndex()
    for i in range(disk_spec.num_striped_disks):
      if disk_spec.disk_type == disk.NFS:
        data_disk = self._GetNfsService().CreateNfsDisk()
      else:
        disk_spec_id = BuildDiskSpecId(spec_index, i)
        data_disk = aws_disk.AwsDisk(
            disk_spec, self.zone, self.machine_type, disk_spec_id)
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

    scratch_disk = self._CreateScratchDiskFromDisks(disk_spec, disks)
    # here, all disks are created (either at vm creation or in line above).
    # But we don't have all the raw device paths,
    # which are necessary for preparing the scratch disk.
    nvme_devices = self.GetNVMEDeviceInfo()
    self.PopulateNVMEDevicePath(scratch_disk, nvme_devices)
    self.UpdateDevicePath(scratch_disk)
    self._PrepareScratchDisk(scratch_disk, disk_spec)

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
            f'{model_number} NVME devices is not supported.')

    # because local devices are assigned in a round robin manner,
    # some local devices might already have been assigned (previous spec_index).
    # remove local devices that have been already been assigned.
    for _, aws_id in self.disk_identifiers_by_device.items():
      if aws_id.path in local_devices:
        local_devices.remove(aws_id.path)

    if not local_devices and not ebs_devices:
      return

    disks = scratch_disk.disks if scratch_disk.is_striped else [scratch_disk]
    for d in disks:
      if d.disk_type == disk.NFS:
        continue
      elif d.disk_type == disk.LOCAL:
        if aws_disk.LocalDriveIsNvme(self.machine_type):
          # assign in a round robin manner. Since NVME local disks
          # are created ignoring all pkb naming conventions and assigned
          # random names on the fly.
          disk_name = self.GetDeviceByDiskSpecId(d.disk_spec_id)
          self.LogDeviceByName(disk_name, None, local_devices.pop())
      elif aws_disk.EbsDriveIsNvme(self.machine_type):
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
      d.device_path = self.GetPathByDevice(device_name)

  def LogDeviceByDiskSpecId(self, disk_spec_id, device_name):
    """Records the deterministic disk_spec order to device name for lookup."""
    self.device_by_disk_spec_id[disk_spec_id] = device_name

  def GetDeviceByDiskSpecId(self, disk_spec_id):
    """Gets the logged device name by disk_spec_id."""
    assert disk_spec_id is not None
    return self.device_by_disk_spec_id.get(disk_spec_id, None)

  def LogDeviceByName(self, device_name, volume_id, path=None):
    """Records disk identifiers to device name for lookup."""
    aws_ids = aws_disk.AWSDiskIdentifiers(volume_id=volume_id, path=path)
    self.disk_identifiers_by_device[device_name] = aws_ids

  def GetVolumeIdByDevice(self, disk_name):
    """Gets the logged volume id by disk_name."""
    assert disk_name is not None
    aws_ids = self.disk_identifiers_by_device.get(disk_name, None)
    assert aws_ids is not None
    return aws_ids.volume_id

  def GetPathByDevice(self, disk_name):
    """Gets the logged device path."""
    assert disk_name is not None
    aws_ids = self.disk_identifiers_by_device.get(disk_name, None)
    assert aws_ids is not None
    return aws_ids.path

  def UpdateDeviceByName(self, device_name, volume_id=None, path=None):
    """Gets the volume id by disk_name."""
    assert device_name is not None
    aws_ids = self.disk_identifiers_by_device.get(device_name, None)
    assert aws_ids is not None
    if aws_ids.volume_id and volume_id:
      assert aws_ids.volume_id == volume_id
    # not asserting path similarity because we need to overwrite the path of
    # ebs nvme devices (set to /dev/xvdb? on creation and re-named to nvme?n?)
    self.LogDeviceByName(device_name, volume_id, path)

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM."""
    util.AddTags(self.id, self.region, **kwargs)
    if self.use_spot_instance:
      util.AddDefaultTags(self.spot_instance_request_id, self.region)

  def InstallCli(self):
    """Installs the AWS cli and credentials on this AWS vm."""
    self.Install('awscli')
    self.Install('aws_credentials')

  def DownloadPreprovisionedData(self, install_path, module_name, filename,
                                 timeout=FIVE_MINUTE_TIMEOUT):
    """Downloads a data file from an AWS S3 bucket with pre-provisioned data.

    Use --aws_preprovisioned_data_bucket to specify the name of the bucket.

    Args:
      install_path: The install path on this VM.
      module_name: Name of the module associated with this data file.
      filename: The name of the file that was downloaded.
      timeout: Timeout value for downloading preprovisionedData, Five minutes
      by default.
    """
    self.InstallCli()
    # TODO(deitz): Add retry logic.
    self.RobustRemoteCommand(GenerateDownloadPreprovisionedDataCommand(
        install_path, module_name, filename), timeout=timeout)

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
    return self.spot_early_termination

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
    result['boot_disk_size'] = self.boot_disk_size
    if self.use_dedicated_host:
      result['num_vms_per_host'] = self.num_vms_per_host
    result['nic_count'] = aws_network.AWS_ENI_COUNT.value
    result['efa'] = FLAGS.aws_efa
    if FLAGS.aws_efa:
      result['efa_version'] = FLAGS.aws_efa_version
      result['efa_count'] = FLAGS.aws_efa_count
    result['preemptible'] = self.use_spot_instance
    return result

  def DiskTypeCreatedOnVMCreation(self, disk_type):
    """Returns whether the data disk has been created during VM creation."""
    return disk_type in aws_disk.AWS_REMOTE_DISK_TYPES + [disk.LOCAL]

  def DiskCreatedOnVMCreation(self, data_disk):
    """Returns whether the disk type has been created during VM creation."""
    return self.DiskTypeCreatedOnVMCreation(data_disk.disk_type)


class ClearBasedAwsVirtualMachine(AwsVirtualMachine,
                                  linux_virtual_machine.ClearMixin):
  IMAGE_NAME_FILTER_PATTERN = 'clear/images/*/clear-*'
  DEFAULT_USER_NAME = 'clear'


class CoreOsBasedAwsVirtualMachine(AwsVirtualMachine,
                                   linux_virtual_machine.CoreOsMixin):
  IMAGE_NAME_FILTER_PATTERN = 'fedora-coreos-*'
  # CoreOS only distinguishes between stable and testing in the description
  IMAGE_DESCRIPTION_FILTER = 'Fedora CoreOS stable *'
  IMAGE_OWNER = CENTOS_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'core'


class Debian9BasedAwsVirtualMachine(AwsVirtualMachine,
                                    linux_virtual_machine.Debian9Mixin):
  # From https://wiki.debian.org/Cloud/AmazonEC2Image/Stretch
  IMAGE_NAME_FILTER_PATTERN = 'debian-stretch-{alternate_architecture}-*'
  IMAGE_OWNER = DEBIAN_9_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'admin'

  def _BeforeSuspend(self):
    """Prepares the aws vm for hibernation."""
    raise NotImplementedError()


class Debian10BasedAwsVirtualMachine(AwsVirtualMachine,
                                     linux_virtual_machine.Debian10Mixin):
  # From https://wiki.debian.org/Cloud/AmazonEC2Image/Buster
  IMAGE_NAME_FILTER_PATTERN = 'debian-10-{alternate_architecture}-*'
  IMAGE_OWNER = DEBIAN_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'admin'


class Debian10BackportsBasedAwsVirtualMachine(
    Debian10BasedAwsVirtualMachine,
    linux_virtual_machine.Debian10BackportsMixin):
  IMAGE_NAME_FILTER_PATTERN = 'debian-10-backports-{alternate_architecture}-*'


class Debian11BasedAwsVirtualMachine(AwsVirtualMachine,
                                     linux_virtual_machine.Debian11Mixin):
  # From https://wiki.debian.org/Cloud/AmazonEC2Image/Bullseye
  IMAGE_NAME_FILTER_PATTERN = 'debian-11-{alternate_architecture}-*'
  IMAGE_OWNER = DEBIAN_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'admin'


class Debian11BackportsBasedAwsVirtualMachine(
    Debian11BasedAwsVirtualMachine,
    linux_virtual_machine.Debian11BackportsMixin):
  IMAGE_NAME_FILTER_PATTERN = 'debian-11-backports-{alternate_architecture}-*'


class UbuntuBasedAwsVirtualMachine(AwsVirtualMachine):
  IMAGE_OWNER = UBUNTU_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'ubuntu'


class Ubuntu1604BasedAwsVirtualMachine(UbuntuBasedAwsVirtualMachine,
                                       linux_virtual_machine.Ubuntu1604Mixin):
  IMAGE_NAME_FILTER_PATTERN = 'ubuntu/images/*/ubuntu-xenial-16.04-{alternate_architecture}-server-20*'

  def _InstallEfa(self):
    super(Ubuntu1604BasedAwsVirtualMachine, self)._InstallEfa()
    self.Reboot()


class Ubuntu1804BasedAwsVirtualMachine(UbuntuBasedAwsVirtualMachine,
                                       linux_virtual_machine.Ubuntu1804Mixin):
  IMAGE_NAME_FILTER_PATTERN = 'ubuntu/images/*/ubuntu-bionic-18.04-{alternate_architecture}-server-20*'


class Ubuntu1804EfaBasedAwsVirtualMachine(
    UbuntuBasedAwsVirtualMachine, linux_virtual_machine.Ubuntu1804EfaMixin):
  IMAGE_OWNER = UBUNTU_EFA_IMAGE_PROJECT
  IMAGE_NAME_FILTER_PATTERN = 'Deep Learning AMI GPU CUDA * (Ubuntu 18.04) *'


class Ubuntu2004BasedAwsVirtualMachine(UbuntuBasedAwsVirtualMachine,
                                       linux_virtual_machine.Ubuntu2004Mixin):
  IMAGE_NAME_FILTER_PATTERN = 'ubuntu/images/*/ubuntu-focal-20.04-{alternate_architecture}-server-20*'


class Ubuntu2004EfaBasedAwsVirtualMachine(
    UbuntuBasedAwsVirtualMachine, linux_virtual_machine.Ubuntu2004EfaMixin):
  IMAGE_OWNER = UBUNTU_EFA_IMAGE_PROJECT
  IMAGE_NAME_FILTER_PATTERN = 'Deep Learning AMI GPU CUDA * (Ubuntu 20.04) *'


class Ubuntu2204BasedAwsVirtualMachine(UbuntuBasedAwsVirtualMachine,
                                       linux_virtual_machine.Ubuntu2204Mixin):
  IMAGE_NAME_FILTER_PATTERN = 'ubuntu/images/*/ubuntu-jammy-22.04-{alternate_architecture}-server-20*'


class JujuBasedAwsVirtualMachine(UbuntuBasedAwsVirtualMachine,
                                 linux_virtual_machine.JujuMixin):
  """Class with configuration for AWS Juju virtual machines."""
  IMAGE_NAME_FILTER_PATTERN = 'ubuntu/images/*/ubuntu-trusty-14.04-{alternate_architecture}-server-20*'


class AmazonLinux2BasedAwsVirtualMachine(
    AwsVirtualMachine, linux_virtual_machine.AmazonLinux2Mixin):
  """Class with configuration for AWS Amazon Linux 2 virtual machines."""
  IMAGE_SSM_PATTERN = '/aws/service/ami-amazon-linux-latest/amzn2-ami-{virt_type}-{architecture}-{disk_type}'


class AmazonNeuronBasedAwsVirtualMachine(AmazonLinux2BasedAwsVirtualMachine,
                                         linux_virtual_machine.AmazonNeuronMixin
                                        ):
  """Class with configuration for AWS Amazon Neuron virtual machines."""
  pass


class AmazonLinux2023BasedAwsVirtualMachine(
    AwsVirtualMachine, linux_virtual_machine.AmazonLinux2023Mixin):
  """Class with configuration for AWS Amazon Linux 2023 virtual machines."""
  IMAGE_SSM_PATTERN = '/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-{architecture}'


class Rhel7BasedAwsVirtualMachine(AwsVirtualMachine,
                                  linux_virtual_machine.Rhel7Mixin):
  """Class with configuration for AWS RHEL 7 virtual machines."""
  # Documentation on finding RHEL images:
  # https://access.redhat.com/articles/3692431
  IMAGE_NAME_FILTER_PATTERN = 'RHEL-7*'
  IMAGE_OWNER = RHEL_IMAGE_PROJECT


class Rhel8BasedAwsVirtualMachine(AwsVirtualMachine,
                                  linux_virtual_machine.Rhel8Mixin):
  """Class with configuration for AWS RHEL 8 virtual machines."""
  # Documentation on finding RHEL images:
  # https://access.redhat.com/articles/3692431
  # All RHEL AMIs are HVM. HVM- blocks HVM_BETA.
  IMAGE_NAME_FILTER_PATTERN = 'RHEL-8*_HVM-*'
  IMAGE_OWNER = RHEL_IMAGE_PROJECT


class Rhel9BasedAwsVirtualMachine(AwsVirtualMachine,
                                  linux_virtual_machine.Rhel9Mixin):
  """Class with configuration for AWS RHEL 9 virtual machines."""
  # Documentation on finding RHEL images:
  # https://access.redhat.com/articles/3692431
  # All RHEL AMIs are HVM. HVM- blocks HVM_BETA.
  IMAGE_NAME_FILTER_PATTERN = 'RHEL-9*_HVM-*'
  IMAGE_OWNER = RHEL_IMAGE_PROJECT


class CentOs7BasedAwsVirtualMachine(AwsVirtualMachine,
                                    linux_virtual_machine.CentOs7Mixin):
  """Class with configuration for AWS CentOS 7 virtual machines."""
  # Documentation on finding the CentOS 7 image:
  # https://wiki.centos.org/Cloud/AWS#x86_64
  IMAGE_NAME_FILTER_PATTERN = 'CentOS Linux 7*'
  IMAGE_OWNER = CENTOS_IMAGE_PROJECT
  DEFAULT_USER_NAME = 'centos'

  def _InstallEfa(self):
    logging.info('Upgrading Centos7 kernel, installing kernel headers and '
                 'rebooting before installing EFA.')
    self.RemoteCommand('sudo yum upgrade -y kernel')
    self.InstallPackages('kernel-devel')
    self.Reboot()
    super()._InstallEfa()


class CentOs8BasedAwsVirtualMachine(AwsVirtualMachine,
                                    linux_virtual_machine.CentOs8Mixin):
  """Class with configuration for AWS CentOS 8 virtual machines."""
  # This describes the official AMIs listed here:
  # https://wiki.centos.org/Cloud/AWS#Official_CentOS_Linux_:_Public_Images
  IMAGE_OWNER = CENTOS_IMAGE_PROJECT
  IMAGE_NAME_FILTER_PATTERN = 'CentOS 8*'
  DEFAULT_USER_NAME = 'centos'


class CentOsStream8BasedAwsVirtualMachine(
    AwsVirtualMachine, linux_virtual_machine.CentOsStream8Mixin):
  """Class with configuration for AWS CentOS Stream 8 virtual machines."""
  # This describes the official AMIs listed here:
  # https://wiki.centos.org/Cloud/AWS#Official_CentOS_Linux_:_Public_Images
  IMAGE_OWNER = CENTOS_IMAGE_PROJECT
  IMAGE_NAME_FILTER_PATTERN = 'CentOS Stream 8*'
  DEFAULT_USER_NAME = 'centos'


class RockyLinux8BasedAwsVirtualMachine(AwsVirtualMachine,
                                        linux_virtual_machine.RockyLinux8Mixin):
  """Class with configuration for AWS Rocky Linux 8 virtual machines."""
  IMAGE_OWNER = ROCKY_LINUX_IMAGE_PROJECT
  IMAGE_NAME_FILTER_PATTERN = 'Rocky-8-*'
  DEFAULT_USER_NAME = 'rocky'


class RockyLinux9BasedAwsVirtualMachine(AwsVirtualMachine,
                                        linux_virtual_machine.RockyLinux9Mixin):
  """Class with configuration for AWS Rocky Linux 9 virtual machines."""
  IMAGE_OWNER = ROCKY_LINUX_IMAGE_PROJECT
  IMAGE_NAME_FILTER_PATTERN = 'Rocky-9-*'
  DEFAULT_USER_NAME = 'rocky'


class CentOsStream9BasedAwsVirtualMachine(
    AwsVirtualMachine, linux_virtual_machine.CentOsStream9Mixin):
  """Class with configuration for AWS CentOS Stream 9 virtual machines."""
  # This describes the official AMIs listed here:
  # https://wiki.centos.org/Cloud/AWS#Official_CentOS_Linux_:_Public_Images
  IMAGE_OWNER = CENTOS_IMAGE_PROJECT
  IMAGE_NAME_FILTER_PATTERN = 'CentOS Stream 9*'


class BaseWindowsAwsVirtualMachine(AwsVirtualMachine,
                                   windows_virtual_machine.BaseWindowsMixin):
  """Support for Windows machines on AWS."""
  DEFAULT_USER_NAME = 'Administrator'

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


class Windows2012CoreAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine, windows_virtual_machine.Windows2012CoreMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2012-R2_RTM-English-64Bit-Core'


class Windows2016CoreAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine, windows_virtual_machine.Windows2016CoreMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2016-English-Core-Base'


class Windows2019CoreAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine, windows_virtual_machine.Windows2019CoreMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2019-English-Core-Base'


class Windows2022CoreAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine, windows_virtual_machine.Windows2022CoreMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2022-English-Core-Base'


class Windows2012DesktopAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2012DesktopMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2012-R2_RTM-English-64Bit-Base'


class Windows2016DesktopAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2016DesktopMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2016-English-Full-Base'


class Windows2019DesktopAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2019DesktopMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2019-English-Full-Base'


class Windows2022DesktopAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2022DesktopMixin):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-Base'


class Windows2019DesktopSQLServer2019StandardAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2019SQLServer2019Standard):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2019-English-Full-SQL_2019_Standard'


class Windows2019DesktopSQLServer2019EnterpriseAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2019SQLServer2019Enterprise):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2019-English-Full-SQL_2019_Enterprise'


class Windows2022DesktopSQLServer2019StandardAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2022SQLServer2019Standard):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-SQL_2019_Standard'


class Windows2022DesktopSQLServer2019EnterpriseAwsVirtualMachine(
    BaseWindowsAwsVirtualMachine,
    windows_virtual_machine.Windows2022SQLServer2019Enterprise):
  IMAGE_SSM_PATTERN = '/aws/service/ami-windows-latest/Windows_Server-2022-English-Full-SQL_2019_Enterprise'


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
