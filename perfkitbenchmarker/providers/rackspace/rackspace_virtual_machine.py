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
"""Class to represent a Rackspace Virtual Machine object.

Zones:
    DFW (Dallas-Fort Worth)
    IAD (Northern Virginia)
    ORD (Chicago)
    LON (London)
    SYD (Sydney)
    HKG (Hong Kong)
Machine Types:
run 'rack servers flavor list'
Images:
run 'rack servers image list'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
import json
import logging
import re
import tempfile

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine as linux_vm
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import providers
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.rackspace import rackspace_disk
from perfkitbenchmarker.providers.rackspace import rackspace_network
from perfkitbenchmarker.providers.rackspace import util
import six
from six.moves import range
from six.moves import zip

FLAGS = flags.FLAGS

CLOUD_CONFIG_TEMPLATE = '''#cloud-config
users:
  - name: {0}
    ssh-authorized-keys:
      - {1}
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    groups: sudo
    shell: /bin/bash

'''

BLOCK_DEVICE_TEMPLATE = '''
source-type=image,
source-id={0},
dest=volume,
size={1},
shutdown=remove,
bootindex=0
'''

LSBLK_REGEX = (r'NAME="(.*)"\s+MODEL="(.*)"\s+SIZE="(.*)"'
               r'\s+TYPE="(.*)"\s+MOUNTPOINT="(.*)"\s+LABEL="(.*)"')
LSBLK_PATTERN = re.compile(LSBLK_REGEX)

UBUNTU_IMAGE = 'Ubuntu 16.04 LTS (Xenial Xerus) (PVHVM)'
RHEL_IMAGE = 'CentOS 7 (PVHVM)'

INSTANCE_EXISTS_STATUSES = frozenset(
    ['BUILD', 'ACTIVE', 'PAUSED', 'SHUTOFF', 'ERROR'])
INSTANCE_DELETED_STATUSES = frozenset(
    ['DELETED'])
INSTANCE_KNOWN_STATUSES = INSTANCE_EXISTS_STATUSES | INSTANCE_DELETED_STATUSES

REMOTE_BOOT_DISK_SIZE_GB = 50


def RenderBlockDeviceTemplate(image, volume_size):
  """Renders template used for the block-device flag in RackCLI.

  Args:
    image: string. Image ID of the source image.
    volume_size: string. Size in GB of desired volume size.

  Returns:
    string value for block-device parameter used when creating a VM.
  """
  blk_params = BLOCK_DEVICE_TEMPLATE.replace('\n', '').format(
      image, str(volume_size))
  return blk_params


class RackspaceVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a
  RackspaceVirtualMachine.

  Attributes:
    project: None or string. Project ID, also known as Tenant ID
    rackspace_region: None or string. Rackspace region to build VM resources.
    rack_profile: None or string. Rack CLI profile configuration.
  """

  CLOUD = providers.RACKSPACE

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(RackspaceVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['project'].present:
      config_values['project'] = flag_values.project
    if flag_values['rackspace_region'].present:
      config_values['rackspace_region'] = flag_values.rackspace_region
    if flag_values['rack_profile'].present:
      config_values['rack_profile'] = flag_values.rack_profile

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(RackspaceVmSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'project': (option_decoders.StringDecoder, {'default': None}),
        'rackspace_region': (option_decoders.StringDecoder, {'default': 'IAD'}),
        'rack_profile': (option_decoders.StringDecoder, {'default': None})})
    return result


class RackspaceVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Rackspace Public Cloud Virtual Machine."""

  CLOUD = providers.RACKSPACE
  DEFAULT_IMAGE = 'CentOS 7 (PVHVM)'

  def __init__(self, vm_spec):
    """Initialize a Rackspace Virtual Machine

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the VM.
    """
    super(RackspaceVirtualMachine, self).__init__(vm_spec)
    self.boot_metadata = {}
    self.boot_device = None
    self.boot_disk_allocated = False
    self.allocated_disks = set()
    self.id = None
    self.image = self.image or self.DEFAULT_IMAGE
    self.region = vm_spec.rackspace_region
    self.project = vm_spec.project
    self.profile = vm_spec.rack_profile
    # Isolated tenant networks are regional, not globally available.
    # Security groups (firewalls) apply to a network, hence they are regional.
    # TODO(meteorfox) Create tenant network if it doesn't exist in the region.
    self.firewall = rackspace_network.RackspaceFirewall.GetFirewall()

  def _CreateDependencies(self):
    """Create dependencies prior creating the VM."""
    # TODO(meteorfox) Create security group (if applies)
    self._UploadSSHPublicKey()

  def _Create(self):
    """Creates a Rackspace VM instance and waits until it's ACTIVE."""
    self._CreateInstance()
    self._WaitForInstanceUntilActive()

  @vm_util.Retry()
  def _PostCreate(self):
    """Gets the VM's information."""
    get_cmd = util.RackCLICommand(self, 'servers', 'instance', 'get')
    get_cmd.flags['id'] = self.id
    stdout, _, _ = get_cmd.Issue()
    resp = json.loads(stdout)
    self.internal_ip = resp['PrivateIPv4']
    self.ip_address = resp['PublicIPv4']
    self.AddMetadata(**self.vm_metadata)

  def _Exists(self):
    """Returns true if the VM exists otherwise returns false."""
    if self.id is None:
      return False
    get_cmd = util.RackCLICommand(self, 'servers', 'instance', 'get')
    get_cmd.flags['id'] = self.id
    stdout, _, _ = get_cmd.Issue(suppress_warning=True)
    try:
      resp = json.loads(stdout)
    except ValueError:
      return False
    status = resp['Status']
    return status in INSTANCE_EXISTS_STATUSES

  def _Delete(self):
    """Deletes a Rackspace VM instance and waits until API returns 404."""
    if self.id is None:
      return
    self._DeleteInstance()
    self._WaitForInstanceUntilDeleted()

  def _DeleteDependencies(self):
    """Deletes dependencies that were need for the VM after the VM has been
    deleted."""
    # TODO(meteorfox) Delete security group (if applies)
    self._DeleteSSHPublicKey()

  def _UploadSSHPublicKey(self):
    """Uploads SSH public key to the VM's region. 1 key per VM per Region."""
    cmd = util.RackCLICommand(self, 'servers', 'keypair', 'upload')
    cmd.flags = OrderedDict([
        ('name', self.name), ('file', self.ssh_public_key)])
    cmd.Issue()

  def _DeleteSSHPublicKey(self):
    """Deletes SSH public key used for a VM."""
    cmd = util.RackCLICommand(self, 'servers', 'keypair', 'delete')
    cmd.flags['name'] = self.name
    cmd.Issue()

  def _CreateInstance(self):
    """Generates and execute command for creating a Rackspace VM."""
    with tempfile.NamedTemporaryFile(dir=vm_util.GetTempDir(),
                                     prefix='user-data') as tf:
      with open(self.ssh_public_key) as f:
        public_key = f.read().rstrip('\n')
      tf.write(CLOUD_CONFIG_TEMPLATE.format(self.user_name, public_key))
      tf.flush()
      create_cmd = self._GetCreateCommand(tf)
      stdout, stderr, _ = create_cmd.Issue()
    if stderr:
      resp = json.loads(stderr)
      raise errors.Error(''.join(
          ('Non-recoverable error has occurred: %s\n' % str(resp),
           'Following command caused the error: %s' % repr(create_cmd),)))
    resp = json.loads(stdout)
    self.id = resp['ID']

  def _GetCreateCommand(self, tf):
    """Generates RackCLI command for creating a Rackspace VM.

    Args:
      tf: file object containing cloud-config script.

    Returns:
      RackCLICommand containing RackCLI arguments to build a Rackspace VM.
    """
    create_cmd = util.RackCLICommand(self, 'servers', 'instance', 'create')
    create_cmd.flags['name'] = self.name
    create_cmd.flags['keypair'] = self.name
    create_cmd.flags['flavor-id'] = self.machine_type
    if FLAGS.rackspace_boot_from_cbs_volume:
      blk_flag = RenderBlockDeviceTemplate(self.image, REMOTE_BOOT_DISK_SIZE_GB)
      create_cmd.flags['block-device'] = blk_flag
    else:
      create_cmd.flags['image-name'] = self.image
    if FLAGS.rackspace_network_id is not None:
      create_cmd.flags['networks'] = ','.join([
          rackspace_network.PUBLIC_NET_ID, rackspace_network.SERVICE_NET_ID,
          FLAGS.rackspace_network_id])
    create_cmd.flags['user-data'] = tf.name
    metadata = ['owner=%s' % FLAGS.owner]
    for key, value in six.iteritems(self.boot_metadata):
      metadata.append('%s=%s' % (key, value))
    create_cmd.flags['metadata'] = ','.join(metadata)
    return create_cmd

  @vm_util.Retry(poll_interval=5, max_retries=720, log_errors=False,
                 retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def _WaitForInstanceUntilActive(self):
    """Waits until instance achieves non-transient state."""
    get_cmd = util.RackCLICommand(self, 'servers', 'instance', 'get')
    get_cmd.flags['id'] = self.id
    stdout, stderr, _ = get_cmd.Issue()
    if stdout:
      instance = json.loads(stdout)
      if instance['Status'] == 'ACTIVE':
        logging.info('VM: %s is up and running.' % self.name)
        return
      elif instance['Status'] == 'ERROR':
        logging.error('VM: %s failed to boot.' % self.name)
        raise errors.VirtualMachine.VmStateError()
    raise errors.Resource.RetryableCreationError(
        'VM: %s is not running. Retrying to check status.' % self.name)

  def _DeleteInstance(self):
    """Executes delete command for removing a Rackspace VM."""
    cmd = util.RackCLICommand(self, 'servers', 'instance', 'delete')
    cmd.flags['id'] = self.id
    stdout, _, _ = cmd.Issue(suppress_warning=True)
    resp = json.loads(stdout)
    if 'result' not in resp or 'Deleting' not in resp['result']:
      raise errors.Resource.RetryableDeletionError()

  @vm_util.Retry(poll_interval=5, max_retries=-1, timeout=300,
                 log_errors=False,
                 retryable_exceptions=(errors.Resource.RetryableDeletionError,))
  def _WaitForInstanceUntilDeleted(self):
    """Waits until instance has been fully removed, or deleted."""
    get_cmd = util.RackCLICommand(self, 'servers', 'instance', 'get')
    get_cmd.flags['id'] = self.id
    stdout, stderr, _ = get_cmd.Issue()
    if stderr:
      resp = json.loads(stderr)
      if 'error' in resp and "couldn't find" in resp['error']:
        logging.info('VM: %s has been successfully deleted.' % self.name)
        return
    instance = json.loads(stdout)
    if instance['Status'] == 'ERROR':
      logging.error('VM: %s failed to delete.' % self.name)
      raise errors.VirtualMachine.VmStateError()

    if instance['Status'] == 'DELETED':
        logging.info('VM: %s has been successfully deleted.' % self.name)
    else:
      raise errors.Resource.RetryableDeletionError(
          'VM: %s has not been deleted. Retrying to check status.' % self.name)

  def AddMetadata(self, **kwargs):
    """Adds metadata to the VM via RackCLI update-metadata command."""
    if not kwargs:
      return
    cmd = util.RackCLICommand(self, 'servers', 'instance', 'update-metadata')
    cmd.flags['id'] = self.id
    cmd.flags['metadata'] = ','.join('{0}={1}'.format(key, value)
                                     for key, value in six.iteritems(kwargs))
    cmd.Issue()

  def OnStartup(self):
    """Executes commands on the VM immediately after it has booted."""
    super(RackspaceVirtualMachine, self).OnStartup()
    self.boot_device = self._GetBootDevice()

  def CreateScratchDisk(self, disk_spec):
    """Creates a VM's scratch disk that will be used for a benchmark.

    Given a data_disk_type it will either create a corresponding Disk object,
    or raise an error that such data disk type is not supported.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.

    Raises:
      errors.Error indicating that the requested 'data_disk_type' is
          not supported.
    """
    if disk_spec.disk_type == rackspace_disk.BOOT:  # Ignore num_striped_disks
      self._AllocateBootDisk(disk_spec)
    elif disk_spec.disk_type == rackspace_disk.LOCAL:
      self._AllocateLocalDisks(disk_spec)
    elif disk_spec.disk_type in rackspace_disk.REMOTE_TYPES:
      self._AllocateRemoteDisks(disk_spec)
    else:
      raise errors.Error('Unsupported data disk type: %s' % disk_spec.disk_type)

  def _AllocateBootDisk(self, disk_spec):
    """Allocate the VM's boot, or system, disk as the scratch disk.

    Boot disk can only be allocated once. If multiple data disks are required
    it will raise an error.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.

    Raises:
      errors.Error when boot disk has already been allocated as a data disk.
    """
    if self.boot_disk_allocated:
      raise errors.Error('Only one boot disk can be created per VM')
    device_path = '/dev/%s' % self.boot_device['name']
    scratch_disk = rackspace_disk.RackspaceBootDisk(
        disk_spec, self.zone, self.project, device_path, self.image)
    self.boot_disk_allocated = True
    self.scratch_disks.append(scratch_disk)
    scratch_disk.Create()
    path = disk_spec.mount_point
    mk_cmd = 'sudo mkdir -p {0}; sudo chown -R $USER:$USER {0};'.format(path)
    self.RemoteCommand(mk_cmd)

  def _AllocateLocalDisks(self, disk_spec):
    """Allocate the VM's local disks (included with the VM), as a data disk(s).

    A local disk can only be allocated once per data disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    block_devices = self._GetBlockDevices()
    free_blk_devices = self._GetFreeBlockDevices(block_devices, disk_spec)
    disks = []
    for i in range(disk_spec.num_striped_disks):
      local_device = free_blk_devices[i]
      disk_name = '%s-local-disk-%d' % (self.name, i)
      device_path = '/dev/%s' % local_device['name']
      local_disk = rackspace_disk.RackspaceLocalDisk(
          disk_spec, disk_name, self.zone, self.project, device_path)
      self.allocated_disks.add(local_disk)
      disks.append(local_disk)
    self._CreateScratchDiskFromDisks(disk_spec, disks)

  def _AllocateRemoteDisks(self, disk_spec):
    """Creates and allocates Rackspace Cloud Block Storage volumes as
    as data disks.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    scratch_disks = []
    for disk_num in range(disk_spec.num_striped_disks):
      volume_name = '%s-volume-%d' % (self.name, disk_num)
      scratch_disk = rackspace_disk.RackspaceRemoteDisk(
          disk_spec, volume_name, self.zone, self.project,
          media=disk_spec.disk_type)
      scratch_disks.append(scratch_disk)
    self._CreateScratchDiskFromDisks(disk_spec, scratch_disks)

  def _GetFreeBlockDevices(self, block_devices, disk_spec):
    """Returns available block devices that are not in used as data disk or as
    a boot disk.

    Args:
      block_devices: list of dict containing information about all block devices
          in the VM.
      disk_spec: virtual_machine.BaseDiskSpec of the disk.

    Returns:
      list of dicts of only block devices that are not being used.

    Raises:
      errors.Error Whenever there are no available block devices.
    """
    free_blk_devices = []
    for dev in block_devices:
      if self._IsDiskAvailable(dev):
        free_blk_devices.append(dev)
    if not free_blk_devices:
      raise errors.Error(
          ''.join(('Machine type %s does not include' % self.machine_type,
                   ' local disks. Please use a different disk_type,',
                   ' or a machine_type that provides local disks.')))
    elif len(free_blk_devices) < disk_spec.num_striped_disks:
      raise errors.Error('Not enough local data disks. '
                         'Requesting %d disk(s) but only %d available.'
                         % (disk_spec.num_striped_disks, len(free_blk_devices)))
    return free_blk_devices

  def _GetBlockDevices(self):
    """Execute command on VM to gather all block devices in the VM.

    Returns:
      list of dicts block devices in the VM.
    """
    stdout, _ = self.RemoteCommand(
        'sudo lsblk -o NAME,MODEL,SIZE,TYPE,MOUNTPOINT,LABEL -n -b -P')
    lines = stdout.splitlines()
    groups = [LSBLK_PATTERN.match(line) for line in lines]
    tuples = [g.groups() for g in groups if g]
    colnames = ('name', 'model', 'size_bytes', 'type', 'mountpoint', 'label',)
    blk_devices = [dict(list(zip(colnames, t))) for t in tuples]
    for d in blk_devices:
      d['model'] = d['model'].rstrip()
      d['label'] = d['label'].rstrip()
      d['size_bytes'] = int(d['size_bytes'])
    return blk_devices

  def _GetBootDevice(self):
    """Returns backing block device where '/' is mounted on.

    Returns:
      dict blk device data

    Raises:
      errors.Error indicates that could not find block device with '/'.
    """
    blk_devices = self._GetBlockDevices()
    boot_blk_device = None
    for dev in blk_devices:
      if dev['mountpoint'] == '/':
        boot_blk_device = dev
        break
    if boot_blk_device is None:  # Unlikely
      raise errors.Error('Could not find disk with "/" root mount point.')
    if boot_blk_device['type'] != 'part':
      return boot_blk_device
    return self._FindBootBlockDevice(blk_devices, boot_blk_device)

  def _FindBootBlockDevice(self, blk_devices, boot_blk_device):
    """Helper method to search for backing block device of a partition."""
    blk_device_name = boot_blk_device['name'].rstrip('0123456789')
    for dev in blk_devices:
      if dev['type'] == 'disk' and dev['name'] == blk_device_name:
        boot_blk_device = dev
        return boot_blk_device

  def _IsDiskAvailable(self, blk_device):
    """Returns True if a block device is available.

    An available disk, is a disk that has not been allocated previously as
    a data disk, or is not being used as boot disk.
    """
    return (blk_device['type'] != 'part' and
            blk_device['name'] != self.boot_device['name'] and
            'config' not in blk_device['label'] and
            blk_device['name'] not in self.allocated_disks)


class DebianBasedRackspaceVirtualMachine(RackspaceVirtualMachine,
                                         linux_vm.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedRackspaceVirtualMachine(RackspaceVirtualMachine,
                                       linux_vm.RhelMixin):
  DEFAULT_IMAGE = RHEL_IMAGE
  
class Ubuntu1604BasedGceVirtualMachine(RackspaceVirtualMachine,
                                       linux_vm.Ubuntu1604Mixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE
  



