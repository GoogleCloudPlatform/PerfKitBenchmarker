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
run 'nova flavor-list'
Images:
run 'nova image-list'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""
import json
import logging
import os
import re
import tempfile
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.rackspace import rackspace_disk
from perfkitbenchmarker.providers.rackspace import \
    rackspace_machine_types as rax
from perfkitbenchmarker.providers.rackspace import rackspace_network
from perfkitbenchmarker.providers.rackspace import util

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
source=image,id={0},dest=volume,size={1},shutdown=remove,bootindex=0
'''

LSBLK_REGEX = (r'NAME="(.*)"\s+MODEL="(.*)"\s+SIZE="(.*)"'
               r'\s+TYPE="(.*)"\s+MOUNTPOINT="(.*)"\s+LABEL="(.*)"')
LSBLK_PATTERN = re.compile(LSBLK_REGEX)

UBUNTU_IMAGE = '28153eac-1bae-4039-8d9f-f8b513241efe'
RHEL_IMAGE = 'c07409c8-0931-40e4-a3bc-4869ecb5931e'


class RackspaceVirtualMachine(virtual_machine.BaseVirtualMachine):

  CLOUD = providers.RACKSPACE
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None

  "Object representing a Rackspace Virtual Machine"
  def __init__(self, vm_spec):
    """Initialize Rackspace virtual machine

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(RackspaceVirtualMachine, self).__init__(vm_spec)
    self.firewall = rackspace_network.RackspaceSecurityGroup.GetFirewall()
    self.id = ''
    self.ip_address6 = ''
    self.mounted_disks = set()
    self.max_local_disks = 0
    self.flavor = self._GetFlavorDetails()
    self.flavor_class = None
    self.boot_device_path = ''
    if 'extra_specs' in self.flavor and self.flavor['extra_specs']:
      flavor_specs = json.loads(self.flavor['extra_specs'])
      self.flavor_class = flavor_specs.get('class', None)
      disk_specs = rax.GetRackspaceDiskSpecs(self.machine_type, flavor_specs)
      self.max_local_disks = disk_specs['max_local_disks']
      self.remote_disk_support = disk_specs['remote_disk_support']
    else:
      raise errors.Error(
          'There was a problem while retrieving machine_type'
          ' information from the cloud provider.')
    self.image = self.image or self.DEFAULT_IMAGE

  def _Create(self):
    """Create a Rackspace instance."""
    with tempfile.NamedTemporaryFile(dir=vm_util.GetTempDir(),
                                     prefix='user-data') as tf:
      with open(self.ssh_public_key) as f:
        public_key = f.read().rstrip('\n')

      script = CLOUD_CONFIG_TEMPLATE.format(self.user_name, public_key)
      tf.write(script)
      tf.flush()

      env = os.environ.copy()
      env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
      create_cmd = self._GetCreateCommand()
      create_cmd.extend([
          '--config-drive', 'true',
          '--user-data', tf.name,
          self.name])
      stdout, _, _ = vm_util.IssueCommand(create_cmd, env=env)
      instance = util.ParseNovaTable(stdout)
      if 'id' in instance:
        self.id = instance['id']
      else:
        raise errors.Resource.RetryableCreationError(
            'There was a problem trying to create instance %s' % self.name)

    if not self._WaitForInstanceUntilActive():
      raise errors.Resource.RetryableCreationError(
          'Failed to create instance')

  def _WaitForInstanceUntilActive(self, max_retries=720, poll_interval_secs=5):
    """Wait until instance achieves non-transient state."""
    env = os.environ.copy()
    env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    getinstance_cmd = [FLAGS.nova_path, 'show', self.id]

    for _ in xrange(max_retries):
      time.sleep(poll_interval_secs)
      stdout, _, _ = vm_util.IssueCommand(getinstance_cmd, env=env)
      instance = util.ParseNovaTable(stdout)
      if 'status' in instance and instance['status'] != 'BUILD':
        return instance['status'] == 'ACTIVE'

    return False

  def _GetCreateCommand(self):
    create_cmd = [
        FLAGS.nova_path, 'boot',
        '--flavor', self.machine_type]

    boot_from_cbs = (
        self.flavor_class in (rax.COMPUTE1_CLASS, rax.MEMORY1_CLASS,) or
        util.FLAGS.boot_from_cbs_volume)

    if self.remote_disk_support and boot_from_cbs:
        blk_params = BLOCK_DEVICE_TEMPLATE.strip('\n').format(
            self.image, str(rax.REMOTE_BOOT_DISK_SIZE_GB))
        create_cmd.extend(['--block-device', blk_params])
    else:
      create_cmd.extend(['--image', self.image])

    return create_cmd

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    env = os.environ.copy()
    env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    getinstance_cmd = [FLAGS.nova_path, 'show', self.id]
    stdout, _, _ = vm_util.IssueCommand(getinstance_cmd, env=env)
    instance = util.ParseNovaTable(stdout)
    if 'status' in instance and instance['status'] == 'ACTIVE':
      self.ip_address = instance['accessIPv4']
      self.ip_address6 = instance['accessIPv6']
      self.internal_ip = instance['private network']
    else:
      raise errors.Error('Unexpected failure.')

  def _Delete(self):
    """Delete a Rackspace VM instance."""
    env = os.environ.copy()
    env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    delete_cmd = [FLAGS.nova_path, 'delete', self.id]
    vm_util.IssueCommand(delete_cmd, env=env)

  def _Exists(self):
    """Returns true if the VM exists."""
    env = os.environ.copy()
    env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    getinstance_cmd = [FLAGS.nova_path, 'show', self.id]
    stdout, stderr, _ = vm_util.IssueCommand(getinstance_cmd, env=env)
    if stdout.strip() == '':
        return False
    instance = util.ParseNovaTable(stdout)
    return (instance.get('OS-EXT-STS:task_state') == 'deleting' or
            instance.get('status') == 'ACTIVE')

  def _GetFlavorDetails(self):
    """Retrieves details about the flavor used to build the instance.

    Returns:
      A dict of properties of the flavor used to build the instance.
    """
    env = os.environ.copy()
    env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    flavor_show_cmd = [FLAGS.nova_path, 'flavor-show', self.machine_type]
    stdout, _, _ = vm_util.IssueCommand(flavor_show_cmd, env=env)
    flavor_properties = util.ParseNovaTable(stdout)
    return flavor_properties

  def _GetBlockDevices(self):
    stdout, _ = self.RemoteCommand(
        'sudo lsblk -o NAME,MODEL,SIZE,TYPE,MOUNTPOINT,LABEL -n -b -P')
    lines = stdout.splitlines()
    groups = [LSBLK_PATTERN.match(line) for line in lines]
    tuples = [g.groups() for g in groups if g]
    colnames = ('name', 'model', 'size_bytes', 'type', 'mountpoint', 'label',)
    blk_devices = [dict(zip(colnames, t)) for t in tuples]
    for d in blk_devices:
      d['model'] = d['model'].rstrip()
      d['label'] = d['label'].rstrip()
      d['size_bytes'] = int(d['size_bytes'])
    return blk_devices

  def _GetBootDevice(self):
    blk_devices = self._GetBlockDevices()
    boot_blk_device = None
    for dev in blk_devices:
      if dev['mountpoint'] == '/':
        boot_blk_device = dev
        break

    if boot_blk_device is None:  # Unlikely
      raise errors.Error('Could not find disk with "/" root mount point.')

    if boot_blk_device['type'] == 'part':
      blk_device_name = boot_blk_device['name'].rstrip('0123456789')
      for dev in blk_devices:
        if dev['type'] == 'disk' and dev['name'] == blk_device_name:
          boot_blk_device = dev
          break
      else:  # Also, unlikely
        raise errors.Error('Could not find disk containing boot partition.')

    return boot_blk_device

  def _ApplyOnMetalDiskTuning(self, disks):
    """Applies recommended I/O scheduler and queue tuning for OnMetal IO
    PCIe SSD drives.
    """
    for d in disks:
      name = d['name']
      cmd = [
          'echo noop | sudo tee /sys/block/%s/queue/scheduler' % name,
          'echo 4096 | sudo tee /sys/block/%s/queue/nr_requests' % name,
          'echo 1024 | sudo tee /sys/block/%s/queue/max_sectors_kb' % name,
          'echo 1 | sudo tee /sys/block/%s/queue/nomerges' % name,
          'echo 512 | sudo tee /sys/block/%s/device/queue_depth' % name]
      self.RemoteCommand('; '.join(cmd))

  def SetupLocalDisks(self):
    """Set up any local drives that exist."""
    if (self.flavor_class == rax.ONMETAL_CLASS and
        FLAGS.rackspace_apply_onmetal_ssd_tuning):
      onmetal_disks = [d for d in self._GetBlockDevices()
                       if d['type'] == 'disk'
                       and rax.ONMETAL_DISK_MODEL in d['model']]
      self._ApplyOnMetalDiskTuning(onmetal_disks)

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    logging.info('Create scratch disk(s) for instance %s.', self.id)
    boot_device = self._GetBootDevice()
    self.boot_device_path = '/dev/%s' % boot_device['name']

    rackspace_disk_type = rax.GetRackspaceDiskType(
        self.machine_type, self.flavor_class, disk_spec.disk_type)

    if rackspace_disk_type == rax.REMOTE:
      disks = self._CreateRemoteDisks(disk_spec)
    elif rackspace_disk_type == rax.LOCAL:  # local
      if disk_spec.disk_type == disk.STANDARD:
        self.SetupLocalDisks()
      disks = self._CreateLocalDisks(disk_spec)
    elif rackspace_disk_type == rax.ROOT_DISK:  # boot
      boot_disk = rackspace_disk.RackspaceEphemeralDisk(
          disk_spec, boot_device['name'], is_root_disk=True)
      disks = [boot_disk]
    else:
      # not supported
      raise errors.Error(
          'Combination of machine_type=%s and scratch_disk_type=%s'
          ' is not supported.' % (self.machine_type, disk_spec.disk_type))

    self._CreateScratchDiskFromDisks(disk_spec, disks)
    logging.info('Created %d scratch disks. Disks=%s' % (len(disks), disks))

  def _CreateRemoteDisks(self, disk_spec):
    disks_names = ['%s-data-%s-%d-%d'
                   % (self.name, rackspace_disk.DISK_TYPE[disk_spec.disk_type],
                      len(self.scratch_disks), i)
                   for i in range(disk_spec.num_striped_disks)]
    disks = [rackspace_disk.RackspaceRemoteDisk(disk_spec, name,
                                                self.zone, self.image)
             for name in disks_names]
    return disks

  def _CreateLocalDisks(self, disk_spec):
    boot_device = self._GetBootDevice()
    blk_devices = self._GetBlockDevices()
    extra_blk_devices = []
    for dev in blk_devices:
      if (dev['type'] != 'part' and dev['name'] != boot_device['name'] and
          'config' not in dev['label'] and
          dev['name'] not in self.mounted_disks):
        extra_blk_devices.append(dev)

    if len(extra_blk_devices) == 0:
      raise errors.Error(
          ('Machine type %s does not include' % self.machine_type,
           ' local disks. Please use a different disk_type,',
           ' or a machine_type that provides local disks.'))
    elif len(extra_blk_devices) < disk_spec.num_striped_disks:
      raise errors.Error(
          'Not enough local data disks.'
          ' Requesting %d disk, but only %d available.'
          % (disk_spec.num_striped_disks, len(extra_blk_devices)))

    disks = []
    for i in range(disk_spec.num_striped_disks):
      local_device = extra_blk_devices[i]
      local_disk = rackspace_disk.RackspaceEphemeralDisk(
          disk_spec, local_device['name'])
      self.mounted_disks.add(local_device['name'])
      disks.append(local_disk)
    return disks

  def GetScratchDir(self, disk_num=0):
    if self.scratch_disks[disk_num].mount_point == '':
      return '/scratch%s' % disk_num

    return super(RackspaceVirtualMachine, self).GetScratchDir(disk_num)

  def MountDisk(self, device_path, mount_path):
    if device_path == self.boot_device_path:
      mk_cmd = ('sudo mkdir -p {0};'
                'sudo chown -R $USER:$USER {0};').format(mount_path)
      self.RemoteCommand(mk_cmd)
    else:
      super(RackspaceVirtualMachine, self).MountDisk(device_path, mount_path)

  def FormatDisk(self, device_path):
    """Formats a disk attached to the VM."""
    if device_path != self.boot_device_path:
      fmt_cmd = ('sudo mke2fs -F -E lazy_itable_init=0 -O '
                 '^has_journal -t ext4 -b 4096 %s' % device_path)
      self.RemoteCommand(fmt_cmd)

  def GetName(self):
    """Get a Rackspace VM's unique name."""
    return self.name

  def AddMetadata(self, **kwargs):
    """Adds metadata to the Rackspace VM"""
    if not kwargs:
        return
    env = os.environ.copy()
    env.update(util.GetDefaultRackspaceNovaEnv(self.zone))
    cmd = [FLAGS.nova_path, 'meta', self.id, 'set']
    for key, value in kwargs.iteritems():
        cmd.append('{0}={1}'.format(key, value))
    vm_util.IssueCommand(cmd, env=env)


class DebianBasedRackspaceVirtualMachine(RackspaceVirtualMachine,
                                         linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedRackspaceVirtualMachine(RackspaceVirtualMachine,
                                       linux_virtual_machine.RhelMixin):
  DEFAULT_IMAGE = RHEL_IMAGE
