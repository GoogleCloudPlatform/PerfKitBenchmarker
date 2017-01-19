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
"""Class to represent a DigitalOcean Virtual Machine object (Droplet).
"""

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.digitalocean import digitalocean_disk
from perfkitbenchmarker.providers.digitalocean import util
from perfkitbenchmarker import providers

UBUNTU_IMAGE = 'ubuntu-14-04-x64'

# DigitalOcean sets up the root account with a temporary
# password that's set as expired, requiring it to be changed
# immediately. This breaks dpkg postinst scripts, for example
# running adduser will produce errors:
#
#   # chfn -f 'RabbitMQ messaging server' rabbitmq
#   You are required to change your password immediately (root enforced)
#   chfn: PAM: Authentication token is no longer valid; new one required
#
# To avoid this, just disable the root password (we don't need it),
# and remove the forced expiration.
CLOUD_CONFIG_TEMPLATE = '''#cloud-config
users:
  - name: {0}
    ssh-authorized-keys:
      - {1}
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    groups: sudo
    shell: /bin/bash
runcmd:
  - [ passwd, -l, root ]
  - [ chage, -d, -1, -I, -1, -E, -1, -M, 999999, root ]
'''


class DigitalOceanVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a DigitalOcean Virtual Machine (Droplet)."""

  CLOUD = providers.DIGITALOCEAN
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None

  def __init__(self, vm_spec):
    """Initialize a DigitalOcean virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(DigitalOceanVirtualMachine, self).__init__(vm_spec)
    self.droplet_id = None
    self.max_local_disks = 1
    self.local_disk_counter = 0
    self.image = self.image or self.DEFAULT_IMAGE

  def _Create(self):
    """Create a DigitalOcean VM instance (droplet)."""
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')

    response, retcode = util.DoctlAndParse(
        ['compute', 'droplet', 'create',
         self.name,
         '--region', self.zone,
         '--size', self.machine_type,
         '--image', self.image,
         '--user-data', CLOUD_CONFIG_TEMPLATE.format(
             self.user_name, public_key),
         '--enable-private-networking',
         '--wait'])
    if retcode:
      raise errors.Resource.RetryableCreationError('Creation failed: %s' %
                                                   (response,))
    self.droplet_id = response[0]['id']

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    response, retcode = util.DoctlAndParse(
        ['compute', 'droplet', 'get', self.droplet_id])
    for interface in response[0]['networks']['v4']:
      if interface['type'] == 'public':
        self.ip_address = interface['ip_address']
      else:
        self.internal_ip = interface['ip_address']

  def _Delete(self):
    """Delete a DigitalOcean VM instance."""

    response, retcode = util.DoctlAndParse(
        ['compute', 'droplet', 'delete', self.droplet_id, '--force'])
    # The command doesn't return the HTTP status code, and the error
    # format is very difficult to parse, so we string
    # search. TODO(noahl): parse the error message.
    if retcode and '404' in response['errors'][0]['detail']:
      return
    elif retcode:
      raise errors.Resource.RetryableDeletionError('Deletion failed: %s' %
                                                   (response,))

  def _Exists(self):
    """Returns true if the VM exists."""

    response, retcode = util.DoctlAndParse(
        ['compute', 'droplet', 'get', self.droplet_id])

    return retcode == 0

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """

    if disk_spec.disk_type == disk.LOCAL:
      if self.scratch_disks and self.scratch_disks[0].disk_type == disk.LOCAL:
        raise errors.Error('DigitalOcean does not support multiple local '
                           'disks.')

      if disk_spec.num_striped_disks != 1:
        raise ValueError('num_striped_disks=%s, but DigitalOcean VMs can only '
                         'have one local disk.' % disk_spec.num_striped_disks)
      # The single unique local disk on DigitalOcean is also the boot
      # disk, so we can't follow the normal procedure of formatting
      # and mounting. Instead, create a folder at the "mount point" so
      # the rest of PKB will work as expected and deliberately skip
      # self._CreateScratchDiskFromDisks.
      self.RemoteCommand('sudo mkdir -p {0} && sudo chown -R $USER:$USER {0}'
                         .format(disk_spec.mount_point))
      self.scratch_disks.append(
          digitalocean_disk.DigitalOceanLocalDisk(disk_spec))
    else:
      disks = []
      for _ in range(disk_spec.num_striped_disks):
        # Disk 0 is the local disk.
        data_disk = digitalocean_disk.DigitalOceanBlockStorageDisk(
            disk_spec, self.zone)
        data_disk.disk_number = self.remote_disk_counter + 1
        self.remote_disk_counter += 1
        disks.append(data_disk)
      self._CreateScratchDiskFromDisks(disk_spec, disks)


class ContainerizedDigitalOceanVirtualMachine(
        DigitalOceanVirtualMachine,
        linux_virtual_machine.ContainerizedDebianMixin):
    DEFAULT_IMAGE = UBUNTU_IMAGE


class DebianBasedDigitalOceanVirtualMachine(DigitalOceanVirtualMachine,
                                            linux_virtual_machine.DebianMixin):
    DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedDigitalOceanVirtualMachine(DigitalOceanVirtualMachine,
                                          linux_virtual_machine.RhelMixin):
    pass
