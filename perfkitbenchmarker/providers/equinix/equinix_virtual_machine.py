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
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.digitalocean import digitalocean_disk
from perfkitbenchmarker.providers.equinix import util
from six.moves import range
from perfkitbenchmarker import linux_virtual_machine as linux_vm
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


class MetalVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing a Baremetal Virtual Machine ."""

  CLOUD = providers.EQUINIX
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None
  

  def __init__(self, vm_spec):
    """Initialize a BareMetal virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(MetalVirtualMachine, self).__init__(vm_spec)
    self.device_id = None
    self.max_local_disks = 1
    self.local_disk_counter = 0
    self.image = self.image or self.DEFAULT_IMAGE
    

  def _Create(self):
    """Create a BareMetal instance ."""
    with open(self.ssh_public_key) as f:
      public_key = f.read().rstrip('\n')

    response, retcode = util.MetalAndParse(
        ['device', 'create', 
         '--hostname', self.name,
         '--metro', self.zone, #metro
         '--plan', self.machine_type, #plan
         '--operating-system', self.image, #OS
         '--userdata', CLOUD_CONFIG_TEMPLATE.format(
             self.user_name, public_key)
         ])
    if retcode:
      raise errors.Resource.RetryableCreationError('Creation failed: %s' %
                                                   (response,))
    self.device_id = response['id']

  @vm_util.Retry()
  def _PostCreate(self):
    """Get the instance's data."""
    response, retcode = util.MetalAndParse(
        ['device', 'get', '-i', self.device_id])
    for interface in response['ip_addresses']:
      if interface['address_family'] != 4:
        continue
      if interface['public'] == True:
        self.ip_address = interface['address']
      else:
        self.internal_ip = interface['address']

  def _Delete(self):
    """Delete a DigitalOcean VM instance."""

    response, retcode = util.MetalAndParse(
        ['device', 'delete', '-i', self.device_id, '--force'])
    # The command doesn't return the HTTP status code, and the error
    # format is very difficult to parse, so we string
    # search. TODO 404 is a standard error and is not in metal json output
    if retcode and '404' in response['errors'][0]['detail']:
      return
    elif retcode:
      raise errors.Resource.RetryableDeletionError('Deletion failed: %s' %
                                                   (response,))

  def _Exists(self):
    """Returns true if the VM exists."""

    response, retcode = util.MetalAndParse(
        ['device', 'get', self.device_id])

    return retcode == 0
#Disk creation needs to be Finished, therefore FIO testing errors will occur unless changed

class Ubuntu1804BasedEquinixVirtualMachine(
    MetalVirtualMachine, linux_vm.Ubuntu1804Mixin):
  """
  Equinix Metal
  """