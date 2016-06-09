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
"""Class to represent a ProfitBricks Virtual Machine object.
"""

import os
import logging
import base64

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import profitbricks
from perfkitbenchmarker.providers.profitbricks import profitbricks_disk
from perfkitbenchmarker.providers.profitbricks import util
from perfkitbenchmarker import providers

PROFITBRICKS_API = profitbricks.PROFITBRICKS_API
FLAGS = flags.FLAGS


class ProfitBricksVirtualMachine(virtual_machine.BaseVirtualMachine):
    """Object representing a ProfitBricks Virtual Machine."""

    CLOUD = providers.PROFITBRICKS
    DEFAULT_IMAGE = None

    def __init__(self, vm_spec):
        """Initialize a ProfitBricks virtual machine.

        Args:
        vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
        """
        super(ProfitBricksVirtualMachine, self).__init__(vm_spec)

        # Get user authentication credentials
        user_config_path = os.path.expanduser(FLAGS.profitbricks_config)

        with open(user_config_path) as f:
            user_creds = f.read().rstrip('\n')
            self.user_token = base64.b64encode(user_creds)

        self.need_dc = True
        self.server_id = None
        self.server_status = None
        self.dc_id = None
        self.dc_status = None
        self.lan_id = None
        self.lan_status = None
        self.max_local_disks = 1
        self.local_disk_counter = 0
        self.image = self.image or self.DEFAULT_IMAGE
        self.ram = FLAGS.profitbricks_ram
        self.cores = FLAGS.profitbricks_cores
        self.profitbricks_disk_type = FLAGS.profitbricks_disk_type
        self.disk_size = FLAGS.profitbricks_disk_size
        self.location = FLAGS.location
        self.zone = FLAGS.zone
        self.user_name = 'root'
        self.header = {
            'Authorization': 'Basic %s' % self.user_token,
            'Content-Type': 'application/vnd.profitbricks.resource+json',
        }

    def _Create(self):
        """Create a ProfitBricks VM instance."""

        # Check if this is the first iteration and we need a DC, or not.
        if self.need_dc:
            # Create a new DC
            self.dc_id, self.dc_status = util.CreateDatacenter(self.header,
                                                               self.location)

            # Create LAN so that we can utilize a public IP address
            self.lan_id, self.lan_status = util.CreateLan(self.header,
                                                          self.dc_id)

            # Switch to false as we now have our DC and LAN to work with
            self.need_dc = False

        # Grab ssh pub key to inject into new VM
        with open(self.ssh_public_key) as f:
            public_key = f.read().rstrip('\n')

        # Find an Ubuntu image that matches our location
        self.image = util.ReturnImage(self.header, self.location)

        # Find necessary specs for the user's machine_type selection
        flavor_ram, flavor_cores = util.ReturnFlavor(self.machine_type)

        if not self.ram:
            self.ram = flavor_ram
        if not self.cores:
            self.cores = flavor_cores

        # Create server POST body
        new_server = {
            'properties': {
                'name': self.name,
                'ram': self.ram,
                'cores': self.cores,
                'availabilityZone': self.zone
            },
            'entities': {
                'volumes': {
                    'items': [
                        {
                            'properties': {
                                'size': self.disk_size,
                                'name': 'volume1',
                                'image': self.image,
                                'bus': 'VIRTIO',
                                'type': self.profitbricks_disk_type,
                                'sshKeys': [public_key]
                            }
                        }
                    ]
                },
                'nics': {
                    'items': [
                        {
                            'properties': {
                                'name': 'nic1',
                                'lan': self.lan_id
                            }
                        }
                    ]
                }
            }
        }

        # Build Server URL
        url = '%s/datacenters/%s/servers' % (PROFITBRICKS_API, self.dc_id)

        # Provision Server
        r = util.PerformRequest('post', url, self.header, json=new_server)
        logging.info('Creating server: %s' % self.name)

        # Parse Required values from response
        self.server_status = r.headers['Location']
        response = r.json()
        self.server_id = response['id']

        # The freshly created server will be in a locked and unusable
        # state for a while, and it cannot be deleted or modified in
        # this state. Wait for the action to finish and check the
        # reported result.
        if not util.WaitFor(self.server_status, self.header):
            raise errors.Resource.RetryableCreationError('Server creation '
                                                         'failed, see log.')

    @vm_util.Retry()
    def _PostCreate(self):
        """Get the instance's public IP address."""

        # Poll server to make sure it's been provisioned
        if util.WaitFor(self.server_status, self.header):

            url = '%s/datacenters/%s/servers/%s?depth=5' % (PROFITBRICKS_API,
                                                            self.dc_id,
                                                            self.server_id)

            # Perform Request
            r = util.PerformRequest('get', url, self.header)
            response = r.json()
            nic = response['entities']['nics']['items'][0]
            self.ip_address = nic['properties']['ips'][0]
        else:
            raise errors.Resource.RetryableCreationError('IP parse failed, '
                                                         'see log.')

    def _Delete(self):
        """Delete a ProfitBricks Datacenter (Including VM instances)."""

        # Build URL
        url = '%s/datacenters/%s' % (PROFITBRICKS_API, self.dc_id)

        # Make call
        logging.info('Deleting Datacenter: %s' % self.dc_id)
        r = util.PerformRequest('delete', url, self.header)

        # Check to make sure deletion has finished
        delete_status = r.headers['Location']
        if not util.WaitFor(delete_status, self.header):
            raise errors.Resource.RetryableCreationError('Datacenter deletion '
                                                         'failed, see log.')

    def _Exists(self):
        """Returns true if the VM exists."""

        # Build URL
        url = '%s/datacenters/%s/servers/%s' % (PROFITBRICKS_API,
                                                self.dc_id, self.server_id)

        # Make call
        r = util.PerformRequest('get', url, self.header, check_status=False)

        # If 404 is returned, resource doesn't exist on provider cloud
        if r.status_code == 404:
            logging.info('Server: %s does not exist.' % self.name)
            return False

        logging.info('Server: %s exists.' % self.name)
        return True

    def CreateScratchDisk(self, disk_spec):
        """Create a VM's scratch disk.

        Args:
          disk_spec: virtual_machine.BaseDiskSpec object of the disk.
        """
        if disk_spec.disk_type != disk.STANDARD:
            raise errors.Error('ProfitBricks does not support disk type %s.' %
                               disk_spec.disk_type)

        if self.scratch_disks:
            # We have a "disk" already, don't add more.
            raise errors.Error('ProfitBricks does not require '
                               'a separate disk.')

        # Just create a local directory at the specified path, don't mount
        # anything.
        self.RemoteCommand('sudo mkdir -p {0} && sudo chown -R $USER:$USER {0}'
                           .format(disk_spec.mount_point))
        self.scratch_disks.append(profitbricks_disk.ProfitBricksDisk(
                                  disk_spec))


class ContainerizedProfitBricksVirtualMachine(
        ProfitBricksVirtualMachine,
        linux_virtual_machine.ContainerizedDebianMixin):
    pass


class DebianBasedProfitBricksVirtualMachine(ProfitBricksVirtualMachine,
                                            linux_virtual_machine.DebianMixin):
    pass


class RhelBasedProfitBricksVirtualMachine(ProfitBricksVirtualMachine,
                                          linux_virtual_machine.RhelMixin):
    pass
