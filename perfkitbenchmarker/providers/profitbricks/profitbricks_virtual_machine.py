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
import yaml

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers import profitbricks
from perfkitbenchmarker.providers.profitbricks import profitbricks_disk
from perfkitbenchmarker.providers.profitbricks import util
from perfkitbenchmarker import providers

PROFITBRICKS_API = profitbricks.PROFITBRICKS_API
FLAGS = flags.FLAGS
TIMEOUT = 1500  # 25 minutes


class CustomMachineTypeSpec(spec.BaseSpec):
  """Properties of a ProfitBricks custom machine type.

  Attributes:
    cores: int. Number of CPU cores for a custom VM.
    ram: int. Amount of RAM in MBs for a custom VM.
  """

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(CustomMachineTypeSpec, cls)._GetOptionDecoderConstructions()
    result.update({'cores': (option_decoders.IntDecoder, {'min': 1}),
                   'ram': (option_decoders.IntDecoder, {'min': 1024})})
    return result


class MachineTypeDecoder(option_decoders.TypeVerifier):
  """Decodes the machine_type option of a ProfitBricks VM config."""

  def __init__(self, **kwargs):
    super(MachineTypeDecoder, self).__init__((basestring, dict), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the machine_type option of a ProfitBricks VM config.

    Args:
      value: Either a string name of a PB machine type or a dict containing
          'cores' and 'ram' keys describing a custom VM.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      If value is a string, returns it unmodified. Otherwise, returns the
      decoded CustomMachineTypeSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    super(MachineTypeDecoder, self).Decode(value, component_full_name,
                                           flag_values)
    if isinstance(value, basestring):
      return value
    return CustomMachineTypeSpec(self._GetOptionFullName(component_full_name),
                                 flag_values=flag_values, **value)


class ProfitBricksVmSpec(virtual_machine.BaseVmSpec):
  """Object containing the information needed to create a
  ProfitBricksVirtualMachine.

  Attributes:
    ram: None or int. RAM value in MB for custom ProfitBricks VM.
    cores: None or int. CPU cores value for custom ProfitBricks VM.
  """

  CLOUD = providers.PROFITBRICKS

  def __init__(self, *args, **kwargs):
    super(ProfitBricksVmSpec, self).__init__(*args, **kwargs)
    if isinstance(self.machine_type, CustomMachineTypeSpec):
      logging.info('Using custom hardware configuration.')
      self.cores = self.machine_type.cores
      self.ram = self.machine_type.ram
      self.machine_type = 'Custom (RAM: {}, Cores: {})'.format(self.ram,
                                                               self.cores)
    else:
      logging.info('Using preset hardware configuration.')
      self.ram, self.cores = util.ReturnFlavor(self.machine_type)


  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(ProfitBricksVmSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['machine_type'].present:
      config_values['machine_type'] = yaml.load(flag_values.machine_type)
    if flag_values['profitbricks_location'].present:
      config_values['location'] = flag_values.profitbricks_location
    if flag_values['profitbricks_boot_volume_type'].present:
      config_values['boot_volume_type'] = \
          flag_values.profitbricks_boot_volume_type
    if flag_values['profitbricks_boot_volume_size'].present:
      config_values['boot_volume_size'] = \
          flag_values.profitbricks_boot_volume_size

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(ProfitBricksVmSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'machine_type': (MachineTypeDecoder, {}),
        'location': (option_decoders.StringDecoder, {'default': 'us/las'}),
        'boot_volume_type': (option_decoders.StringDecoder, {'default': 'HDD'}),
        'boot_volume_size': (option_decoders.IntDecoder, {'default': 10,
                                                          'min': 10})})
    return result


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

        self.server_id = None
        self.server_status = None
        self.dc_id = None
        self.dc_status = None
        self.lan_id = None
        self.lan_status = None
        self.max_local_disks = 1
        self.local_disk_counter = 0
        self.ram = vm_spec.ram
        self.cores = vm_spec.cores
        self.machine_type = vm_spec.machine_type
        self.image = self.image or self.DEFAULT_IMAGE
        self.boot_volume_type = vm_spec.boot_volume_type
        self.boot_volume_size = vm_spec.boot_volume_size
        self.location = vm_spec.location
        self.user_name = 'root'
        self.header = {
            'Authorization': 'Basic %s' % self.user_token,
            'Content-Type': 'application/vnd.profitbricks.resource+json',
        }

    def _Create(self):
        """Create a ProfitBricks VM instance."""

        # Grab ssh pub key to inject into new VM
        with open(self.ssh_public_key) as f:
            public_key = f.read().rstrip('\n')

        # Find an Ubuntu image that matches our location
        self.image = util.ReturnImage(self.header, self.location)

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
                                'size': self.boot_volume_size,
                                'name': 'boot volume',
                                'image': self.image,
                                'type': self.boot_volume_type,
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
        logging.info('Creating VM: %s' % self.name)

        # Parse Required values from response
        self.server_status = r.headers['Location']
        response = r.json()
        self.server_id = response['id']

        # The freshly created server will be in a locked and unusable
        # state for a while, and it cannot be deleted or modified in
        # this state. Wait for the action to finish and check the
        # reported result.
        if not self._WaitUntilReady(self.server_status):
            raise errors.Error('VM creation failed, see log.')

    @vm_util.Retry()
    def _PostCreate(self):
        """Get the instance's public IP address."""

        # Build URL
        url = '%s/datacenters/%s/servers/%s?depth=5' % (PROFITBRICKS_API,
                                                        self.dc_id,
                                                        self.server_id)

        # Perform Request
        r = util.PerformRequest('get', url, self.header)
        response = r.json()
        nic = response['entities']['nics']['items'][0]
        self.ip_address = nic['properties']['ips'][0]

    def _Delete(self):
        """Delete a ProfitBricks VM."""

        # Build URL
        url = '%s/datacenters/%s/servers/%s' % (PROFITBRICKS_API, self.dc_id,
                                                self.server_id)

        # Make call
        logging.info('Deleting VM: %s' % self.server_id)
        r = util.PerformRequest('delete', url, self.header)

        # Check to make sure deletion has finished
        delete_status = r.headers['Location']
        if not self._WaitUntilReady(delete_status):
            raise errors.Error('VM deletion failed, see log.')

    def _CreateDependencies(self):
        """Create a data center and LAN prior to creating VM."""

        # Create data center
        self.dc_id, self.dc_status = util.CreateDatacenter(self.header,
                                                           self.location)
        if not self._WaitUntilReady(self.dc_status):
            raise errors.Error('Data center creation failed, see log.')

        # Create LAN
        self.lan_id, self.lan_status = util.CreateLan(self.header,
                                                      self.dc_id)
        if not self._WaitUntilReady(self.lan_status):
            raise errors.Error('LAN creation failed, see log.')

    def _DeleteDependencies(self):
        """Delete a data center and LAN."""

        # Build URL
        url = '%s/datacenters/%s' % (PROFITBRICKS_API, self.dc_id)

        # Make call to delete data center
        logging.info('Deleting Datacenter: %s' % self.dc_id)
        r = util.PerformRequest('delete', url, self.header)

        # Check to make sure deletion has finished
        delete_status = r.headers['Location']
        if not self._WaitUntilReady(delete_status):
            raise errors.Error('Data center deletion failed, see log.')

    @vm_util.Retry(timeout=TIMEOUT, log_errors=False)
    def _WaitUntilReady(self, status_url):
        """Returns true if the ProfitBricks resource is ready."""

        # Poll resource for status update
        logging.info('Polling ProfitBricks resource.')
        r = util.PerformRequest('get', status_url, self.header)
        response = r.json()
        status = response['metadata']['status']

        # Keep polling resource until a "DONE" state is returned
        if status != 'DONE':
            raise Exception  # Exception triggers vm_util.Retry to go again

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
