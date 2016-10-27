# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Class to represent an Azure Virtual Machine object.

Zones:
run 'azure vm location list'
Machine Types:
http://msdn.microsoft.com/en-us/library/azure/dn197896.aspx
Images:
run 'azure vm image list'

All VM specifics are self-contained and the class provides methods to
operate on the VM: boot, shutdown, etc.
"""

import json

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS


# Per-VM resources are defined here.
class AzurePublicIPAddress(resource.BaseResource):
  def __init__(self, location, name):
    super(AzurePublicIPAddress, self).__init__()
    self.location = location
    self.name = name
    self.resource_group = azure_network.GetResourceGroup()

  def _Create(self):
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'public-ip', 'create',
         '--location', self.location,
         self.name] + self.resource_group.args)

  def _Exists(self):
    stdout, _, retcode = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'public-ip', 'show',
         '--json',
         self.name] + self.resource_group.args)

    return retcode == 0 and stdout != '{}\n'

  def GetIPAddress(self):
    stdout, _ = vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'network', 'public-ip', 'show',
         '--json',
         self.name] + self.resource_group.args)

    response = json.loads(stdout)
    return response['ipAddress']

  def _Delete(self):
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'public-ip', 'delete',
         '--resource-group', self.resource_group.name,
         '--quiet',
         self.name])


class AzureNIC(resource.BaseResource):
  def __init__(self, subnet, name):
    super(AzureNIC, self).__init__()
    self.subnet = subnet
    self.name = name
    self.resource_group = azure_network.GetResourceGroup()
    self.location = self.subnet.vnet.location
    self.args = ['--nic-name', self.name]

  def _Create(self):
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'nic', 'create',
         '--location', self.location,
         '--subnet-vnet-name', self.subnet.vnet.name,
         '--subnet-name', self.subnet.name,
         self.name] + self.resource_group.args)

  def _Exists(self):
    # Same deal as AzurePublicIPAddress. 'show' doesn't error out if
    # the resource doesn't exist, but no-op 'set' does.
    stdout, _, retcode = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'nic', 'show',
         '--json',
         self.name] + self.resource_group.args)

    return retcode == 0 and stdout != '{}\n'

  def GetInternalIP(self):
    """Grab some data."""

    stdout, _ = vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'network', 'nic', 'show',
         '--json',
         self.name] + self.resource_group.args)

    response = json.loads(stdout)
    return response['ipConfigurations'][0]['privateIPAddress']

  def _Delete(self):
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'network', 'nic', 'delete',
         '--quiet',
         self.name] + self.resource_group.args)


class AzureVirtualMachineMetaClass(virtual_machine.AutoRegisterVmMeta):
  """Metaclass for AzureVirtualMachine.

  Registers default image pattern for each operating system.
  """

  def __init__(cls, name, bases, dct):
    super(AzureVirtualMachineMetaClass, cls).__init__(name, bases, dct)
    if hasattr(cls, 'OS_TYPE'):
      assert cls.OS_TYPE, '{0} did not override OS_TYPE'.format(cls.__name__)
      assert cls.IMAGE_URN, (
          '{0} did not override IMAGE_URN'.format(cls.__name__))


class AzureVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an Azure Virtual Machine."""

  __metaclass__ = AzureVirtualMachineMetaClass
  CLOUD = providers.AZURE
  # Subclasses should override IMAGE_URN.
  IMAGE_URN = None

  def __init__(self, vm_spec):
    """Initialize an Azure virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(AzureVirtualMachine, self).__init__(vm_spec)
    self.network = azure_network.AzureNetwork.GetNetwork(self)
    self.firewall = azure_network.AzureFirewall.GetFirewall()
    self.max_local_disks = 1

    self.resource_group = azure_network.GetResourceGroup()
    self.public_ip = AzurePublicIPAddress(self.zone, self.name + '-public-ip')
    self.nic = AzureNIC(self.network.subnet,
                        self.name + '-nic')
    self.storage_account = self.network.storage_account

    disk_spec = disk.BaseDiskSpec('azure_os_disk')
    self.os_disk = azure_disk.AzureDisk(disk_spec,
                                        self.name,
                                        self.machine_type,
                                        self.storage_account,
                                        None,
                                        is_image=True)

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.public_ip.Create()
    self.nic.Create()

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    if not self.os_disk.is_image:
      self.os_disk.Delete()

    # Delete the public IP after the NIC to avoid an error that you
    # can't delete a public IP associated with a NIC.
    self.nic.Delete()
    self.public_ip.Delete()

  def _Create(self):
    create_cmd = (
        [azure.AZURE_PATH, 'vm', 'create',
         '--location', self.zone,
         '--image-urn', self.IMAGE_URN,
         '--os-type', 'Linux',
         '--ssh-publickey-file', self.ssh_public_key,
         '--vm-size', self.machine_type,
         '--public-ip-name', self.public_ip.name,
         '--storage-account-name', self.storage_account.name,
         '--admin-username', self.user_name,
         self.name] +
        self.resource_group.args +
        self.network.vnet.args +
        self.network.subnet.args +
        self.nic.args)

    if self.password:
      create_cmd.extend(['--admin-password', self.password])
    else:
      create_cmd.extend(['--ssh-publickey-file', self.ssh_public_key])
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'vm', 'delete',
         '--quiet',
         self.name] + self.resource_group.args)

  def _Exists(self):
    """Returns true if the VM exists and attempts to get some data."""

    # 'azure vm show' returns 0 even if the VM exists, and the no-op
    # 'set' trick doesn't work like it does for other resources
    # because 'vm set' doesn't allow no-ops, so we are forced to
    # actually parse the results of 'show'.
    stdout, _, retcode = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'vm', 'show',
         '--json',
         self.name] + self.resource_group.args)

    return retcode == 0 and stdout != '{}\n'

  @vm_util.Retry()
  def _PostCreate(self):
    """Get VM data."""
    stdout, _ = vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'show',
         '--json',
         self.name] + self.resource_group.args)
    response = json.loads(stdout)
    self.os_disk.name = response['storageProfile']['osDisk']['name']
    self.os_disk.created = True
    self.internal_ip = self.nic.GetInternalIP()
    self.ip_address = self.public_ip.GetIPAddress()

  def AddMetadata(self, **tags):
    tag_string = ';'.join(
        ['%s=%s' % (key, value)
         for key, value in tags.iteritems()])
    vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'set',
         self.name,
         '--tags', tag_string] +
        self.resource_group.args)

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    disks = []

    for disk_idx in xrange(disk_spec.num_striped_disks):
      data_disk = azure_disk.AzureDisk(disk_spec, self.name,
                                       self.machine_type, self.storage_account,
                                       disk_idx)
      if disk_spec.disk_type == disk.LOCAL:
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


class DebianBasedAzureVirtualMachine(AzureVirtualMachine,
                                     linux_virtual_machine.DebianMixin):
  IMAGE_URN = 'Canonical:UbuntuServer:14.04.4-LTS:latest'


class RhelBasedAzureVirtualMachine(AzureVirtualMachine,
                                   linux_virtual_machine.RhelMixin):
  IMAGE_URN = 'RedHat:RHEL:7.2:latest'


class WindowsAzureVirtualMachine(AzureVirtualMachine,
                                 windows_virtual_machine.WindowsMixin):
  IMAGE_URN = 'MicrosoftWindowsServer:WindowsServer:2012-R2-Datacenter:latest'

  def __init__(self, vm_spec):
    super(WindowsAzureVirtualMachine, self).__init__(vm_spec)
    self.user_name = self.name
    self.password = vm_util.GenerateRandomWindowsPassword()

  def _PostCreate(self):
    super(WindowsAzureVirtualMachine, self)._PostCreate()
    config_dict = {'commandToExecute': windows_virtual_machine.STARTUP_SCRIPT}
    config = json.dumps(config_dict)
    vm_util.IssueRetryableCommand(
        [azure.AZURE_PATH, 'vm', 'extension', 'set',
         '--vm-name', self.name,
         'CustomScriptExtension',
         'Microsoft.Compute',
         '1.4',
         '--public-config=%s' % config] + self.resource_group.args)
