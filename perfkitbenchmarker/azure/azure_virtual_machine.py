# Copyright 2014 Google Inc. All rights reserved.
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
from perfkitbenchmarker.azure import azure_disk
from perfkitbenchmarker.azure import azure_network

FLAGS = flags.FLAGS

AZURE_PATH = 'azure'
UBUNTU_IMAGE = ('b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-'
                '14_04_1-LTS-amd64-server-20150123-en-us-30GB')
CENTOS_IMAGE = ('0b11de9248dd4d87b18621318e037d37__RightImage-'
                'CentOS-7.0-x64-v14.2.1')


class AzureService(resource.BaseResource):
  """Object representing an Azure Service."""

  def __init__(self, name, affinity_group_name):
    super(AzureService, self).__init__()
    self.name = name
    self.affinity_group_name = affinity_group_name

  def _Create(self):
    """Creates the Azure service."""
    create_cmd = [AZURE_PATH,
                  'service',
                  'create',
                  '--affinitygroup=%s' % self.affinity_group_name,
                  self.name]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the Azure service."""
    delete_cmd = [AZURE_PATH,
                  'service',
                  'delete',
                  '--quiet',
                  self.name]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the service exists."""
    show_cmd = [AZURE_PATH,
                'service',
                'show',
                '--json',
                self.name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True


class AzureVirtualMachine(virtual_machine.BaseVirtualMachine):
  """Object representing an Azure Virtual Machine."""

  DEFAULT_ZONE = 'East US'
  DEFAULT_MACHINE_TYPE = 'Small'
  # Subclasses should override the default image.
  DEFAULT_IMAGE = None

  def __init__(self, vm_spec):
    """Initialize a Azure virtual machine.

    Args:
      vm_spec: virtual_machine.BaseVirtualMachineSpec object of the vm.
    """
    super(AzureVirtualMachine, self).__init__(vm_spec)
    self.network = azure_network.AzureNetwork.GetNetwork(self.zone)
    self.service = AzureService(self.name,
                                self.network.affinity_group.name)
    disk_spec = disk.BaseDiskSpec(None, None, None)
    self.os_disk = azure_disk.AzureDisk(disk_spec, self.name)
    self.max_local_disks = 1
    self.local_disk_counter = 0

  @classmethod
  def SetVmSpecDefaults(cls, vm_spec):
    """Updates the VM spec with cloud specific defaults."""
    if vm_spec.machine_type is None:
      vm_spec.machine_type = cls.DEFAULT_MACHINE_TYPE
    if vm_spec.zone is None:
      vm_spec.zone = cls.DEFAULT_ZONE
    if vm_spec.image is None:
      vm_spec.image = cls.DEFAULT_IMAGE

  def _CreateDependencies(self):
    """Create VM dependencies."""
    self.service.Create()

  def _DeleteDependencies(self):
    """Delete VM dependencies."""
    self.os_disk.Delete()
    self.service.Delete()

  def _Create(self):
    create_cmd = [AZURE_PATH,
                  'vm',
                  'create',
                  '--ssh-cert=%s' % vm_util.GetCertPath(),
                  '--ssh=22',
                  '--no-ssh-password',
                  '--affinity-group=%s' % self.network.affinity_group.name,
                  '--virtual-network-name=%s' % self.network.vnet.name,
                  '--vm-size=%s' % self.machine_type,
                  self.name,
                  self.image,
                  self.user_name]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    delete_cmd = [AZURE_PATH,
                  'vm',
                  'delete',
                  '--quiet',
                  self.name]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the VM exists and attempts to get some data."""
    show_cmd = [AZURE_PATH,
                'vm',
                'show',
                '--json',
                self.name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True

  @vm_util.Retry()
  def _PostCreate(self):
    """Get VM data."""
    show_cmd = [AZURE_PATH,
                'vm',
                'show',
                '--json',
                self.name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd)
    response = json.loads(stdout)
    self.os_disk.name = response['OSDisk']['name']
    self.os_disk.created = True
    self.internal_ip = response['IPAddress']
    self.ip_address = response['VirtualIPAddresses'][0]['address']

  def CreateScratchDisk(self, disk_spec):
    """Create a VM's scratch disk.

    Args:
      disk_spec: virtual_machine.BaseDiskSpec object of the disk.
    """
    if disk_spec.disk_type == disk.LOCAL:
      self.local_disk_counter += disk_spec.num_striped_disks
      if self.local_disk_counter > self.max_local_disks:
        raise errors.Error('Not enough local disks.')

    # Instantiate the disk(s) that we want to create.
    disks = [azure_disk.AzureDisk(disk_spec, self.name)
             for _ in range(disk_spec.num_striped_disks)]

    self._CreateScratchDiskFromDisks(disk_spec, disks)


  def GetLocalDisks(self):
    """Returns a list of local disks on the VM.

    Returns:
      A list of strings, where each string is the absolute path to the local
          disks on the VM (e.g. '/dev/sdb').
    """
    return ['/dev/sdb']


class DebianBasedAzureVirtualMachine(AzureVirtualMachine,
                                     linux_virtual_machine.DebianMixin):
  DEFAULT_IMAGE = UBUNTU_IMAGE


class RhelBasedAzureVirtualMachine(AzureVirtualMachine,
                                   linux_virtual_machine.RhelMixin):
  DEFAULT_IMAGE = CENTOS_IMAGE
