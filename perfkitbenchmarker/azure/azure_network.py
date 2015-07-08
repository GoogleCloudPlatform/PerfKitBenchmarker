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

"""Module containing classes related to Azure VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in
the same project. See http://msdn.microsoft.com/library/azure/jj156007.aspx
for more information about Azure Virtual Networks.
"""

import json
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
AZURE_PATH = 'azure'
MAX_NAME_LENGTH = 24
SSH_PORT = 22
# Azure Storage Account types. See
# http://azure.microsoft.com/en-us/pricing/details/storage/ for more information
# about the different types.
LRS = 'LRS'
PLRS = 'PLRS'
ZRS = 'ZRS'
GRS = 'GRS'
RAGRS = 'RAGRS'

flags.DEFINE_enum(
    'azure_storage_type', LRS,
    [LRS, PLRS, ZRS, GRS, RAGRS],
    'The type of storage account to create. See '
    'http://azure.microsoft.com/en-us/pricing/details/storage/ for more '
    'information. To use remote ssd scratch disks, you must use PLRS. If you '
    'use PLRS, you must use the DS series of machines, or else VM creation '
    'will fail.')


class AzureFirewall(network.BaseFirewall):
  """An object representing the Azure Firewall equivalent.

  On Azure, endpoints are used to open ports instead of firewalls.
  """

  def AllowPort(self, vm, port):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      port: The local port to open.
    """
    if vm.is_static or port == SSH_PORT:
      return
    create_cmd = [AZURE_PATH,
                  'vm',
                  'endpoint',
                  'create',
                  vm.name,
                  str(port)]
    vm_util.IssueRetryableCommand(
        create_cmd + ['--endpoint-protocol=tcp'])
    vm_util.IssueRetryableCommand(
        create_cmd + ['--endpoint-protocol=udp'])

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class AzureAffinityGroup(resource.BaseResource):
  """Object representing an Azure Affinity Group."""

  def __init__(self, name, zone):
    super(AzureAffinityGroup, self).__init__()
    self.name = name
    self.zone = zone

  def _Create(self):
    """Creates the affinity group."""
    create_cmd = [AZURE_PATH,
                  'account',
                  'affinity-group',
                  'create',
                  '--location=%s' % self.zone,
                  '--label=%s' % self.name,
                  self.name]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the affinity group."""
    delete_cmd = [AZURE_PATH,
                  'account',
                  'affinity-group',
                  'delete',
                  '--quiet',
                  self.name]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the affinity group exists."""
    show_cmd = [AZURE_PATH,
                'account',
                'affinity-group',
                'show',
                '--json',
                self.name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True


class AzureStorageAccount(resource.BaseResource):
  """Object representing an Azure Storage Account."""

  def __init__(self, name, storage_type):
    super(AzureStorageAccount, self).__init__()
    self.name = name
    self.storage_type = storage_type

  def _Create(self):
    """Creates the storage account."""
    create_cmd = [AZURE_PATH,
                  'storage',
                  'account',
                  'create',
                  '--affinity-group=%s' % self.name,
                  '--type=%s' % self.storage_type,
                  self.name]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the storage account."""
    delete_cmd = [AZURE_PATH,
                  'storage',
                  'account',
                  'delete',
                  '--quiet',
                  self.name]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the storage account exists."""
    show_cmd = [AZURE_PATH,
                'storage',
                'account',
                'show',
                '--json',
                self.name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True


class AzureVirtualNetwork(resource.BaseResource):
  """Object representing an Azure Virtual Network."""

  def __init__(self, name):
    super(AzureVirtualNetwork, self).__init__()
    self.name = name

  def _Create(self):
    """Creates the virtual network."""
    create_cmd = [AZURE_PATH,
                  'network',
                  'vnet',
                  'create',
                  '--affinity-group=%s' % self.name,
                  self.name]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the virtual network."""
    delete_cmd = [AZURE_PATH,
                  'network',
                  'vnet',
                  'delete',
                  '--quiet',
                  self.name]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the storage account exists."""
    show_cmd = [AZURE_PATH,
                'network',
                'vnet',
                'show',
                '--json',
                self.name]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
    try:
      json.loads(stdout)
    except ValueError:
      return False
    return True


class AzureNetwork(network.BaseNetwork):
  """Object representing an Azure Network."""

  def __init__(self, zone):
    super(AzureNetwork, self).__init__(zone)
    name = ('perfkit%s%s' %
            (FLAGS.run_uri, str(uuid.uuid4())[-12:])).lower()[:MAX_NAME_LENGTH]
    self.affinity_group = AzureAffinityGroup(name, zone)
    self.storage_account = AzureStorageAccount(name, FLAGS.azure_storage_type)
    self.vnet = AzureVirtualNetwork(name)

  @vm_util.Retry()
  def Create(self):
    """Creates the actual network."""
    self.affinity_group.Create()

    self.storage_account.Create()

    self.vnet.Create()

  def Delete(self):
    """Deletes the actual network."""
    self.vnet.Delete()

    self.storage_account.Delete()

    self.affinity_group.Delete()
