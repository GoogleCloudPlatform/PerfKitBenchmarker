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
"""Module containing classes related to Azure VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in
the same project. See http://msdn.microsoft.com/library/azure/jj156007.aspx
for more information about Azure Virtual Networks.
"""

import json
import logging
import threading

from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_placement_group
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS
SSH_PORT = 22

DEFAULT_LOCATION = 'eastus2'

LOCATION = 'location'
ZONE = 'zone'


def GetResourceGroup(zone=None):
  """Get the resource group for the current benchmark."""
  spec = context.GetThreadBenchmarkSpec()
  # This is protected by spec.networks_lock, so there's no race
  # condition with checking for the attribute and then creating a
  # resource group.
  try:
    return spec.azure_resource_group
  except AttributeError:
    group = AzureResourceGroup(
        'pkb%s-%s' % (FLAGS.run_uri, spec.uid), zone=zone)
    spec.azure_resource_group = group
    return group


class AzureResourceGroup(resource.BaseResource):
  """A Resource Group, the basic unit of Azure provisioning."""

  def __init__(self,
               name,
               zone=None,
               use_existing=False,
               timeout_minutes=None,
               raise_on_create_failure=True):
    super(AzureResourceGroup, self).__init__()
    self.name = name
    self.use_existing = use_existing
    self.timeout_minutes = timeout_minutes
    self.raise_on_create_failure = raise_on_create_failure
    # A resource group's location doesn't affect the location of
    # actual resources, but we need to choose *some* location for every
    # benchmark, even if the user doesn't specify one.
    self.location = util.GetLocationFromZone(
        FLAGS.zones[0] if FLAGS.zones else zone or DEFAULT_LOCATION)
    # Whenever an Azure CLI command needs a resource group, it's
    # always specified the same way.
    self.args = ['--resource-group', self.name]

  def _Create(self):
    if not self.use_existing:
      # A resource group can own resources in multiple zones, but the
      # group itself needs to have a location. Therefore,
      # FLAGS.zones[0].
      _, _, retcode = vm_util.IssueCommand(
          [
              azure.AZURE_PATH, 'group', 'create', '--name', self.name,
              '--location', self.location, '--tags'
          ] + util.GetTags(self.timeout_minutes),
          raise_on_failure=False)

      if retcode and self.raise_on_create_failure:
        raise errors.Resource.RetryableCreationError(
            'Error creating Azure resource group')

  def _Exists(self):
    stdout, _, _ = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'group', 'show', '--name', self.name],
        suppress_warning=True,
        raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def _Delete(self):
    # Ignore delete failures (potentially already deleted) and timeouts as
    # delete should complete even if we stop waiting for the response.
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'group', 'delete', '--yes', '--name', self.name],
        timeout=600,
        raise_on_failure=False,
        raise_on_timeout=False)

  def AddTag(self, key, value):
    """Add a single tag to an existing Resource Group.

    Args:
      key: tag key
      value: tag value

    Raises:
      errors.resource.CreationError on failure.
    """
    tag_cmd = [
        azure.AZURE_PATH, 'group', 'update', '--name', self.name, '--set',
        'tags.' + util.FormatTag(key, value)
    ]
    _, _, retcode = vm_util.IssueCommand(tag_cmd, raise_on_failure=False)
    if retcode:
      raise errors.Resource.CreationError('Error tagging Azure resource group.')


class AzureStorageAccount(resource.BaseResource):
  """Object representing an Azure Storage Account."""

  total_storage_accounts = 0

  def __init__(self,
               storage_type,
               location,
               name,
               kind=None,
               access_tier=None,
               resource_group=None,
               use_existing=False,
               raise_on_create_failure=True):
    super(AzureStorageAccount, self).__init__()
    self.storage_type = storage_type
    self.name = name
    self.resource_group = resource_group or GetResourceGroup()
    self.location = location
    self.kind = kind or 'Storage'
    self.use_existing = use_existing
    self.raise_on_create_failure = raise_on_create_failure

    AzureStorageAccount.total_storage_accounts += 1

    if kind == 'BlobStorage':
      self.access_tier = access_tier or 'Hot'
    else:
      # Access tiers are only valid for blob storage accounts.
      assert access_tier is None
      self.access_tier = access_tier

  def _Create(self):
    """Creates the storage account."""
    if not self.use_existing:
      create_cmd = [
          azure.AZURE_PATH, 'storage', 'account', 'create', '--kind', self.kind,
          '--sku', self.storage_type, '--name', self.name, '--tags'
      ] + util.GetTags(
          self.resource_group.timeout_minutes) + self.resource_group.args
      if self.location:
        create_cmd.extend(['--location', self.location])
      if self.kind == 'BlobStorage':
        create_cmd.extend(['--access-tier', self.access_tier])
      vm_util.IssueCommand(
          create_cmd, raise_on_failure=self.raise_on_create_failure)

  def _PostCreate(self):
    """Get our connection string and our keys."""
    self.connection_string = util.GetAzureStorageConnectionString(
        self.name, self.resource_group.args)
    self.connection_args = ['--connection-string', self.connection_string]
    self.key = util.GetAzureStorageAccountKey(self.name,
                                              self.resource_group.args)

  def _Delete(self):
    """Deletes the storage account."""
    delete_cmd = [
        azure.AZURE_PATH, 'storage', 'account', 'delete', '--name', self.name,
        '--yes'
    ] + self.resource_group.args
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def _Exists(self):
    """Returns true if the storage account exists."""
    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH, 'storage', 'account', 'show', '--output', 'json',
            '--name', self.name
        ] + self.resource_group.args,
        suppress_warning=True,
        raise_on_failure=False)

    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False


class AzureVirtualNetwork(network.BaseNetwork):
  """Object representing an Azure Virtual Network.

  The benchmark spec contains one instance of this class per region, which an
  AzureNetwork may retrieve or create via AzureVirtualNetwork.GetForLocation.

  Attributes:
    name: string. Name of the virtual network.
    resource_group: Resource Group instance that network belongs to.
    location: string. Azure location of the network.
  """

  vnet_lock = threading.Lock()

  CLOUD = providers.AZURE

  def __init__(self, spec, location, name, number_subnets):
    super(AzureVirtualNetwork, self).__init__(spec)
    self.name = name
    self.resource_group = GetResourceGroup()
    self.location = location
    self.args = ['--vnet-name', self.name]
    self.address_index = 0
    self.address_spaces = [
        '10.%s.0.0/16' % zone_num for zone_num in range(number_subnets)
    ]
    self.is_created = False

  @classmethod
  def GetForLocation(cls, spec, location, name, number_subnets=1):
    """Retrieves or creates an AzureVirtualNetwork.

    Args:
      spec: BaseNetworkSpec. Spec for Azure Network.
      location: string. Azure region name.
      name: string. Azure Network Name.
      number_subnets: int. Optional. Number of subnets that network will
        contain.

    Returns:
      AzureVirtualNetwork. If an AzureVirtualNetwork for the same region already
      exists in the benchmark spec, that instance is returned. Otherwise, a new
      AzureVirtualNetwork is created and returned.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    key = cls.CLOUD, LOCATION, location
    # Because this method is only called from the AzureNetwork constructor,
    # which is only called from AzureNetwork.GetNetwork, we already hold the
    # benchmark_spec.networks_lock.
    number_subnets = max(number_subnets, len(FLAGS.zones))
    if key not in benchmark_spec.networks:
      benchmark_spec.networks[key] = cls(spec, location, name, number_subnets)
    return benchmark_spec.networks[key]

  def GetNextAddressSpace(self):
    """Returns the next available address space for next subnet."""
    with self.vnet_lock:
      assert self.address_index < len(
          self.address_spaces), 'Only allocated {} addresses'.format(
              len(self.address_spaces))
      next_address_space = self.address_spaces[self.address_index]
      self.address_index += 1
      return next_address_space

  def Create(self):
    """Creates the virtual network."""
    with self.vnet_lock:
      if self.is_created:
        return

      logging.info('Creating %d Azure subnets in %s', len(self.address_spaces),
                   self.location)
      vm_util.IssueRetryableCommand([
          azure.AZURE_PATH, 'network', 'vnet', 'create', '--location', self
          .location, '--name', self.name, '--address-prefixes'
      ] + self.address_spaces + self.resource_group.args)

      self.is_created = True

  def Delete(self):
    """Deletes the virtual network."""
    pass

  @vm_util.Retry()
  def Exists(self):
    """Returns true if the virtual network exists."""
    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH, 'network', 'vnet', 'show', '--output', 'json',
            '--name', self.name
        ] + self.resource_group.args,
        suppress_warning=True,
        raise_on_failure=False)

    return bool(json.loads(stdout))


class AzureSubnet(resource.BaseResource):
  """Object representing an Azure Subnet."""

  def __init__(self, vnet, name):
    super(AzureSubnet, self).__init__()
    self.resource_group = GetResourceGroup()
    self.vnet = vnet
    self.name = name
    self.args = ['--subnet', self.name]
    self.address_space = None

  def _Create(self):
    # Avoids getting additional address space when create retries.
    if not self.address_space:
      self.address_space = self.vnet.GetNextAddressSpace()

    vm_util.IssueCommand([
        azure.AZURE_PATH, 'network', 'vnet', 'subnet', 'create', '--vnet-name',
        self.vnet.name, '--address-prefix', self.address_space, '--name',
        self.name
    ] + self.resource_group.args)

  @vm_util.Retry()
  def _Exists(self):
    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH, 'network', 'vnet', 'subnet', 'show',
            '--vnet-name', self.vnet.name, '--output', 'json', '--name',
            self.name
        ] + self.resource_group.args,
        raise_on_failure=False)

    return bool(json.loads(stdout))

  def _Delete(self):
    pass


class AzureNetworkSecurityGroup(resource.BaseResource):
  """Object representing an Azure Network Security Group."""

  def __init__(self, location, subnet, name):
    super(AzureNetworkSecurityGroup, self).__init__()

    self.location = location
    self.subnet = subnet
    self.name = name
    self.resource_group = GetResourceGroup()
    self.args = ['--nsg', self.name]

    self.rules_lock = threading.Lock()
    # Mapping of (start_port, end_port, source) -> rule name, used to
    # deduplicate rules. We expect duplicate rules because PKB will
    # call AllowPort() for each VM on a subnet, but the rules are
    # actually applied to the entire subnet.
    self.rules = {}
    # True if the special 'DenyAll' rule is present.
    self.have_deny_all_rule = False

  def _Create(self):
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'network', 'nsg', 'create', '--location',
        self.location, '--name', self.name
    ] + self.resource_group.args)

  @vm_util.Retry()
  def _Exists(self):
    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH, 'network', 'nsg', 'show', '--output', 'json',
            '--name', self.name
        ] + self.resource_group.args,
        raise_on_failure=False)

    return bool(json.loads(stdout))

  def _Delete(self):
    pass

  def AttachToSubnet(self):
    vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'network', 'vnet', 'subnet', 'update', '--name',
        self.subnet.name, '--network-security-group', self.name
    ] + self.resource_group.args + self.subnet.vnet.args)

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Open a port or port range.

    Args:
      vm: the virtual machine to open the port for.
      start_port: either a single port or the start of a range.
      end_port: if given, the end of the port range.
      source_range: List of source CIDRs to allow for this port. If None, all
        sources are allowed. i.e. ['0.0.0.0/0']

    Raises:
      ValueError: when there are too many firewall rules.
    """
    source_range = source_range or ['0.0.0.0/0']
    end_port = end_port or start_port

    source_range.sort()
    # Replace slashes as they are not allowed in an azure rule name.
    source_range_str = ','.join(source_range).replace('/', '_')
    rule = (start_port, end_port, source_range_str)
    with self.rules_lock:
      if rule in self.rules:
        return
      port_range = '%s-%s' % (start_port, end_port)
      rule_name = 'allow-%s-%s' % (port_range, source_range_str)
      # Azure priorities are between 100 and 4096, but we reserve 4095
      # for the special DenyAll rule created by DisallowAllPorts.
      rule_priority = 100 + len(self.rules)
      if rule_priority >= 4095:
        raise ValueError('Too many firewall rules!')
      self.rules[rule] = rule_name

    network_cmd = [
        azure.AZURE_PATH, 'network', 'nsg', 'rule', 'create', '--name',
        rule_name, '--destination-port-range', port_range, '--access', 'Allow',
        '--priority',
        str(rule_priority)
    ] + ['--source-address-prefixes'] + source_range
    network_cmd.extend(self.resource_group.args + self.args)
    vm_util.IssueRetryableCommand(network_cmd)


class AzureFirewall(network.BaseFirewall):
  """A fireall on Azure is a Network Security Group.

  NSGs are per-subnet, but this class is per-provider, so we just
  proxy methods through to the right NSG instance.
  """

  CLOUD = providers.AZURE

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The local port to open.
      end_port: if given, open the range [start_port, end_port].
      source_range: unsupported at present.
    """

    vm.network.nsg.AllowPort(
        vm, start_port, end_port=end_port, source_range=source_range)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class AzureNetwork(network.BaseNetwork):
  """Locational network components.

  A container object holding all of the network-related objects that
  we need for an Azure zone (aka location).
  """

  CLOUD = providers.AZURE

  def __init__(self, spec):
    super(AzureNetwork, self).__init__(spec)
    self.resource_group = GetResourceGroup()
    self.location = util.GetLocationFromZone(self.zone)
    self.availability_zone = util.GetAvailabilityZoneFromZone(self.zone)
    placement_group_spec = azure_placement_group.AzurePlacementGroupSpec(
        'AzurePlacementGroupSpec',
        flag_values=FLAGS,
        zone=self.zone,
        resource_group=self.resource_group.name)

    is_dedicated_host = bool(FLAGS.dedicated_hosts)
    in_availability_zone = bool(self.availability_zone)
    no_placement_group = (
        FLAGS.placement_group_style == placement_group.PLACEMENT_GROUP_NONE)
    cluster_placement_group = (
        FLAGS.placement_group_style == placement_group.PLACEMENT_GROUP_CLUSTER)
    spread_placement_group = (
        FLAGS.placement_group_style == placement_group.PLACEMENT_GROUP_SPREAD)

    # With dedicated hosting and/or an availability zone, an availability set
    # cannot be created
    if is_dedicated_host or no_placement_group or (in_availability_zone and
                                                   spread_placement_group):
      self.placement_group = None
    elif cluster_placement_group:
      self.placement_group = azure_placement_group.AzureProximityGroup(
          placement_group_spec)
    else:
      self.placement_group = azure_placement_group.AzureAvailSet(
          placement_group_spec)

    # Storage account names can't include separator characters :(.
    storage_account_prefix = 'pkb%s' % FLAGS.run_uri

    # Storage account names must be 3-24 characters long and use
    # numbers and lower-case letters only, which leads us to this
    # awful naming scheme.
    suffix = 'storage%d' % AzureStorageAccount.total_storage_accounts
    self.storage_account = AzureStorageAccount(
        FLAGS.azure_storage_type, self.location,
        storage_account_prefix[:24 - len(suffix)] + suffix)
    prefix = '%s-%s' % (self.resource_group.name, self.location)
    self.vnet = AzureVirtualNetwork.GetForLocation(spec, self.location,
                                                   prefix + '-vnet')
    subnet_name = self.vnet.name
    if self.availability_zone:
      subnet_name += '-' + self.availability_zone
    subnet_name += '-subnet'
    self.subnet = AzureSubnet(self.vnet, subnet_name)
    self.nsg = AzureNetworkSecurityGroup(self.location, self.subnet,
                                         self.subnet.name + '-nsg')

  @vm_util.Retry()
  def Create(self):
    """Creates the network."""
    # If the benchmark includes multiple zones,
    # self.resource_group.Create() will be called more than once. But
    # BaseResource will prevent us from running the underlying Azure
    # commands more than once, so that is fine.
    self.resource_group.Create()

    if self.placement_group:
      self.placement_group.Create()

    self.storage_account.Create()

    self.vnet.Create()

    self.subnet.Create()

    self.nsg.Create()
    self.nsg.AttachToSubnet()

  def Delete(self):
    """Deletes the network."""
    # If the benchmark includes multiple zones, this will be called
    # multiple times, but there will be no bad effects from multiple
    # deletes.
    self.resource_group.Delete()

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    return (cls.CLOUD, ZONE, spec.zone)
