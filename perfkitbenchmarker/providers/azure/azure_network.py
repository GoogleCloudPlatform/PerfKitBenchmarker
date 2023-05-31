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
from typing import Optional

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_placement_group
from perfkitbenchmarker.providers.azure import flags as azure_flags
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS
SSH_PORT = 22

flags.DEFINE_boolean('azure_infiniband', False,
                     'Install Mellanox OpenFabrics drivers')
_AZ_VERSION_LOG = flags.DEFINE_boolean(
    'azure_version_log', True, 'Log az --version.'
)

DEFAULT_REGION = 'eastus2'

REGION = 'region'
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
  _az_lock = threading.Condition()
  _az_version: Optional[str] = None

  def __init__(self,
               name,
               zone=None,
               use_existing=False,
               timeout_minutes=None,
               raise_on_create_failure=True):
    super(AzureResourceGroup, self).__init__()
    AzureResourceGroup._log_az_version()
    self.name = name
    self.use_existing = use_existing
    self.timeout_minutes = timeout_minutes
    self.raise_on_create_failure = raise_on_create_failure
    # A resource group's region doesn't affect the region of
    # actual resources, but we need to choose *some* region for every
    # benchmark, even if the user doesn't specify one.
    self.region = util.GetRegionFromZone(
        FLAGS.zone[0] if FLAGS.zone else zone or DEFAULT_REGION)
    # Whenever an Azure CLI command needs a resource group, it's
    # always specified the same way.
    self.args = ['--resource-group', self.name]

  def _Create(self):
    if not self.use_existing:
      # A resource group can own resources in multiple zones, but the
      # group itself needs to have a region. Therefore,
      # FLAGS.zone[0].
      _, _, retcode = vm_util.IssueCommand(
          [
              azure.AZURE_PATH, 'group', 'create', '--name', self.name,
              '--location', self.region, '--tags'
          ] + util.GetTags(self.timeout_minutes),
          raise_on_failure=False)

      if retcode and self.raise_on_create_failure:
        raise errors.Resource.RetryableCreationError(
            'Error creating Azure resource group')

  def _Exists(self):
    stdout, _, _ = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'group', 'show', '--name', self.name],
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

  @classmethod
  def _log_az_version(cls):
    if not _AZ_VERSION_LOG.value:
      return

    with cls._az_lock:
      if not cls._az_version:
        cls.az_version, _, _ = vm_util.IssueCommand(
            [azure.AZURE_PATH, '--version'],
            raise_on_failure=True)


class AzureStorageAccount(resource.BaseResource):
  """Object representing an Azure Storage Account."""

  total_storage_accounts = 0

  def __init__(self,
               storage_type,
               region,
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
    self.region = region
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
      if self.region:
        create_cmd.extend(['--location', self.region])
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
        raise_on_failure=False)

    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False


class AzureVirtualNetwork(network.BaseNetwork):
  """Object representing an Azure Virtual Network.

  The benchmark spec contains one instance of this class per region, which an
  AzureNetwork may retrieve or create via AzureVirtualNetwork.GetForRegion.

  Attributes:
    name: string. Name of the virtual network.
    resource_group: Resource Group instance that network belongs to.
    region: string. Azure region of the network.
  """

  # Initializes an address space for a new AzureVirtualNetwork
  _regional_network_count = 0
  vnet_lock = threading.Lock()

  CLOUD = provider_info.AZURE

  def __init__(self, spec, region, name, number_subnets):
    super(AzureVirtualNetwork, self).__init__(spec)
    self.name = name
    self.resource_group = GetResourceGroup()
    self.region = region
    self.args = ['--vnet-name', self.name]
    self.address_index = 0
    self.regional_index = AzureVirtualNetwork._regional_network_count
    self.address_spaces = []
    for zone_num in range(number_subnets):
      self.address_spaces.append(
          network.GetCidrBlock(self.regional_index, zone_num))
    self.is_created = False

  @classmethod
  def GetForRegion(cls, spec, region, name, number_subnets=1
                   ) -> Optional['AzureVirtualNetwork']:
    """Retrieves or creates an AzureVirtualNetwork.

    Args:
      spec: BaseNetworkSpec. Spec for Azure Network.
      region: string. Azure region name.
      name: string. Azure Network Name.
      number_subnets: int. Optional. Number of subnets that network will
        contain.

    Returns:
      AzureVirtualNetwork | None. If AZURE_SUBNET_ID is specified, an existing
      network is used via a subnet, and PKB does not add it to the benchmark
      spec so this method returns None.
      If an AzureVirtualNetwork for the same region already exists in the
      benchmark spec, that instance is returned. Otherwise, a new
      AzureVirtualNetwork is created and returned.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    if azure_flags.AZURE_SUBNET_ID.value:
      # AzureVirtualNetworks are not Resources, so we just return None here as
      # the network exists but will be accessed via the subnet.
      return None
    key = cls.CLOUD, REGION, region
    # Because this method is only called from the AzureNetwork constructor,
    # which is only called from AzureNetwork.GetNetwork, we already hold the
    # benchmark_spec.networks_lock.
    number_subnets = max(number_subnets, len(FLAGS.zone))
    if key not in benchmark_spec.regional_networks:
      benchmark_spec.regional_networks[key] = cls(spec, region, name,
                                                  number_subnets)
      AzureVirtualNetwork._regional_network_count += 1
    return benchmark_spec.regional_networks[key]

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
                   self.region)
      vm_util.IssueRetryableCommand([
          azure.AZURE_PATH, 'network', 'vnet', 'create', '--location', self
          .region, '--name', self.name, '--address-prefixes'
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
        raise_on_failure=False)

    return bool(json.loads(stdout))


class AzureSubnet(resource.BaseResource):
  """Object representing an Azure Subnet."""

  def __init__(self, vnet, name):
    super(AzureSubnet, self).__init__()
    if azure_flags.AZURE_SUBNET_ID.value:
      # use pre-existing subnet
      self.id = azure_flags.AZURE_SUBNET_ID.value
      self.user_managed = True
    else:
      # create a new subnet.
      # id could be set after _Exists succeeds, but is only used by AzureNIC()
      # before AzureSubnet.Create() is called.
      self.id = None
      self.user_managed = False

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

  def __init__(self, region, subnet, name):
    super(AzureNetworkSecurityGroup, self).__init__()

    self.region = region
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
        self.region, '--name', self.name
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

  def _GetRulePriority(self, rule, rule_name):
    # Azure priorities are between 100 and 4096, but we reserve 4095
    # for the special DenyAll rule created by DisallowAllPorts.
    rule_priority = 100 + len(self.rules)
    if rule_priority >= 4095:
      raise ValueError('Too many firewall rules!')
    self.rules[rule] = rule_name
    return rule_priority

  def AttachToSubnet(self):
    vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'network', 'vnet', 'subnet', 'update', '--name',
        self.subnet.name, '--network-security-group', self.name
    ] + self.resource_group.args + self.subnet.vnet.args)

  def AllowPort(
      self,
      vm,
      start_port,
      end_port=None,
      source_range=None,
      protocol='*'):
    """Open a port or port range.

    Args:
      vm: the virtual machine to open the port for.
      start_port: either a single port or the start of a range.
      end_port: if given, the end of the port range.
      source_range: List of source CIDRs to allow for this port. If None, all
        sources are allowed. i.e. ['0.0.0.0/0']
      protocol: Network protocol this rule applies to. One of {*, Ah, Esp, Icmp,
        Tcp, Udp}. See
        https://learn.microsoft.com/en-us/cli/azure/network/nsg/rule?view=azure-cli-latest#az-network-nsg-rule-create

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
      rule_priority = self._GetRulePriority(rule, rule_name)

    network_cmd = [
        azure.AZURE_PATH, 'network', 'nsg', 'rule', 'create', '--name',
        rule_name, '--destination-port-range', port_range,
        '--protocol', protocol, '--access', 'Allow',
        '--priority',
        str(rule_priority)
    ] + ['--source-address-prefixes'] + source_range
    network_cmd.extend(self.resource_group.args + self.args)
    vm_util.IssueRetryableCommand(network_cmd)

  def AllowIcmp(self):
    source_address = '0.0.0.0/0'
    # '*' in Azure represents all ports
    rule = ('*', source_address)
    rule_name = 'allow-icmp'
    with self.rules_lock:
      if rule in self.rules:
        return
      rule_priority = self._GetRulePriority(rule, rule_name)
      network_cmd = [
          azure.AZURE_PATH, 'network', 'nsg', 'rule', 'create', '--name',
          rule_name, '--access', 'Allow', '--source-address-prefixes',
          source_address, '--source-port-ranges', '*',
          '--destination-port-ranges', '*', '--priority',
          str(rule_priority), '--protocol', 'Icmp'
      ]
      network_cmd.extend(self.resource_group.args + self.args)
      vm_util.IssueRetryableCommand(network_cmd)


class AzureFirewall(network.BaseFirewall):
  """A firewall on Azure is a Network Security Group.

  NSGs are per-subnet, but this class is per-provider, so we just
  proxy methods through to the right NSG instance.
  """

  CLOUD = provider_info.AZURE

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Opens a port on the firewall.

    This is a no-op if there is no firewall configured for the VM.  This can
    happen when using an already existing subnet.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The local port to open.
      end_port: if given, open the range [start_port, end_port].
      source_range: unsupported at present.
    """

    if vm.network.nsg:
      vm.network.nsg.AllowPort(
          vm, start_port, end_port=end_port, source_range=source_range)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the ICMP protocol for.
    """

    vm.network.nsg.AllowIcmp()


class AzureNetwork(network.BaseNetwork):
  """Locational network components.

  A container object holding all of the network-related objects that
  we need for an Azure zone (aka region).
  """

  CLOUD = provider_info.AZURE

  def __init__(self, spec):
    super(AzureNetwork, self).__init__(spec)
    self.resource_group = GetResourceGroup()
    self.region = util.GetRegionFromZone(self.zone)
    self.availability_zone = util.GetAvailabilityZoneFromZone(self.zone)

    is_dedicated_host = bool(FLAGS.dedicated_hosts)
    in_availability_zone = bool(self.availability_zone)

    # Placement Group
    no_placement_group = (
        not FLAGS.placement_group_style or
        FLAGS.placement_group_style == placement_group.PLACEMENT_GROUP_NONE)
    has_optional_pg = (FLAGS.placement_group_style ==
                       placement_group.PLACEMENT_GROUP_CLOSEST_SUPPORTED)
    if no_placement_group:
      self.placement_group = None
    elif has_optional_pg and len(set(FLAGS.zone)) > 1:
      logging.warning(
          'inter-zone/inter-region tests do not support placement groups. '
          'Placement group style set to none.'
      )
      self.placement_group = None
    elif len(set(FLAGS.zone)) > 1:
      raise errors.Benchmarks.UnsupportedConfigError(
          'inter-zone/inter-region tests do not support placement groups. '
          'Use placement group style closest_supported.')
    # With dedicated hosting and/or an availability zone, an availability set
    # cannot be created
    elif (FLAGS.placement_group_style == azure_placement_group.AVAILABILITY_SET
          and (is_dedicated_host or in_availability_zone)):
      self.placement_group = None
    else:
      placement_group_spec = azure_placement_group.AzurePlacementGroupSpec(
          'AzurePlacementGroupSpec',
          flag_values=FLAGS,
          zone=self.zone,
          resource_group=self.resource_group.name)

      self.placement_group = azure_placement_group.AzurePlacementGroup(
          placement_group_spec)

    # Storage account names can't include separator characters :(.
    storage_account_prefix = 'pkb%s' % FLAGS.run_uri

    # Storage account names must be 3-24 characters long and use
    # numbers and lower-case letters only, which leads us to this
    # awful naming scheme.
    suffix = 'storage%d' % AzureStorageAccount.total_storage_accounts
    self.storage_account = AzureStorageAccount(
        FLAGS.azure_storage_type, self.region,
        storage_account_prefix[:24 - len(suffix)] + suffix)

    # Length restriction from https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules#microsoftnetwork  pylint: disable=line-too-long
    prefix = '%s-%s' % (self.resource_group.name, self.region)
    vnet_name = prefix + '-vnet'
    if len(vnet_name) > 64:
      vnet_name = prefix[:59] + '-vnet'
    self.vnet = AzureVirtualNetwork.GetForRegion(spec, self.region, vnet_name)
    subnet_name = vnet_name
    if self.availability_zone:
      subnet_name += '-' + self.availability_zone
    subnet_name += '-subnet'
    self.subnet = AzureSubnet(self.vnet, subnet_name)
    if azure_flags.AZURE_SUBNET_ID.value:
      # usage of an nsg is not currently supported with an existing subnet.
      self.nsg = None
    else:
      self.nsg = AzureNetworkSecurityGroup(self.region, self.subnet,
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

    if self.vnet:
      self.vnet.Create()
    if self.subnet:
      self.subnet.Create()
    if self.nsg:
      self.nsg.Create()
      self.nsg.AttachToSubnet()

  def Delete(self):
    """Deletes the network."""
    # If the benchmark includes multiple zones, this will be called
    # multiple times, but there will be no bad effects from multiple
    # deletes.
    self.resource_group.Delete()

  def Peer(self, peering_network):
    """Peers the network with the peering_network.

    This method is used for VPC peering. It will connect 2 VPCs together.

    Args:
      peering_network: BaseNetwork. The network to peer with.
    """

    # Skip Peering if the networks are the same
    if self.vnet is peering_network.vnet:
      return

    spec = network.BaseVPCPeeringSpec(self.vnet,
                                      peering_network.vnet)
    self.vpc_peering = AzureVpcPeering(spec)
    peering_network.vpc_peering = self.vpc_peering
    self.vpc_peering.Create()

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    return (cls.CLOUD, ZONE, spec.zone)


class AzureVpcPeering(network.BaseVPCPeering):
  """Object containing all information needed to create a VPC Peering Object."""

  def _Create(self):
    """Creates the peering object."""
    self.name = '%s-%s-%s' % (self.network_a.resource_group.name,
                              self.network_a.region, self.network_b.region)

    # Creates Peering Connection
    create_cmd = [
        azure.AZURE_PATH, 'network', 'vnet', 'peering', 'create',
        '--name', self.name,
        '--vnet-name', self.network_a.name,
        '--remote-vnet', self.network_b.name,
        '--allow-vnet-access'
    ] + self.network_a.resource_group.args

    vm_util.IssueRetryableCommand(create_cmd)

    # Accepts Peering Connection
    accept_cmd = [
        azure.AZURE_PATH, 'network', 'vnet', 'peering', 'create',
        '--name', self.name,
        '--vnet-name', self.network_b.name,
        '--remote-vnet', self.network_a.name,
        '--allow-vnet-access'
    ] + self.network_b.resource_group.args
    vm_util.IssueRetryableCommand(accept_cmd)

    logging.info('Created VPC peering between %s and %s',
                 self.network_a.address_spaces[0],
                 self.network_b.address_spaces[0])

  def _Delete(self):
    """Deletes the peering connection."""
    # Gets Deleted with resource group deletion
    pass
