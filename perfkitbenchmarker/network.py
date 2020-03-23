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

"""Module containing abstract classes related to VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in the
same project.
"""

from enum import Enum

from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import resource


class NetType(Enum):
  DEFAULT = 'default'
  SINGLE = 'single'
  MULTI = 'multi'


class BaseFirewall(object):
  """An object representing the Base Firewall."""

  CLOUD = None

  @classmethod
  def GetFirewall(cls):
    """Returns a BaseFirewall.

    This method is used instead of directly calling the class's constructor.
    It creates BaseFirewall instances and registers them.
    If a BaseFirewall object has already been registered, that object
    will be returned rather than creating a new one. This enables multiple
    VMs to call this method and all share the same BaseFirewall object.
    """
    if cls.CLOUD is None:
      raise errors.Error('Firewalls should have CLOUD attributes.')
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetFirewall called in a thread without a '
                         'BenchmarkSpec.')
    with benchmark_spec.firewalls_lock:
      key = cls.CLOUD
      if key not in benchmark_spec.firewalls:
        benchmark_spec.firewalls[key] = cls()
      return benchmark_spec.firewalls[key]

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the ICMP protocol for.
    """
    pass

  def AllowPort(self, vm, start_port, end_port=None):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port in a range of ports to open.
      end_port: The last port in a range of ports to open. If None, only
        start_port will be opened.
    """
    pass

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class BaseNetworkSpec(object):
  """Object containing all information needed to create a Network."""

  def __init__(self, zone=None, cidr=None):
    """Initializes the BaseNetworkSpec.

    Args:
      zone: The zone in which to create the network.
      cidr: The subnet this network belongs to in CIDR notation
    """
    self.zone = zone
    self.cidr = cidr

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)


class BaseVPNGW(object):
  """An object representing the Base VPN GW."""
  CLOUD = None

  def __init__(self, zone=None, cidr=None):
    """Initializes the BaseVPNGW.

    Args:
      zone: The zone in which to create the VPNGW.
    """
    self.zone = zone
    self.cidr = cidr
    self.require_target_to_init = False  # True if we need taget GW up front (AWS)

  @classmethod
  def GetVPNGW(cls):
    """Returns a BaseVPNGW.
    This method is used instead of directly calling the class's constructor.
    It creates BaseVPNGW instances and registers them.
    If a BaseVPNGW object has already been registered, that object
    will be returned rather than creating a new one. This enables multiple
    VMs to call this method and all share the same BaseVPN object.
    """
    if cls.CLOUD is None:
      raise errors.Error('VPN GWs should have CLOUD attributes.')
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetVPN called in a thread without a '
                         'BenchmarkSpec.')
    with benchmark_spec.vpn_gateways_lock:
      key = cls.CLOUD
      if key not in benchmark_spec.vpn_gateways:
        benchmark_spec.vpn_gateways[key] = cls()
      return benchmark_spec.vpn_gateways[key]

  def IsTunnelConfigured(self, tunnel_config):
    """ Returns True if the tunnel_config is complete.

    Args:
     tunnel_config: The tunnel_config of the tunnel to check.
    Returns:
       boolean.
    """
    pass

  def IsTunnelReady(self, tunnel_id):
    """Returns True if the tunnel is ready.

    Args:
      tunnel_id: The id of the tunnel to check.

    Returns:
      boolean.
    """
    pass

  def ConfigureTunnel(self, tunnel_config):
    """Updates the tunnel_config object with new information.

    Each provider may require different information to setup a VPN tunnel,
    and all information needed to configure the tunnel may not be available
    up front. Incremental updates to tunnel_config are made by calling this
    function on each endpoint until either both endpoint tunnels are configured
    or no more updates can be made.

    Args:
      tunnel_config: The tunnel_config object of the tunnel to configure.
    """
    pass

  def Create(self):
    """Creates the actual VPN GW."""
    pass

  def Delete(self):
      """Deletes the actual VPN GW."""
      pass


class BaseNetwork(object):
  """Object representing a Base Network."""

  CLOUD = None

  def __init__(self, spec):
    self.zone = spec.zone
    self.cidr = spec.cidr

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns a BaseNetworkSpec created from VM attributes."""
    return BaseNetworkSpec(zone=vm.zone, cidr=vm.cidr)

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    if cls.CLOUD is None:
      raise errors.Error('Networks should have CLOUD attributes.')
    return (cls.CLOUD, spec.zone)

  @classmethod
  def GetNetwork(cls, vm):
    """Returns a BaseNetwork.

    This method is used instead of directly calling the class's constructor.
    It creates BaseNetwork instances and registers them. If a BaseNetwork
    object has already been registered with the same key, that object
    will be returned rather than creating a new one. This enables multiple
    VMs to call this method and all share the same BaseNetwork object.

    Args:
      vm: The VM for which the Network is being created.
    """
    return cls.GetNetworkFromNetworkSpec(cls._GetNetworkSpecFromVm(vm))

  @staticmethod
  def FormatCidrString(cidr_raw):
    """Format CIDR string for use in resource name.

    Removes or replaces illegal characters from CIDR.
    eg '10.128.0.0/9' -> '10-128-0-0-9'

    Args:
      cidr_raw: The unformatted CIDR string.
    Returns:
      A CIDR string suitable for use in resource names.
    Raises:
      Error: Invalid CIDR format
    """

    delim = r'-'  # Safe delimiter for most providers
    int_regex = r'[0-9]+'
    octets_mask = regex_util.ExtractAllMatches(int_regex, str(cidr_raw))
    if len(octets_mask) != 5:  # expecting 4 octets plus 1 prefix mask.
      raise ValueError('Invalid CIDR format: "{0}"'.format(cidr_raw))
    return delim.join(octets_mask)

  @classmethod
  def GetNetworkFromNetworkSpec(cls, spec):
    """Returns a BaseNetwork.

    This method is used instead of directly calling the class's constructor.
    It creates BaseNetwork instances and registers them. If a BaseNetwork
    object has already been registered with the same key, that object
    will be returned rather than creating a new one. This enables multiple
    VMs to call this method and all share the same BaseNetwork object.

    Args:
      spec: The network spec for the network.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    key = cls._GetKeyFromNetworkSpec(spec)

    #  Grab the list of other networks to setup firewalls, forwarding, etc.
    if not hasattr(spec, 'custom_subnets'):
      spec.__setattr__('custom_subnets', benchmark_spec.custom_subnets)

    with benchmark_spec.networks_lock:
      if key not in benchmark_spec.networks:
        benchmark_spec.networks[key] = cls(spec)
      return benchmark_spec.networks[key]

  def Create(self):
    """Creates the actual network."""
    pass

  def Delete(self):
    """Deletes the actual network."""
    pass

  def Peer(self, peering_network):
    """Peers the network with the peering_network.

    This method is used for VPC peering. It will connect 2 VPCs together.

    Args:
      peering_network: BaseNetwork. The network to peer with.
    """
    pass


class BaseVPCPeeringSpec(object):
  """Object containing all information needed to create a VPC Peering Object."""

  def __init__(self, network_a=None, network_b=None):
    """Initializes BaseVPCPeeringSpec.

    Args:
      network_a: BaseNetwork. The network initiating the peering.
      network_b: BaseNetwork. The network to be peered to.
    """
    self.network_a = network_a
    self.network_b = network_b

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)


class BaseVPCPeering(resource.BaseResource):
  """Base class for VPC Peering.

  This class holds VPC Peering methods and attributes relating to the
  VPC Peering as a cloud resource.

  Attributes:
    network_a: BaseNetwork. The network initiating the peering.
    network_b: BaseNetwork. The network to be peered to.
  """

  RESOURCE_TYPE = 'BaseVPCPeering'

  def __init__(self, vpc_peering_spec):
    """Initialize BaseVPCPeering class.

    Args:
      vpc_peering_spec: BaseVPCPeeringSpec. Spec for VPC peering object.
    """
    super(BaseVPCPeering, self).__init__()
    self.network_a = vpc_peering_spec.network_a
    self.network_b = vpc_peering_spec.network_b
