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
"""Module containing classes related to GCE VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in the
same project. See https://developers.google.com/compute/docs/networking for
more information about GCE VM networking.
"""


import json
import logging
import threading
from typing import Any, Dict, List, Set, Tuple, Union

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import vpn_service
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_placement_group
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
NETWORK_RANGE = '10.0.0.0/8'
ALLOW_ALL = 'tcp:1-65535,udp:1-65535,icmp'

_PLACEMENT_GROUP_PREFIXES = frozenset(
    ['c2', 'c3', 'n2', 'n2d', 'c2d', 'c3a', 'c3d', 'a2', 'a3', 'g2', 'h3', 'c4']
)


class GceVpnGateway(network.BaseVpnGateway):
  """Object representing a GCE VPN Gateway."""

  CLOUD = provider_info.GCP

  def __init__(
      self, name: str, network_name: str, region: str, cidr: str, project: str
  ):
    super().__init__()

    self.forwarding_rules: Dict[str, GceForwardingRule] = {}
    self.forwarding_rules_lock = threading.Lock()
    self.tunnels: Dict[str, GceStaticTunnel] = {}
    self.routes: Dict[str, GceRoute] = {}
    self.ip_resource = None
    self.vpn_gateway_resource = GceVpnGatewayResource(
        name, network_name, region, cidr, project
    )
    self.vpn_gateway_resource_lock = threading.Lock()

    self.name = name
    self.network_name = network_name
    self.region = region
    self.cidr = cidr
    self.project = project

    self.ip_address = None
    self.ip_address_lock = threading.Lock()
    self.created = False
    self.require_target_to_init = False
    self.routing = None
    self.psk = None

    # Add gateway to benchmark spec at init().
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error(
          'GetNetwork called in a thread without a BenchmarkSpec.'
      )
    key = self.name
    with benchmark_spec.vpn_gateways_lock:
      if key not in benchmark_spec.vpn_gateways:
        benchmark_spec.vpn_gateways[key] = self

  def ConfigureTunnel(self, tunnel_config: vpn_service.TunnelConfig):
    """Updates tunnel config with new information.

    Args:
      tunnel_config: The tunnel configuration for this VPN.
    """
    logging.debug('Configuring Tunnel with params:')
    logging.debug(tunnel_config)

    # update tunnel_config if needed
    if self.name not in tunnel_config.endpoints:
      logging.debug(
          'tunnel_config: This endpoint isnt registered yet... %s', self.name
      )
      tunnel_config.endpoints[self.name] = {
          'is_configured': False,
          'cidr': self.cidr,
          'project': self.project,
          'network_name': self.network_name,
          'region': self.region,
          'require_target_to_init': self.require_target_to_init,
      }

    # attach public IP to this gateway if doesnt exist
    # and update tunnel_config if needed
    # requires project, region, name
    with self.ip_address_lock:
      if not self.ip_address:
        if not self.ip_resource:
          self.ip_resource = GceIPAddress(self.project, self.region, self.name)
          self.ip_resource.Create()
        self.ip_address = self.ip_resource.ip_address
      if 'ip_address' not in tunnel_config.endpoints[self.name]:
        logging.debug('tunnel_config: Configuring IP for %s', self.name)
        tunnel_config.endpoints[self.name]['ip_address'] = self.ip_address

    # configure forwarding
    # requires: -
    with self.forwarding_rules_lock:
      if len(self.forwarding_rules) == 3:
        logging.debug('tunnel_config: Forwarding already configured, skipping')
      else:
        logging.debug('tunnel_config: Setting up forwarding')
        self._SetupForwarding(tunnel_config)

    # Abort if we don't have a target info configured yet
    if len(tunnel_config.endpoints) < 2:
      logging.debug(
          'tunnel_config: Only found %d endpoints... '
          'waiting for target to configure',
          len(tunnel_config.endpoints),
      )
      return

    # Get target endpoint config key
    target_endpoint = [
        k for k in tunnel_config.endpoints.keys() if k not in self.name
    ][0]

    # configure tunnel resources
    # requires: target_ip_address, IKE version (default 1),
    if 'ip_address' not in tunnel_config.endpoints[target_endpoint]:
      logging.debug(
          'tunnel_config: Target IP needed... waiting for target to configure'
      )
      return
    if not hasattr(tunnel_config, 'psk'):
      logging.debug('tunnel_config: PSK not provided... setting to runid')
      tunnel_config.psk = 'key' + FLAGS.run_uri
    self._SetupTunnel(tunnel_config)

    # configure routing
    # requires: next_hop_tunnel_id, target_cidr,
    # TODO(dlott) Should be str | None, but that requires making endpoints a
    # proper class rather than a dictionary of string and bool. See TunnelConfig
    dest_cidr: Any = tunnel_config.endpoints[target_endpoint].get('cidr')
    if not dest_cidr or not dest_cidr.strip():
      logging.debug(
          'tunnel_config: destination CIDR needed... '
          'waiting for target to configure'
      )
      return
    self._SetupRouting(
        tunnel_config.suffix,
        tunnel_config.endpoints[self.name]['tunnel_id'],
        tunnel_config.endpoints[target_endpoint]['cidr'],
    )

    tunnel_config.endpoints[self.name]['is_configured'] = True

  def IsTunnelReady(self, tunnel_id: str) -> bool:
    """Returns True if the tunnel is up and ready for traffic.

    Args:
      tunnel_id: The id of the tunnel to check.

    Returns:
      boolean.
    """
    return self.tunnels[tunnel_id].IsReady()

  def _SetupTunnel(self, tunnel_config: vpn_service.TunnelConfig):
    """Register a new GCE VPN tunnel for this endpoint.

    Args:
      tunnel_config: VPN tunnel configuration.
    """
    target_endpoint = [
        k for k in tunnel_config.endpoints.keys() if k not in self.name
    ][0]
    project = tunnel_config.endpoints[self.name]['project']
    region = tunnel_config.endpoints[self.name]['region']
    vpn_gateway_id = self.name
    target_ip = tunnel_config.endpoints[target_endpoint]['ip_address']
    assert isinstance(target_ip, str)
    psk = tunnel_config.psk
    ike_version = tunnel_config.ike_version
    suffix = tunnel_config.suffix
    name = 'tun-' + self.name + '-' + suffix
    if name not in self.tunnels:
      self.tunnels[name] = GceStaticTunnel(
          project, region, name, vpn_gateway_id, target_ip, ike_version, psk
      )
      self.tunnels[name].Create()
      tunnel_config.endpoints[self.name]['tunnel_id'] = name

  def _SetupForwarding(self, tunnel_config: vpn_service.TunnelConfig):
    """Create IPSec forwarding rules.

    Forwards ESP protocol, and UDP 500/4500 for tunnel setup.

    Args:
      tunnel_config: The tunnel configuration for this VPN.
    """
    if len(self.forwarding_rules) == 3:
      return  # backout if already set
    suffix = tunnel_config.suffix
    # GCP doesnt like uppercase names?!?
    fr_udp500_name = 'fr-udp500-%s-%s' % (suffix, FLAGS.run_uri)
    fr_udp4500_name = 'fr-udp4500-%s-%s' % (suffix, FLAGS.run_uri)
    fr_esp_name = 'fr-esp-%s-%s' % (suffix, FLAGS.run_uri)

    if fr_udp500_name not in self.forwarding_rules:
      fr_udp500 = GceForwardingRule(fr_udp500_name, 'UDP', self, 500)
      self.forwarding_rules[fr_udp500_name] = fr_udp500
      fr_udp500.Create()
    if fr_udp4500_name not in self.forwarding_rules:
      fr_udp4500 = GceForwardingRule(fr_udp4500_name, 'UDP', self, 4500)
      self.forwarding_rules[fr_udp4500_name] = fr_udp4500
      fr_udp4500.Create()
    if fr_esp_name not in self.forwarding_rules:
      fr_esp = GceForwardingRule(fr_esp_name, 'ESP', self)
      self.forwarding_rules[fr_esp_name] = fr_esp
      fr_esp.Create()

  def _SetupRouting(self, suffix: str, next_hop_tun: str, dest_cidr: str):
    """Create IPSec routing rules between the source and target gateways."""

    route_name = 'route-' + self.name + '-' + suffix
    if route_name not in self.routes:
      self.routes[route_name] = GceRoute(
          route_name,
          dest_cidr,
          self.network_name,
          next_hop_tun,
          self.region,
          self.project,
      )
      self.routes[route_name].Create()

  def Create(self):
    """Creates the actual VpnGateway."""
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error(
          'GetNetwork called in a thread without a BenchmarkSpec.'
      )
    if self.created:
      return
    self.vpn_gateway_resource.Create()

    self.created = True

  def Delete(self):
    """Deletes the actual VpnGateway."""
    if self.ip_resource:
      self.ip_resource.Delete()

    if self.tunnels:
      background_tasks.RunThreaded(
          lambda tun: self.tunnels[tun].Delete(), list(self.tunnels.keys())
      )

    if self.forwarding_rules:
      background_tasks.RunThreaded(
          lambda fr: self.forwarding_rules[fr].Delete(),
          list(self.forwarding_rules.keys()),
      )

    if self.routes:
      background_tasks.RunThreaded(
          lambda route: self.routes[route].Delete(), list(self.routes.keys())
      )

    if self.vpn_gateway_resource:
      self.vpn_gateway_resource.Delete()

    self.created = False


class GceVpnGatewayResource(resource.BaseResource):
  """Object representing a GCE VPN Gateway Resource."""

  def __init__(
      self, name: str, network_name: str, region: str, cidr: str, project: str
  ):
    super().__init__()
    self.name = name
    self.network_name = network_name
    self.region = region
    self.cidr = cidr
    self.project = project

  def _Create(self):
    cmd = util.GcloudCommand(
        self, 'compute', 'target-vpn-gateways', 'create', self.name
    )
    cmd.flags['network'] = self.network_name
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Exists(self):
    cmd = util.GcloudCommand(
        self, 'compute', 'target-vpn-gateways', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode

  def _Delete(self):
    cmd = util.GcloudCommand(
        self, 'compute', 'target-vpn-gateways', 'delete', self.name
    )
    cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)


class GceIPAddress(resource.BaseResource):
  """Object representing a GCE IP address."""

  def __init__(
      self, project: str, region: str, name: str, subnet: str | None = None
  ):
    super().__init__()
    self.project = project
    self.region = region
    self.name = name
    self.subnet = subnet
    self.ip_address = None

  def _Create(self):
    """Allocates a public IP for the VPN gateway."""
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'create', self.name)
    cmd.flags['region'] = self.region
    if self.subnet is not None:
      cmd.flags['subnet'] = self.subnet
    cmd.Issue()

  def _PostCreate(self):
    cmd = util.GcloudCommand(
        self, 'compute', 'addresses', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    cmd.flags['format'] = 'value(address)'
    stdout, _, _ = cmd.Issue()
    self.ip_address = stdout.rstrip()

  def _Delete(self):
    """Deletes a public IP for the VPN gateway."""
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'delete', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)

  def _Exists(self) -> bool:
    """Returns True if the IP address exists."""
    cmd = util.GcloudCommand(
        self, 'compute', 'addresses', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode

  def _IsReady(self) -> bool:
    """Returns True if the IP address is reserved."""
    cmd = util.GcloudCommand(
        self, 'compute', 'addresses', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    cmd.flags['format'] = 'value(status)'
    stdout, _, _ = cmd.Issue()
    return stdout.rstrip() == 'RESERVED'


class GceStaticTunnel(resource.BaseResource):
  """An object representing a GCE Tunnel."""

  def __init__(
      self,
      project: str,
      region: str,
      name: str,
      vpn_gateway_id: str,
      target_ip: str,
      ike_version: str,
      psk: str,
  ):
    super().__init__()
    self.project = project
    self.region = region
    self.name = name
    self.vpn_gateway_id = vpn_gateway_id
    self.target_ip = target_ip
    self.ike_version = ike_version
    self.psk = psk

  def _Create(self):
    """Creates the Tunnel."""
    cmd = util.GcloudCommand(
        self, 'compute', 'vpn-tunnels', 'create', self.name
    )
    cmd.flags['peer-address'] = self.target_ip
    cmd.flags['target-vpn-gateway'] = self.vpn_gateway_id
    cmd.flags['ike-version'] = self.ike_version
    cmd.flags['local-traffic-selector'] = '0.0.0.0/0'
    cmd.flags['remote-traffic-selector'] = '0.0.0.0/0'
    cmd.flags['shared-secret'] = self.psk
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Delete(self):
    """Delete IPSec tunnel."""
    cmd = util.GcloudCommand(
        self, 'compute', 'vpn-tunnels', 'delete', self.name
    )
    cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)

  def _Exists(self) -> bool:
    """Returns True if the tunnel exists."""
    cmd = util.GcloudCommand(
        self, 'compute', 'vpn-tunnels', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode

  def IsReady(self) -> bool:
    cmd = util.GcloudCommand(
        self, 'compute', 'vpn-tunnels', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    response = cmd.Issue()
    return 'established' in str(response).lower()


class GceRoute(resource.BaseResource):
  """An object representing a GCE Route."""

  def __init__(
      self,
      route_name: str,
      dest_cidr: str,
      network_name: str,
      next_hop_tun: str,
      next_hop_region: str,
      project: str,
  ):
    super().__init__()
    self.name = route_name
    self.dest_cidr = dest_cidr
    self.next_hop_region = next_hop_region
    self.next_hop_tun = next_hop_tun
    self.network_name = network_name
    self.project = project

  def _Create(self):
    """Creates the Route."""
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'create', self.name)
    cmd.flags['destination-range'] = self.dest_cidr
    cmd.flags['network'] = self.network_name
    cmd.flags['next-hop-vpn-tunnel'] = self.next_hop_tun
    cmd.flags['next-hop-vpn-tunnel-region'] = self.next_hop_region
    cmd.Issue()

  def _Delete(self):
    """Delete route."""
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'delete', self.name)
    cmd.Issue(raise_on_failure=False)

  def _Exists(self) -> bool:
    """Returns True if the Route exists."""
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'describe', self.name)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode


class GceForwardingRule(resource.BaseResource):
  """An object representing a GCE Forwarding Rule."""

  def __init__(
      self,
      name: str,
      protocol: str,
      src_vpn_gateway: GceVpnGateway,
      port: int | None = None,
  ):
    super().__init__()
    self.name = name
    self.protocol = protocol
    self.port = port
    self.target_name = src_vpn_gateway.name
    self.target_ip = src_vpn_gateway.ip_address
    self.src_region = src_vpn_gateway.region
    self.project = src_vpn_gateway.project

  def __eq__(self, other: 'GceForwardingRule') -> bool:
    """Defines equality to make comparison easy."""
    return (
        self.name == other.name
        and self.protocol == other.protocol
        and self.port == other.port
        and self.target_name == other.target_name
        and self.target_ip == other.target_ip
        and self.src_region == other.src_region
    )

  def _Create(self):
    """Creates the Forwarding Rule."""
    cmd = util.GcloudCommand(
        self, 'compute', 'forwarding-rules', 'create', self.name
    )
    cmd.flags['ip-protocol'] = self.protocol
    if self.port:
      cmd.flags['ports'] = self.port
    cmd.flags['address'] = self.target_ip
    cmd.flags['target-vpn-gateway'] = self.target_name
    cmd.flags['region'] = self.src_region
    cmd.Issue()

  def _Delete(self):
    """Deletes the Forwarding Rule."""
    cmd = util.GcloudCommand(
        self, 'compute', 'forwarding-rules', 'delete', self.name
    )
    cmd.flags['region'] = self.src_region
    cmd.Issue(raise_on_failure=False)

  def _Exists(self) -> bool:
    """Returns True if the Forwarding Rule exists."""
    cmd = util.GcloudCommand(
        self, 'compute', 'forwarding-rules', 'describe', self.name
    )
    cmd.flags['region'] = self.src_region
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode


class GceFirewallRule(resource.BaseResource):
  """An object representing a GCE Firewall Rule."""

  def __init__(
      self,
      name: str,
      project: str,
      allow: str,
      network_name: str,
      source_range: str | None = None,
  ):
    super().__init__()
    self.name = name
    self.project = project
    self.allow = allow
    self.network_name = network_name
    self.source_range = source_range

  def __eq__(self, other: 'GceFirewallRule') -> bool:
    """Defines equality to make comparison easy."""
    return (
        self.name == other.name
        and self.allow == other.allow
        and self.project == other.project
        and self.network_name == other.network_name
        and self.source_range == other.source_range
    )

  def _Create(self):
    """Creates the Firewall Rule."""
    cmd = util.GcloudCommand(
        self, 'compute', 'firewall-rules', 'create', self.name
    )
    cmd.flags['allow'] = self.allow
    cmd.flags['network'] = self.network_name
    if self.source_range:
      cmd.flags['source-ranges'] = self.source_range
    # Gcloud create commands may still create firewalls despite being rate
    # limited.
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode:
      if cmd.rate_limited and 'already exists' in stderr:
        return
      debug_text = 'Ran: {%s}\nReturnCode:%s\nSTDOUT: %s\nSTDERR: %s' % (
          ' '.join(cmd.GetCommand()),
          retcode,
          stdout,
          stderr,
      )
      raise errors.VmUtil.IssueCommandError(debug_text)

  def _Delete(self):
    """Deletes the Firewall Rule."""
    cmd = util.GcloudCommand(
        self, 'compute', 'firewall-rules', 'delete', self.name
    )
    cmd.Issue(raise_on_failure=False)

  def _Exists(self) -> bool:
    """Returns True if the Firewall Rule exists."""
    cmd = util.GcloudCommand(
        self, 'compute', 'firewall-rules', 'describe', self.name
    )
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode


class GceFirewall(network.BaseFirewall):
  """An object representing the GCE Firewall."""

  CLOUD = provider_info.GCP

  def __init__(self):
    """Initialize GCE firewall class."""
    self._lock = threading.Lock()
    # TODO(user): make the key always have the same number of elements
    self.firewall_rules: Dict[Tuple[Any, ...], GceFirewallRule] = {}
    self.firewall_icmp_rules: Dict[Tuple[Any, ...], GceFirewallRule] = {}

  def AllowPort(
      self,
      vm,  # gce_virtual_machine.GceVirtualMachine
      start_port: int,
      end_port: int | None = None,
      source_range: List[str] | None = None,
  ):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
      source_range: List of source CIDRs to allow for this port. If none, all
        sources are allowed.
    """
    if vm.is_static:
      return
    if source_range:
      source_range = ','.join(source_range)
    with self._lock:
      if end_port is None:
        end_port = start_port
      if vm.cidr:  # Allow multiple networks per zone.
        cidr_string = network.BaseNetwork.FormatCidrString(vm.cidr)
        firewall_name = 'perfkit-firewall-%s-%s-%d-%d' % (
            cidr_string,
            FLAGS.run_uri,
            start_port,
            end_port,
        )
        key = (vm.project, vm.cidr, start_port, end_port, source_range)
      else:
        firewall_name = 'perfkit-firewall-%s-%d-%d' % (
            FLAGS.run_uri,
            start_port,
            end_port,
        )
        key = (vm.project, start_port, end_port, source_range)
      if key in self.firewall_rules:
        return
      allow = ','.join(
          '{}:{}-{}'.format(protocol, start_port, end_port)
          for protocol in ('tcp', 'udp')
      )
      firewall_rule = GceFirewallRule(
          firewall_name,
          vm.project,
          allow,
          vm.network.network_resource.name,
          source_range,
      )
      self.firewall_rules[key] = firewall_rule
      firewall_rule.Create()

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    for firewall_rule in self.firewall_rules.values():
      firewall_rule.Delete()
    for firewall_rule in self.firewall_icmp_rules.values():
      firewall_rule.Delete()

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the ICMP protocol for.
    """
    if vm.is_static:
      return
    with self._lock:
      if vm.cidr:  # Allow multiple networks per zone.
        cidr_string = network.BaseNetwork.FormatCidrString(vm.cidr)
        firewall_name = 'perfkit-firewall-icmp-%s-%s' % (
            cidr_string,
            FLAGS.run_uri,
        )
        key = (vm.project, vm.cidr)
      else:
        firewall_name = 'perfkit-firewall-icmp-%s' % FLAGS.run_uri
        key = vm.project

      if key in self.firewall_icmp_rules:
        return

      allow = 'ICMP'
      firewall_rule = GceFirewallRule(
          firewall_name, vm.project, allow, vm.network.network_resource.name
      )
      self.firewall_icmp_rules[key] = firewall_rule
      firewall_rule.Create()


class GceNetworkSpec(network.BaseNetworkSpec):
  """Object representing a GCE Network specification."""

  def __init__(
      self,
      project: str | None = None,
      zone: str | None = None,
      cidr: str | None = None,
      mtu: int | None = None,
      machine_type: str | None = None,
      subnet_names: List[str] | None = None,
      **kwargs,
  ):
    """Initializes the GceNetworkSpec.

    Args:
      project: The project for which the Network should be created.
      zone: The zone for which the subnet and/or network should be created.
      cidr: The CIDR to use for the network.
      mtu: The MTU (max transmission unit) to use, if any.
      machine_type: The machine type of VM's in the network.
      subnet_names: List of existing subnets.
      **kwargs: Additional key word arguments passed to BaseNetworkSpec.
    """
    super().__init__(**kwargs)
    self.project = project
    self.zone = zone
    self.cidr = cidr
    self.mtu = mtu
    self.machine_type = machine_type
    self.subnet_names = subnet_names


class GceNetworkResource(resource.BaseResource):
  """Object representing a GCE Network resource."""

  def __init__(
      self, name: str, mode: str, project: str, mtu: int | None = None
  ):
    super().__init__()
    self.name = name
    self.mode = mode
    self.project = project
    self.mtu = mtu

  def _Create(self):
    """Creates the Network resource."""
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'create', self.name)
    cmd.flags['subnet-mode'] = self.mode
    if self.mtu:
      cmd.flags['mtu'] = self.mtu
    cmd.Issue()

  def _Delete(self):
    """Deletes the Network resource."""
    if FLAGS.gce_firewall_rules_clean_all:
      for firewall_rule in self._GetAllFirewallRules():
        firewall_rule.Delete()

    cmd = util.GcloudCommand(self, 'compute', 'networks', 'delete', self.name)
    cmd.Issue(raise_on_failure=False)

  def _Exists(self) -> bool:
    """Returns True if the Network resource exists."""
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'describe', self.name)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode

  def _GetAllFirewallRules(self) -> List[GceFirewallRule]:
    """Returns all the firewall rules that use the network."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'list')
    cmd.flags['filter'] = 'network=%s' % self.name

    stdout, _, _ = cmd.Issue()
    result = json.loads(stdout)
    return [
        GceFirewallRule(
            entry['name'], self.project, ALLOW_ALL, self.name, NETWORK_RANGE
        )
        for entry in result
    ]


class GceSubnetResource(resource.BaseResource):
  """Object representing a GCE subnet resource."""

  def __init__(
      self,
      name: str,
      network_name: str,
      region: str,
      addr_range: str,
      project: str,
  ):
    super().__init__()
    self.name = name
    self.network_name = network_name
    self.region = region
    self.addr_range = addr_range
    self.project = project

  def UpdateProperties(self) -> None:
    """Updates the properties of the subnet resource."""
    cmd = util.GcloudCommand(
        self, 'compute', 'networks', 'subnets', 'describe', self.name
    )
    cmd.flags['region'] = self.region
    stdout, _, _ = cmd.Issue()
    json_details = json.loads(stdout)
    self.network_name = json_details['network'].split('/')[-1]
    self.addr_range = json_details['ipCidrRange']

  def _Create(self):
    cmd = util.GcloudCommand(
        self, 'compute', 'networks', 'subnets', 'create', self.name
    )
    cmd.flags['network'] = self.network_name
    cmd.flags['region'] = self.region
    cmd.flags['range'] = self.addr_range
    cmd.Issue()

  def _Exists(self) -> bool:
    cmd = util.GcloudCommand(
        self, 'compute', 'networks', 'subnets', 'describe', self.name
    )
    if self.region:
      cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return not retcode

  def _Delete(self):
    cmd = util.GcloudCommand(
        self, 'compute', 'networks', 'subnets', 'delete', self.name
    )
    if self.region:
      cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)


def IsPlacementGroupCompatible(machine_type: str):
  """Returns True if VMs of 'machine_type' can be put in a placement group."""
  prefix = machine_type.split('-')[0]
  return prefix in _PLACEMENT_GROUP_PREFIXES


class GceNetwork(network.BaseNetwork):
  """Object representing a GCE Network."""

  CLOUD = provider_info.GCP

  def __init__(self, network_spec: GceNetworkSpec):
    super().__init__(network_spec)
    self.project: str | None = network_spec.project
    self.vpn_gateway: Dict[str, GceVpnGateway] = {}

    #  Figuring out the type of network here.
    #  Precedence: User Managed > MULTI > SINGLE > DEFAULT
    self.net_type = network.NetType.DEFAULT.value
    self.cidr = NETWORK_RANGE
    if FLAGS.gce_subnet_region:
      self.net_type = network.NetType.SINGLE.value
      self.cidr = FLAGS.gce_subnet_addr
    if network_spec.cidr:
      self.net_type = network.NetType.MULTI.value
      self.cidr = network_spec.cidr
    self.mtu = network_spec.mtu

    # TODO(user): Create separate Network objects for each network name.
    self.is_existing_network = True
    self.subnet_names = []
    self.primary_subnet_name = None
    if network_spec.subnet_names:
      self.subnet_names = network_spec.subnet_names
    elif gcp_flags.GCE_NETWORK_NAMES.value:
      self.subnet_names = gcp_flags.GCE_NETWORK_NAMES.value
    else:
      self.subnet_names = self._MakeGceNetworkName()
      self.is_existing_network = False
    if not isinstance(self.subnet_names, list):
      self.subnet_names = [self.subnet_names]
    self.primary_subnet_name = self.subnet_names[0]

    self.network_resources = []
    self.subnet_resources = []
    mode = gcp_flags.GCE_NETWORK_TYPE.value
    self.subnet_resource = None

    # Handle GCP network attachment on legacy networks.
    if not self.is_existing_network or mode == 'legacy':
      for name in self.subnet_names:
        if mode != 'custom':
          mode = 'auto'
        self.network_resources.append(
            GceNetworkResource(name, mode, self.project, self.mtu)
        )

    # Create GCP network subnet for custom mode.
    if mode == 'custom':
      subnet_region = util.GetRegionFromZone(network_spec.zone)
      for name in self.subnet_names:
        self.subnet_resources.append(
            GceSubnetResource(
                name, name, subnet_region, self.cidr, self.project
            )
        )
      self.subnet_resource = GceSubnetResource(
          self.primary_subnet_name,
          self.primary_subnet_name,
          subnet_region,
          self.cidr,
          self.project,
      )
    self.network_resource = GceNetworkResource(
        self.primary_subnet_name, mode, self.project, self.mtu
    )
    # Stage FW rules.
    self.all_nets = self._GetNetworksFromSpec(
        network_spec
    )  # Holds the different networks in this run.
    # Holds FW rules for any external subnets.
    self.external_nets_rules: Dict[str, GceFirewallRule] = {}

    #  Set the default rule to allow all traffic within this network's subnet.
    firewall_name = self._MakeGceFWRuleName()
    self.default_firewall_rule = GceFirewallRule(
        firewall_name,
        self.project,
        ALLOW_ALL,
        self.primary_subnet_name,
        self.cidr,
    )

    # Set external rules to allow traffic from other subnets in this benchmark.
    for ext_net in self.all_nets:
      if ext_net == self.cidr:
        continue  # We've already added our own network to the default rule.
      rule_name = self._MakeGceFWRuleName(dst_cidr=ext_net)
      self.external_nets_rules[rule_name] = GceFirewallRule(
          rule_name, self.project, ALLOW_ALL, self.primary_subnet_name, ext_net
      )

    # Add VpnGateways to the network.
    if FLAGS.use_vpn:
      for gatewaynum in range(0, FLAGS.vpn_service_gateway_count):
        vpn_gateway_name = 'vpngw-%s-%s-%s' % (
            util.GetRegionFromZone(network_spec.zone),
            gatewaynum,
            FLAGS.run_uri,
        )
        self.vpn_gateway[vpn_gateway_name] = GceVpnGateway(
            vpn_gateway_name,
            self.primary_subnet_name,
            util.GetRegionFromZone(network_spec.zone),
            network_spec.cidr,
            self.project,
        )

    # Placement Group
    no_placement_group = (
        not FLAGS.placement_group_style
        or FLAGS.placement_group_style == placement_group.PLACEMENT_GROUP_NONE
    )
    has_optional_pg = (
        FLAGS.placement_group_style
        == placement_group.PLACEMENT_GROUP_CLOSEST_SUPPORTED
    )
    if no_placement_group:
      self.placement_group = None
    elif has_optional_pg and not IsPlacementGroupCompatible(
        network_spec.machine_type
    ):
      logging.warning(
          'machine type %s does not support placement groups. '
          'Placement group style set to none.',
          network_spec.machine_type,
      )
      self.placement_group = None
    elif has_optional_pg and len(set(FLAGS.zone)) > 1:
      logging.warning(
          'inter-zone/inter-region tests do not support placement groups. '
          'Placement group style set to none.'
      )
      self.placement_group = None
    elif not IsPlacementGroupCompatible(network_spec.machine_type):
      raise errors.Benchmarks.UnsupportedConfigError(
          f'machine type {network_spec.machine_type} does not support '
          'placement groups. Use placement group style none.'
      )
    elif len(set(FLAGS.zone)) > 1:
      raise errors.Benchmarks.UnsupportedConfigError(
          'inter-zone/inter-region tests do not support placement groups. '
          'Use placement group style closest_supported.'
      )
    else:
      placement_group_spec = gce_placement_group.GcePlacementGroupSpec(
          'GcePlacementGroupSpec',
          flag_values=FLAGS,
          zone=network_spec.zone,
          project=self.project,
          num_vms=self._GetNumberVms(),
      )
      self.placement_group = gce_placement_group.GcePlacementGroup(
          placement_group_spec
      )

  def _GetNetworksFromSpec(self, network_spec: GceNetworkSpec) -> Set[str]:
    """Returns a list of distinct CIDR networks for this benchmark.

    All GCP networks that aren't set explicitly with a vm_group cidr property
    are assigned to the default network. The default network is either
    specified as a single region with the gce_subnet_region and
    gce_subnet_addr flags, or assigned to an auto created /20 in each region
    if no flags are passed.

    Args:
      network_spec: The network spec for the network.

    Returns:
      A set of CIDR strings used by this benchmark.
    """
    nets = set()
    gce_default_subnet = (
        FLAGS.gce_subnet_addr if FLAGS.gce_subnet_region else NETWORK_RANGE
    )

    if hasattr(network_spec, 'custom_subnets'):
      for _, v in network_spec.custom_subnets.items():
        if not v['cidr'] and v['cloud'] != 'GCP':
          pass  # @TODO handle other providers defaults in net_util
        elif not v['cidr']:
          nets.add(gce_default_subnet)
        else:
          nets.add(v['cidr'])
    return nets

  def _MakeGceNetworkName(
      self,
      net_type: str | None = None,
      cidr: str | None = None,
      uri: str | None = None,
  ) -> str:
    """Build the current network's name string.

    Uses current instance properties if none provided.
    Must match regex: r'(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'

    Args:
      net_type: One of ['default', 'single', 'multi']
      cidr: The CIDR range of this network.
      uri: A network suffix (if different than FLAGS.run_uri)

    Returns:
      String The name of this network.
    """
    net_type = net_type or self.net_type
    cidr = cidr or self.cidr
    uri = uri or FLAGS.run_uri

    name = 'pkb-network-%s' % uri  # Assume the default network naming.

    if net_type in (network.NetType.SINGLE.value, network.NetType.MULTI.value):
      name = 'pkb-network-%s-%s-%s' % (
          net_type,
          self.FormatCidrString(cidr),
          uri,
      )

    return name

  def _MakeGceFWRuleName(
      self,
      net_type: str | None = None,
      src_cidr: str | None = None,
      dst_cidr: str | None = None,
      port_range_lo: str | None = None,
      port_range_hi: str | None = None,
      uri: str | None = None,
  ) -> str:
    """Build a firewall name string.

    Firewall rule names must be unique within a project so we include source
    and destination nets to disambiguate.

    Args:
      net_type: One of ['default', 'single', 'multi']
      src_cidr: The CIDR range of this network.
      dst_cidr: The CIDR range of the remote network.
      port_range_lo: The low port to open
      port_range_hi: The high port to open in range.
      uri: A firewall suffix (if different than FLAGS.run_uri)

    Returns:
      The name of this firewall rule.
    """
    net_type = net_type or self.net_type
    uri = uri or FLAGS.run_uri
    src_cidr = src_cidr or self.cidr
    dst_cidr = dst_cidr or self.cidr

    prefix = None if src_cidr == dst_cidr else 'perfkit-firewall'
    src_cidr = (
        'internal' if src_cidr == dst_cidr else self.FormatCidrString(src_cidr)
    )
    dst_cidr = self.FormatCidrString(dst_cidr)
    port_lo = port_range_lo
    port_hi = None if port_range_lo == port_range_hi else port_range_hi

    firewall_name = '-'.join(
        str(i)
        for i in (prefix, net_type, src_cidr, dst_cidr, port_lo, port_hi, uri)
        if i
    )

    return firewall_name

  @staticmethod
  def _GetNetworkSpecFromVm(vm) -> GceNetworkSpec:
    """Returns a BaseNetworkSpec created from VM attributes."""
    return GceNetworkSpec(
        project=vm.project,
        zone=vm.zone,
        cidr=vm.cidr,
        mtu=vm.mtu,
        machine_type=vm.machine_type,
        subnet_names=vm.subnet_names,
    )

  @classmethod
  def _GetKeyFromNetworkSpec(
      cls, spec
  ) -> Union[Tuple[str, str], Tuple[str, str, str], Tuple[str, str, str, str]]:
    """Returns a key used to register Network instances."""
    network_key = (cls.CLOUD, spec.project)
    if spec.cidr:
      network_key += (spec.cidr,)
    if spec.subnet_names:
      if isinstance(spec.subnet_names, list):
        network_key += (','.join(spec.subnet_names),)
      else:
        network_key += (spec.subnet_names,)
    return network_key

  def _GetNumberVms(self) -> int:
    """Counts the number of VMs to be used in this benchmark.

    Cannot do a len(benchmark_spec.vms) as that hasn't been populated yet.  Go
    through all the group_specs and sum up the vm_counts.

    Returns:
      Count of the number of VMs in the benchmark.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    return sum(
        (group_spec.vm_count - len(group_spec.static_vms))
        for group_spec in benchmark_spec.config.vm_groups.values()
    )

  def Create(self):
    """Creates the actual network."""
    if not self.is_existing_network:
      self.network_resource.Create()
      if self.subnet_resource:
        self.subnet_resource.Create()
      if self.default_firewall_rule:
        self.default_firewall_rule.Create()
      if self.external_nets_rules:
        background_tasks.RunThreaded(
            lambda rule: self.external_nets_rules[rule].Create(),
            list(self.external_nets_rules.keys()),
        )
      if getattr(self, 'vpn_gateway', False):
        background_tasks.RunThreaded(
            lambda gateway: self.vpn_gateway[gateway].Create(),
            list(self.vpn_gateway.keys()),
        )
    if self.placement_group:
      self.placement_group.Create()

  def Delete(self):
    """Deletes the actual network."""
    if self.placement_group:
      self.placement_group.Delete()
    if not self.is_existing_network:
      if getattr(self, 'vpn_gateway', False):
        background_tasks.RunThreaded(
            lambda gateway: self.vpn_gateway[gateway].Delete(),
            list(self.vpn_gateway.keys()),
        )
      if self.default_firewall_rule.created:
        self.default_firewall_rule.Delete()
      if self.external_nets_rules:
        background_tasks.RunThreaded(
            lambda rule: self.external_nets_rules[rule].Delete(),
            list(self.external_nets_rules.keys()),
        )
      if self.subnet_resource:
        self.subnet_resource.Delete()
      self.network_resource.Delete()
