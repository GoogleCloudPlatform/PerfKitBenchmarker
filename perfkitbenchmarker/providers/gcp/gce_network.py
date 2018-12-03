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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading
import collections
import logging
import uuid

from perfkitbenchmarker import flags, context, errors
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util
import six

FLAGS = flags.FLAGS
NETWORK_RANGE = '10.0.0.0/8'  # @TODO alias rfc1918 addys for single rule
NETWORK_RANGE2 = '192.168.0.0/16'
ALLOW_ALL = 'tcp:1-65535,udp:1-65535,icmp'


class GceVPNGW(network.BaseVPNGW):
  CLOUD = providers.GCP

  def __init__(self, name, network_name, region, cidr, project):
    super(GceVPNGW, self).__init__()

    self.forwarding_rules = {}
    self.tunnels = {}
    self.routes = {}
    self.ip_resource = None
    self.vpngw_resource = GceVPNGWResource(name, network_name, region, cidr, project)

    self.name = name
    self.network_name = network_name
    self.region = region
    self.cidr = cidr
    self.project = project

    self.ip_address = None
    self.created = False
#     self.suffix = collections.defaultdict(dict)  # @TODO remove - holds uuid tokens for naming/finding things (double dict)
    self.require_target_to_init = False
    self.routing = None
    self.psk = None

  def ConfigureTunnel(self, tunnel_config):
    network.BaseVPNGW.ConfigureTunnel(self, tunnel_config)
    logging.info('Configuring Tunnel with params:')
    logging.info(tunnel_config)

    # update tunnel_config if needed
    if self.name not in tunnel_config.endpoints:
      logging.info('tunnel_config: This endpoint isnt registered yet... %s' % self.name)
      tunnel_config.endpoints[self.name] = {}
      tunnel_config.endpoints[self.name]['is_configured'] = False
      tunnel_config.endpoints[self.name]['cidr'] = self.cidr
      tunnel_config.endpoints[self.name]['project'] = self.project
      tunnel_config.endpoints[self.name]['network_name'] = self.network_name
      tunnel_config.endpoints[self.name]['region'] = self.region
      tunnel_config.endpoints[self.name]['require_target_to_init'] = self.require_target_to_init

    # attach public IP to this GW if doesnt exist
    # and update tunnel_config if needed
    # requires project, region, name
    if not self.ip_address:
      if not self.ip_resource:
        self.ip_resource = GceIPAddress(self.project, self.region, self.name)
        self.ip_resource.Create()
      self.ip_address = self.ip_resource.ip_address
    if 'ip_address' not in tunnel_config.endpoints[self.name]:
      logging.info('tunnel_config: Configuring IP for %s' % self.name)
      tunnel_config.endpoints[self.name]['ip_address'] = self.ip_address
#       logging.info(tunnel_config)

    # configure forwarding
    # requires: -
    if len(self.forwarding_rules) == 3:
      logging.info('tunnel_config: Forwarding already configured, skipping')
    else:
      logging.info('tunnel_config: Setting up forwarding')
      self._SetupForwarding(tunnel_config)

    # Abort if we don't have a target info configured yet
    if len(tunnel_config.endpoints) < 2:
      logging.info('tunnel_config: Only found %d endpoints... waiting for target to configure' % len(tunnel_config.endpoints))
      return

    # Get target endpoint config key
    target_endpoint = [k for k in tunnel_config.endpoints.keys() if k not in self.name][0]

    # configure tunnel resources
    # requires: target_ip_address, IKE version (default 1),
    if 'ip_address' not in tunnel_config.endpoints[target_endpoint]:
      logging.info('tunnel_config: Target IP needed... waiting for target to configure')
      return
    if not hasattr(tunnel_config, 'psk'):
      logging.info('tunnel_config: PSK not provided... setting to runid')
      tunnel_config.psk = 'key' + FLAGS.run_uri
#     if 'suffix' not in tunnel_config.endpoints[self.name]:
#       tunnel_config.endpoints[self.name]['suffix'] = format(uuid.uuid4().fields[1], 'x')  # unique enough
#       logging.info('tunnel_config: setting suffix to %s, %s' % (tunnel_config.endpoints[self.name]['suffix'], type(tunnel_config.endpoints[self.name]['suffix'])))

#     if 'ike_version' not in tunnel_config.endpoints[self.name]:
#       tunnel_config.endpoints[self.name]['ike_version'] = '2'
    self._SetupTunnel(tunnel_config)

    # configure routing
    # requires: next_hop_tunnel_id, target_cidr,
    dest_cidr = tunnel_config.endpoints[target_endpoint].get('cidr')
#     logging.info('tunnel_config: dest_cidr- %s, %s, %s' % (dest_cidr, tunnel_config.endpoints[target_endpoint]['cidr'], dest_cidr.strip()))
    if not dest_cidr or not dest_cidr.strip():
      logging.info('tunnel_config: destination CIDR needed... waiting for target to configure')
      return
    self._SetupRouting(
        tunnel_config.suffix,
        tunnel_config.endpoints[self.name]['tunnel_id'],
        tunnel_config.endpoints[target_endpoint]['cidr'])

    tunnel_config.endpoints[self.name]['is_configured'] = True

  def IsTunnelReady(self, tunnel_id):
    return self.tunnels[tunnel_id].IsReady()

  def _SetupTunnel(self, tunnel_config):
    target_endpoint = [k for k in tunnel_config.endpoints.keys() if k not in self.name][0]
    project = tunnel_config.endpoints[self.name]['project']
    region = tunnel_config.endpoints[self.name]['region']
    vpngw_id = self.name
    target_ip = tunnel_config.endpoints[target_endpoint]['ip_address']
    psk = tunnel_config.psk
    ike_version = tunnel_config.ike_version
    suffix = tunnel_config.suffix
    name = 'tun-' + self.name + '-' + suffix
    if name not in self.tunnels:
      self.tunnels[name] = GceStaticTunnel(project, region, name, vpngw_id, target_ip, ike_version, psk)
      self.tunnels[name].Create()
      tunnel_config.endpoints[self.name]['tunnel_id'] = name

  def _SetupForwarding(self, tunnel_config):
    """Create IPSec forwarding rules
    Forwards ESP protocol, and UDP 500/4500 for tunnel setup

    Args:
      source_gw: The BaseVPN object to add forwarding rules to.
    """
    if len(self.forwarding_rules) == 3:
      return  # backout if already set

#     self.suffix[suffix]['fr_suffix'] = self.region + '-' + suffix
#     # GCP doesnt like uppercase names?!?
#     fr_UDP500_name = ('fr-udp500-%s-%s' %
#                       (self.suffix[suffix]['fr_suffix'], FLAGS.run_uri))
#     fr_UDP4500_name = ('fr-udp4500-%s-%s' %
#                        (self.suffix[suffix]['fr_suffix'], FLAGS.run_uri))
#     fr_ESP_name = ('fr-esp-%s-%s' %
#                    (self.suffix[suffix]['fr_suffix'], FLAGS.run_uri))
    suffix = tunnel_config.suffix
    # GCP doesnt like uppercase names?!?
    fr_UDP500_name = ('fr-udp500-%s-%s' %
                      (suffix, FLAGS.run_uri))
    fr_UDP4500_name = ('fr-udp4500-%s-%s' %
                       (suffix, FLAGS.run_uri))
    fr_ESP_name = ('fr-esp-%s-%s' %
                   (suffix, FLAGS.run_uri))

    # with self._lock:
    if fr_UDP500_name not in self.forwarding_rules:
      fr_UDP500 = GceForwardingRule(
          fr_UDP500_name, 'UDP', self, 500)
      self.forwarding_rules[fr_UDP500_name] = fr_UDP500
      fr_UDP500.Create()
    if fr_UDP4500_name not in self.forwarding_rules:
      fr_UDP4500 = GceForwardingRule(
          fr_UDP4500_name, 'UDP', self, 4500)
      self.forwarding_rules[fr_UDP4500_name] = fr_UDP4500
      fr_UDP4500.Create()
    if fr_ESP_name not in self.forwarding_rules:
      fr_ESP = GceForwardingRule(
          fr_ESP_name, 'ESP', self)
      self.forwarding_rules[fr_ESP_name] = fr_ESP
      fr_ESP.Create()

  def _SetupRouting(self, suffix, next_hop_tun, dest_cidr):
    """Create IPSec routing rules between the source gw and the target gw.

    Args:
      target_gw: The VPNGW object to point routing rules at.
    """

    route_name = 'route-' + self.name + '-' + suffix
    if route_name not in self.routes:
      self.routes[route_name] = GceRoute(route_name, dest_cidr, self.network_name, next_hop_tun, self.region, self.project, self.region)
      self.routes[route_name].Create()

  def Create(self):
    """Creates the actual VPNGW."""
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    # with self._lock:
    if self.created:
      return
    if self.vpngw_resource:
      self.vpngw_resource.Create()
    key = self.name
    # with benchmark_spec.vpngws_lock:
    if key not in benchmark_spec.vpngws:
      benchmark_spec.vpngws[key] = self

    self.created = True
    return benchmark_spec.vpngws[key]

  def Delete(self):
    """Deletes the actual VPNGW."""
    if self.ip_resource:
      self.ip_resource.Delete()

    if self.tunnels:
      for tun in self.tunnels:
        self.tunnels[tun].Delete()

    if self.forwarding_rules:
      for fr in self.forwarding_rules:
        self.forwarding_rules[fr].Delete()

    if self.routes:
      for route in self.routes:
        self.routes[route].Delete()

    if self.vpngw_resource:
      self.vpngw_resource.Delete()

    self.created = False


class GceVPNGWResource(resource.BaseResource):

  def __init__(self, name, network_name, region, cidr, project):
    super(GceVPNGWResource, self).__init__()
    self.name = name
    self.network_name = network_name
    self.region = region
    self.cidr = cidr
    self.project = project

  def _Create(self):
    cmd = util.GcloudCommand(self, 'compute', 'target-vpn-gateways', 'create',
                             self.name)
    cmd.flags['network'] = self.network_name
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Exists(self):
    cmd = util.GcloudCommand(self, 'compute', 'target-vpn-gateways', 'describe',
                             self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def _Delete(self):
    cmd = util.GcloudCommand(self, 'compute', 'target-vpn-gateways', 'delete',
                             self.name)
    cmd.flags['region'] = self.region
    cmd.Issue()


class GceIPAddress(resource.BaseResource):

  def __init__(self, project, region, name):
    super(GceIPAddress, self).__init__()
    self.project = project
    self.region = region
    self.name = name
    self.ip_address = None

  def _Create(self):
    """ Allocates a public IP for the VPN GW """
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'create', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _PostCreate(self):
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'describe', self.name)
    cmd.flags['region'] = self.region
    cmd.flags['format'] = 'value(address)'
    stdout, _, _ = cmd.Issue()
    self.ip_address = stdout.encode('ascii', 'ignore').rstrip()

  def _Delete(self):
    """ Deletes a public IP for the VPN GW """
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'delete', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Exists(self):
    """Returns True if the IP address exists."""
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'describe',
                             self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode


class GceStaticTunnel(resource.BaseResource):
  """An object representing a GCE Tunnel."""

  def __init__(self, project, region, name, vpngw_id, target_ip, ike_version, psk):
    super(GceStaticTunnel, self).__init__()
    self.project = project
    self.region = region
    self.name = name
    self.vpngw_id = vpngw_id
    self.target_ip = target_ip
    self.ike_version = ike_version
    self.psk = psk

  def _Create(self):
    """Creates the Tunnel."""
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'create', self.name)
    cmd.flags['peer-address'] = self.target_ip
    cmd.flags['target-vpn-gateway'] = self.vpngw_id
    cmd.flags['ike-version'] = self.ike_version
    cmd.flags['local-traffic-selector'] = '0.0.0.0/0'
    cmd.flags['remote-traffic-selector'] = '0.0.0.0/0'
    cmd.flags['shared-secret'] = self.psk
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Delete(self):
    """Delete IPSec tunnel
    """
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'delete', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue()

  def _Exists(self):
    """Returns True if the tunnel exists."""
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'describe', self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def IsReady(self):
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'describe', self.name)
    cmd.flags['region'] = self.region
    response = cmd.Issue(suppress_warning=True)
#     logging.info('GCP Tunnel Status: %s' % repr(response))
    return 'established' in str(response).lower()


class GceRoute(resource.BaseResource):
  """An object representing a GCE Route."""

  def __init__(self, route_name, dest_cidr, network_name, next_hop_tun, next_hop_region, project, region):
    super(GceRoute, self).__init__()
    self.name = route_name
    self.dest_cidr = dest_cidr
    self.next_hop_region = next_hop_region
    self.next_hop_tun = next_hop_tun
    self.network_name = network_name
    self.project = project
    self.region = region

  def _Create(self):
    """Creates the Route."""
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'create', self.name)
    cmd.flags['destination-range'] = self.dest_cidr
    cmd.flags['network'] = self.network_name
    cmd.flags['next-hop-vpn-tunnel'] = self.next_hop_tun
    cmd.flags['next-hop-vpn-tunnel-region'] = self.next_hop_region
    cmd.Issue()
#     return self.id

  def _Delete(self):
    """Delete route

    Args:
      route: The route name to delete
    """
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'delete', self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns True if the Route exists."""
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'describe',
                             self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode


class GceForwardingRule(resource.BaseResource):
  """An object representing a GCE Forwarding Rule."""

  def __init__(self, name, protocol, src_vpngw, port=None):
    super(GceForwardingRule, self).__init__()
    self.name = name
    self.protocol = protocol
    self.port = port
    self.target_name = src_vpngw.name
    self.target_ip = src_vpngw.ip_address
    self.src_region = src_vpngw.region
    self.project = src_vpngw.project

  def __eq__(self, other):
    """Defines equality to make comparison easy."""
    return (self.name == other.name and
            self.protocol == other.protocol and
            self.port == other.port and
            self.target_name == other.target_name and
            self.target_ip == other.target_ip and
            self.src_region == other.src_region)

  def _Create(self):
    """Creates the Forwarding Rule."""
    cmd = util.GcloudCommand(self, 'compute', 'forwarding-rules', 'create',
                             self.name)
    cmd.flags['ip-protocol'] = self.protocol
    if self.port:
      cmd.flags['ports'] = self.port
    cmd.flags['address'] = self.target_ip
    cmd.flags['target-vpn-gateway'] = self.target_name
    cmd.flags['region'] = self.src_region
    cmd.Issue()

  def _Delete(self):
    """Deletes the Forwarding Rule."""
    cmd = util.GcloudCommand(self, 'compute', 'forwarding-rules', 'delete',
                             self.name)
    cmd.flags['region'] = self.src_region
    cmd.Issue()

  def _Exists(self):
    """Returns True if the Forwarding Rule exists."""
    cmd = util.GcloudCommand(self, 'compute', 'forwarding-rules', 'describe',
                             self.name)
    cmd.flags['region'] = self.src_region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode


class GceFirewallRule(resource.BaseResource):
  """An object representing a GCE Firewall Rule."""

  def __init__(self, name, project, allow, network_name, source_range=None):
    super(GceFirewallRule, self).__init__()
    self.name = name
    self.project = project
    self.allow = allow
    self.network_name = network_name
    self.source_range = source_range

  def __eq__(self, other):
    """Defines equality to make comparison easy."""
    return (self.name == other.name and
            self.allow == other.allow and
            self.project == other.project and
            self.network_name == other.network_name and
            self.source_range == other.source_range)

  def _Create(self):
    """Creates the Firewall Rule."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'create',
                             self.name)
    cmd.flags['allow'] = self.allow
    cmd.flags['network'] = self.network_name
    if self.source_range:
      cmd.flags['source-ranges'] = self.source_range
    cmd.Issue()

  def _Delete(self):
    """Deletes the Firewall Rule."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'delete',
                             self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns True if the Firewall Rule exists."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'describe',
                             self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode


class GceFirewall(network.BaseFirewall):
  """An object representing the GCE Firewall."""

  CLOUD = providers.GCP

  def __init__(self):
    """Initialize GCE firewall class."""
    self._lock = threading.Lock()
    self.firewall_rules = {}

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
      source_range: The source ip range to allow for this port.
    """
    if vm.is_static:
      return
    with self._lock:
      if end_port is None:
        end_port = start_port
      firewall_name = ('perfkit-firewall-%s-%s-%d-%d' %
                       (vm.zone, FLAGS.run_uri, start_port, end_port))
      key = (vm.project, vm.zone, start_port, end_port)
      if key in self.firewall_rules:
        return
      allow = ','.join('{0}:{1}-{2}'.format(protocol, start_port, end_port)
                       for protocol in ('tcp', 'udp'))
      firewall_rule = GceFirewallRule(
          firewall_name, vm.project, allow,
          vm.network.network_resource.name, source_range)
      self.firewall_rules[key] = firewall_rule
      firewall_rule.Create()

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    for firewall_rule in six.itervalues(self.firewall_rules):
      firewall_rule.Delete()


class GceNetworkSpec(network.BaseNetworkSpec):

  def __init__(self, project=None, **kwargs):
    """Initializes the GceNetworkSpec.

    Args:
      project: The project for which the Network should be created.
      **kwargs: Additional key word arguments passed to BaseNetworkSpec.
    """
    super(GceNetworkSpec, self).__init__(**kwargs)
    self.project = project


class GceNetworkResource(resource.BaseResource):
  """Object representing a GCE Network resource."""

  def __init__(self, name, mode, project):
    super(GceNetworkResource, self).__init__()
    self.name = name
    self.mode = mode
    self.project = project

  def _Create(self):
    """Creates the Network resource."""
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'create', self.name)
    cmd.flags['subnet-mode'] = self.mode
    cmd.Issue()

  def _Delete(self):
    """Deletes the Network resource."""
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'delete', self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns True if the Network resource exists."""
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode


class GceSubnetResource(resource.BaseResource):

  def __init__(self, name, network_name, region, addr_range, project):
    super(GceSubnetResource, self).__init__()
    self.name = name
    self.network_name = network_name
    self.region = region
    self.addr_range = addr_range
    self.project = project

  def _Create(self):
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'subnets', 'create',
                             self.name)
    cmd.flags['network'] = self.network_name
    cmd.flags['region'] = self.region
    cmd.flags['range'] = self.addr_range
    cmd.Issue()

  def _Exists(self):
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'subnets', 'describe',
                             self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def _Delete(self):
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'subnets', 'delete',
                             self.name)
    cmd.flags['region'] = self.region
    cmd.Issue()


class GceNetwork(network.BaseNetwork):
  """Object representing a GCE Network."""

  CLOUD = providers.GCP

  def __init__(self, network_spec):
    super(GceNetwork, self).__init__(network_spec)
    self.project = network_spec.project
    name = FLAGS.gce_network_name or 'vpnpkb-network-%s' % FLAGS.run_uri
    self.vpngw = {}

    # add support for zone, cidr, and separate networks
    if network_spec.zone and network_spec.cidr:
      name = FLAGS.gce_network_name or 'pkb-vpnnetwork-%s-%s' % (network_spec.zone, FLAGS.run_uri)
      FLAGS.gce_subnet_region = util.GetRegionFromZone(network_spec.zone)
      FLAGS.gce_subnet_addr = network_spec.cidr

    mode = 'auto' if FLAGS.gce_subnet_region is None else 'custom'
    self.network_resource = GceNetworkResource(name, mode, self.project)
    if FLAGS.gce_subnet_region is None:
      self.subnet_resource = None
    else:
      #  do we need distinct subnet_name? subnets duplicate network_name now
      #  subnet_name = FLAGS.gce_subnet_name or 'pkb-subnet-%s' % FLAGS.run_uri
      self.subnet_resource = GceSubnetResource(name, name,
                                               FLAGS.gce_subnet_region,
                                               FLAGS.gce_subnet_addr,
                                               self.project)

    firewall_name = 'default-internal-%s' % FLAGS.run_uri
    # allow 192.168.0.0/16 addresses
    firewall_name2 = 'default-internal2-%s' % FLAGS.run_uri
    # add support for zone, cidr, and separate networks
    # @TODO alias RFC1918 private networks in a single rule
    if network_spec.zone and network_spec.cidr:
      firewall_name = 'default-internal-%s-%s' % (network_spec.zone, FLAGS.run_uri)
      firewall_name2 = 'default-internal2-%s-%s' % (network_spec.zone, FLAGS.run_uri)
      self.NETWORK_RANGE = network_spec.cidr
    self.default_firewall_rule = GceFirewallRule(
        firewall_name, self.project, ALLOW_ALL, name, NETWORK_RANGE)
    self.default_firewall_rule2 = GceFirewallRule(
        firewall_name2, self.project, ALLOW_ALL, name, NETWORK_RANGE2)
    # add VPNGW to the network
    if network_spec.zone and network_spec.cidr and FLAGS.use_vpn:
      for gwnum in range(0, FLAGS.vpn_service_gateway_count):
        vpngw_name = 'vpngw-%s-%s-%s' % (
            util.GetRegionFromZone(network_spec.zone), gwnum, FLAGS.run_uri)
        self.vpngw[vpngw_name] = GceVPNGW(
            vpngw_name, name, util.GetRegionFromZone(network_spec.zone),
            network_spec.cidr, self.project)

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns a BaseNetworkSpec created from VM attributes."""
    return GceNetworkSpec(project=vm.project, zone=vm.zone, cidr=vm.cidr)

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    if spec.zone and spec.cidr:
      return (cls.CLOUD, spec.project, spec.zone)
    return (cls.CLOUD, spec.project)

  def Create(self):
    """Creates the actual network."""
    if not FLAGS.gce_network_name:
      self.network_resource.Create()
      if self.subnet_resource:
        self.subnet_resource.Create()
      self.default_firewall_rule.Create()
      self.default_firewall_rule2.Create()
      if getattr(self, 'vpngw', False):
        for gw in self.vpngw:
          self.vpngw[gw].Create()

  def Delete(self):
    """Deletes the actual network."""
    if not FLAGS.gce_network_name:
      if getattr(self, 'vpngw', False):
        for gw in self.vpngw:
          self.vpngw[gw].Delete()
      self.default_firewall_rule.Delete()
      self.default_firewall_rule2.Delete()
      if self.subnet_resource:
        self.subnet_resource.Delete()
#       if getattr(self, 'vpngw', False):
#         for gw in self.vpngw:
#           self.vpngw[gw].Delete()
      self.network_resource.Delete()
