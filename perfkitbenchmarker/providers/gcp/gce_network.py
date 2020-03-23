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

import json
import logging
import threading

from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource

from perfkitbenchmarker.providers.gcp import util
import six

FLAGS = flags.FLAGS
NETWORK_RANGE = '10.0.0.0/8'
ALLOW_ALL = 'tcp:1-65535,udp:1-65535,icmp'


class GceVPNGW(network.BaseVPNGW):
  CLOUD = providers.GCP

  def __init__(self, name, network_name, region, cidr, project):
    super(GceVPNGW, self).__init__()

    self.forwarding_rules = {}
    self.tunnels = {}
    self.routes = {}
    self.ip_resource = None
    self.vpn_gw_resource = GceVPNGWResource(name, network_name, region, cidr, project)

    self.name = name
    self.network_name = network_name
    self.region = region
    self.cidr = cidr
    self.project = project

    self.ip_address = None
    self.created = False
    self.require_target_to_init = False
    self.routing = None
    self.psk = None

    # Add gateway to benchmark spec at init().
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    key = self.name
    with benchmark_spec.vpn_gws_lock:
      if key not in benchmark_spec.vpn_gws:
        benchmark_spec.vpn_gws[key] = self

  def ConfigureTunnel(self, tunnel_config):
    network.BaseVPNGW.ConfigureTunnel(self, tunnel_config)
    logging.info('Configuring Tunnel with params:')
    logging.info(tunnel_config)

    # update tunnel_config if needed
    if self.name not in tunnel_config.endpoints:
      logging.info('tunnel_config: This endpoint isnt registered yet... %s' % self.name)
      tunnel_config.endpoints[self.name] = {'is_configured': False,
                                            'cidr': self.cidr,
                                            'project': self.project,
                                            'network_name':  self.network_name,
                                            'region': self.region,
                                            'require_target_to_init': self.require_target_to_init,
                                            }

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
    self._SetupTunnel(tunnel_config)

    # configure routing
    # requires: next_hop_tunnel_id, target_cidr,
    dest_cidr = tunnel_config.endpoints[target_endpoint].get('cidr')
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
    vpn_gw_id = self.name
    target_ip = tunnel_config.endpoints[target_endpoint]['ip_address']
    psk = tunnel_config.psk
    ike_version = tunnel_config.ike_version
    suffix = tunnel_config.suffix
    name = 'tun-' + self.name + '-' + suffix
    if name not in self.tunnels:
      self.tunnels[name] = GceStaticTunnel(project, region, name, vpn_gw_id, target_ip, ike_version, psk)
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
    suffix = tunnel_config.suffix
    # GCP doesnt like uppercase names?!?
    fr_UDP500_name = ('fr-udp500-%s-%s' %
                      (suffix, FLAGS.run_uri))
    fr_UDP4500_name = ('fr-udp4500-%s-%s' %
                       (suffix, FLAGS.run_uri))
    fr_ESP_name = ('fr-esp-%s-%s' %
                   (suffix, FLAGS.run_uri))

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
    if self.created:
      return
    if self.vpn_gw_resource:
      self.vpn_gw_resource.Create()

    self.created = True

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

    if self.vpn_gw_resource:
      self.vpn_gw_resource.Delete()

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
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode

  def _Delete(self):
    cmd = util.GcloudCommand(self, 'compute', 'target-vpn-gateways', 'delete',
                             self.name)
    cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)


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
    self.ip_address = stdout.encode('ascii', 'ignore').rstrip().decode()

  def _Delete(self):
    """ Deletes a public IP for the VPN GW """
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'delete', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the IP address exists."""
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'describe',
                             self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode


class GceStaticTunnel(resource.BaseResource):
  """An object representing a GCE Tunnel."""

  def __init__(self, project, region, name, vpn_gw_id, target_ip, ike_version, psk):
    super(GceStaticTunnel, self).__init__()
    self.project = project
    self.region = region
    self.name = name
    self.vpn_gw_id = vpn_gw_id
    self.target_ip = target_ip
    self.ike_version = ike_version
    self.psk = psk

  def _Create(self):
    """Creates the Tunnel."""
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'create', self.name)
    cmd.flags['peer-address'] = self.target_ip
    cmd.flags['target-vpn-gateway'] = self.vpn_gw_id
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
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the tunnel exists."""
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'describe', self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode

  def IsReady(self):
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'describe', self.name)
    cmd.flags['region'] = self.region
    response = cmd.Issue(suppress_warning=True)
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
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the Route exists."""
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'describe',
                             self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode


class GceForwardingRule(resource.BaseResource):
  """An object representing a GCE Forwarding Rule."""

  def __init__(self, name, protocol, src_vpn_gw, port=None):
    super(GceForwardingRule, self).__init__()
    self.name = name
    self.protocol = protocol
    self.port = port
    self.target_name = src_vpn_gw.name
    self.target_ip = src_vpn_gw.ip_address
    self.src_region = src_vpn_gw.region
    self.project = src_vpn_gw.project

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
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the Forwarding Rule exists."""
    cmd = util.GcloudCommand(self, 'compute', 'forwarding-rules', 'describe',
                             self.name)
    cmd.flags['region'] = self.src_region
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
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
    # Gcloud create commands may still create firewalls despite being rate
    # limited.
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode:
      if cmd.rate_limited and 'already exists' in stderr:
        return
      debug_text = ('Ran: {%s}\nReturnCode:%s\nSTDOUT: %s\nSTDERR: %s' %
                    (' '.join(cmd.GetCommand()), retcode, stdout, stderr))
      raise errors.VmUtil.IssueCommandError(debug_text)

  def _Delete(self):
    """Deletes the Firewall Rule."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'delete',
                             self.name)
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the Firewall Rule exists."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'describe',
                             self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
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
        firewall_name = ('perfkit-firewall-%s-%s-%d-%d' %
                         (cidr_string, FLAGS.run_uri, start_port, end_port))
        key = (vm.project, vm.cidr, start_port, end_port, source_range)
      else:
        firewall_name = ('perfkit-firewall-%s-%d-%d' %
                         (FLAGS.run_uri, start_port, end_port))
        key = (vm.project, start_port, end_port, source_range)
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
    if FLAGS.gce_firewall_rules_clean_all:
      for firewall_rule in self._GetAllFirewallRules():
        firewall_rule.Delete()

    cmd = util.GcloudCommand(self, 'compute', 'networks', 'delete', self.name)
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns True if the Network resource exists."""
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'describe', self.name)
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode

  def _GetAllFirewallRules(self):
    """Returns all the firewall rules that use the network."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'list')
    cmd.flags['filter'] = 'network=%s' % self.name

    stdout, _, _ = cmd.Issue(suppress_warning=True)
    result = json.loads(stdout)
    return [GceFirewallRule(entry['name'], self.project, ALLOW_ALL, self.name,
                            NETWORK_RANGE) for entry in result]


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
    if self.region:
      cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    return not retcode

  def _Delete(self):
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'subnets', 'delete',
                             self.name)
    if self.region:
      cmd.flags['region'] = self.region
    cmd.Issue(raise_on_failure=False)


class GceNetwork(network.BaseNetwork):
  """Object representing a GCE Network."""

  CLOUD = providers.GCP

  def __init__(self, network_spec):
    super(GceNetwork, self).__init__(network_spec)
    self.project = network_spec.project
    self.vpn_gw = {}

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

    name = self._MakeGceNetworkName()

    subnet_region = (FLAGS.gce_subnet_region if not network_spec.cidr else
                     util.GetRegionFromZone(network_spec.zone))
    mode = 'auto' if subnet_region is None else 'custom'
    self.network_resource = GceNetworkResource(name, mode, self.project)
    if subnet_region is None:
      self.subnet_resource = None
    else:
      self.subnet_resource = GceSubnetResource(name, name, subnet_region,
                                               self.cidr, self.project)

    # Stage FW rules.
    self.all_nets = self._GetNetworksFromSpec(
        network_spec)  # Holds the different networks in this run.
    self.external_nets_rules = {}  # Holds FW rules for any external subnets.

    #  Set the default rule to allow all traffic within this network's subnet.
    firewall_name = self._MakeGceFWRuleName()
    self.default_firewall_rule = GceFirewallRule(
        firewall_name, self.project, ALLOW_ALL, name, self.cidr)

    # Set external rules to allow traffic from other subnets in this benchmark.
    for ext_net in self.all_nets:
      if ext_net == self.cidr:
        continue  # We've already added our own network to the default rule.
      rule_name = self._MakeGceFWRuleName(dst_cidr=ext_net)
      self.external_nets_rules[rule_name] = GceFirewallRule(rule_name,
                                                            self.project,
                                                            ALLOW_ALL, name,
                                                            ext_net)

    # Add VPNGWs to the network.
    if FLAGS.use_vpn:
      for gwnum in range(0, FLAGS.vpn_service_gateway_count):
        vpn_gw_name = 'vpngw-%s-%s-%s' % (
            util.GetRegionFromZone(network_spec.zone), gwnum, FLAGS.run_uri)
        self.vpn_gw[vpn_gw_name] = GceVPNGW(
            vpn_gw_name, name, util.GetRegionFromZone(network_spec.zone),
            network_spec.cidr, self.project)

  def _GetNetworksFromSpec(self, network_spec):
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
    gce_default_subnet = (FLAGS.gce_subnet_addr if FLAGS.gce_subnet_region
                          else NETWORK_RANGE)

    if hasattr(network_spec, 'custom_subnets'):
      for (_, v) in network_spec.custom_subnets.items():
        if not v['cidr'] and v['cloud'] != 'GCP':
          pass  # @TODO handle other providers defaults in net_util
        elif not v['cidr']:
          nets.add(gce_default_subnet)
        else:
          nets.add(v['cidr'])
    return nets

  def _MakeGceNetworkName(self, net_type=None, cidr=None, uri=None):
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
    if FLAGS.gce_network_name:  # Return user managed network name if defined.
      return FLAGS.gce_network_name

    net_type = net_type or self.net_type
    cidr = cidr or self.cidr
    uri = uri or FLAGS.run_uri

    name = 'pkb-network-%s' % uri  # Assume the default network naming.

    if net_type in (network.NetType.SINGLE.value,
                    network.NetType.MULTI.value):
      name = 'pkb-network-%s-%s-%s' % (
          net_type, self.FormatCidrString(cidr), uri)

    return name

  def _MakeGceFWRuleName(self, net_type=None, src_cidr=None, dst_cidr=None,
                         port_range_lo=None, port_range_hi=None, uri=None):
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
    src_cidr = 'internal' if src_cidr == dst_cidr else self.FormatCidrString(
        src_cidr)
    dst_cidr = self.FormatCidrString(dst_cidr)
    port_lo = port_range_lo
    port_hi = None if port_range_lo == port_range_hi else port_range_hi

    firewall_name = '-'.join(str(i) for i in (
        prefix, net_type, src_cidr, dst_cidr,
        port_lo, port_hi, uri) if i)

    return firewall_name

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns a BaseNetworkSpec created from VM attributes."""
    return GceNetworkSpec(project=vm.project, zone=vm.zone, cidr=vm.cidr)

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    if spec.cidr:
      return (cls.CLOUD, spec.project, spec.cidr)
    return (cls.CLOUD, spec.project)

  def Create(self):
    """Creates the actual network."""
    if not FLAGS.gce_network_name:
      self.network_resource.Create()
      if self.subnet_resource:
        self.subnet_resource.Create()
      if self.default_firewall_rule:
        self.default_firewall_rule.Create()
      if self.external_nets_rules:
        for rule in self.external_nets_rules:
          self.external_nets_rules[rule].Create()
      if getattr(self, 'vpn_gw', False):
        for gw in self.vpn_gw:
          self.vpn_gw[gw].Create()

  def Delete(self):
    """Deletes the actual network."""
    if not FLAGS.gce_network_name:
      if getattr(self, 'vpn_gw', False):
        for gw in self.vpn_gw:
          self.vpn_gw[gw].Delete()
      if self.default_firewall_rule.created:
        self.default_firewall_rule.Delete()
      if self.external_nets_rules:
        for rule in self.external_nets_rules:
          self.external_nets_rules[rule].Delete()
      if self.subnet_resource:
        self.subnet_resource.Delete()
      self.network_resource.Delete()
