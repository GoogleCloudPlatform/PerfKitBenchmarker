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

from perfkitbenchmarker import flags, context, errors
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util
import six

FLAGS = flags.FLAGS
NETWORK_RANGE = '10.0.0.0/8'
ALLOW_ALL = 'tcp:1-65535,udp:1-65535,icmp'


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


class GceVPNGW(network.BaseVPNGW):
  CLOUD = providers.GCP

  def __init__(self, name, network_name, region, cidr, project):
    super(GceVPNGW, self).__init__()
    self._lock = threading.Lock()
    self.forwarding_rules = {}
    self.tunnels = {}
    self.routes = {}
    self.name = name
    self.network_name = network_name
    self.region = region
    self.cidr = cidr
    self.project = project
    self.IP_ADDR = None
    self.vpngw_resource = GceVPNGWResource(name, network_name, region, cidr, project)
    self.created = False

  def AllocateIP(self):
    """ Allocates a public IP for the VPN GW """
    #  create the address on our gw
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'create', self.name)
    cmd.flags['region'] = self.region

    cmd.Issue()
#     cmd.flags['format'] = 'json'
#     stdout, _, _  = cmd.Issue()
#     result = json.loads(stdout)
#     self.IP_ADDR = result['address']

    #  store the address to this object
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'describe', self.name)
    cmd.flags['region'] = self.region
    # cmd.flags['format'] = '\"value(address)\"'
    cmd.flags['format'] = 'value(address)'
    stdout, _, _ = cmd.Issue()
    self.IP_ADDR = stdout.encode('ascii', 'ignore').rstrip()
    # works at CLI but not here?!?
    # gcloud compute addresses describe vpngw-us-central1-f0d624e8 --format "value(address)" --quiet --project cloud-performance-tool --region us-central1
    # STDERR: ERROR: (gcloud.compute.addresses.describe) Name expected [default *HERE* "value(address)"].

  def DeleteIP(self):
    """ Deletes a public IP for the VPN GW """
    #  delete the address on our gw
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'delete', self.name)
    cmd.flags['region'] = self.region
    cmd.Issue()

  def IPExists(self):
    """Returns True if the Route exists."""
    cmd = util.GcloudCommand(self, 'compute', 'addresses', 'describe',
                             self.name)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode


  def SetupForwarding(self):
    """Create IPSec forwarding rules between the source gw and the target gw.
    Forwards ESP protocol, and UDP 500/4500 for tunnel setup

    Args:
      source_gw: The BaseVPN object to add forwarding rules to.
      target_gw: The BaseVPN object to point forwarding rules at.
    """
    # GCP doesnt like uppercase names?!?
    fr_UDP500_name = ('fr-udp500-%s-%s' %
                      (self.region, FLAGS.run_uri))
    fr_UDP4500_name = ('fr-udp4500-%s-%s' %
                       (self.region, FLAGS.run_uri))
    fr_ESP_name = ('fr-esp-%s-%s' %
                   (self.region, FLAGS.run_uri))
    with self._lock:
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

  def SetupTunnel(self, target_gw, psk):
    """Create IPSec tunnel between the source gw and the target gw.

    Args:
      source_gw: The BaseVPN object to add forwarding rules to.
      target_gw: The BaseVPN object to point forwarding rules at.
      psk: preshared key (or run uri for now)
    """
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'create', 'tunnel' + self.name)
    cmd.flags['peer-address'] = target_gw.IP_ADDR
    cmd.flags['target-vpn-gateway'] = self.name
    cmd.flags['ike-version'] = '2'
    cmd.flags['local-traffic-selector'] = '0.0.0.0/0'
    cmd.flags['remote-traffic-selector'] = '0.0.0.0/0'
    cmd.flags['shared-secret'] = psk
    cmd.flags['region'] = self.region
    self.tunnels['tunnel' + self.name] = cmd.Issue()

  def DeleteTunnel(self, tunnel):
    """Delete IPSec tunnel
    """
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'delete', tunnel)
    cmd.flags['region'] = self.region
    cmd.Issue()

  def TunnelExists(self, tunnel):
    """Returns True if the Route exists."""
    cmd = util.GcloudCommand(self, 'compute', 'vpn-tunnels', 'describe', tunnel)
    cmd.flags['region'] = self.region
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def SetupRouting(self, target_gw):
    """Create IPSec forwarding rules between the source gw and the target gw.
    Forwards ESP protocol, and UDP 500/4500 for tunnel setup

    Args:
      source_gw: The BaseVPN object to add forwarding rules to.
      target_gw: The BaseVPN object to point forwarding rules at.
    """
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'create', 'route' + self.name)
    cmd.flags['destination-range'] = target_gw.cidr
    cmd.flags['network'] = self.network_name
    cmd.flags['next-hop-vpn-tunnel'] = 'tunnel' + self.name
    cmd.flags['next-hop-vpn-tunnel-region'] = self.region
    self.routes['route' + self.name] = cmd.Issue()

  def DeleteRoute(self, route):
    """Delete route

    Args:
      route: The route name to delete
    """
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'delete', route)
    cmd.Issue()

  def RouteExists(self, route):
    """Returns True if the Route exists."""
    cmd = util.GcloudCommand(self, 'compute', 'routes', 'describe',
                             route)
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode


  def Create(self):
    """Creates the actual VPNGW."""
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    with self._lock:
      if self.created:
        return
      if self.vpngw_resource:
        self.vpngw_resource.Create()
    key = self.name
    with benchmark_spec.vpngws_lock:
      if key not in benchmark_spec.vpngws:
        benchmark_spec.vpngws[key] = self
      return benchmark_spec.vpngws[key]
      self.created = True

  def Delete(self):
    """Deletes the actual VPNGW."""
    if self.IP_ADDR and self.IPExists():
      self.DeleteIP()

    if self.forwarding_rules:
      for fr in self.forwarding_rules:
        self.forwarding_rules[fr].Delete()

    if self.tunnels:
      for tun in self.tunnels:
        if self.TunnelExists(tun):
          self.DeleteTunnel(tun)

    if self.routes:
      for route in self.routes:
        if self.RouteExists(route):
          self.DeleteTunnel(route)

    self.created = False

    # vpngws need deleted last
    if self.vpngw_resource:
      self.vpngw_resource.Delete()


class GceForwardingRule(resource.BaseResource):
  """An object representing a GCE Forwarding Rule."""

  def __init__(self, name, protocol, src_vpngw, port=None):
    super(GceForwardingRule, self).__init__()
    self.name = name
    self.protocol = protocol
    self.port = port
    self.target_name = src_vpngw.name
    self.target_ip = src_vpngw.IP_ADDR
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
    # add support for zone, cidr, and separate networks
    if network_spec.zone and network_spec.cidr:
      firewall_name = 'default-internal-%s-%s' % (network_spec.zone, FLAGS.run_uri)
      self.NETWORK_RANGE = network_spec.cidr
    self.default_firewall_rule = GceFirewallRule(
        firewall_name, self.project, ALLOW_ALL, name, NETWORK_RANGE)
    # add VPNGW to the network
    if network_spec.zone and network_spec.cidr and FLAGS.use_vpn:
      vpngw_name = 'vpngw-%s-%s' % (
          util.GetRegionFromZone(network_spec.zone), FLAGS.run_uri)
      self.vpngw = GceVPNGW(
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
      if getattr(self, 'vpngw', False):
        self.vpngw.Create()

  def Delete(self):
    """Deletes the actual network."""
    if not FLAGS.gce_network_name:
      self.default_firewall_rule.Delete()
      if self.subnet_resource:
        self.subnet_resource.Delete()
      if getattr(self, 'vpngw', False):
        self.vpngw.Delete()
      self.network_resource.Delete()
