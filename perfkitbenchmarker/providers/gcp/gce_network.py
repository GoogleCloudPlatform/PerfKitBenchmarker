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
import threading

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

    subnet_region = FLAGS.gce_subnet_region if not network_spec.cidr else \
        util.GetRegionFromZone(network_spec.zone)
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
    gce_default_subnet = FLAGS.gce_subnet_addr if FLAGS.gce_subnet_region \
        else NETWORK_RANGE

    if network_spec.custom_subnets:
      for (k, v) in network_spec.custom_subnets.items():
        if not v['cidr'] and not v['cloud'] == 'GCP':
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

    _net_type = self.net_type if not net_type else net_type
    _cidr = self.cidr if not cidr else cidr
    _uri = FLAGS.run_uri if not uri else uri

    name = 'pkb-network-%s' % _uri  # Assume the default network naming.

    if _net_type in (network.NetType.SINGLE.value,
                     network.NetType.MULTI.value):
      name = 'pkb-network-%s-%s-%s' % (
          _net_type, self.FormatCidrString(_cidr), _uri)

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
    Returns: The name of this firewall rule.
    """

    _net_type = self.net_type if not net_type else net_type
    _uri = FLAGS.run_uri if not uri else uri
    _src_cidr = self.cidr if not src_cidr else src_cidr
    _dst_cidr = self.cidr if not dst_cidr else dst_cidr
    _prefix = None if _src_cidr == _dst_cidr else 'perfkit-firewall'
    _src_cidr = 'internal' if _src_cidr == _dst_cidr else self.FormatCidrString(
        _src_cidr)
    _dst_cidr = self.FormatCidrString(_dst_cidr)
    _port_lo = port_range_lo
    _port_hi = None if port_range_lo == port_range_hi else port_range_hi

    firewall_name = '-'.join(str(i) for i in (
        _prefix, _net_type, _src_cidr, _dst_cidr,
        _port_lo, _port_hi, _uri) if i)

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

  def Delete(self):
    """Deletes the actual network."""
    if not FLAGS.gce_network_name:
      if self.default_firewall_rule.created:
        self.default_firewall_rule.Delete()
      if self.external_nets_rules:
        for rule in self.external_nets_rules:
          self.external_nets_rules[rule].Delete()
      if self.subnet_resource:
        self.subnet_resource.Delete()
      self.network_resource.Delete()
