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
from perfkitbenchmarker import regex_util
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
    name = FLAGS.gce_network_name or 'pkb-network-%s' % FLAGS.run_uri

    self.net_type = 'default'  # Default network 'auto' mode. Single VPC with subnets in all regions
    self.subnet_addr = NETWORK_RANGE
    self.net_name = network.BaseNetwork.FormatCidrString(NETWORK_RANGE)

    if FLAGS.gce_subnet_region:  # Overrides auto network when set. Creates one VPC network in single region.
      self.net_type = 'single'
      self.subnet_addr = FLAGS.gce_subnet_addr
      self.net_name = network.BaseNetwork.FormatCidrString(FLAGS.gce_subnet_addr)
      name = FLAGS.gce_network_name or 'pkb-network-%s-%s-%s' % (self.net_type, self.net_name, FLAGS.run_uri)
    if network_spec.cidr:  # Creates multiple VPC networks when set.
      self.net_type = 'multi'
      self.subnet_addr = network_spec.cidr
      self.net_name = network.BaseNetwork.FormatCidrString(network_spec.cidr)
      name = FLAGS.gce_network_name or 'pkb-network-%s-%s-%s' % (self.net_type, self.net_name, FLAGS.run_uri)

    self.subnet_region = FLAGS.gce_subnet_region if network_spec.cidr is None else util.GetRegionFromZone(network_spec.zone)
    mode = 'auto' if self.subnet_region is None else 'custom'
    self.network_resource = GceNetworkResource(name, mode, self.project)
    if self.subnet_region is None:
      self.subnet_resource = None
    else:
      self.subnet_resource = GceSubnetResource(name, name,
                                               self.subnet_region,
                                               self.subnet_addr,
                                               self.project)

    self.all_nets = []  # Holds the different networks we learn about.
    self.external_nets_rules = {}  # Holds FW rules for any external subnets.
    self.default_subnet = None  # Holds the default subnet for this run if used.

    firewall_name = '%s-%s-%s-%s' % (self.net_type, 'internal', self.net_name, FLAGS.run_uri)
    self.default_firewall_rule = GceFirewallRule(
        firewall_name, self.project, ALLOW_ALL, name, self.subnet_addr)
    self.all_nets.append(self.net_name)
    if self.net_type != 'multi':  # We're using the default network.
      self.default_subnet = self.subnet_addr

    # Add firewall rules for other networks if they exist.
    if network_spec.custom_subnets:
      for net_group, cidr in network_spec.custom_subnets.items():
        this_net_string = None
        this_net = None
        if cidr is None and self.default_subnet is None:  # only add default rule once
          self.default_subnet = FLAGS.gce_subnet_addr if FLAGS.gce_subnet_region else NETWORK_RANGE
          this_net_string = network.BaseNetwork.FormatCidrString(self.default_subnet)
          this_net = self.default_subnet
        elif cidr:
          this_net_string = network.BaseNetwork.FormatCidrString(cidr)
          this_net = cidr
        if this_net and this_net_string not in self.all_nets:
          rule_name = '%s-%s-%s-%s' % (self.net_type, self.net_name, this_net_string, FLAGS.run_uri)
          self.external_nets_rules[rule_name] = GceFirewallRule(rule_name, self.project, ALLOW_ALL, name, this_net)
          self.all_nets.append(this_net_string)


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
