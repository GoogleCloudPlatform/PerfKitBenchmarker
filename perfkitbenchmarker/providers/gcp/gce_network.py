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

import threading

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker import providers

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
    """Initialize GCE firewall class.

    Args:
      project: The GCP project name under which firewall is created.
    """
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
      firewall_name = ('perfkit-firewall-%s-%d-%d' %
                       (FLAGS.run_uri, start_port, end_port))
      key = (vm.project, start_port, end_port)
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
    for firewall_rule in self.firewall_rules.itervalues():
      firewall_rule.Delete()


class GceNetworkSpec(network.BaseNetworkSpec):

  def __init__(self, project=None, **kwargs):
    """Initializes the GceNetworkSpec.

    Args:
      project: The project for which the Network should be created.
      kwargs: Additional key word arguments passed to BaseNetworkSpec.
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
    cmd.flags['mode'] = self.mode
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
    _, _, retcode = cmd.Issue(suppress_warning=True)
    return not retcode

  def _Delete(self):
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'subnets', 'delete',
                             self.name)
    cmd.Issue()



class GceNetwork(network.BaseNetwork):
  """Object representing a GCE Network."""

  CLOUD = providers.GCP

  def __init__(self, network_spec):
    super(GceNetwork, self).__init__(network_spec)
    self.project = network_spec.project
    name = FLAGS.gce_network_name or 'pkb-network-%s' % FLAGS.run_uri
    mode = 'auto' if FLAGS.gce_subnet_region is None else 'custom'
    self.network_resource = GceNetworkResource(name, mode, self.project)
    if FLAGS.gce_subnet_region is None:
      self.subnet_resource = None
    else:
      self.subnet_resource = GceSubnetResource(name, name,
                                               FLAGS.gce_subnet_region,
                                               FLAGS.gce_subnet_addr,
                                               self.project)
    firewall_name = 'default-internal-%s' % FLAGS.run_uri
    self.default_firewall_rule = GceFirewallRule(
        firewall_name, self.project, ALLOW_ALL, name, NETWORK_RANGE)

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns a BaseNetworkSpec created from VM attributes."""
    return GceNetworkSpec(project=vm.project)

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    return (cls.CLOUD, spec.project)

  def Create(self):
    """Creates the actual network."""
    if FLAGS.gce_network_name is None:
      self.network_resource.Create()
      if self.subnet_resource:
        self.subnet_resource.Create()
      self.default_firewall_rule.Create()

  def Delete(self):
    """Deletes the actual network."""
    if FLAGS.gce_network_name is None:
      self.default_firewall_rule.Delete()
      if self.subnet_resource:
        self.subnet_resource.Delete()
      self.network_resource.Delete()
