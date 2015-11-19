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

FLAGS = flags.FLAGS
GCP = 'GCP'


class GceFirewallRule(resource.BaseResource):
  """An object representing a GCE Firewall Rule."""

  def __init__(self, name, project, port, network):
    super(GceFirewallRule, self).__init__()
    self.name = name
    self.project = project
    self.port = port
    self.network_name = network.network_resource.name

  def __eq__(self, other):
    """Defines equality to make comparison easy."""
    return (self.name == other.name and
            self.port == other.port and
            self.project == other.project and
            self.network_name == other.network_name)

  def _Create(self):
    """Creates the Firewall Rule."""
    cmd = util.GcloudCommand(self, 'compute', 'firewall-rules', 'create',
                             self.name)
    cmd.flags['allow'] = ','.join('{0}:{1}'.format(protocol, self.port)
                                  for protocol in ('tcp', 'udp'))
    cmd.flags['network'] = self.network_name
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

  CLOUD = GCP

  def __init__(self):
    """Initialize GCE firewall class.

    Args:
      project: The GCP project name under which firewall is created.
    """
    self._lock = threading.Lock()
    self.firewall_rules = {}

  def AllowPort(self, vm, port):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      port: The local port to open.
    """
    if vm.is_static:
      return
    with self._lock:
      firewall_name = ('perfkit-firewall-%s-%d' %
                       (FLAGS.run_uri, port))
      key = (vm.project, port)
      if key in self.firewall_rules:
        return
      firewall_rule = GceFirewallRule(
          firewall_name, vm.project, port, vm.network)
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

  def __init__(self, name, project):
    super(GceNetworkResource, self).__init__()
    self.name = name
    self.project = project

  def _Create(self):
    """Creates the Network resource."""
    cmd = util.GcloudCommand(self, 'compute', 'networks', 'create', self.name)
    cmd.flags['range'] = '10.0.0.0/16'
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


class GceNetwork(network.BaseNetwork):
  """Object representing a GCE Network."""

  CLOUD = GCP

  def __init__(self, network_spec):
    super(GceNetwork, self).__init__(network_spec)
    self.project = network_spec.project
    name = 'pkb-network-%s' % FLAGS.run_uri
    self.network_resource = GceNetworkResource(name, self.project)

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
    self.network_resource.Create()

  def Delete(self):
    """Deletes the actual network."""
    self.network_resource.Delete()
