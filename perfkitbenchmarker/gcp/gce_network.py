# Copyright 2014 Google Inc. All rights reserved.
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
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.gcp import util


FLAGS = flags.FLAGS


class GceFirewallRule(resource.BaseResource):
  """An object representing a GCE Firewall Rule."""

  def __init__(self, name, project, port):
    super(GceFirewallRule, self).__init__()
    self.name = name
    self.project = project
    self.port = port

  def __eq__(self, other):
    """Defines equality to make comparison easy."""
    return (self.name == other.name and
            self.port == other.port and
            self.project == other.project)

  def _Create(self):
    """Creates the Firewall Rule."""
    create_cmd = [FLAGS.gcloud_path,
                  'compute',
                  'firewall-rules',
                  'create',
                  self.name,
                  '--allow', 'tcp:%d' % self.port, 'udp:%d' % self.port]
    create_cmd.extend(util.GetDefaultGcloudFlags(self))
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the Firewall Rule."""
    delete_cmd = [FLAGS.gcloud_path,
                  'compute',
                  'firewall-rules',
                  'delete',
                  self.name]
    delete_cmd.extend(util.GetDefaultGcloudFlags(self))
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns True if the Firewall Rule exists."""
    describe_cmd = [FLAGS.gcloud_path,
                    'compute',
                    'firewall-rules',
                    'describe',
                    self.name]
    describe_cmd.extend(util.GetDefaultGcloudFlags(self))
    _, _, retcode = vm_util.IssueCommand(describe_cmd, suppress_warning=True)
    if retcode:
      return False
    return True


class GceFirewall(network.BaseFirewall):
  """An object representing the GCE Firewall."""

  def __init__(self, project):
    """Initialize GCE firewall class.

    Args:
      project: The GCP project name under which firewall is created.
    """
    self._lock = threading.Lock()
    self.firewall_rules = []
    self.project = project

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
      firewall_rule = GceFirewallRule(firewall_name, self.project, port)
      if firewall_rule in self.firewall_rules:
        return
      self.firewall_rules.append(firewall_rule)
      firewall_rule.Create()

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    for firewall_rule in self.firewall_rules:
      firewall_rule.Delete()


class GceNetwork(network.BaseNetwork):
  """Object representing a GCE Network."""

  def Create(self):
    """Creates the actual network."""
    pass

  def Delete(self):
    """Deletes the actual network."""
    pass
