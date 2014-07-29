#!/usr/bin/env python
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

"""Module containing abstract classes related to VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in the
same project.
"""

import gflags as flags

FLAGS = flags.FLAGS


class BaseFirewall(object):
  """An object representing the Base Firewall."""

  def __init__(self, project):
    """Initialize firewall class.

    Args:
      project: The project firewall belongs to.
    """
    self.project = project

  def AllowPort(self, vm, port):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      port: The local port to open.
    """
    pass

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class BaseNetwork(object):
  """Object representing a Base Network."""

  def __init__(self, zone=None):
    self.zone = zone
    self.created = False

  def Create(self):
    """Creates the actual network."""
    self.created = True

  def Delete(self):
    """Deletes the actual network."""
    pass
