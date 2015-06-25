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

import threading

from perfkitbenchmarker import flags

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

  # Dictionary holding all BaseNetwork objects.
  networks = {}

  # Lock used to serialize the instantiation of new BaseNetwork objects.
  _class_lock = threading.Lock()

  def __init__(self, zone=None):
    self.zone = zone

  @classmethod
  def GetNetwork(cls, zone):
    """Returns the network corresponding to the given zone."""
    with cls._class_lock:
      # This probably will never happen, but we want to ensure that
      # networks from different clouds never share the same entry, so
      # in addition to using the zone, we also use the class as part
      # of the key.
      key = (cls, zone)
      if key not in cls.networks:
        cls.networks[key] = cls(zone)
      return cls.networks[key]

  def Create(self):
    """Creates the actual network."""
    pass

  def Delete(self):
    """Deletes the actual network."""
    pass
