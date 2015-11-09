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

"""Module containing abstract classes related to VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in the
same project.
"""

import threading


class BaseFirewall(object):
  """An object representing the Base Firewall."""

  # Lock used to serialize the instantiation of new BaseFirewall objects.
  _class_lock = threading.Lock()

  @classmethod
  def GetFirewall(cls, firewalls_dict):
    """Returns the Firewall."""
    with cls._class_lock:
      if cls not in firewalls_dict:
        firewalls_dict[cls] = cls()
      return firewalls_dict[cls]

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

  # Lock used to serialize the instantiation of new BaseNetwork objects.
  _class_lock = threading.Lock()

  def __init__(self, zone=None):
    self.zone = zone

  @classmethod
  def GetNetwork(cls, zone, networks_dict):
    """Returns the network corresponding to the given zone."""
    with cls._class_lock:
      # This probably will never happen, but we want to ensure that
      # networks from different clouds never share the same entry, so
      # in addition to using the zone, we also use the class as part
      # of the key.
      key = (cls, zone)
      if key not in networks_dict:
        networks_dict[key] = cls(zone)
      return networks_dict[key]

  def Create(self):
    """Creates the actual network."""
    pass

  def Delete(self):
    """Deletes the actual network."""
    pass
