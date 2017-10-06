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

from perfkitbenchmarker import context
from perfkitbenchmarker import errors


class BaseFirewall(object):
  """An object representing the Base Firewall."""

  CLOUD = None

  @classmethod
  def GetFirewall(cls):
    """Returns a BaseFirewall.

    This method is used instead of directly calling the class's constructor.
    It creates BaseFirewall instances and registers them.
    If a BaseFirewall object has already been registered, that object
    will be returned rather than creating a new one. This enables multiple
    VMs to call this method and all share the same BaseFirewall object.
    """
    if cls.CLOUD is None:
      raise errors.Error('Firewalls should have CLOUD attributes.')
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetFirewall called in a thread without a '
                         'BenchmarkSpec.')
    with benchmark_spec.firewalls_lock:
      key = cls.CLOUD
      if key not in benchmark_spec.firewalls:
        benchmark_spec.firewalls[key] = cls()
      return benchmark_spec.firewalls[key]

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the ICMP protocol for.
    """
    pass

  def AllowPort(self, vm, start_port, end_port=None):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port in a range of ports to open.
      end_port: The last port in a range of ports to open. If None, only
        start_port will be opened.
    """
    pass

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class BaseNetworkSpec(object):
  """Object containing all information needed to create a Network."""

  def __init__(self, zone=None):
    """Initializes the BaseNetworkSpec.

    Args:
      zone: The zone in which to create the network.
    """
    self.zone = zone


class BaseNetwork(object):
  """Object representing a Base Network."""

  CLOUD = None

  def __init__(self, spec):
    self.zone = spec.zone

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns a BaseNetworkSpec created from VM attributes."""
    return BaseNetworkSpec(zone=vm.zone)

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    if cls.CLOUD is None:
      raise errors.Error('Networks should have CLOUD attributes.')
    return (cls.CLOUD, spec.zone)

  @classmethod
  def GetNetwork(cls, vm):
    """Returns a BaseNetwork.

    This method is used instead of directly calling the class's constructor.
    It creates BaseNetwork instances and registers them. If a BaseNetwork
    object has already been registered with the same key, that object
    will be returned rather than creating a new one. This enables multiple
    VMs to call this method and all share the same BaseNetwork object.

    Args:
      vm: The VM for which the Network is being created.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    spec = cls._GetNetworkSpecFromVm(vm)
    key = cls._GetKeyFromNetworkSpec(spec)
    with benchmark_spec.networks_lock:
      if key not in benchmark_spec.networks:
        benchmark_spec.networks[key] = cls(spec)
      return benchmark_spec.networks[key]

  def Create(self):
    """Creates the actual network."""
    pass

  def Delete(self):
    """Deletes the actual network."""
    pass
