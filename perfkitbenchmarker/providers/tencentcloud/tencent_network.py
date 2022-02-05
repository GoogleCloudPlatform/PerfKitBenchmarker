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
same project. See https://cloud.tencent.com/document/product/213/5220 for
more information about Tencent Cloud VM networking.
"""


import json
import logging
import threading
import uuid

from absl import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.alicloud import util
from six.moves import range

FLAGS = flags.FLAGS
MAX_NAME_LENGTH = 128


class TencentVpc(resource.BaseResource):
  """An object representing an Tencent Cloud VPC."""

  def __init__(self, name, region):
    pass

  def _Create(self):
    """Creates the VPC."""
    pass

  def _Exists(self):
    """Returns true if the VPC exists."""
    pass

  @vm_util.Retry(poll_interval=5, max_retries=30, log_errors=False)
  def _WaitForVpcStatus(self, status_list):
    """Waits until VPC's status is in status_list"""
    pass

  def _Delete(self):
    """Delete's the VPC."""
    pass


class TencentVSwitch(resource.BaseResource):
  """An object representing an Tencent Cloud VSwitch."""

  def __init__(self, name, zone, vpc_id):
    pass

  def _Create(self):
    """Creates the VSwitch."""
    pass

  def _Delete(self):
    """Deletes the VSwitch."""
    pass

  def _Exists(self):
    """Returns true if the VSwitch exists."""
    pass


class TencentSecurityGroup(resource.BaseResource):
  """Object representing an Tencent Cloud Security Group."""

  def __init__(self, name, region, use_vpc=True, vpc_id=None):
    pass

  def _Create(self):
    """Creates the security group."""
    pass

  def _Delete(self):
    """Deletes the security group."""
    pass

  def _Exists(self):
    """Returns true if the security group exists."""
    pass


class TencentFirewall(network.BaseFirewall):
  """An object representing the Tencent Cloud Firewall."""

  CLOUD = providers.TENCENTCLOUD

  def __init__(self):
    self.firewall_set = set()
    self._lock = threading.Lock()

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the ICMP protocol for.
    """
    pass

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port in a range of ports to open.
      end_port: The last port in a range of ports to open. If None, only
        start_port will be opened.
      source_range: unsupported at present.
    """
    pass

  def _AllowPort(self, vm, port):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      port: The local port to open.
    """
    pass

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class TencentNetwork(network.BaseNetwork):
  """Object representing a Tencent Cloud Network."""

  CLOUD = providers.TENCENTCLOUD

  def __init__(self, spec):
    pass

  @vm_util.Retry()
  def Create(self):
    """Creates the network."""
    pass

  def Delete(self):
    """Deletes the network."""
    pass