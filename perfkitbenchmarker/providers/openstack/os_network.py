# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
""""Module containing classes related to OpenStack Networking."""

import threading
import time

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker.providers.openstack import utils
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

MAX_PORT = 65535


class OpenStackFirewall(network.BaseFirewall):
  """
  An object representing OpenStack Firewall based on Secure Groups.
  """

  CLOUD = providers.OPENSTACK

  def __init__(self):
    self._lock = threading.Lock()  # Guards security-group rule set
    self.sec_group_rules_set = set()

    with self._lock:
      cmd = utils.OpenStackCLICommand(self, 'security group', 'show',
                                      'perfkit_sc_group')
      stdout, stderr, _ = cmd.Issue(suppress_warning=True)
      if stderr:
        cmd = utils.OpenStackCLICommand(self, 'security group', 'create',
                                        'perfkit_sc_group')
        cmd.Issue()

  def AllowICMP(self, vm, icmp_type=-1, icmp_code=-1):
    """Creates a Security Group Rule on the Firewall to allow/disallow
    ICMP traffic.

    Args:
      vm: The BaseVirtualMachine object to allow ICMP traffic to.
      icmp_type: ICMP type to allow. If none given then allows all types.
      icmp_code: ICMP code to allow. If none given then allows all codes.
    """
    if vm.is_static:
      return

    sec_group_rule = ('icmp', icmp_type, icmp_code, vm.group_id)
    with self._lock:
      cmd = utils.OpenStackCLICommand(vm, 'security group rule', 'create',
                                      vm.group_id)
      cmd.flags['dst-port'] = '%d:%d' % (icmp_type, icmp_code)
      cmd.flags['proto'] = 'icmp'
      if sec_group_rule in self.sec_group_rules_set:
        return
      cmd.Issue(suppress_warning=True)
      self.sec_group_rules_set.add(sec_group_rule)

  def AllowPort(self, vm, port, to_port=None):
    """Creates a Security Group Rule on the Firewall to allow for both TCP
    and UDP network traffic on given port, or port range.

    Args:
        vm: The BaseVirtualMachine object to open the port for.
        port: The local port to open.
        to_port: The last port to open in range of ports to open. If None,
          then only the single 'port' is open.
    """
    if vm.is_static:
      return

    if to_port is None:
      to_port = port

    sec_group_rule = (port, to_port, vm.group_id)

    with self._lock:
      cmd = utils.OpenStackCLICommand(vm, 'security group rule', 'create',
                                      vm.group_id)
      cmd.flags['dst-port'] = '%d:%d' % (port, to_port)
      if sec_group_rule in self.sec_group_rules_set:
        return
      for prot in ('tcp', 'udp',):
        cmd.flags['proto'] = prot
        cmd.Issue(suppress_warning=True)
      self.sec_group_rules_set.add(sec_group_rule)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class OpenStackPublicNetwork(object):

  def __init__(self, pool):
    self.__nclient = utils.NovaClient()
    # TODO(meteorfox) Is this lock needed? Callers to these methods seem
    # to be using a lock already. Should we make callers use a lock, or
    # should this object handle the lock?
    self.__floating_ip_lock = threading.Lock()
    self.ip_pool_name = pool

  def allocate(self):
    with self.__floating_ip_lock:
      return self.__nclient.floating_ips.create(pool=self.ip_pool_name)

  def release(self, floating_ip):
    f_id = floating_ip.id
    if self.__nclient.floating_ips.get(f_id):
      with self.__floating_ip_lock:
        if self.__nclient.floating_ips.get(f_id):
          self.__nclient.floating_ips.delete(floating_ip)
          while self.__nclient.floating_ips.findall(id=f_id):
            time.sleep(1)

  def get_or_create(self):
    with self.__floating_ip_lock:
      floating_ips = self.__nclient.floating_ips.findall(instance_id=None,
                                                         pool=self.ip_pool_name)
    if floating_ips:
      return floating_ips[0]
    else:
      return self.allocate()

  def is_attached(self, floating_ip):
    return self.__nclient.floating_ips.get(floating_ip.id).fixed_ip is not None
