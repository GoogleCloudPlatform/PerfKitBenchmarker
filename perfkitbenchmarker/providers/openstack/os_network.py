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

import json
import threading

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
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
      cmd.flags['dst-port'] = str(icmp_type)
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


class OpenStackFloatingIPPool(object):

  _floating_ip_lock = threading.Lock()  # Guards floating IP allocation/release

  def __init__(self, pool_name):
    self.ip_pool_name = pool_name

  def get_or_create(self, vm):
    with self._floating_ip_lock:
      cmd = utils.OpenStackCLICommand(vm, 'ip', 'floating', 'list')
      stdout, stderr, _ = cmd.Issue()
      floating_ip_dict_list = json.loads(stdout)
      for flip_dict in floating_ip_dict_list:
        if flip_dict['Pool'] == self.ip_pool_name and flip_dict['ID'] is None:
          d = {}
          for k, v in flip_dict.iteritems():
            new_key = k.lower.replace(' ', '_')  # Transforms keys
            d[new_key] = v
          return d
      return self._allocate(vm)

  def _allocate(self, vm):
    cmd = utils.OpenStackCLICommand(vm, 'ip', 'floating', 'create',
                                    self.ip_pool_name)
    stdout, stderr, _ = cmd.Issue()
    if stderr.strip():  # Strip spaces
      raise errors.Config.InvalidValue(
          'Could not allocate a floating ip from the pool "%s".'
          % self.ip_pool_name)
    floating_ip_dict = json.loads(stdout)
    return floating_ip_dict

  def release(self, vm, floating_ip_dict):
    # TODO(meteorfox): Change floating IP commands to OpenStack CLI once
    # support for them is added.
    show_cmd = [FLAGS.openstack_neutron_path,
                'floatingip-show', floating_ip_dict['id'], '--format', 'json']
    stdout, stderr, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
    if stderr:
      return  # Not found, moving on
    updated_flip_dict = json.loads(stdout)
    with self._floating_ip_lock:
      delete_cmd = [FLAGS.openstack_neutron_path,
                    'floatingip-delete', updated_flip_dict['id'],
                    '--format', 'json']
      stdout, stderr, _ = vm_util.IssueCommand(delete_cmd)

  def is_attached(self, floating_ip_dict):
    with self._floating_ip_lock:
      show_cmd = ['neutron', 'floatingip-show', floating_ip_dict['id'],
                  '--format', 'json']
      stdout, stderr, _ = vm_util.IssueCommand(show_cmd, suppress_warning=True)
      if stderr:
        return  # Not found, moving on
      updated_flip_dict = json.loads(stdout)
      return updated_flip_dict['port_id']
