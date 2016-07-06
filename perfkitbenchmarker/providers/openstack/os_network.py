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


NOVA_FLOAT_IP_LIST_CMD = 'floating-ip-list'
OSC_IP_CMD = 'ip'
OSC_FLOATING_SUBCMD = 'floating'
OSC_SEC_GROUP_CMD = 'security group'
OSC_SEC_GROUP_RULE_CMD = 'security group rule'

SC_GROUP_NAME = 'perfkit_sc_group'

ICMP = 'icmp'
TCP = 'tcp'
UDP = 'udp'

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
      cmd = utils.OpenStackCLICommand(self, OSC_SEC_GROUP_CMD, 'show',
                                      SC_GROUP_NAME)
      stdout, stderr, _ = cmd.Issue(suppress_warning=True)
      if stderr:
        cmd = utils.OpenStackCLICommand(self, OSC_SEC_GROUP_CMD, 'create',
                                        SC_GROUP_NAME)
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

    sec_group_rule = (ICMP, icmp_type, icmp_code, vm.group_id)
    with self._lock:
      if sec_group_rule in self.sec_group_rules_set:
        return
      cmd = utils.OpenStackCLICommand(vm, OSC_SEC_GROUP_RULE_CMD, 'create',
                                      vm.group_id)
      cmd.flags['dst-port'] = str(icmp_type)
      cmd.flags['proto'] = ICMP
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
      if sec_group_rule in self.sec_group_rules_set:
        return
      cmd = utils.OpenStackCLICommand(vm, OSC_SEC_GROUP_RULE_CMD, 'create',
                                      vm.group_id)
      cmd.flags['dst-port'] = '%d:%d' % (port, to_port)
      for prot in (TCP, UDP,):
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

  def associate(self, vm):
    with self._floating_ip_lock:
      floating_ip = self._get_or_create(vm)
      cmd = utils.OpenStackCLICommand(vm, OSC_IP_CMD, OSC_FLOATING_SUBCMD,
                                      'add', floating_ip['ip'], vm.id)
      del cmd.flags['format']  # Command does not support json output format
      _, stderr, _ = cmd.Issue()
      if stderr:
        raise errors.Error(stderr)
      return floating_ip

  def _get_or_create(self, vm):
    list_cmd = [FLAGS.openstack_nova_path, NOVA_FLOAT_IP_LIST_CMD]
    stdout, stderr, _ = vm_util.IssueCommand(list_cmd)
    floating_ip_dict_list = utils.ParseFloatingIPTable(stdout)
    for floating_ip_dict in floating_ip_dict_list:
      if (floating_ip_dict['pool'] == self.ip_pool_name and
              floating_ip_dict['instance_id'] is None):
        return floating_ip_dict
    return self._allocate(vm)

  def _allocate(self, vm):
    cmd = utils.OpenStackCLICommand(vm, OSC_IP_CMD, OSC_FLOATING_SUBCMD,
                                    'create', self.ip_pool_name)
    stdout, stderr, _ = cmd.Issue()
    if stderr.strip():  # Strip spaces
      raise errors.Config.InvalidValue(
          'Could not allocate a floating ip from the pool "%s".'
          % self.ip_pool_name)
    floating_ip_dict = json.loads(stdout)
    # Convert OSC format to nova's format
    floating_ip_dict['ip'] = floating_ip_dict['floating_ip_address']
    del floating_ip_dict['floating_ip_address']
    return floating_ip_dict

  def release(self, vm, floating_ip_dict):
    cmd = utils.OpenStackCLICommand(vm, OSC_IP_CMD, OSC_FLOATING_SUBCMD, 'show',
                                    floating_ip_dict['id'])
    stdout, stderr, _ = cmd.Issue(suppress_warning=True)
    if stderr:
      return  # Not found, moving on
    updated_floating_ip_dict = json.loads(stdout)
    with self._floating_ip_lock:
      delete_cmd = utils.OpenStackCLICommand(vm, OSC_IP_CMD,
                                             OSC_FLOATING_SUBCMD, 'delete',
                                             updated_floating_ip_dict['id'])
      del delete_cmd.flags['format']  # Command not support json output format
      stdout, stderr, _ = delete_cmd.Issue(suppress_warning=True)
