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

from collections import namedtuple
import json
import threading

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker.providers.openstack import utils
from perfkitbenchmarker import providers


OSC_FLOATING_IP_CMD = 'floating ip'
OSC_SEC_GROUP_CMD = 'security group'
OSC_SEC_GROUP_RULE_CMD = 'security group rule'

SC_GROUP_NAME = 'perfkit_sc_group'

ICMP = 'icmp'
TCP = 'tcp'
UDP = 'udp'

FLOATING_IP_ADDRESS = 'floating_ip_address'
FLOATING_IP_ID = 'id'
FLOATING_NETWORK_ID = 'floating_network_id'

FLOATING_IP_KEYS = (FLOATING_IP_ADDRESS, FLOATING_IP_ID, FLOATING_NETWORK_ID,)

FLAGS = flags.FLAGS

MAX_PORT = 65535
MIN_PORT = 1


OpenStackFloatingIP = namedtuple('OpenStackFloatingIP', FLOATING_IP_KEYS)


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
        del cmd.flags['format']  # Command does not support json output
        cmd.Issue()

  def AllowICMP(self, vm, icmp_type=-1, icmp_code=-1, source_range=None):
    """Creates a Security Group Rule on the Firewall to allow/disallow
    ICMP traffic.

    Args:
      vm: The BaseVirtualMachine object to allow ICMP traffic to.
      icmp_type: ICMP type to allow. If none given then allows all types.
      icmp_code: ICMP code to allow. If none given then allows all codes.
      source_range: The source IP range to allow ICMP traffic.
    """
    if vm.is_static:
      return

    sec_group_rule = (ICMP, icmp_type, icmp_code, vm.group_id)
    with self._lock:
      if sec_group_rule in self.sec_group_rules_set:
        return
      cmd = utils.OpenStackCLICommand(vm, OSC_SEC_GROUP_RULE_CMD, 'create',
                                      vm.group_id)
      if source_range:
        cmd.flags['src-ip'] = source_range
      cmd.flags['dst-port'] = str(icmp_type)
      cmd.flags['proto'] = ICMP
      cmd.Issue(suppress_warning=True)
      self.sec_group_rules_set.add(sec_group_rule)

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Creates a Security Group Rule on the Firewall to allow for both TCP
    and UDP network traffic on given port, or port range.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
      source_range: The source IP range to allow traffic for these ports.
    """
    if vm.is_static:
      return

    if end_port is None:
      end_port = start_port

    sec_group_rule = (start_port, end_port, vm.group_id)

    with self._lock:
      if sec_group_rule in self.sec_group_rules_set:
        return
      cmd = utils.OpenStackCLICommand(vm, OSC_SEC_GROUP_RULE_CMD, 'create',
                                      vm.group_id)
      if source_range:
        cmd.flags['src-ip'] = source_range
      cmd.flags['dst-port'] = '%d:%d' % (start_port, end_port)
      for prot in (TCP, UDP,):
        cmd.flags['proto'] = prot
        cmd.Issue(suppress_warning=True)
      self.sec_group_rules_set.add(sec_group_rule)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class OpenStackFloatingIPPool(object):

  _floating_ip_lock = threading.Lock()  # Guards floating IP allocation/release

  def __init__(self, floating_network_id):
    self.floating_network_id = floating_network_id

  def associate(self, vm):
    with self._floating_ip_lock:
      floating_ip_obj = self._get_or_create(vm)
      cmd = utils.OpenStackCLICommand(vm, 'server add floating ip', vm.id,
                                      floating_ip_obj.floating_ip_address)
      del cmd.flags['format']  # Command does not support json output format
      _, stderr, _ = cmd.Issue()
      if stderr:
        raise errors.Error(stderr)
      return floating_ip_obj

  def _get_or_create(self, vm):
    list_cmd = utils.OpenStackCLICommand(vm, OSC_FLOATING_IP_CMD, 'list')
    stdout, stderr, _ = list_cmd.Issue()
    if stderr:
      raise errors.Error(stderr)
    floating_ip_dict_list = json.loads(stdout)

    for floating_ip_dict in floating_ip_dict_list:
      if (floating_ip_dict['Floating Network'] == self.floating_network_id
              and floating_ip_dict['Port'] is None):
        # Due to inconsistent output, we need to convert the keys
        floating_ip_obj = OpenStackFloatingIP(
            floating_ip_address=floating_ip_dict['Floating IP Address'],
            floating_network_id=floating_ip_dict['Floating Network'],
            id=floating_ip_dict['ID']
        )
        return floating_ip_obj
    return self._allocate(vm)

  def _allocate(self, vm):
    cmd = utils.OpenStackCLICommand(vm, OSC_FLOATING_IP_CMD, 'create',
                                    self.floating_network_id)
    stdout, stderr, _ = cmd.Issue()
    if stderr.strip():  # Strip spaces
      raise errors.Config.InvalidValue(
          'Could not allocate a floating ip from the floating network "%s".'
          % self.floating_network_id)
    floating_ip_dict = json.loads(stdout)
    # Extract subset of returned keys
    floating_ip_obj = OpenStackFloatingIP(
        floating_ip_address=floating_ip_dict['floating_ip_address'],
        floating_network_id=floating_ip_dict['floating_network_id'],
        id=floating_ip_dict['id']
    )
    return floating_ip_obj

  def release(self, vm, floating_ip_obj):
    cmd = utils.OpenStackCLICommand(vm, OSC_FLOATING_IP_CMD, 'show',
                                    floating_ip_obj.id)
    stdout, stderr, _ = cmd.Issue(suppress_warning=True)
    if stderr:
      return  # Not found, moving on
    floating_ip_dict = json.loads(stdout)
    with self._floating_ip_lock:
      delete_cmd = utils.OpenStackCLICommand(vm, OSC_FLOATING_IP_CMD, 'delete',
                                             floating_ip_dict['id'])
      del delete_cmd.flags['format']  # Command not support json output format
      stdout, stderr, _ = delete_cmd.Issue(suppress_warning=True)
