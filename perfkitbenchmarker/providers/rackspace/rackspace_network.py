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
"""Module containing classes related to Rackspace VM networking.

The SecurityGroup class provides a way of opening VM ports via
Security group rules.
"""
import json
import threading

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.rackspace import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS

INGRESS = 'ingress'
EGRESS = 'egress'
SEC_GROUP_DIRECTIONS = frozenset([INGRESS, EGRESS])

IPV4 = 'ipv4'
IPV6 = 'ipv6'
ETHER_TYPES = frozenset([IPV4, IPV6])

TCP = 'tcp'
UDP = 'udp'
ICMP = 'icmp'
SEC_GROUP_PROTOCOLS = frozenset([TCP, UDP, ICMP])

PORT_RANGE_MIN = '1'
PORT_RANGE_MAX = '65535'

PUBLIC_NET_ID = '00000000-0000-0000-0000-000000000000'
SERVICE_NET_ID = '11111111-1111-1111-1111-111111111111'
DEFAULT_SUBNET_CIDR = '192.168.0.0/16'

SSH_PORT = 22


class RackspaceSecurityGroup(resource.BaseResource):
  """An object representing a Rackspace Security Group."""

  def __init__(self, name):
    super(RackspaceSecurityGroup, self).__init__()
    self.name = name
    self.id = None

  def _Create(self):
    cmd = util.RackCLICommand(self, 'networks', 'security-group', 'create')
    cmd.flags['name'] = self.name
    stdout, stderr, _ = cmd.Issue()
    resp = json.loads(stdout)
    self.id = resp['ID']

  def _Delete(self):
    if self.id is None:
      return
    cmd = util.RackCLICommand(self, 'networks', 'security-group', 'delete')
    cmd.flags['id'] = self.id
    cmd.Issue()

  def _Exists(self):
    if self.id is None:
      return False
    cmd = util.RackCLICommand(self, 'networks', 'security-group', 'get')
    cmd.flags['id'] = self.id
    stdout, stderr, _ = cmd.Issue()
    return not stderr


class RackspaceSecurityGroupRule(resource.BaseResource):
  """An object representing a Security Group Rule."""

  def __init__(self, sec_group_rule_name, sec_group_id, direction=INGRESS,
               ip_ver=IPV4, protocol=TCP, port_range_min=PORT_RANGE_MIN,
               port_range_max=PORT_RANGE_MAX, source_cidr=None):
    super(RackspaceSecurityGroupRule, self).__init__()
    self.id = None
    self.name = sec_group_rule_name
    self.sec_group_id = sec_group_id
    assert direction in SEC_GROUP_DIRECTIONS
    self.direction = direction
    assert ip_ver in ETHER_TYPES
    self.ip_ver = ip_ver
    assert protocol in SEC_GROUP_PROTOCOLS
    self.protocol = protocol
    assert (int(PORT_RANGE_MIN) <= int(port_range_min) <= int(PORT_RANGE_MAX))
    self.port_range_min = port_range_min
    assert (int(PORT_RANGE_MIN) <= int(port_range_max) <= int(PORT_RANGE_MAX))
    self.port_range_max = port_range_max
    assert int(port_range_min) <= int(port_range_max)
    self.source_cidr = source_cidr

  def __eq__(self, other):
    # Name does not matter
    return (self.sec_group_id == other.sec_group_id and
            self.direction == other.direction and
            self.ip_ver == other.ip_ver and
            self.protocol == other.protocol and
            self.source_cidr == other.source_cidr)

  def _Create(self):
    cmd = util.RackCLICommand(self, 'networks', 'security-group-rule', 'create')
    cmd.flags['security-group-id'] = self.sec_group_id
    cmd.flags['direction'] = self.direction
    cmd.flags['ether-type'] = self.ip_ver
    cmd.flags['protocol'] = self.protocol
    cmd.flags['port-range-min'] = self.port_range_min
    cmd.flags['port-range-max'] = self.port_range_max
    if self.source_cidr:
      cmd.flags['remote-ip-prefix'] = self.source_cidr
    stdout, stderr, _ = cmd.Issue()
    resp = json.loads(stdout)
    self.id = resp['ID']

  def _Delete(self):
    if self.id is None:
      return
    cmd = util.RackCLICommand(self, 'networks', 'security-group-rule', 'delete')
    cmd.flags['id'] = self.id
    cmd.Issue()

  def _Exists(self):
    if self.id is None:
      return False
    cmd = util.RackCLICommand(self, 'networks', 'security-group-rule', 'get')
    cmd.flags['id'] = self.id
    stdout, stderr, _ = cmd.Issue()
    return not stderr


class RackspaceSubnet(resource.BaseResource):
  """An object that represents a Rackspace Subnet,"""

  def __init__(self, network_id, cidr, ip_ver, name=None, tenant_id=None):
    super(RackspaceSubnet, self).__init__()
    self.id = None
    self.network_id = network_id
    self.cidr = cidr
    self.ip_ver = ip_ver
    self.name = name
    self.tenant_id = tenant_id

  def _Create(self):
    cmd = util.RackCLICommand(self, 'networks', 'subnet', 'create')
    cmd.flags['network-id'] = self.network_id
    cmd.flags['cidr'] = self.cidr
    cmd.flags['ip-version'] = self.ip_ver
    if self.name:
      cmd.flags['name'] = self.name
    if self.tenant_id:
      cmd.flags['tenant-id'] = self.tenant_id
    stdout, stderr, _ = cmd.Issue()
    resp = json.loads(stdout)
    self.id = resp['ID']

  def _Delete(self):
    if self.id is None:
      return
    cmd = util.RackCLICommand(self, 'networks', 'subnet', 'delete')
    cmd.flags['id'] = self.id
    cmd.Issue()

  def _Exists(self):
    if self.id is None:
      return False
    cmd = util.RackCLICommand(self, 'networks', 'subnet', 'get')
    cmd.flags['id'] = self.id
    stdout, stderr, _ = cmd.Issue()
    return not stderr


class RackspaceNetworkSpec(network.BaseNetworkSpec):
  """Object containing the information needed to create a Rackspace network."""

  def __init__(self, tenant_id=None, region=None, **kwargs):
    super(RackspaceNetworkSpec, self).__init__(**kwargs)
    self.tenant_id = tenant_id
    self.region = region


class RackspaceNetworkResource(resource.BaseResource):
  """Object representing a Rackspace Network Resource."""

  def __init__(self, name, tenant_id=None):
    super(RackspaceNetworkResource, self).__init__()
    self.name = name
    self.tenant_id = tenant_id
    self.id = None

  def _Create(self):
    cmd = util.RackCLICommand(self, 'networks', 'network', 'create')
    cmd.flags['name'] = self.name
    if self.tenant_id:
      cmd.flags['tenant-id'] = self.tenant_id
    stdout, _, _ = cmd.Issue()
    resp = json.loads(stdout)
    if resp['ID']:
      self.id = resp['ID']

  def _Delete(self):
    if self.id is None:
      return
    cmd = util.RackCLICommand(self, 'networks', 'network', 'delete')
    cmd.flags['id'] = self.id
    cmd.Issue()

  def _Exists(self):
    if self.id is None:
      return False
    cmd = util.RackCLICommand(self, 'networks', 'network', 'get')
    cmd.flags['id'] = self.id
    stdout, stderr, _ = cmd.Issue()
    return not stderr


class RackspaceNetwork(network.BaseNetwork):
  """An object representing a Rackspace Network."""

  CLOUD = providers.RACKSPACE

  def __init__(self, network_spec):
    super(RackspaceNetwork, self).__init__(network_spec)
    self.tenant_id = network_spec.tenant_id
    name = FLAGS.rackspace_network_name or 'pkb-network-%s' % FLAGS.run_uri
    self.network_resource = RackspaceNetworkResource(name, self.tenant_id)
    self.subnet = RackspaceSubnet(self.network_resource.id, DEFAULT_SUBNET_CIDR,
                                  ip_ver='4', name='subnet-%s' % name,
                                  tenant_id=self.tenant_id)
    self.security_group = RackspaceSecurityGroup('default-internal-%s' % name)
    self.default_firewall_rules = []

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    return RackspaceNetworkSpec(tenant_id=vm.tenant_id, region=vm.zone)

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    return (cls.CLOUD, spec.tenant_id, spec.region)

  def Create(self):
    if FLAGS.rackspace_network_name is None:
      self.network_resource.Create()
      self.subnet.Create()
      self.security_group.Create()
      self.default_firewall_rules = self._GenerateDefaultRules(
          self.security_group.id, self.network_resource.name)
      for rule in self.default_firewall_rules:
        rule.Create()

  def Delete(self):
    if FLAGS.rackspace_network_name is None:
      for rule in self.default_firewall_rules:
        rule.Delete()
      self.security_group.Delete()
      self.subnet.Delete()
      self.network_resource.Delete()

  def _GenerateDefaultRules(self, sec_group_id, network_name):
    firewall_rules = [
        RackspaceSecurityGroupRule(
            sec_group_rule_name='tcp-default-internal-%s' % network_name,
            sec_group_id=sec_group_id,
            direction=INGRESS,
            ip_ver=IPV4,
            protocol=TCP),
        RackspaceSecurityGroupRule(
            sec_group_rule_name='udp-default-internal-%s' % network_name,
            sec_group_id=sec_group_id,
            direction=INGRESS,
            ip_ver=IPV4, protocol=UDP),
        RackspaceSecurityGroupRule(
            sec_group_rule_name='icmp-default-internal-%s' % network_name,
            sec_group_id=sec_group_id,
            direction=INGRESS,
            ip_ver=IPV4, protocol=ICMP)]
    return firewall_rules


class RackspaceFirewall(network.BaseFirewall):
  """An object representing a Rackspace Security Group applied to PublicNet and
  ServiceNet."""

  CLOUD = providers.RACKSPACE

  def __init__(self):
    # TODO(meteorfox) Support a Firewall per region
    self._lock = threading.Lock()  # Guards security-group creation/deletion
    self.firewall_rules = {}

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    # At Rackspace all ports are open by default
    # TODO(meteorfox) Implement security groups support
    if FLAGS.rackspace_use_security_group:
      raise NotImplementedError()

  def DisallowAllPorts(self):
    if FLAGS.rackspace_use_security_group:
      raise NotImplementedError()
