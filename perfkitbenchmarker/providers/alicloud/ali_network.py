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
"""Module containing classes related to AliCloud VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in the
same project. See https://developers.google.com/compute/docs/networking for
more information about AliCloud VM networking.
"""
import threading
import json
import uuid
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.alicloud import util
from perfkitbenchmarker import resource
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS
MAX_NAME_LENGTH = 128


class AliVpc(resource.BaseResource):
  """An object representing an AliCloud VPC."""

  def __init__(self, name, region):
    super(AliVpc, self).__init__()
    self.region = region
    self.id = None
    self.name = name

  def _Create(self):
    """Creates the VPC."""
    create_cmd = util.ALI_PREFIX + [
        'ecs',
        'CreateVpc',
        '--VpcName %s' % self.name,
        '--RegionId %s' % self.region,
        '--CidrBlock 10.0.0.0/16']
    create_cmd = util.GetEncodedCmd(create_cmd)
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['VpcId']

  def _Exists(self):
    """Returns true if the VPC exists."""
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeVpcs',
        '--RegionId %s' % self.region,
        '--VpcId %s' % self.id]
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    vpcs = response['Vpcs']['Vpc']
    assert len(vpcs) < 2, 'Too many VPCs.'
    return len(vpcs) > 0

  @vm_util.Retry(poll_interval=5, max_retries=30, log_errors=False)
  def _WaitForVpcStatus(self, status_list):
    """Waits until VPC's status is in status_list"""
    logging.info('Waits until the status of VPC is in status_list: %s',
                 status_list)
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeVpcs',
        '--RegionId %s' % self.region,
        '--VpcId %s' % self.id]
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    vpcs = response['Vpcs']['Vpc']
    assert len(vpcs) == 1
    vpc_status = response['Vpcs']['Vpc'][0]['Status']
    assert vpc_status in status_list

  def _Delete(self):
    """Delete's the VPC."""
    delete_cmd = util.ALI_PREFIX + [
        'ecs',
        'DeleteVpc',
        '--RegionId %s' % self.region,
        '--VpcId %s' % self.id]
    delete_cmd = util.GetEncodedCmd(delete_cmd)
    vm_util.IssueCommand(delete_cmd)


class AliVSwitch(resource.BaseResource):
  """An object representing an AliCloud VSwitch."""

  def __init__(self, name, zone, vpc_id):
    super(AliVSwitch, self).__init__()
    self.region = util.GetRegionByZone(zone)
    self.id = None
    self.vpc_id = vpc_id
    self.zone = zone
    self.name = name

  def _Create(self):
    """Creates the VSwitch."""
    create_cmd = util.ALI_PREFIX + [
        'ecs',
        'CreateVSwitch',
        '--VSwitchName %s' % self.name,
        '--ZoneId %s' % self.zone,
        '--RegionId %s' % self.region,
        '--CidrBlock 10.0.0.0/24',
        '--VpcId %s' % self.vpc_id,
    ]
    create_cmd = util.GetEncodedCmd(create_cmd)
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['VSwitchId']

  def _Delete(self):
    """Deletes the VSwitch."""
    delete_cmd = util.ALI_PREFIX + [
        'ecs',
        'DeleteVSwitch',
        '--RegionId %s' % self.region,
        '--VSwitchId %s' % self.id]
    delete_cmd = util.GetEncodedCmd(delete_cmd)
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the VSwitch exists."""
    describe_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeVSwitches',
        '--RegionId %s' % self.region,
        '--VpcId %s' % self.vpc_id,
        '--ZoneId %s' % self.zone]
    describe_cmd = util.GetEncodedCmd(describe_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    vswitches = response['VSwitches']['VSwitch']
    assert len(vswitches) < 2, 'Too many VSwitches.'
    return len(vswitches) > 0


class AliSecurityGroup(resource.BaseResource):
  """Object representing an AliCloud Security Group."""

  def __init__(self, name, region, use_vpc=True, vpc_id=None):
    super(AliSecurityGroup, self).__init__()
    self.name = name
    self.region = region
    self.use_vpc = use_vpc
    self.vpc_id = vpc_id

  def _Create(self):
    """Creates the security group."""
    create_cmd = util.ALI_PREFIX + [
        'ecs',
        'CreateSecurityGroup',
        '--SecurityGroupName %s' % self.name,
        '--RegionId %s' % self.region]
    if self.use_vpc:
      create_cmd.append('--VpcId %s' % self.vpc_id)
    create_cmd = util.GetEncodedCmd(create_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    self.group_id = json.loads(stdout)['SecurityGroupId']

  def _Delete(self):
    """Deletes the security group."""
    delete_cmd = util.ALI_PREFIX + [
        'ecs',
        'DeleteSecurityGroup',
        '--RegionId %s' % self.region,
        '--SecurityGroupId %s' % self.group_id]
    delete_cmd = util.GetEncodedCmd(delete_cmd)
    vm_util.IssueRetryableCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the security group exists."""
    show_cmd = util.ALI_PREFIX + [
        'ecs',
        'DescribeSecurityGroups',
        '--RegionId %s' % self.region,
        '--SecurityGroupId %s' % self.group_id]
    show_cmd = util.GetEncodedCmd(show_cmd)
    stdout, _ = vm_util.IssueRetryableCommand(show_cmd)
    response = json.loads(stdout)
    securityGroups = response['SecurityGroups']['SecurityGroup']
    assert len(securityGroups) < 2, 'Too many securityGroups.'
    if not securityGroups:
      return False
    return True



class AliFirewall(network.BaseFirewall):
  """An object representing the AliCloud Firewall."""

  CLOUD = providers.ALICLOUD

  def __init__(self):
    self.firewall_set = set()
    self._lock = threading.Lock()

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the ICMP protocol for.
    """
    if vm.is_static:
      return
    with self._lock:
      authorize_cmd = util.ALI_PREFIX + [
          'ecs',
          'AuthorizeSecurityGroup',
          '--IpProtocol ICMP',
          '--PortRange -1/-1',
          '--SourceCidrIp 0.0.0.0/0',
          '--RegionId %s' % vm.region,
          '--SecurityGroupId %s' % vm.group_id]
      if FLAGS.ali_use_vpc:
        authorize_cmd.append('--NicType intranet')
      authorize_cmd = util.GetEncodedCmd(authorize_cmd)
      vm_util.IssueRetryableCommand(authorize_cmd)

  def AllowPort(self, vm, port):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      port: The local port to open.
    """
    if vm.is_static:
      return
    entry = (port, vm.group_id)
    if entry in self.firewall_set:
      return
    with self._lock:
      if entry in self.firewall_set:
        return
      for protocol in ('tcp', 'udp'):
        authorize_cmd = util.ALI_PREFIX + [
            'ecs',
            'AuthorizeSecurityGroup',
            '--IpProtocol %s' % protocol,
            '--PortRange %s/%s' % (port, port),
            '--SourceCidrIp 0.0.0.0/0',
            '--RegionId %s' % vm.region,
            '--SecurityGroupId %s' % vm.group_id]
        if FLAGS.ali_use_vpc:
          authorize_cmd.append('--NicType intranet')
        authorize_cmd = util.GetEncodedCmd(authorize_cmd)
        vm_util.IssueRetryableCommand(authorize_cmd)
      self.firewall_set.add(entry)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class AliNetwork(network.BaseNetwork):
  """Object representing a AliCloud Network."""

  CLOUD = providers.ALICLOUD

  def __init__(self, spec):
    super(AliNetwork, self).__init__(spec)
    self.name = (
        'perfkit-%s-%s' % (FLAGS.run_uri, str(uuid.uuid4())[-12:]))
    self.region = util.GetRegionByZone(spec.zone)
    self.use_vpc = FLAGS.ali_use_vpc
    if self.use_vpc:
      self.vpc = AliVpc(self.name, self.region)
      self.vswitch = None
      self.security_group = None
    else:
      self.security_group = \
          AliSecurityGroup(self.name, self.region, use_vpc=False)

  @vm_util.Retry()
  def Create(self):
    """Creates the network."""
    if self.use_vpc:
      self.vpc.Create()
      self.vpc._WaitForVpcStatus(['Available'])
      if self.vswitch is None:
        self.vswitch = AliVSwitch(self.name, self.zone, self.vpc.id)
      self.vswitch.Create()

      if self.security_group is None:
        self.security_group = AliSecurityGroup(self.name,
                                               self.region,
                                               use_vpc=True,
                                               vpc_id=self.vpc.id)
      self.security_group.Create()
    else:
      self.security_group.Create()

  def Delete(self):
    """Deletes the network."""
    if self.use_vpc:
      self.security_group.Delete()
      self.vswitch.Delete()
      self.security_group.Delete()
      self.vpc.Delete()
    else:
      self.security_group.Delete()
