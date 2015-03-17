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

"""Module containing classes related to AWS VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in
the same project. See https://aws.amazon.com/documentation/vpc/
for more information about AWS Virtual Private Clouds.
"""

import json
import threading
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.aws import util

FLAGS = flags.FLAGS


class AwsFirewall(network.BaseFirewall):
  """An object representing the AWS Firewall."""

  def __init__(self, project):
    self.firewall_set = set()
    self._lock = threading.Lock()

  def __getstate__(self):
    """Implements getstate to allow pickling (since locks can't be pickled)."""
    d = self.__dict__.copy()
    del d['_lock']
    return d

  def __setstate__(self, state):
    """Restores the lock after the object is unpickled."""
    self.__dict__ = state
    self._lock = threading.Lock()

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
      authorize_cmd = util.AWS_PREFIX + [
          'ec2',
          'authorize-security-group-ingress',
          '--region=%s' % vm.region,
          '--group-id=%s' % vm.group_id,
          '--port=%s' % port,
          '--cidr=0.0.0.0/0']
      vm_util.IssueRetryableCommand(
          authorize_cmd + ['--protocol=tcp'])
      vm_util.IssueRetryableCommand(
          authorize_cmd + ['--protocol=udp'])
      self.firewall_set.add(entry)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class AwsVpc(resource.BaseResource):
  """An object representing an Aws VPC."""

  def __init__(self, region):
    super(AwsVpc, self).__init__()
    self.region = region
    self.id = None

  def _Create(self):
    """Creates the VPC."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpc',
        '--region=%s' % self.region,
        '--cidr-block=10.0.0.0/16']
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['Vpc']['VpcId']
    self._EnableDnsHostnames()
    util.AddDefaultTags(self.id, self.region)

  def _EnableDnsHostnames(self):
    """Sets the enableDnsHostnames attribute of this VPC to True.

    By default, instances launched in non-default VPCs are assigned an
    unresolvable hostname. This breaks the hadoop benchmark.  Setting the
    enableDnsHostnames attribute to 'true' on the VPC resolves this. See:
    http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_DHCP_Options.html
    """
    enable_hostnames_command = util.AWS_PREFIX + [
        'ec2',
        'modify-vpc-attribute',
        '--region=%s' % self.region,
        '--vpc-id', self.id,
        '--enable-dns-hostnames',
        '{ "Value": true }']

    vm_util.IssueRetryableCommand(enable_hostnames_command)

  def _Delete(self):
    """Delete's the VPC."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpc',
        '--region=%s' % self.region,
        '--vpc-id=%s' % self.id]
    vm_util.IssueRetryableCommand(delete_cmd)


class AwsSubnet(resource.BaseResource):
  """An object representing an Aws subnet."""

  def __init__(self, zone, vpc_id):
    super(AwsSubnet, self).__init__()
    self.zone = zone
    self.region = zone[:-1]
    self.vpc_id = vpc_id
    self.id = None

  def _Create(self):
    """Creates the subnet."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-subnet',
        '--region=%s' % self.region,
        '--vpc-id=%s' % self.vpc_id,
        '--cidr-block=10.0.0.0/24',
        '--availability-zone=%s' % self.zone]
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['Subnet']['SubnetId']
    util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the subnet."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-subnet',
        '--region=%s' % self.region,
        '--subnet-id=%s' % self.id]
    vm_util.IssueRetryableCommand(delete_cmd)


class AwsInternetGateway(resource.BaseResource):
  """An object representing an Aws Internet Gateway."""

  def __init__(self, region):
    super(AwsInternetGateway, self).__init__()
    self.region = region
    self.vpc_id = None
    self.id = None
    self.attached = False

  def _Create(self):
    """Creates the internet gateway."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-internet-gateway',
        '--region=%s' % self.region]
    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['InternetGateway']['InternetGatewayId']
    util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the internet gateway."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-internet-gateway',
        '--region=%s' % self.region,
        '--internet-gateway-id=%s' % self.id]
    vm_util.IssueRetryableCommand(delete_cmd)

  def Attach(self, vpc_id):
    """Attaches the internetgateway to the VPC."""
    if not self.attached:
      self.vpc_id = vpc_id
      attach_cmd = util.AWS_PREFIX + [
          'ec2',
          'attach-internet-gateway',
          '--region=%s' % self.region,
          '--internet-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id]
      vm_util.IssueRetryableCommand(attach_cmd)
      self.attached = True

  def Detach(self):
    """Detaches the internetgateway from the VPC."""
    if self.attached:
      detach_cmd = util.AWS_PREFIX + [
          'ec2',
          'detach-internet-gateway',
          '--region=%s' % self.region,
          '--internet-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id]
      vm_util.IssueRetryableCommand(detach_cmd)
      self.attached = False


class AwsRouteTable(resource.BaseResource):
  """An object representing a route table."""

  def __init__(self, region, vpc_id):
    super(AwsRouteTable, self).__init__()
    self.region = region
    self.vpc_id = vpc_id

  def _Create(self):
    """Creates the route table.

    This is a no-op since every VPC has a default route table.
    """
    pass

  def _Delete(self):
    """Deletes the route table.

    This is a no-op since the default route table gets deleted with the VPC.
    """
    pass

  @vm_util.Retry()
  def _PostCreate(self):
    """Gets data about the route table."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-route-tables',
        '--region=%s' % self.region,
        '--filters=Name=vpc-id,Values=%s' % self.vpc_id]
    stdout, _ = vm_util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    self.id = response['RouteTables'][0]['RouteTableId']

  def CreateRoute(self, internet_gateway_id):
    """Adds a route to the internet gateway."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-route',
        '--region=%s' % self.region,
        '--route-table-id=%s' % self.id,
        '--gateway-id=%s' % internet_gateway_id,
        '--destination-cidr-block=0.0.0.0/0']
    vm_util.IssueRetryableCommand(create_cmd)


class AwsPlacementGroup(resource.BaseResource):
  """Object representing an AWS Placement Group.

  Attributes:
    region: The AWS region the Placement Group is in.
    name: The name of the Placement Group.
  """

  def __init__(self, region):
    """Init method for AwsPlacementGroup.

    Args:
      region: A string containing the AWS region of the Placement Group.
    """
    super(AwsPlacementGroup, self).__init__()
    self.name = (
        'perfkit-%s-%s' % (FLAGS.run_uri, str(uuid.uuid4())[-12:]))
    self.region = region

  def _Create(self):
    """Creates the Placement Group."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-placement-group',
        '--region=%s' % self.region,
        '--group-name=%s' % self.name,
        '--strategy=cluster']
    vm_util.IssueRetryableCommand(create_cmd)

  def _Delete(self):
    """Deletes the Placement Group."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-placement-group',
        '--region=%s' % self.region,
        '--group-name=%s' % self.name]
    vm_util.IssueRetryableCommand(delete_cmd)


class AwsNetwork(network.BaseNetwork):
  """Object representing an AWS Network.

  Attributes:
    region: The AWS region the Network is in.
    vpc_id: The id of the Network's Virtual Private Cloud (VPC).
    subnet_id: The id of the Subnet of the Network's VPC.
    internet_gateway_id: The id of the Network's Internet Gateway.
    route_table_id: The id of the Route Table of the Networks's VPC.
  """

  def __init__(self, zone):
    """Initializes AwsNetwork instances.

    Args:
      zone: The Availability Zone that the Network corresponds to.
    """
    super(AwsNetwork, self).__init__(zone)
    self.region = zone[:-1]
    self.vpc = AwsVpc(self.region)
    self.internet_gateway = AwsInternetGateway(self.region)
    self.subnet = None
    self.route_table = None
    self.placement_group = AwsPlacementGroup(self.region)

  def Create(self):
    """Creates the network."""
    self.vpc.Create()

    self.internet_gateway.Create()
    self.internet_gateway.Attach(self.vpc.id)

    if self.route_table is None:
      self.route_table = AwsRouteTable(self.region, self.vpc.id)
    self.route_table.Create()
    self.route_table.CreateRoute(self.internet_gateway.id)

    if self.subnet is None:
      self.subnet = AwsSubnet(self.zone, self.vpc.id)
    self.subnet.Create()

    self.placement_group.Create()

  def Delete(self):
    """Deletes the network."""
    self.placement_group.Delete()

    if self.subnet:
      self.subnet.Delete()

    self.internet_gateway.Detach()
    self.internet_gateway.Delete()

    self.vpc.Delete()
