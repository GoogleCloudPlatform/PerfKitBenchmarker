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

"""Module containing classes related to AWS VM networking.

The Firewall class provides a way of opening VM ports. The Network class allows
VMs to communicate via internal ips and isolates PerfKitBenchmarker VMs from
others in
the same project. See https://aws.amazon.com/documentation/vpc/
for more information about AWS Virtual Private Clouds.
"""

import json
import logging
import threading
import uuid

from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker import providers

FLAGS = flags.FLAGS


REGION = 'region'
ZONE = 'zone'


class AwsFirewall(network.BaseFirewall):
  """An object representing the AWS Firewall."""

  CLOUD = providers.AWS

  def __init__(self):
    self.firewall_set = set()
    self._lock = threading.Lock()

  def AllowPort(self, vm, start_port, end_port=None):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
    """
    if vm.is_static:
      return
    self.AllowPortInSecurityGroup(vm.region, vm.group_id, start_port, end_port)

  def AllowPortInSecurityGroup(self, region, security_group,
                               start_port, end_port=None):
    """Opens a port on the firewall for a security group.

    Args:
      region: The region of the security group
      security_group: The security group in which to open the ports
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
    """
    if end_port is None:
      end_port = start_port
    entry = (start_port, end_port, region, security_group)
    if entry in self.firewall_set:
      return
    with self._lock:
      if entry in self.firewall_set:
        return
      authorize_cmd = util.AWS_PREFIX + [
          'ec2',
          'authorize-security-group-ingress',
          '--region=%s' % region,
          '--group-id=%s' % security_group,
          '--port=%s-%s' % (start_port, end_port),
          '--cidr=0.0.0.0/0']
      util.IssueRetryableCommand(
          authorize_cmd + ['--protocol=tcp'])
      util.IssueRetryableCommand(
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

    # Subnets are assigned per-AZ.
    # _subnet_index tracks the next unused 10.0.x.0/24 block.
    self._subnet_index = 0
    # Lock protecting _subnet_index
    self._subnet_index_lock = threading.Lock()
    self.default_security_group_id = None

  def _Create(self):
    """Creates the VPC."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpc',
        '--region=%s' % self.region,
        '--cidr-block=10.0.0.0/16']
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['Vpc']['VpcId']
    self._EnableDnsHostnames()
    util.AddDefaultTags(self.id, self.region)

  def _PostCreate(self):
    """Looks up the VPC default security group."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-security-groups',
        '--region', self.region,
        '--filters',
        'Name=group-name,Values=default',
        'Name=vpc-id,Values=' + self.id]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    response = json.loads(stdout)
    groups = response['SecurityGroups']
    if len(groups) != 1:
      raise ValueError('Expected one security group, got {} in {}'.format(
          len(groups), response))
    self.default_security_group_id = groups[0]['GroupId']
    logging.info('Default security group ID: %s',
                 self.default_security_group_id)

  def _Exists(self):
    """Returns true if the VPC exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpcs',
        '--region=%s' % self.region,
        '--filter=Name=vpc-id,Values=%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    vpcs = response['Vpcs']
    assert len(vpcs) < 2, 'Too many VPCs.'
    return len(vpcs) > 0

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

    util.IssueRetryableCommand(enable_hostnames_command)

  def _Delete(self):
    """Deletes the VPC."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpc',
        '--region=%s' % self.region,
        '--vpc-id=%s' % self.id]
    vm_util.IssueCommand(delete_cmd)

  def NextSubnetCidrBlock(self):
    """Returns the next available /24 CIDR block in this VPC.

    Each VPC has a 10.0.0.0/16 CIDR block.
    Each subnet is assigned a /24 within this allocation.
    Calls to this method return the next unused /24.

    Returns:
      A string representing the next available /24 block, in CIDR notation.
    Raises:
      ValueError: when no additional subnets can be created.
    """
    with self._subnet_index_lock:
      if self._subnet_index >= (1 << 8) - 1:
        raise ValueError('Exceeded subnet limit ({0}).'.format(
            self._subnet_index))
      cidr = '10.0.{0}.0/24'.format(self._subnet_index)
      self._subnet_index += 1
    return cidr


class AwsSubnet(resource.BaseResource):
  """An object representing an Aws subnet."""

  def __init__(self, zone, vpc_id, cidr_block='10.0.0.0/24'):
    super(AwsSubnet, self).__init__()
    self.zone = zone
    self.region = util.GetRegionFromZone(zone)
    self.vpc_id = vpc_id
    self.id = None
    self.cidr_block = cidr_block

  def _Create(self):
    """Creates the subnet."""

    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-subnet',
        '--region=%s' % self.region,
        '--vpc-id=%s' % self.vpc_id,
        '--cidr-block=%s' % self.cidr_block]
    if not util.IsRegion(self.zone):
      create_cmd.append('--availability-zone=%s' % self.zone)

    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['Subnet']['SubnetId']
    util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the subnet."""
    logging.info('Deleting subnet %s. This may fail if all instances in the '
                 'subnet have not completed termination, but will be retried.',
                 self.id)
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-subnet',
        '--region=%s' % self.region,
        '--subnet-id=%s' % self.id]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the subnet exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-subnets',
        '--region=%s' % self.region,
        '--filter=Name=subnet-id,Values=%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    subnets = response['Subnets']
    assert len(subnets) < 2, 'Too many subnets.'
    return len(subnets) > 0


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
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
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
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the internet gateway exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-internet-gateways',
        '--region=%s' % self.region,
        '--filter=Name=internet-gateway-id,Values=%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    internet_gateways = response['InternetGateways']
    assert len(internet_gateways) < 2, 'Too many internet gateways.'
    return len(internet_gateways) > 0

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
      util.IssueRetryableCommand(attach_cmd)
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
      util.IssueRetryableCommand(detach_cmd)
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
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
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
    util.IssueRetryableCommand(create_cmd)


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
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the Placement Group."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-placement-group',
        '--region=%s' % self.region,
        '--group-name=%s' % self.name]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the Placement Group exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-placement-groups',
        '--region=%s' % self.region,
        '--filter=Name=group-name,Values=%s' % self.name]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    placement_groups = response['PlacementGroups']
    assert len(placement_groups) < 2, 'Too many placement groups.'
    return len(placement_groups) > 0


class _AwsRegionalNetwork(network.BaseNetwork):
  """Object representing regional components of an AWS network.

  The benchmark spec contains one instance of this class per region, which an
  AwsNetwork may retrieve or create via _AwsRegionalNetwork.GetForRegion.

  Attributes:
    region: string. The AWS region.
    vpc: an AwsVpc instance.
    internet_gateway: an AwsInternetGateway instance.
    route_table: an AwsRouteTable instance. The default route table.
  """

  def __init__(self, region):
    self.region = region
    self.vpc = AwsVpc(self.region)
    self.internet_gateway = AwsInternetGateway(region)
    self.route_table = None
    self.created = False

    # Locks to ensure that a single thread creates / deletes the instance.
    self._create_lock = threading.Lock()

    # Tracks the number of AwsNetworks using this _AwsRegionalNetwork.
    # Incremented by Create(); decremented by Delete();
    # When a Delete() call decrements _reference_count to 0, the RegionalNetwork
    # is destroyed.
    self._reference_count = 0
    self._reference_count_lock = threading.Lock()

  @classmethod
  def GetForRegion(cls, region):
    """Retrieves or creates an _AwsRegionalNetwork.

    Args:
      region: string. AWS region name.

    Returns:
      _AwsRegionalNetwork. If an _AwsRegionalNetwork for the same region already
      exists in the benchmark spec, that instance is returned. Otherwise, a new
      _AwsRegionalNetwork is created and returned.
    """
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    key = cls.CLOUD, REGION, region
    # Because this method is only called from the AwsNetwork constructor, which
    # is only called from AwsNetwork.GetNetwork, we already hold the
    # benchmark_spec.networks_lock.
    if key not in benchmark_spec.networks:
      benchmark_spec.networks[key] = cls(region)
    return benchmark_spec.networks[key]

  def Create(self):
    """Creates the network."""
    with self._reference_count_lock:
      assert self._reference_count >= 0, self._reference_count
      self._reference_count += 1

    # Access here must be synchronized. The first time the block is executed,
    # the network will be created. Subsequent attempts to create the
    # network block until the initial attempt completes, then return.
    with self._create_lock:
      if self.created:
        return

      self.vpc.Create()

      self.internet_gateway.Create()
      self.internet_gateway.Attach(self.vpc.id)

      if self.route_table is None:
        self.route_table = AwsRouteTable(self.region, self.vpc.id)
      self.route_table.Create()
      self.route_table.CreateRoute(self.internet_gateway.id)

      self.created = True

  def Delete(self):
    """Deletes the network."""
    # Only actually delete if there are no more references.
    with self._reference_count_lock:
      assert self._reference_count >= 1, self._reference_count
      self._reference_count -= 1
      if self._reference_count:
        return

    self.internet_gateway.Detach()
    self.internet_gateway.Delete()
    self.vpc.Delete()


class AwsNetwork(network.BaseNetwork):
  """Object representing an AWS Network.

  Attributes:
    region: The AWS region the Network is in.
    regional_network: The AwsRegionalNetwor for 'region'.
    subnet: the AwsSubnet for this zone.
    placement_group: An AwsPlacementGroup instance.
  """

  CLOUD = providers.AWS

  def __init__(self, spec):
    """Initializes AwsNetwork instances.

    Args:
      spec: A BaseNetworkSpec object.
    """
    super(AwsNetwork, self).__init__(spec)
    self.region = util.GetRegionFromZone(spec.zone)
    self.regional_network = _AwsRegionalNetwork.GetForRegion(self.region)
    self.subnet = None
    self.placement_group = AwsPlacementGroup(self.region)

  def Create(self):
    """Creates the network."""
    self.regional_network.Create()

    if self.subnet is None:
      cidr = self.regional_network.vpc.NextSubnetCidrBlock()
      self.subnet = AwsSubnet(self.zone, self.regional_network.vpc.id,
                              cidr_block=cidr)
      self.subnet.Create()
    self.placement_group.Create()

  def Delete(self):
    """Deletes the network."""
    if self.subnet:
      self.subnet.Delete()
    self.placement_group.Delete()
    self.regional_network.Delete()

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    return (cls.CLOUD, ZONE, spec.zone)
