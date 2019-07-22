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
import re
import xmltodict


from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

flags.DEFINE_string('aws_vpc', None,
                    'The static AWS VPC id to use. Default creates a new one')
flags.DEFINE_string(
    'aws_subnet', None,
    'The static AWS subnet id to use.  Default creates a new one')
FLAGS = flags.FLAGS


REGION = 'region'
ZONE = 'zone'


class AwsFirewall(network.BaseFirewall):
  """An object representing the AWS Firewall."""

  CLOUD = providers.AWS

  def __init__(self):
    self.firewall_set = set()
    self._lock = threading.Lock()

  def AllowIcmp(self, region, security_group, cidr):
    # aws ec2 authorize-security-group-ingress
    # aws ec2 authorize-security-group-ingress --group-id sg-05075517a1daed16a --ip-permissions IpProtocol=icmp,FromPort=-1,ToPort=-1,IpRanges=[{CidrIp=0.0.0.0/0}]
    # --ip-permissions IpProtocol=icmp,FromPort=-1,ToPort=-1,IpRanges=[{CidrIp=0.0.0.0/0}]
    entry = (-1, -1, region, security_group)
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
          '--protocol=icmp',
          '--port=-1',
          '--cidr=%s' % cidr]
      util.IssueRetryableCommand(
          authorize_cmd)
      self.firewall_set.add(entry)

  def AllowPort(self, vm, start_port, end_port=None, source_range=None):
    """Opens a port on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the port for.
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
      source_range: List of source CIDRs to allow for this port. If None, all
        sources are allowed. i.e. ['0.0.0.0/0']
    """
    if vm.is_static or vm.network.is_static:
      return
    self.AllowPortInSecurityGroup(vm.region, vm.group_id, start_port, end_port,
                                  source_range)

  def AllowPortInSecurityGroup(self,
                               region,
                               security_group,
                               start_port,
                               end_port=None,
                               source_range=None):
    """Opens a port on the firewall for a security group.

    Args:
      region: The region of the security group
      security_group: The security group in which to open the ports
      start_port: The first local port to open in a range.
      end_port: The last local port to open in a range. If None, only start_port
        will be opened.
      source_range: List of source CIDRs to allow for this port.
    """
    end_port = end_port or start_port
    source_range = source_range or ['0.0.0.0/0']
    for source in source_range:
      entry = (start_port, end_port, region, security_group, source)
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
            '--cidr=%s' % source,
        ]
        util.IssueRetryableCommand(authorize_cmd + ['--protocol=tcp'])
        util.IssueRetryableCommand(authorize_cmd + ['--protocol=udp'])
        self.firewall_set.add(entry)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class AwsVpc(resource.BaseResource):
  """An object representing an Aws VPC."""

  def __init__(self, region, vpc_id=None, cidr_block='10.0.0.0/16'):
    super(AwsVpc, self).__init__()
    self.region = region
    self.id = vpc_id
    # Subnets are assigned per-AZ.
    # _subnet_index tracks the next unused 10.0.x.0/24 block.
    self._subnet_index = 0
    # Lock protecting _subnet_index
    self._subnet_index_lock = threading.Lock()
    self.default_security_group_id = None
    if self.id:
      self._SetSecurityGroupId()

    self.cidr_block = cidr_block

  def _Create(self):
    """Creates the VPC."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpc',
        '--region=%s' % self.region,
        '--cidr-block=%s' % self.cidr_block]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['Vpc']['VpcId']
    self._EnableDnsHostnames()
    util.AddDefaultTags(self.id, self.region)

  def _PostCreate(self):
    self._SetSecurityGroupId()

  def _SetSecurityGroupId(self):
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

  def __init__(self, zone, vpc_id, cidr_block='10.0.0.0/24', subnet_id=None):
    super(AwsSubnet, self).__init__(subnet_id is not None)
    self.zone = zone
    self.region = util.GetRegionFromZone(zone)
    self.vpc_id = vpc_id
    self.id = subnet_id
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
    return bool(self.GetDict())

  def GetDict(self):
    """The 'aws ec2 describe-subnets' for this VPC / subnet id.

    Returns:
      A dict of the single subnet or an empty dict if there are no subnets.

    Raises:
      AssertionError: If there is more than one subnet.
    """
    describe_cmd = util.AWS_PREFIX + [
        'ec2', 'describe-subnets',
        '--region=%s' % self.region,
        '--filter=Name=vpc-id,Values=%s' % self.vpc_id
    ]
    if self.id:
      describe_cmd.append('--filter=Name=subnet-id,Values=%s' % self.id)
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    subnets = response['Subnets']
    assert len(subnets) < 2, 'Too many subnets.'
    return subnets[0] if subnets else {}


class AwsInternetGateway(resource.BaseResource):
  """An object representing an Aws Internet Gateway."""

  def __init__(self, region, vpc_id=None):
    super(AwsInternetGateway, self).__init__(vpc_id is not None)
    self.region = region
    self.vpc_id = None
    self.id = None
    self.attached = False
    if vpc_id:
      self.vpc_id = vpc_id
      self.id = self.GetDict()['InternetGatewayId']
      self.attached = True

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
    return bool(self.GetDict())

  def GetDict(self):
    """The 'aws ec2 describe-internet-gateways' for this VPC / gateway id.

    Returns:
      A dict of the single gateway or an empty dict if there are no gateways.

    Raises:
      AssertionError: If there is more than one internet gateway.
    """
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-internet-gateways',
        '--region=%s' % self.region,
    ]
    if self.id:
      describe_cmd.append('--filter=Name=internet-gateway-id,Values=%s' %
                          self.id)
    if self.vpc_id:
      describe_cmd.append('--filter=Name=attachment.vpc-id,Values=%s' %
                          self.vpc_id)
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    internet_gateways = response['InternetGateways']
    assert len(internet_gateways) < 2, 'Too many internet gateways.'
    return internet_gateways[0] if internet_gateways else {}

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
    if self.attached and not self.user_managed:
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

  CLOUD = providers.AWS

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)

  def __init__(self, region, vpc_id=None, cidr_block='10.0.0.0/16'):
    self.region = region
    self.vpc = AwsVpc(self.region, cidr_block, vpc_id)
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
  def GetForRegion(cls, region, vpc_id=None, cidr_block='10.0.0.0/16'):
    """Retrieves or creates an _AwsRegionalNetwork.

    Args:
      region: string. AWS region name.
      vpc_id: string. AWS VPC id.

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
      benchmark_spec.networks[key] = cls(region, vpc_id, cidr_block)
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


class AwsNetworkSpec(network.BaseNetworkSpec):
  """Configuration for creating an AWS network."""

  def __init__(self, zone, vpc_id=None, subnet_id=None):
    super(AwsNetworkSpec, self).__init__(zone)
    if vpc_id or subnet_id:
      logging.info('Confirming vpc (%s) and subnet (%s) selections', vpc_id,
                   subnet_id)
      my_subnet = AwsSubnet(self.zone, vpc_id, subnet_id=subnet_id).GetDict()
      self.vpc_id = my_subnet['VpcId']
      self.subnet_id = my_subnet['SubnetId']
      self.cidr_block = my_subnet['CidrBlock']
      logging.info('Using vpc %s subnet %s cidr %s', self.vpc_id,
                   self.subnet_id, self.cidr_block)
    else:
      self.vpc_id = None
      self.subnet_id = None
      self.cidr_block = None


class AwsNetwork(network.BaseNetwork):
  """Object representing an AWS Network.

  Attributes:
    region: The AWS region the Network is in.
    regional_network: The AwsRegionalNetwork for 'region'.
    subnet: the AwsSubnet for this zone.
    placement_group: An AwsPlacementGroup instance.
  """

  CLOUD = providers.AWS

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)

  def __init__(self, spec):
    """Initializes AwsNetwork instances.

    Args:
      spec: An AwsNetworkSpec object.
    """
    super(AwsNetwork, self).__init__(spec)
    self.region = util.GetRegionFromZone(spec.zone)
    if spec.zone and spec.cidr and FLAGS.use_vpn:
      self.regional_network = _AwsRegionalNetwork.GetForRegion(self.region, spec.vpc_id, spec.cidr)
    else:
      self.regional_network = _AwsRegionalNetwork.GetForRegion(self.region, spec.vpc_id)
    self.subnet = None
    self.cidr = None
    self.placement_group = AwsPlacementGroup(self.region)
    self.is_static = False
    if spec.vpc_id:
      self.is_static = True
      self.subnet = AwsSubnet(
          self.zone,
          spec.vpc_id,
          cidr_block=spec.cidr_block,
          subnet_id=spec.subnet_id)

    self.vpngw = {}
    self.az = spec.zone

    name = 'vpnpkb-network-%s' % FLAGS.run_uri
    if spec.zone and spec.cidr and FLAGS.use_vpn:
      self.cidr = spec.cidr
      for tunnelnum in range(0, FLAGS.vpn_service_tunnel_count):
        vpngw_name = 'vpngw-%s-%s-%s' % (
            spec.zone, tunnelnum, FLAGS.run_uri)
        self.vpngw[vpngw_name] = AwsVPNGW(
            vpngw_name, name, spec.zone,
            spec.cidr)
  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns an AwsNetworkSpec created from VM attributes and flags."""
    return AwsNetworkSpec(vm.zone, FLAGS.aws_vpc, FLAGS.aws_subnet)

  def Create(self):
    """Creates the network."""
    self.regional_network.Create()

    if FLAGS.use_vpn and self.subnet is None:
      self.subnet = AwsSubnet(self.zone, self.regional_network.vpc.id,
                              cidr_block=self.cidr)
    if self.subnet is None:
      cidr = self.regional_network.vpc.NextSubnetCidrBlock()
      self.subnet = AwsSubnet(self.zone, self.regional_network.vpc.id,
                              cidr_block=cidr)
      self.subnet.Create()
    self.placement_group.Create()
    if getattr(self, 'vpngw', False):
      for gw in self.vpngw:
        self.vpngw[gw].Create()

  def Delete(self):
    """Deletes the network."""
    if getattr(self, 'vpngw', False):
        for gw in self.vpngw:
          self.vpngw[gw].Delete()
    if self.subnet:
      self.subnet.Delete()
    self.placement_group.Delete()
    self.regional_network.Delete()

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    return (cls.CLOUD, ZONE, spec.zone)


class AwsVPNGWResource(resource.BaseResource):

  def __init__(self, name, network_name, az, cidr):
    super(AwsVPNGWResource, self).__init__()
    self.name = name
    self.network_name = network_name
    self.az = az
    self.region = util.GetRegionFromZone(self.az)
    self.cidr = cidr
    self.attached = False
    self.id = None
    self.vpc_id = None
    self.ip_address = None
    self.vpn_cnxn = None
    self.psk = None
    self.routing = None
    # self.project = project

# {
#     "VpnGateway": {
#         "AmazonSideAsn": 64512,
#         "State": "available",
#         "Type": "ipsec.1",
#         "VpnGatewayId": "vgw-9a4cacf3",
#         "VpcAttachments": []
#     }
# }
  def _Create(self):
    """Creates the VPN gateway."""

    '''
    --availability-zone (string)
    The Availability Zone for the virtual private gateway.
--type (string)
    The type of VPN connection this virtual private gateway supports.
    Possible values:
        ipsec.1
--amazon-side-asn (long)
    A private Autonomous System Number (ASN) for the Amazon side of a BGP session. If you're using a 16-bit ASN, it must be in the 64512 to 65534 range. If you're using a 32-bit ASN, it must be in the 4200000000 to 4294967294 range.
    Default: 64512
  '''
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-gateway',
        '--region=%s' % self.region,
        '--availability-zone=%s' % self.az,
        '--type=%s' % 'ipsec.1']
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
#     logging.log(logging.INFO, response)
    self.id = response['VpnGateway']['VpnGatewayId']
    util.AddDefaultTags(self.id, self.region)

#   describe-vpn-gateways
# [--filters <value>]
# [--vpn-gateway-ids <value>]
# [--dry-run | --no-dry-run]
# [--cli-input-json <value>]
# [--generate-cli-skeleton <value>]
  def _Exists(self):
    """Returns true if the vpn gateway exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpn-gateways',
        '--region=%s' % self.region,
        #         '--region=%s' % self.region,
        '--filter=Name=vpn-gateway-id,Values=%s' % self.id]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
#     logging.log(logging.INFO, response)
    vpn_gateways = response['VpnGateways']
    state = vpn_gateways[0]['State']
#     logging.log(logging.INFO, 'state: %s' % state)
#     logging.log(logging.INFO, 'vpngw_len: '.join(str(len(vpn_gateways))))
    assert len(vpn_gateways) < 2, 'Too many VPN gateways.'
    return len(vpn_gateways) > 0 and state != 'deleted'

#   delete-vpn-gateway
# --vpn-gateway-id <value>
# [--dry-run | --no-dry-run]
# [--cli-input-json <value>]
# [--generate-cli-skeleton <value>]
  def _Delete(self):
    """Deletes the vpn gateway."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpn-gateway',
        '--region=%s' % self.region,
        '--vpn-gateway-id=%s' % self.id]
    vm_util.IssueCommand(delete_cmd)

  def Attach(self, vpc_id):
    """Attaches the vpn gateway to the VPC."""
    if not self.attached:
      self.vpc_id = vpc_id
      attach_cmd = util.AWS_PREFIX + [
          'ec2',
          'attach-vpn-gateway',
          '--region=%s' % self.region,
          '--vpn-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id]
      util.IssueRetryableCommand(attach_cmd)
      self.attached = True

  def Detach(self):
    """Detaches the vpn gateway from the VPC."""
    if self.attached:
      detach_cmd = util.AWS_PREFIX + [
          'ec2',
          'detach-vpn-gateway',
          '--region=%s' % self.region,
          '--vpn-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id]
      util.IssueRetryableCommand(detach_cmd)
      self.attached = False

  def AllowRoutePropogation(self, route_table_id):
    cmd = util.AWS_PREFIX + [
        'ec2',
        'enable-vgw-route-propagation',
        '--region=%s' % self.region,
        '--gateway-id=%s' % self.id,
        '--route-table-id=%s' % route_table_id]
    util.IssueRetryableCommand(cmd)

  def GetDefaultSecurityGroup(self):
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-security-groups',
        '--region=%s' % self.region]
    response = vm_util.IssueCommand(cmd)
    response = json.loads(response)
    for sg in response['SecurityGroups']:
      if sg['GroupName'] == 'default':
        return sg['GroupId']


class AwsVPNGW(network.BaseVPNGW):
  CLOUD = providers.AWS

  def __init__(self, name, network_name, az, cidr):
    super(AwsVPNGW, self).__init__()
#     self._lock = threading.Lock()

    self.forwarding_rules = {}
    self.tunnels = {}
    self.routes = {}
    self.vpngw_resource = AwsVPNGWResource(name, network_name, az, cidr)

    self.name = name
    self.network_name = network_name
    self.az = az
    self.region = util.GetRegionFromZone(self.az)
    self.cidr = cidr
    self.ip_address = None

    self.created = False  # managed by BaseResource
#     self.endpoint = None
    self.attached = False
    self.psk = None
#     self.routing = None  # @TODO static/dynamic
    self.require_target_to_init = True
    self.customer_gw = None
    self.cgw_id = None
    self.vpn_connection = None
    self.route_table_id = None

  def ConfigureTunnel(self, tunnel_config):
    network.BaseVPNGW.ConfigureTunnel(self, tunnel_config)
    logging.info('Configuring Tunnel with params:')
    logging.info(tunnel_config)

    # update tunnel_config if needed
    if self.name not in tunnel_config.endpoints:
      logging.info('tunnel_config: This endpoint isnt registered yet... %s' % self.name)
      tunnel_config.endpoints[self.name] = {}
      tunnel_config.endpoints[self.name]['is_configured'] = False
      tunnel_config.endpoints[self.name]['cidr'] = self.cidr
      tunnel_config.endpoints[self.name]['network_name'] = self.network_name
      tunnel_config.endpoints[self.name]['region'] = self.region
      tunnel_config.endpoints[self.name]['az'] = self.az
      tunnel_config.endpoints[self.name]['require_target_to_init'] = self.require_target_to_init

    # Abort if we don't have a target info configured yet
    if len(tunnel_config.endpoints) < 2:
      logging.info('tunnel_config: Only found %d endpoints... waiting for target to configure' % len(tunnel_config.endpoints))
      return

    target_endpoint = [k for k in tunnel_config.endpoints.keys() if k not in self.name][0]

    # setup customer gw
    # requires: target_ip
    if 'ip_address' not in tunnel_config.endpoints[target_endpoint]:
      logging.info('tunnel_config: require target ip to configure... punting for now')
      return
    if self.customer_gw is None and tunnel_config.endpoints[target_endpoint]['ip_address'] is not None:
      logging.info('tunnel_config: Creating cuustomer gw for IP %s' % tunnel_config.endpoints[target_endpoint]['ip_address'])
      self.customer_gw = AwsCustomerGW(self.region, tunnel_config.endpoints[target_endpoint]['ip_address'])
      self.customer_gw.Create()
      self.cgw_id = self.customer_gw.id
      logging.info("tunnel_config: Created customer gw with id: %s" % self.customer_gw.id)

    net = _AwsRegionalNetwork.GetForRegion(self.region)
    if not self.attached:
      logging.info("tunnel_config: Attaching VGW %s to VPC %s" % (self.vpngw_resource.id, net.vpc.id))
      self.vpngw_resource.Attach(net.vpc.id)
      self.attached = True
      self.vpngw_resource.AllowRoutePropogation(net.route_table.id)

    # Create VPN connection
    # requires: cgw_id,
    if not self.vpn_connection:
      logging.info("tunnel_config: Creating AWS VPN Connection with PSK %s" % self.psk)
      self.vpn_connection = AwsVPNConnection(self.cgw_id, self.vpngw_resource.id, self.region, psk=self.psk)
      self.vpn_connection.Create()
      self.ip_address = self.vpn_connection.ip
      tunnel_config.endpoints[self.name]['ip_address'] = self.ip_address
      tunnel_config.endpoints[self.name]['tunnel_id'] = self.vpn_connection.id

    if not tunnel_config.endpoints[target_endpoint]['cidr']:
      logging.info("tunnel_config: Target CIDR needed for route creation... returning")
      return

    if not tunnel_config.endpoints[self.name]['is_configured']:
      AwsFirewall.GetFirewall().AllowIcmp(self.region, net.vpc.default_security_group_id, tunnel_config.endpoints[target_endpoint]['cidr'])
      self.vpn_connection.Create_VPN_Cnxn_Route(tunnel_config.endpoints[target_endpoint]['cidr'])

    tunnel_config.endpoints[self.name]['is_configured'] = True

  def IsTunnelReady(self, tunnel_id):
    return True

  def Create(self):
    """Creates the actual VPNGW."""
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error('GetNetwork called in a thread without a '
                         'BenchmarkSpec.')
    # with self._lock:
    if self.created:
      return
    if self.vpngw_resource:
      self.vpngw_resource.Create()
    key = self.name
    # with benchmark_spec.vpngws_lock:
    if key not in benchmark_spec.vpngws:
      benchmark_spec.vpngws[key] = self
    return benchmark_spec.vpngws[key]
    self.created = True

  def Delete(self):
    """Deletes the actual VPNGW."""
    if self.vpn_connection and self.vpn_connection._Exists():
      self.vpn_connection.Delete()

    if self.customer_gw and self.customer_gw.Exists():
      self.customer_gw.Delete()

#     if self.forwarding_rules:
#       for fr in self.forwarding_rules:
#         self.forwarding_rules[fr].Delete()

#     if self.tunnels:
#       for tun in self.tunnels:
#         if self.TunnelExists(tun):
#           self.DeleteTunnel(tun)

#     if self.routes:
#       for route in self.routes:
#         if self.RouteExists(route):
#           self.DeleteRoute(route)
#     self.created = False

    # vpngws need deleted last
    if self.vpngw_resource:
      self.vpngw_resource.Detach()
      self.vpngw_resource.Delete()


class AwsVPNConnection(resource.BaseResource):
  """An object representing a Aws VPNConnection."""

  def __init__(self, cgw_id, vpg_id, region, psk=None, routing="static", type="ipsec.1"):
    super(AwsVPNConnection, self).__init__()
    self.cgw_id = cgw_id
    self.vpg_id = vpg_id
    self.region = region
    self.type = type
    self.id = None
    self.routing = routing
    self.psk = FLAGS.run_uri
    self.cnxn_details = None
    self.ip = None  # do we need to support failover (2nd IP?)

  def _Create(self):
    """creates VPN cnxn and 2 vpn tunnels to CGW."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-connection',
        #     '--debug',
        '--region=%s' % self.region,
        '--customer-gateway-id=%s' % self.cgw_id,
        '--vpn-gateway-id=%s' % self.vpg_id,
        '--type=%s' % self.type,
        '--options=%s' % self._getCnxnOpts()]
    response, _ = util.IssueRetryableCommand(create_cmd)
#     logging.log(logging.INFO, response)
    response_xml = response[response.find("<"):response.rfind(">") + 1].replace("\\n", "").replace("\\", "")
    self.cnxn_details = xmltodict.parse(response_xml)
    response_json = re.sub(r'<.*>', '', response)
    response_json = json.loads(response_json)
#     logging.log(logging.INFO, self.id)
#     logging.log(logging.INFO, response_json)
    self.id = response_json["VpnConnection"]["VpnConnectionId"]
    #     self.ip = response_json["VpnConnection"]["VgwTelemetry"][0]["OutsideIpAddress"] # @TODO find "VgwTelemetry" from aws docs
    response_xml_dict = xmltodict.parse(response_xml)
    self.ip = response_xml_dict["vpn_connection"]["ipsec_tunnel"][0]["vpn_gateway"]["tunnel_outside_address"]["ip_address"].encode()

#     logging.log(logging.INFO, response_xml_dict)
#   @vm_util.Retry()
#   def _PostCreate(self):
#     """Gets data about the VPN cnxn."""
#     cmd = util.AWS_PREFIX + [
#     'ec2',
#     'describe-vpn-connections',
#     '--region=%s' % self.region,
#     '--filter=Name=vpn-connection-id,Values=%s' % self.id]
#     response = vm_util.IssueCommand(cmd)
#     logging.log(logging.INFO, response)
#     response, _ = util.IssueRetryableCommand(cmd)
#     logging.log(logging.INFO, response)
#     #response = response[0]
# #     response_xml = response[response.find("<"):response.rfind(">") + 1].replace("\\n","").replace("\\","")
# #     logging.log(logging.INFO, response_xml)
# #     response_xml_dict = xmltodict.parse(response_xml)
#     response_json = re.sub(r'<.*>','',response.rstrip())
#     response_json = json.loads(response_json)
#     self.ip = response_json["VpnConnection"]["VgwTelemetry"][0]["OutsideIpAddress"] # @TODO top level cnxn/endpont bean?

  def _Delete(self):
    "Deletes the VPN Connection"
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpn-connection',
        '--region=%s' % self.region,
        '--vpn-connection-id=%s' % self.id]
    response, _ = util.IssueRetryableCommand(delete_cmd)
#     logging.log(logging.INFO, response)

  def _Exists(self):
    """Returns True if the VPNConnection exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpn-connections',
        '--region=%s' % self.region,
        '--filter=Name=vpn-connection-id,Values=%s' % self.id]
    response = vm_util.IssueCommand(cmd)
#     logging.log(logging.INFO, response)
    response, _ = util.IssueRetryableCommand(cmd)
#     logging.log(logging.INFO, response)
    # response = response[0]
#     response_xml = response[response.find("<"):response.rfind(">") + 1].replace("\\n", "").replace("\\", "")
#     logging.log(logging.INFO, response_xml)
#     response_xml_dict = xmltodict.parse(response_xml)
    response_json = re.sub(r'<.*>', '', response)
    response_json = json.loads(response_json)
    return len(response_json["VpnConnections"]) > 0 and response_json["VpnConnections"][0]["State"] in ["available", "pending"]
#     logging.log(logging.INFO, self.id)
#     logging.log(logging.INFO, response_json)
#     logging.log(logging.INFO, response_xml_dict)

  def IsReady(self):
    """Returns True if the VPNConnection exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpn-connections',
        '--region=%s' % self.region,
        '--filter=Name=vpn-connection-id,Values=%s' % self.id]
    response = vm_util.IssueCommand(cmd)
#     logging.log(logging.INFO, response)
    response, _ = util.IssueRetryableCommand(cmd)
#     logging.log(logging.INFO, response)
    # response = response[0]
#     response_xml = response[response.find("<"):response.rfind(">") + 1].replace("\\n", "").replace("\\", "")
#     logging.log(logging.INFO, response_xml)
#     response_xml_dict = xmltodict.parse(response_xml)
    response_json = re.sub(r'<.*>', '', response)
    response_json = json.loads(response_json)
    return len(response_json["VpnConnections"]) > 0 and response_json["VpnConnections"][0]["State"] in ["available"]
#     logging.log(logging.INFO, self.id)
#     logging.log(logging.INFO, response_json)
#     logging.log(logging.INFO, response_xml_dict)

  def Create_VPN_Cnxn_Route(self, target_cidr):
    """sets up routes to target."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-connection-route',
        '--region=%s' % self.region,
        '--destination-cidr-block=%s' % target_cidr,
        '--vpn-connection-id=%s' % self.id]
    response, _ = util.IssueRetryableCommand(create_cmd)
#     response = json.loads(response)
#     logging.log(logging.INFO, response)

  def _getCnxnOpts(self):
    """
    --options StaticRoutesOnly=boolean,
    TunnelOptions=[{TunnelInsideCidr=string,PreSharedKey=string},
                   {TunnelInsideCidr=string,PreSharedKey=string}]
    """

    """
    eg
    aws ec2 create-vpn-connection
      --type ipsec.1
      --customer-gateway-id cgw-b4de3fdd
      --vpn-gateway-id vgw-f211f09b
      --options "{"StaticRoutesOnly":false,
                  "TunnelOptions":[{
                    "TunnelInsideCidr":"169.254.12.0/30",
                    "PreSharedKey":"ExamplePreSharedKey1"},
                   {"TunnelInsideCidr":"169.254.13.0/30",
                    "PreSharedKey":"ExamplePreSharedKey2"}]}"
    """
    # should probably have tuples for insidecidr/psk
    opts = {}
    logging.info("Get VPN Cnxn Options<empty>: %s " % str(opts))
    opts["StaticRoutesOnly"] = self.routing == "static"
    logging.info("Get VPN Cnxn Options<routing>:  %s" % str(opts))
    logging.info("Get VPN Cnxn Options<routing>:  %s" % repr(opts))
    logging.info("Get VPN Cnxn Options<self.psk>:  %s" % str(self.psk))
#     if self.psk != None:
    key = 'key' + FLAGS.run_uri  # psk restrictions on some runuris
    opts["TunnelOptions"] = [{'PreSharedKey': key}, {'PreSharedKey': key}]
    logging.info("Get VPN Cnxn Options<tun_opts>:  %s" % str(opts))
#     return json.dumps(opts).replace("\"", "\\\"")
    # https://docs.aws.amazon.com/cli/latest/userguide/cli-using-param.html
    logging.info("Get VPN Cnxn Options:  %s" % str(json.dumps(json.dumps(opts))[1:-1].replace('\\', '')))
    return json.dumps(json.dumps(opts))[1:-1].replace('\\', '')


class AwsCustomerGW(resource.BaseResource):
  """An object representing an AwsCustomerGW."""

  def __init__(self, region, target_ip, *args, **kwargs):
    super(AwsCustomerGW, self).__init__()
    self.id = None
    self.target_ip = target_ip
    self.region = region

  def __eq__(self, other):
    """Defines equality to make comparison easy."""
    return (self.id == other.id)

  def _Create(self):
    """Creates the customer gateway."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-customer-gateway',
        '--region=%s' % self.region,
        '--public-ip=%s' % self.target_ip,
        '--bgp-asn=%s' % '64513',
        '--type=%s' % 'ipsec.1']
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['CustomerGateway']['CustomerGatewayId']

  def _Delete(self):
    """Deletes the AwsCustomerGW."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-customer-gateway',
        '--region=%s' % self.region,
        '--customer-gateway-id=%s' % self.id]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)

  def _Exists(self):
    """Returns True if the AwsCustomerGW exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-customer-gateways',
        '--region=%s' % self.region,
        '--customer-gateway-ids=%s' % self.id]
    response, _ = util.IssueRetryableCommand(cmd)
#     logging.log(logging.INFO, response)
    response = json.loads(response)
    return len(response["CustomerGateways"]) > 0 and response["CustomerGateways"][0]["State"] in ["available", "pending"]

  def _ExistsforIP(self):
    """Returns True if the AwsCustomerGW exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-customer-gateways',
        '--region=%s' % self.region,
        '--filter=Name=ip-address,Values=%s' % self.target_ip]
    response, _ = util.IssueRetryableCommand(cmd)
#     logging.log(logging.INFO, response)
    response = json.loads(response)
    return len(response["CustomerGateways"]) > 0 and response["VpnConnections"][0]["State"] in ["available", "pending"]
