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

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_placement_group
from perfkitbenchmarker.providers.aws import aws_vpc_endpoint
from perfkitbenchmarker.providers.aws import util

_AWS_VPC = flags.DEFINE_string(
    'aws_vpc',
    None,
    'The static AWS VPC id to use. If unset, creates a new VPC.',
)
_AWS_SUBNET = flags.DEFINE_string(
    'aws_subnet',
    None,
    'The static AWS subnet id to use.  Set value to "default" to use '
    'default subnet. If unset, creates a new subnet.',
)
AWS_ENI_COUNT = flags.DEFINE_integer(
    'aws_eni_count', 1, 'The number of ENIs per instance.'
)
AWS_NETWORK_CARD_COUNT = flags.DEFINE_integer(
    'aws_network_card_count', 1, 'The number of network cards per instance.'
)
flags.DEFINE_bool('aws_efa', False, 'Whether to use an Elastic Fiber Adapter.')
flags.DEFINE_string(
    'aws_efa_version',
    '1.12.1',
    'Version of AWS EFA to use (must also pass in --aws_efa).',
)
flags.DEFINE_integer('aws_efa_count', 1, 'The number of EFAs per instance.')
flags.DEFINE_multi_enum(
    'aws_endpoint', [], ['s3'], 'List of AWS endpoints to create'
)

FLAGS = flags.FLAGS


REGION = 'region'
ZONE = 'zone'

NON_PLACEMENT_GROUP_PREFIXES = frozenset(
    ['t2', 'm3', 't3', 't3a', 't4g', 'vt1', 'm7i-flex']
)
# Reference -
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html#network-cards
DUAL_NETWORK_CARD_MACHINES = frozenset([
    'c6in.32xlarge',
    'c6in.metal',
    'dl1.24xlarge',
    'hpc6id.32xlarge',
    'hpc7a.12xlarge',
    'hpc7a.24xlarge',
    'hpc7a.48xlarge',
    'hpc7a.96xlarge',
    'm6idn.32xlarge',
    'm6idn.metal',
    'm6in.32xlarge',
    'm6in.metal',
    'p4d.24xlarge',
    'p4de.24xlarge',
    'p5.48xlarge',
    'p5en.48xlarge',
    'r6idn.32xlarge',
    'r6idn.metal',
    'r6in.32xlarge',
    'r6in.metal',
    'trn1.32xlarge',
    'trn1n.32xlarge',
])


class AwsFirewall(network.BaseFirewall):
  """An object representing the AWS Firewall."""

  CLOUD = provider_info.AWS

  def __init__(self):
    self.firewall_set = set()
    self.firewall_icmp_set = set()
    self._lock = threading.Lock()

  def AllowIcmp(self, vm):
    """Opens the ICMP protocol on the firewall.

    Args:
      vm: The BaseVirtualMachine object to open the ICMP protocol for.
    """
    source = '0.0.0.0/0'

    # region, group_id, source
    entry = (vm.region, vm.group_id, source)
    with self._lock:
      if entry in self.firewall_icmp_set:
        return
      # When defining ICMP firewall rules using the aws cli,
      # port specifies the type of ICMP traffic allowed,
      # with -1 meaning all ICMP types
      # https://docs.aws.amazon.com/cli/latest/reference/ec2/authorize-security-group-ingress.html
      authorize_cmd = util.AWS_PREFIX + [
          'ec2',
          'authorize-security-group-ingress',
          '--region=%s' % vm.region,
          '--group-id=%s' % vm.group_id,
          '--protocol=icmp',
          '--port=-1',
          '--cidr=%s' % source,
      ]
      util.IssueRetryableCommand(authorize_cmd)
      self.firewall_icmp_set.add(entry)

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
    self.AllowPortInSecurityGroup(
        vm.region, vm.group_id, start_port, end_port, source_range
    )

  def AllowPortInSecurityGroup(
      self, region, security_group, start_port, end_port=None, source_range=None
  ):
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
        continue
      if self._RuleExists(region, security_group, start_port, end_port, source):
        self.firewall_set.add(entry)
        continue
      with self._lock:
        if entry in self.firewall_set:
          continue
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

  def _RuleExists(self, region, security_group, start_port, end_port, source):
    """Whether the firewall rule exists in the VPC."""
    query_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-security-groups',
        '--region=%s' % region,
        '--group-ids=%s' % security_group,
        '--filters',
        'Name=ip-permission.cidr,Values={}'.format(source),
        'Name=ip-permission.from-port,Values={}'.format(start_port),
        'Name=ip-permission.to-port,Values={}'.format(end_port),
    ]
    stdout, _ = util.IssueRetryableCommand(query_cmd)
    # "groups" will be an array of all the matching firewall rules
    groups = json.loads(stdout)['SecurityGroups']
    return bool(groups)

  def DisallowAllPorts(self):
    """Closes all ports on the firewall."""
    pass


class AwsVpc(resource.BaseResource):
  """An object representing an Aws VPC."""

  def __init__(self, region, vpc_id=None, regional_network_index=0):
    super().__init__(vpc_id is not None)
    self.region = region
    self.regional_network_index = regional_network_index
    self.cidr = network.GetCidrBlock(self.regional_network_index, 0, 16)
    self.id = vpc_id
    # Subnets are assigned per-AZ.
    # _subnet_index tracks the next unused 10.x.y.0/24 block.
    self._subnet_index = 0
    # Lock protecting _subnet_index
    self._subnet_index_lock = threading.Lock()
    self.default_security_group_id = None
    if self.id:
      self._SetSecurityGroupId()
    self._endpoints = [
        aws_vpc_endpoint.CreateEndpointService(service, self)
        for service in set(FLAGS.aws_endpoint)
    ]

  def _Create(self):
    """Creates the VPC."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpc',
        '--region=%s' % self.region,
        '--cidr-block=%s' % self.cidr,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(
        create_cmd, raise_on_failure=False
    )
    if 'VpcLimitExceeded' in stderr:
      raise errors.Benchmarks.QuotaFailure(stderr)
    if retcode:
      raise errors.Resource.CreationError(
          'Failed to create Vpc: %s return code: %s' % (retcode, stderr)
      )

    response = json.loads(stdout)
    self.id = response['Vpc']['VpcId']
    self._EnableDnsHostnames()
    util.AddDefaultTags(self.id, self.region)

  def _PostCreate(self):
    self._SetSecurityGroupId()
    for endpoint in self._endpoints:
      endpoint.Create()

  def _SetSecurityGroupId(self):
    """Looks up the VPC default security group."""
    groups = self.GetSecurityGroups('default')
    if len(groups) != 1:
      raise ValueError(
          'Expected one security group, got {} in {}'.format(
              len(groups), groups
          )
      )
    self.default_security_group_id = groups[0]['GroupId']
    logging.info(
        'Default security group ID: %s', self.default_security_group_id
    )
    if FLAGS.aws_efa:
      self._AllowSelfOutBound()

  def GetSecurityGroups(self, group_name=None):
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-security-groups',
        '--region',
        self.region,
        '--filters',
        'Name=vpc-id,Values=' + self.id,
    ]
    if group_name:
      cmd.append('Name=group-name,Values={}'.format(group_name))
    stdout, _, _ = vm_util.IssueCommand(cmd)
    return json.loads(stdout)['SecurityGroups']

  def _Exists(self):
    """Returns true if the VPC exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpcs',
        '--region=%s' % self.region,
        '--filter=Name=vpc-id,Values=%s' % self.id,
    ]
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
        '--vpc-id',
        self.id,
        '--enable-dns-hostnames',
        '{ "Value": true }',
    ]

    util.IssueRetryableCommand(enable_hostnames_command)

  def _PreDelete(self):
    """See base class.

    Deletes the AWS endpoints if created.
    """
    for endpoint in self._endpoints:
      endpoint.Delete()

  def _Delete(self):
    """Deletes the VPC."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpc',
        '--region=%s' % self.region,
        '--vpc-id=%s' % self.id,
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

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
        raise ValueError(
            'Exceeded subnet limit ({}).'.format(self._subnet_index)
        )
      cidr = network.GetCidrBlock(
          self.regional_network_index, self._subnet_index
      )
      self._subnet_index += 1
    return cidr

  @vm_util.Retry()
  def _AllowSelfOutBound(self):
    """Allow outbound connections on all ports in the default security group.

    Details: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html
    """
    cmd = util.AWS_PREFIX + [
        'ec2',
        'authorize-security-group-egress',
        '--region',
        self.region,
        '--group-id',
        self.default_security_group_id,
        '--protocol',
        'all',
        '--source-group',
        self.default_security_group_id,
    ]
    try:
      vm_util.IssueCommand(cmd)
    except errors.VmUtil.IssueCommandError as ex:
      # do not retry if this rule already exists
      if ex.message.find('InvalidPermission.Duplicate') == -1:  # pytype: disable=attribute-error  # enable-nested-classes
        raise ex

  def AllowVpcPeerInBound(self, peer_vpc):
    """Allow inbound connections on all ports in the default security group from peer vpc.

    Args:
      peer_vpc: AwsVpc. Peer vpc to allow inbound traffic from.
    """
    cmd = util.AWS_PREFIX + [
        'ec2',
        'authorize-security-group-ingress',
        '--region=%s' % self.region,
        '--group-id=%s' % self.default_security_group_id,
        '--protocol=%s' % 'all',
        '--cidr=%s' % peer_vpc.cidr,
    ]
    vm_util.IssueRetryableCommand(cmd)


class AwsSubnet(resource.BaseResource):
  """An object representing an Aws subnet."""

  def __init__(self, zone, vpc_id, cidr_block='10.0.0.0/24', subnet_id=None):
    super().__init__(subnet_id is not None)
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
        '--cidr-block=%s' % self.cidr_block,
    ]
    if not util.IsRegion(self.zone):
      create_cmd.append('--availability-zone=%s' % self.zone)

    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['Subnet']['SubnetId']
    util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the subnet."""
    logging.info(
        'Deleting subnet %s. This may fail if all instances in the '
        'subnet have not completed termination, but will be retried.',
        self.id,
    )
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-subnet',
        '--region=%s' % self.region,
        '--subnet-id=%s' % self.id,
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

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
        'ec2',
        'describe-subnets',
        '--region=%s' % self.region,
        '--filter=Name=vpc-id,Values=%s' % self.vpc_id,
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
    super().__init__(vpc_id is not None)
    self.region = region
    self.vpc_id = None
    self.id = None
    self.attached = False
    if vpc_id:
      self.vpc_id = vpc_id
      self.id = self.GetDict().get('InternetGatewayId')
      # if a gateway was found then it is attached to this VPC
      self.attached = bool(self.id)

  def _Create(self):
    """Creates the internet gateway."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-internet-gateway',
        '--region=%s' % self.region,
    ]
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
        '--internet-gateway-id=%s' % self.id,
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

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
      describe_cmd.append(
          '--filter=Name=internet-gateway-id,Values=%s' % self.id
      )
    elif self.vpc_id:
      # Only query with self.vpc_id if the self.id is NOT set -- after calling
      # Detach() this object will set still have a vpc_id but will be filtered
      # out in a query if using attachment.vpc-id.
      # Using self.vpc_id instead of self.attached as the init phase always
      # sets it to False.
      describe_cmd.append(
          '--filter=Name=attachment.vpc-id,Values=%s' % self.vpc_id
      )
    else:
      raise errors.Error('Must have a VPC id or a gateway id')
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    internet_gateways = response['InternetGateways']
    assert len(internet_gateways) < 2, 'Too many internet gateways.'
    return internet_gateways[0] if internet_gateways else {}

  def Attach(self, vpc_id):
    """Attaches the internet gateway to the VPC."""
    if not self.attached:
      self.vpc_id = vpc_id
      attach_cmd = util.AWS_PREFIX + [
          'ec2',
          'attach-internet-gateway',
          '--region=%s' % self.region,
          '--internet-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id,
      ]
      util.IssueRetryableCommand(attach_cmd)
      self.attached = True

  def Detach(self):
    """Detaches the internet gateway from the VPC."""

    def _suppress_failure(stdout, stderr, retcode):
      """Suppresses Detach failure when internet gateway is in a bad state."""
      del stdout  # unused
      if retcode and (
          'InvalidInternetGatewayID.NotFound' in stderr
          or 'Gateway.NotAttached' in stderr
      ):
        return True
      return False

    if self.attached and not self.user_managed:
      detach_cmd = util.AWS_PREFIX + [
          'ec2',
          'detach-internet-gateway',
          '--region=%s' % self.region,
          '--internet-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id,
      ]
      util.IssueRetryableCommand(detach_cmd, suppress_failure=_suppress_failure)
      self.attached = False


class AwsRouteTable(resource.BaseResource):
  """An object representing a route table."""

  def __init__(self, region, vpc_id):
    super().__init__()
    self.region = region
    self.vpc_id = vpc_id
    self.id: str = None  # set by _PostCreate

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
    self.id = self.GetDict()[0]['RouteTableId']

  def GetDict(self):
    """Returns an array of the currently existing routes for this VPC."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-route-tables',
        '--region=%s' % self.region,
        '--filters=Name=vpc-id,Values=%s' % self.vpc_id,
    ]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    return json.loads(stdout)['RouteTables']

  def RouteExists(self):
    """Returns true if the 0.0.0.0/0 route already exists."""
    route_tables = self.GetDict()
    if not route_tables:
      return False
    for route in route_tables[0].get('Routes', []):
      if route.get('DestinationCidrBlock') == '0.0.0.0/0':
        return True
    return False

  def CreateRoute(self, internet_gateway_id):
    """Adds a route to the internet gateway."""
    if self.RouteExists():
      logging.info('Internet route already exists.')
      return
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-route',
        '--region=%s' % self.region,
        '--route-table-id=%s' % self.id,
        '--gateway-id=%s' % internet_gateway_id,
        '--destination-cidr-block=0.0.0.0/0',
    ]
    util.IssueRetryableCommand(create_cmd)

  def CreateVpcPeeringRoute(self, vpc_peering_id, destination_cidr):
    """Adds a route to peer VPC."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-route',
        '--region=%s' % self.region,
        '--route-table-id=%s' % self.id,
        '--vpc-peering-connection-id=%s' % vpc_peering_id,
        '--destination-cidr-block=%s' % destination_cidr,
    ]
    util.IssueRetryableCommand(create_cmd)


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

  _regional_network_count = 0
  _regional_network_lock = threading.Lock()

  CLOUD = provider_info.AWS

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)

  def __init__(self, region, vpc_id=None):
    self.region = region
    self.internet_gateway = AwsInternetGateway(region, vpc_id)
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

    # Each regional network needs unique cidr_block for VPC peering.
    with _AwsRegionalNetwork._regional_network_lock:
      self.vpc = AwsVpc(
          self.region, vpc_id, _AwsRegionalNetwork._regional_network_count
      )
      self.cidr_block = network.GetCidrBlock(
          _AwsRegionalNetwork._regional_network_count
      )
      _AwsRegionalNetwork._regional_network_count += 1

  @classmethod
  def GetForRegion(cls, region, vpc_id=None):
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
      raise errors.Error(
          'GetNetwork called in a thread without a BenchmarkSpec.'
      )
    key = cls.CLOUD, REGION, region
    # Because this method is only called from the AwsNetwork constructor, which
    # is only called from AwsNetwork.GetNetwork, we already hold the
    # benchmark_spec.networks_lock.
    if key not in benchmark_spec.regional_networks:
      benchmark_spec.regional_networks[key] = cls(region, vpc_id)
    return benchmark_spec.regional_networks[key]

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

    if self.created:
      self.internet_gateway.Detach()
      self.internet_gateway.Delete()
      self.vpc.Delete()


class AwsNetworkSpec(network.BaseNetworkSpec):
  """Configuration for creating an AWS network."""

  def __init__(self, zone, vpc_id=None, subnet_id=None, machine_type=None):
    super().__init__(zone)
    self.machine_type = machine_type
    if vpc_id or subnet_id:
      logging.info(
          'Confirming vpc (%s) and subnet (%s) selections', vpc_id, subnet_id
      )
      my_subnet = AwsSubnet(self.zone, vpc_id, subnet_id=subnet_id).GetDict()
      self.vpc_id = my_subnet['VpcId']
      self.subnet_id = my_subnet['SubnetId']
      self.cidr_block = my_subnet['CidrBlock']
      logging.info(
          'Using vpc %s subnet %s cidr %s',
          self.vpc_id,
          self.subnet_id,
          self.cidr_block,
      )
    else:
      self.vpc_id = None
      self.subnet_id = None
      self.cidr_block = None


def _get_default_vpc_id(region: str) -> str:
  """Returns the default VPC ID for the region.

  Creates a default VPC if one did not exist previously


  Args:
    region: Region of the default VPC.

  Returns: Default VPC ID

  Raises:
    UnsupportedConfigError: When default VPC does not exist and cannot be
      created.
  """
  vpc_cmd = util.AWS_PREFIX + [
      'ec2',
      'describe-vpcs',
      '--region',
      region,
      '--filters',
      'Name=isDefault,Values=true',
  ]
  stdout, _ = vm_util.IssueRetryableCommand(vpc_cmd)
  vpcs = json.loads(stdout)['Vpcs']
  if vpcs:
    return vpcs[0]['VpcId']
  create_cmd = util.AWS_PREFIX + [
      'ec2',
      'create-default-vpc',
      '--region',
      region,
  ]
  stdout, _, ret = vm_util.IssueCommand(create_cmd, raise_on_failure=False)
  if ret:
    raise errors.Benchmarks.UnsupportedConfigError(
        f'AWS default VPC does not exist for region {region}.'
    )
  return json.loads(stdout)['Vpc']['VpcId']


def _get_default_subnet_id(zone: str) -> str:
  """Returns the default subnet ID for the zone.

  Creates a default subnet if one did not exist previously


  Args:
    zone: Zone of the default subnet.

  Returns: Default Subnet ID

  Raises:
    UnsupportedConfigError: When default subnet does not exist and cannot be
      created.
  """
  region = util.GetRegionFromZone(zone)
  subnet_cmd = util.AWS_PREFIX + [
      'ec2',
      'describe-subnets',
      '--region',
      region,
      '--filter',
      f'Name=availabilityZone,Values={zone}',
      'Name=defaultForAz,Values=true',
  ]
  stdout, _ = vm_util.IssueRetryableCommand(subnet_cmd)
  subnets = json.loads(stdout)['Subnets']
  if subnets:
    return subnets[0]['SubnetId']
  create_cmd = util.AWS_PREFIX + [
      'ec2',
      'create-default-subnet',
      '--region',
      region,
      '--availability-zone',
      zone,
  ]
  stdout, _, ret = vm_util.IssueCommand(create_cmd, raise_on_failure=False)
  if ret:
    raise errors.Benchmarks.UnsupportedConfigError(
        f'AWS default subnet does not exist for zone {zone}.'
    )
  return json.loads(stdout)['Subnet']['SubnetId']


def _is_placement_group_compatible(machine_type):
  """Returns True if VMs of 'machine_type' can be put in a placement group."""
  prefix = machine_type.split('.')[0]
  return prefix not in NON_PLACEMENT_GROUP_PREFIXES


class AwsNetwork(network.BaseNetwork):
  """Object representing an AWS Network.

  Attributes:
    region: The AWS region the Network is in.
    regional_network: The AwsRegionalNetwork for 'region'.
    subnet: the AwsSubnet for this zone.
    placement_group: An AwsPlacementGroup instance.
  """

  CLOUD = provider_info.AWS

  def __repr__(self):
    return '%s(%r)' % (self.__class__, self.__dict__)

  def __init__(self, spec):
    """Initializes AwsNetwork instances.

    Args:
      spec: An AwsNetworkSpec object.
    """
    super().__init__(spec)
    self.region = util.GetRegionFromZone(spec.zone)
    self.regional_network = _AwsRegionalNetwork.GetForRegion(
        self.region, spec.vpc_id
    )
    self.subnet = None
    self.vpc_peering = None

    # Placement Group
    no_placement_group = (
        not FLAGS.placement_group_style
        or FLAGS.placement_group_style == placement_group.PLACEMENT_GROUP_NONE
    )
    has_optional_pg = (
        FLAGS.placement_group_style
        == placement_group.PLACEMENT_GROUP_CLOSEST_SUPPORTED
    )
    if no_placement_group:
      self.placement_group = None
    elif has_optional_pg and not _is_placement_group_compatible(
        spec.machine_type
    ):
      logging.warning(
          'machine type %s does not support placement groups. '
          'Placement group style set to none.',
          spec.machine_type,
      )
      self.placement_group = None
    elif has_optional_pg and len(set(FLAGS.zone)) > 1:
      logging.warning(
          'inter-zone/inter-region tests do not support placement groups. '
          'Placement group style set to none.'
      )
      self.placement_group = None
    elif not _is_placement_group_compatible(spec.machine_type):
      raise errors.Benchmarks.UnsupportedConfigError(
          f'machine type {spec.machine_type} does not support '
          'placement groups. Use placement group style none.'
      )
    elif len(set(FLAGS.zone)) > 1:
      raise errors.Benchmarks.UnsupportedConfigError(
          'inter-zone/inter-region tests do not support placement groups. '
          'Use placement group style closest_supported.'
      )
    else:
      placement_group_spec = aws_placement_group.AwsPlacementGroupSpec(
          'AwsPlacementGroupSpec', flag_values=FLAGS, zone=spec.zone
      )
      self.placement_group = aws_placement_group.AwsPlacementGroup(
          placement_group_spec
      )
    self.is_static = False
    if spec.vpc_id:
      self.is_static = True
      self.subnet = AwsSubnet(
          self.zone,
          spec.vpc_id,
          cidr_block=self.regional_network.cidr_block,
          subnet_id=spec.subnet_id,
      )

  @staticmethod
  def _GetNetworkSpecFromVm(vm):
    """Returns an AwsNetworkSpec created from VM attributes and flags."""
    if _AWS_SUBNET.value == 'default':
      vpc_id = _get_default_vpc_id(vm.region)
      subnet_id = _get_default_subnet_id(vm.zone)
    else:
      vpc_id = _AWS_VPC.value
      subnet_id = _AWS_SUBNET.value
    return AwsNetworkSpec(vm.zone, vpc_id, subnet_id, vm.machine_type)

  def Create(self):
    """Creates the network."""
    self.regional_network.Create()

    if self.subnet is None:
      cidr = self.regional_network.vpc.NextSubnetCidrBlock()
      self.subnet = AwsSubnet(
          self.zone, self.regional_network.vpc.id, cidr_block=cidr
      )
      self.subnet.Create()
    if self.placement_group:
      self.placement_group.Create()

  def Delete(self):
    """Deletes the network."""
    if self.subnet:
      self.subnet.Delete()
    if self.placement_group:
      self.placement_group.Delete()
    if hasattr(self, 'vpc_peering') and self.vpc_peering:
      self.vpc_peering.Delete()
    self.regional_network.Delete()

  def Peer(self, peering_network):
    """Peers the network with the peering_network.

    This method is used for VPC peering. It will connect 2 VPCs together.

    Args:
      peering_network: BaseNetwork. The network to peer with.
    """

    # Skip Peering if the networks are the same
    if self.regional_network is peering_network.regional_network:
      return

    spec = network.BaseVPCPeeringSpec(
        self.regional_network, peering_network.regional_network
    )
    self.vpc_peering = AwsVpcPeering(spec)
    peering_network.vpc_peering = self.vpc_peering
    self.vpc_peering.Create()

  @classmethod
  def _GetKeyFromNetworkSpec(cls, spec):
    """Returns a key used to register Network instances."""
    return (cls.CLOUD, ZONE, spec.zone)


class AwsVpcPeering(network.BaseVPCPeering):
  """Object containing all information needed to create a VPC Peering Object."""

  def _Create(self):
    """Creates the peering object.

    Documentation on creating a vpc object:
    https://docs.aws.amazon.com/vpc/latest/peering/vpc-pg.pdf
    """
    # Creates Peering Connection
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpc-peering-connection',
        '--region=%s' % self.network_a.region,
        '--peer-region=%s' % self.network_b.region,
        '--vpc-id=%s' % self.network_a.vpc.id,
        '--peer-vpc-id=%s' % self.network_b.vpc.id,
    ]

    stdout, _ = vm_util.IssueRetryableCommand(create_cmd)
    response = json.loads(stdout)

    self.id = response['VpcPeeringConnection']['VpcPeeringConnectionId']

    # Accepts Peering Connection
    accept_cmd = util.AWS_PREFIX + [
        'ec2',
        'accept-vpc-peering-connection',
        '--region=%s' % self.network_b.region,
        '--vpc-peering-connection-id=%s' % self.id,
    ]
    vm_util.IssueRetryableCommand(accept_cmd)

    util.AddDefaultTags(self.id, self.network_a.region)
    logging.info(
        'Creating VPC peering between %s and %s',
        self.network_a.vpc.cidr,
        self.network_b.vpc.cidr,
    )

    # Adds VPC peering to both networks' route tables
    self.network_a.route_table.CreateVpcPeeringRoute(
        self.id, self.network_b.vpc.cidr
    )
    self.network_b.route_table.CreateVpcPeeringRoute(
        self.id, self.network_a.vpc.cidr
    )

    # Updates security group to allow inbound traffic from peering networks
    self.network_a.vpc.AllowVpcPeerInBound(self.network_b.vpc)
    self.network_b.vpc.AllowVpcPeerInBound(self.network_a.vpc)

  def _Delete(self):
    """Creates the deletes the peering object."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpc-peering-connection',
        '--region=%s' % self.network_a.region,
        '--vpc-peering-connection-id=%s' % self.id,
    ]
    vm_util.IssueCommand(delete_cmd)
