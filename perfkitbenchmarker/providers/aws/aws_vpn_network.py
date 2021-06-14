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

"""Module containing classes related to AWS VPN networking.

Components needed for the AWS side of a site-to-site VPN can be managed by the
classes in this module. Customer and VPN Gateway are AWS representations of the
physical VPN appliances on each side.
See https://docs.aws.amazon.com/vpn/latest/s2svpn/VPC_VPN.html
for more information about AWS Virtual Private Networks.
"""

import json
import logging

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import aws
from perfkitbenchmarker.providers.aws import util, aws_network

FLAGS = flags.FLAGS


class AwsVpnGatewayResource(resource.BaseResource):
  """ClaSS representing an AWS VPNGatewayResource."""

  def __init__(self, name, network_name, az, cidr):
    """initializes an AwsVPNGatewayResource

      See https://docs.aws.amazon.com/vpn/latest/s2svpn/VPC_VPN.html
      """
    super(AwsVpnGatewayResource, self).__init__()
    self.name = name
    self.network_name = network_name
    self.az = az
    self.region = util.GetRegionFromZone(self.az)
    self.cidr = cidr
    self.attached = False
    self.id = None
    self.vpc_id = None
    self.ip_address = None
    self.vpn_connection = None
    self.psk = None
    self.ike_version = 'ipsec.1'
    self.routing = None

  def _Create(self):
    """Creates the VPN gateway.

      See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_CreateVpnGateway.html
      """
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-gateway',
        '--region=%s' % self.region,
        '--availability-zone=%s' % self.az,
        '--type=%s' % self.ike_version
    ]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['VpnGateway']['VpnGatewayId']
    util.AddDefaultTags(self.id, self.region)


  def _Exists(self):
    """Returns true if the vpn gateway exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpn-gateways',
        '--region=%s' % self.region,
        '--filter=Name=vpn-gateway-id,Values=%s' % self.id
    ]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    vpn_gateways = response['VpnGateways']
    state = vpn_gateways[0]['State']
    assert len(vpn_gateways) < 2, 'Too many VPN gateways.'
    return len(vpn_gateways) > 0 and state != 'deleted'

  def _Delete(self):
    """Deletes the vpn gateway.

    See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DeleteVpnGateway.html
    """
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpn-gateway',
        '--region=%s' % self.region,
        '--vpn-gateway-id=%s' % self.id
    ]
    vm_util.IssueCommand(delete_cmd)

  def Attach(self, vpc_id):
    """Attaches the vpn gateway to the VPC.

    See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_AttachVpnGateway.html
    """
    if not self.attached:
      self.vpc_id = vpc_id
      attach_cmd = util.AWS_PREFIX + [
          'ec2',
          'attach-vpn-gateway',
          '--region=%s' % self.region,
          '--vpn-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id
      ]
      util.IssueRetryableCommand(attach_cmd)
      self.attached = True

  def Detach(self):
    """Detaches the vpn gateway from the VPC.

    See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DetachVpnGateway.html
    """
    if self.attached:
      detach_cmd = util.AWS_PREFIX + [
          'ec2',
          'detach-vpn-gateway',
          '--region=%s' % self.region,
          '--vpn-gateway-id=%s' % self.id,
          '--vpc-id=%s' % self.vpc_id
      ]
      util.IssueRetryableCommand(detach_cmd)
      self.attached = False

  def AllowRoutePropogation(self, route_table_id):
    """Propagate routes to the specified route table of a VPC.

    See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_EnableVgwRoutePropagation.html

    Args:
      route_table_id: The ID of the route table.
    """
    cmd = util.AWS_PREFIX + [
        'ec2',
        'enable-vgw-route-propagation',
        '--region=%s' % self.region,
        '--gateway-id=%s' % self.id,
        '--route-table-id=%s' % route_table_id
    ]
    util.IssueRetryableCommand(cmd)

  def GetDefaultSecurityGroup(self):
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-security-groups',
        '--region=%s' % self.region
    ]
    response = vm_util.IssueCommand(cmd)
    response = json.loads(response)
    for sg in response['SecurityGroups']:
      if sg['GroupName'] == 'default':
        return sg['GroupId']


class AwsVpnGateway(network.BaseVpnGateway):
  """Class representing an AWS VPN Gateway."""
  CLOUD = aws.CLOUD

  def __init__(self, name, network_name, az, cidr):
    """construct a new AwsVpnGateway

    A VpnGateway is one end of a VpnConnection. VpnService attempts to
    setup VpnConnections for provider gateway pairs by relaying configuration
    information between them via shared dictionary [vpn_service.TunnelConfig].
    VpnGateway implementations expose a ConfigureTunnel() entrypoint into the
    provider specific connection logic. See BaseVpnGateway.ConfigureTunnel()

    AWS object instances required for Site-to-Site VPN connections include:
    Customer and VPC endpoints, routes, forwarding rules, tunnels, etc.
    See https://docs.aws.amazon.com/vpn/latest/s2svpn/SetUpVPNConnections.html
    """
    super(AwsVpnGateway, self).__init__()

    self.forwarding_rules = {}
    self.tunnels = {}
    self.routes = {}
    self.vpn_gateway_resource = AwsVpnGatewayResource(
        name, network_name, az, cidr)

    self.name = name
    self.network_name = network_name
    self.az = az
    self.region = util.GetRegionFromZone(self.az)
    self.cidr = cidr
    self.ip_address = None

    self.created = False  # managed by BaseResource
    self.attached = False
    self.psk = None
    self.routing = None  # @TODO static/dynamic
    self.require_target_to_init = True
    self.customer_gw = None
    self.cgw_id = None
    self.vpn_connection = None
    self.route_table_id = None

  def ConfigureTunnel(self, tunnel_config):
    """Entrypoint to AWS specific VPN setup process

    tunnel_config is a shared dictionary updated by both endpoints as VPN
    construction progresses. If there's any new info from the other gateway
    available we use it to build up our end of the tunnel. When no further
     progress is possible (or needed) we write our updates back to tunnel_config
    and hand it back to the caller.

    High level AwsVpnConnection setup process:

    Prerequisites
    Step 1: Create a customer gateway
    Step 2: Create a target gateway
    Step 3: Configure routing
    Step 4: Update your security group
    Step 5: Create a Site-to-Site VPN connection
    Step 6: Download the configuration file
    Step 7: Configure the customer gateway device
    See https://docs.aws.amazon.com/vpn/latest/s2svpn/SetUpVPNConnections.html

    Args:
      tunnel_config:

    Returns:
        tunnel_config:
    """
    logging.info('Configuring Tunnel with params:')
    logging.info(tunnel_config)

    # update tunnel_config if needed
    if self.name not in tunnel_config.endpoints:
      logging.info(
          'tunnel_config: This endpoint isnt registered yet... %s' % self.name
      )
      tunnel_config.endpoints[self.name] = {}
      tunnel_config.endpoints[self.name]['is_configured'] = False
      tunnel_config.endpoints[self.name]['cidr'] = self.cidr
      tunnel_config.endpoints[self.name]['network_name'] = self.network_name
      tunnel_config.endpoints[self.name]['region'] = self.region
      tunnel_config.endpoints[self.name]['az'] = self.az
      tunnel_config.endpoints[
          self.name]['require_target_to_init'] = self.require_target_to_init

    # Abort if we don't have a target info configured yet
    if len(tunnel_config.endpoints) < 2:
      logging.info(
          'tunnel_config: Only found %d endpoints... waiting for target to '
          'configure' % len(tunnel_config.endpoints)
      )
      return

    target_endpoint = [
        k for k in tunnel_config.endpoints.keys() if k not in self.name
    ][0]

    # setup customer gw
    # requires: target_ip
    if 'ip_address' not in tunnel_config.endpoints[target_endpoint]:
      logging.info(
          'tunnel_config: require target ip to configure... punting for now'
      )
      return
    if self.customer_gw is None and tunnel_config.endpoints[target_endpoint][
        'ip_address'] is not None:
      logging.info(
          'tunnel_config: Creating cuustomer gw for IP %s' %
          tunnel_config.endpoints[target_endpoint]['ip_address']
      )
      self.customer_gw = AwsCustomerGateway(
          self.region,
          tunnel_config.endpoints[target_endpoint]['ip_address']
      )
      self.customer_gw.Create()
      self.cgw_id = self.customer_gw.id
      logging.info(
          "tunnel_config: Created customer gw with id: %s" % self.customer_gw.id
      )

    net = aws_network._AwsRegionalNetwork.GetForRegion(self.region, cidr_block=self.cidr)
    if not self.attached:
      logging.info(
          "tunnel_config: Attaching VGW %s to VPC %s" %
          (self.vpn_gateway_resource.id,
           net.vpc.id)
      )
      self.vpn_gateway_resource.Attach(net.vpc.id)
      self.attached = True
      self.vpn_gateway_resource.AllowRoutePropogation(net.route_table.id)

    # Create VPN connection
    # requires: cgw_id,
    if not self.vpn_connection:
      logging.info(
          "tunnel_config: Creating AWS VPN Connection with PSK %s" % self.psk
      )
      self.vpn_connection = AwsVpnConnection(
          self.cgw_id,
          self.vpn_gateway_resource.id,
          self.region,
          tunnel_config.shared_key,
          routing= tunnel_config.routing,
          type="ipsec.1"
      )
      self.vpn_connection.Create()
      self.ip_address = self.vpn_connection.ip
      tunnel_config.endpoints[self.name]['ip_address'] = self.ip_address
      tunnel_config.endpoints[self.name]['tunnel_id'] = self.vpn_connection.id

    if not tunnel_config.endpoints[target_endpoint]['cidr']:
      logging.info(
          "tunnel_config: Target CIDR needed for route creation... returning"
      )
      return

    if not tunnel_config.endpoints[self.name]['is_configured']:
      aws_network.AwsFirewall.GetFirewall().AllowIcmp(None,
        region=self.region,
        security_group=net.vpc.default_security_group_id,
        cidr=tunnel_config.endpoints[target_endpoint]['cidr']
      )
      self.vpn_connection.Create_VPN_Connection_Route(
          tunnel_config.endpoints[target_endpoint]['cidr']
      )

    tunnel_config.endpoints[self.name]['is_configured'] = True

  def IsTunnelReady(self, tunnel_id):
    return True

  def Create(self):
    """Creates the actual VPNGateway."""
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error(
          'GetNetwork called in a thread without a '
          'BenchmarkSpec.'
      )
    if self.created:
      return
    if self.vpn_gateway_resource:
      self.vpn_gateway_resource.Create()
    key = self.name
    if key not in benchmark_spec.vpn_gateways:
      benchmark_spec.vpn_gateways[key] = self
    return benchmark_spec.vpn_gateways[key]
    self.created = True

  def Delete(self):
    """Deletes the actual VPNGateway."""
    if self.vpn_connection and self.vpn_connection._Exists():
      self.vpn_connection.Delete()

    if self.customer_gw and self.customer_gw._Exists():
      self.customer_gw.Delete()

    # vpn_gateways need deleted last
    if self.vpn_gateway_resource:
      self.vpn_gateway_resource.Detach()
      self.vpn_gateway_resource.Delete()


class AwsVpnConnection(resource.BaseResource):
  """An object representing a Aws VPNConnection."""

  def __init__(
      self,
      cgw_id,
      vpg_id,
      region,
      psk,
      routing="static",
      type="ipsec.1"
  ):
    super(AwsVpnConnection, self).__init__()
    self.cgw_id = cgw_id
    self.vpg_id = vpg_id
    self.region = region
    self.type = type
    self.id = None
    self.routing = routing
    self.psk = psk
    self.connection_details = None
    self.ip = None  # do we need to support failover (2nd IP?)
    self.customer_config_xml = None

  def _Create(self):
    """creates VPN connection and 2 vpn tunnels to CGW."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-connection',
        '--region=%s' % self.region,
        '--customer-gateway-id=%s' % self.cgw_id,
        '--vpn-gateway-id=%s' % self.vpg_id,
        '--type=%s' % self.type,
        '--options=%s' % self._getConnectionOpts()
    ]
    response, _ = util.IssueRetryableCommand(create_cmd)
    response_json = json.loads(response)
    self.id = response_json['VpnConnection']['VpnConnectionId']
    self.ip = response_json['VpnConnection']['Options']['TunnelOptions'][0]['OutsideIpAddress']
    self.customer_config_xml = response_json['VpnConnection']['CustomerGatewayConfiguration']

  def _Delete(self):
    "Deletes the VPN Connection"
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-vpn-connection',
        '--region=%s' % self.region,
        '--vpn-connection-id=%s' % self.id
    ]
    response, _ = util.IssueRetryableCommand(delete_cmd)

  def _Exists(self):
    """Returns True if the VPNConnection exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpn-connections',
        '--region=%s' % self.region,
        '--filter=Name=vpn-connection-id,Values=%s' % self.id
    ]
    response, _ = util.IssueRetryableCommand(cmd)
    response_json = json.loads(response)
    return (
        len(response_json["VpnConnections"]) > 0 and
        response_json["VpnConnections"][0]["State"] in ["available","pending"]
    )

  def IsReady(self):
    """Returns True if the VPNConnection exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-vpn-connections',
        '--region=%s' % self.region,
        '--filter=Name=vpn-connection-id,Values=%s' % self.id
    ]
    response, _ = util.IssueRetryableCommand(cmd)
    response_json = json.loads(response)
    return (
        len(response_json["VpnConnections"]) > 0 and
        response_json["VpnConnections"][0]["State"] in ["available"]
    )

  def Create_VPN_Connection_Route(self, target_cidr):
    """sets up routes to target."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-connection-route',
        '--region=%s' % self.region,
        '--destination-cidr-block=%s' % target_cidr,
        '--vpn-connection-id=%s' % self.id
    ]
    response, _ = util.IssueRetryableCommand(create_cmd)

  def _getConnectionOpts(self):
    """See https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_VpnConnectionOptions.html"""
    opts = {}
    opts["StaticRoutesOnly"] = self.routing == "static"

    opts["TunnelOptions"] = [
        {
            'PreSharedKey': self.psk
        },
        {
            'PreSharedKey': self.psk
        },
    ]
    return json.dumps(opts)

class AwsCustomerGateway(resource.BaseResource):
  """An object representing an AwsCustomerGateway."""

  def __init__(self, region, target_ip, *args, **kwargs):
    super(AwsCustomerGateway, self).__init__()
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
        '--type=%s' % 'ipsec.1'
    ]
    response, _, _ = vm_util.IssueCommand(create_cmd)
    response_json = json.loads(response)
    self.id = response_json['CustomerGateway']['CustomerGatewayId']

  def _Delete(self):
    """Deletes the AwsCustomerGateway."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-customer-gateway',
        '--region=%s' % self.region,
        '--customer-gateway-id=%s' % self.id
    ]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)

  def _Exists(self):
    """Returns True if the AwsCustomerGateway exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-customer-gateways',
        '--region=%s' % self.region,
        '--customer-gateway-ids=%s' % self.id
    ]
    response, _ = util.IssueRetryableCommand(cmd)
    response = json.loads(response)
    return (
        len(response["CustomerGateways"]) > 0 and
        response["CustomerGateways"][0]["State"] in ["available",
                                                     "pending"]
    )

  def _ExistsforIP(self):
    """Returns True if the AwsCustomerGateway exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-customer-gateways',
        '--region=%s' % self.region,
        '--filter=Name=ip-address,Values=%s' % self.target_ip
    ]
    response, _ = util.IssueRetryableCommand(cmd)
    response = json.loads(response)
    return (
        len(response["CustomerGateways"]) > 0 and
        response["VpnConnections"][0]["State"] in ["available",
                                                   "pending"]
    )
