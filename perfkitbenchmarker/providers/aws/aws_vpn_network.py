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
import re
import threading
import uuid
import xmltodict

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import aws
from perfkitbenchmarker.providers.aws import util, aws_network

FLAGS = flags.FLAGS


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
            A private Autonomous System Number (ASN) for the Amazon side of a BGP session.
            If you're using a 16-bit ASN, it must be in the 64512 to 65534 range.
            If you're using a 32-bit ASN, it must be in the 4200000000 to 4294967294 range.
            Default: 64512
            '''
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-gateway',
        '--region=%s' % self.region,
        '--availability-zone=%s' % self.az,
        '--type=%s' % 'ipsec.1'
    ]
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
        '--filter=Name=vpn-gateway-id,Values=%s' % self.id
    ]
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
        '--vpn-gateway-id=%s' % self.id
    ]
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
          '--vpc-id=%s' % self.vpc_id
      ]
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
          '--vpc-id=%s' % self.vpc_id
      ]
      util.IssueRetryableCommand(detach_cmd)
      self.attached = False

  def AllowRoutePropogation(self, route_table_id):
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


class AwsVPNGW(network.BaseVpnGateway):
  CLOUD = aws.CLOUD

  def __init__(self, name, network_name, az, cidr):
    super(AwsVPNGW, self).__init__()
    #     self._lock = threading.Lock()

    self.forwarding_rules = {}
    self.tunnels = {}
    self.routes = {}
    self.vpn_gateway_resource = AwsVPNGWResource(name, network_name, az, cidr)

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
    # network.BaseVpnGateway.ConfigureTunnel(self, tunnel_config)
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
      self.customer_gw = AwsCustomerGW(
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
      self.vpn_connection = AwsVPNConnection(
          self.cgw_id,
          self.vpn_gateway_resource.id,
          self.region,
          psk=self.psk
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
      aws_network.AwsFirewall.GetFirewall().AllowIcmpVpn(
          self.region,
          net.vpc.default_security_group_id,
          tunnel_config.endpoints[target_endpoint]['cidr']
      )
      self.vpn_connection.Create_VPN_Cnxn_Route(
          tunnel_config.endpoints[target_endpoint]['cidr']
      )

    tunnel_config.endpoints[self.name]['is_configured'] = True

  def IsTunnelReady(self, tunnel_id):
    return True

  def Create(self):
    """Creates the actual VPNGW."""
    benchmark_spec = context.GetThreadBenchmarkSpec()
    if benchmark_spec is None:
      raise errors.Error(
          'GetNetwork called in a thread without a '
          'BenchmarkSpec.'
      )
    # with self._lock:
    if self.created:
      return
    if self.vpn_gateway_resource:
      self.vpn_gateway_resource.Create()
    key = self.name
    # with benchmark_spec.vpn_gateways_lock:
    if key not in benchmark_spec.vpn_gateways:
      benchmark_spec.vpn_gateways[key] = self
    return benchmark_spec.vpn_gateways[key]
    self.created = True

  def Delete(self):
    """Deletes the actual VPNGW."""
    if self.vpn_connection and self.vpn_connection._Exists():
      self.vpn_connection.Delete()

    if self.customer_gw and self.customer_gw._Exists():
      self.customer_gw.Delete()

    # vpn_gateways need deleted last
    if self.vpn_gateway_resource:
      self.vpn_gateway_resource.Detach()
      self.vpn_gateway_resource.Delete()


class AwsVPNConnection(resource.BaseResource):
  """An object representing a Aws VPNConnection."""

  def __init__(
      self,
      cgw_id,
      vpg_id,
      region,
      psk=None,
      routing="static",
      type="ipsec.1"
  ):
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
        # '--debug',
        '--region=%s' % self.region,
        '--customer-gateway-id=%s' % self.cgw_id,
        '--vpn-gateway-id=%s' % self.vpg_id,
        '--type=%s' % self.type,
        '--options=%s' % self._getCnxnOpts()
    ]
    response, _ = util.IssueRetryableCommand(create_cmd)
    response_xml = response[response.find("<"):response.rfind(">") +
                            1].replace("\\n",
                                       "").replace("\\",
                                                   "")
    self.cnxn_details = xmltodict.parse(response_xml)
    response_json = re.sub(r'<.*>', '', response)
    response_json = json.loads(response_json)
    self.id = response_json["VpnConnection"]["VpnConnectionId"]
    #     self.ip = response_json["VpnConnection"]["VgwTelemetry"][0][
    #     "OutsideIpAddress"] # @TODO find "VgwTelemetry" from aws docs
    response_xml_dict = xmltodict.parse(response_xml)
    self.ip = response_xml_dict["vpn_connection"]["ipsec_tunnel"][0][
        "vpn_gateway"]["tunnel_outside_address"]["ip_address"]

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
    response_json = re.sub(r'<.*>', '', response)
    response_json = json.loads(response_json)
    return (
        len(response_json["VpnConnections"]) > 0 and
        response_json["VpnConnections"][0]["State"] in ["available",
                                                        "pending"]
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
    response_json = re.sub(r'<.*>', '', response)
    response_json = json.loads(response_json)
    return (
        len(response_json["VpnConnections"]) > 0 and
        response_json["VpnConnections"][0]["State"] in ["available"]
    )

  def Create_VPN_Cnxn_Route(self, target_cidr):
    """sets up routes to target."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-vpn-connection-route',
        '--region=%s' % self.region,
        '--destination-cidr-block=%s' % target_cidr,
        '--vpn-connection-id=%s' % self.id
    ]
    response, _ = util.IssueRetryableCommand(create_cmd)

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

    key = 'key' + FLAGS.run_uri  # psk restrictions on some runuris
    opts["TunnelOptions"] = [
        {
            'PreSharedKey': key
        },
        {
            'PreSharedKey': key
        },
    ]
    logging.info("Get VPN Cnxn Options<tun_opts>:  %s" % str(opts))
    # https://docs.aws.amazon.com/cli/latest/userguide/cli-using-param.html
    logging.info(
        "Get VPN Cnxn Options:  %s" %
        str(json.dumps(json.dumps(opts))[1:-1].replace('\\',
                                                       ''))
    )
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
        '--type=%s' % 'ipsec.1'
    ]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.id = response['CustomerGateway']['CustomerGatewayId']

  def _Delete(self):
    """Deletes the AwsCustomerGW."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-customer-gateway',
        '--region=%s' % self.region,
        '--customer-gateway-id=%s' % self.id
    ]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)

  def _Exists(self):
    """Returns True if the AwsCustomerGW exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-customer-gateways',
        '--region=%s' % self.region,
        '--customer-gateway-ids=%s' % self.id
    ]
    response, _ = util.IssueRetryableCommand(cmd)
    #     logging.log(logging.INFO, response)
    response = json.loads(response)
    return (
        len(response["CustomerGateways"]) > 0 and
        response["CustomerGateways"][0]["State"] in ["available",
                                                     "pending"]
    )

  def _ExistsforIP(self):
    """Returns True if the AwsCustomerGW exists."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-customer-gateways',
        '--region=%s' % self.region,
        '--filter=Name=ip-address,Values=%s' % self.target_ip
    ]
    response, _ = util.IssueRetryableCommand(cmd)
    #     logging.log(logging.INFO, response)
    response = json.loads(response)
    return (
        len(response["CustomerGateways"]) > 0 and
        response["VpnConnections"][0]["State"] in ["available",
                                                   "pending"]
    )
