# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_network."""

import json
import unittest
from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

ROUTE_ID = 'rtb-1234'


def RouteTable(cidr_block):
  return {
      'RouteTables': [{
          'RouteTableId': ROUTE_ID,
          'Routes': [{
              'DestinationCidrBlock': cidr_block
          }]
      }]
  }


REGION = 'us-west-1'
VPC_ID = 'vpc-1234'
SG_DEFAULT = 'sg-1234'
QUERY_ROUTE_NONE = ('describe-route-tables', {'RouteTables': []})
QUERY_ROUTE_NO_DEFAULT = ('describe-route-tables', RouteTable('192.168.0.0/16'))
QUERY_ROUTE_HAS_DEFAULT = ('describe-route-tables', RouteTable('0.0.0.0/0'))
CREATE_ROUTE = ('create-route', {})
GATEWAY_ID = 'igw-1234'
DESCRIBE_SECURITY_GROUPS_ONLY_DEFAULT = ('describe-security-groups', {
    'SecurityGroups': [{
        'GroupId': SG_DEFAULT
    }]
})
VPC_QUERY_NONE = ('describe-vpcs', {'Vpcs': []})
VPC_QUERY_ONE = ('describe-vpcs', {'Vpcs': [{'VpcId': VPC_ID}]})
VPC_CREATE = ('create-vpc', {'Vpc': {'VpcId': VPC_ID}})
MODIFY_VPC = ('modify-vpc-attribute', {})
CREATE_GATEWAY = ('create-internet-gateway', {
    'InternetGateway': {
        'InternetGatewayId': GATEWAY_ID
    }
})
GATEWAY_QUERY_NONE = ('describe-internet-gateways', {'InternetGateways': []})
GATEWAY_QUERY_ONE = ('describe-internet-gateways', {
    'InternetGateways': [{
        'InternetGatewayId': GATEWAY_ID
    }]
})
ATTACH_GATEWAY = ('attach-internet-gateway', {})
DETACH_GATEWAY = ('detach-internet-gateway', {})

DESCRIBE_FW_NONE = ('describe-security-groups', {'SecurityGroups': []})
DESCRIBE_FW_ONE = ('describe-security-groups', {'SecurityGroups': [1]})
CREATE_FW = ('authorize-security-group-ingress', {})

FLAGS = flags.FLAGS
_COMPONENT = 'test_component'


def AwsResponse(json_obj):
  return (json.dumps(json_obj), '', 0)


def FindAwsCommand(cmd):
  for word in cmd:
    if word not in ('aws', 'ec2', '--region', '--output', 'json'):
      return word


def AwsFilter(name, value):
  return 'Name={},Values={}'.format(name, value)


class BaseAwsTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(BaseAwsTest, self).setUp()
    self.mock_aws = mock.patch.object(vm_util, 'IssueCommand').start()
    self.mock_tags = mock.patch.object(util, 'AddDefaultTags').start()

  def tearDown(self):
    super(BaseAwsTest, self).tearDown()
    mock.patch.stopall()

  def SetExpectedCommands(self, *commands):
    self.mock_aws.reset_mock()
    self.mock_aws.side_effect = [AwsResponse(res) for _, res in commands]
    self.expected_commands = [command for command, _ in commands]

  def assertCommandsCalled(self):
    for expected_call, found_call in zip(self.expected_commands,
                                         self.mock_aws.call_args_list):
      self.assertEqual(expected_call, FindAwsCommand(found_call[0][0]))
    self.assertEqual(
        len(self.expected_commands), len(self.mock_aws.call_args_list))

  def assertLastCommandContains(self, want_phrase):
    self.assertIn(want_phrase, ' '.join(self.mock_aws.call_args[0][0]))

  def assertLastCommandDoesNotContain(self, phrase):
    self.assertNotIn(phrase, ' '.join(self.mock_aws.call_args[0][0]))


class AwsVpcTableTest(BaseAwsTest):

  def testInitWithVpc(self):
    self.SetExpectedCommands(DESCRIBE_SECURITY_GROUPS_ONLY_DEFAULT)
    vpc = aws_network.AwsVpc(REGION, VPC_ID)
    self.assertEqual(VPC_ID, vpc.id)
    self.assertEqual(SG_DEFAULT, vpc.default_security_group_id)
    self.assertCommandsCalled()

  def testExistsVpcsNone(self):
    self.SetExpectedCommands(VPC_QUERY_NONE)
    vpc = aws_network.AwsVpc(REGION)
    self.assertFalse(vpc._Exists())
    self.assertCommandsCalled()

  def testExistsVpcsOne(self):
    self.SetExpectedCommands(VPC_QUERY_ONE)
    vpc = aws_network.AwsVpc(REGION)
    self.assertTrue(vpc._Exists())
    self.assertCommandsCalled()

  def testCreate(self):
    self.SetExpectedCommands(VPC_CREATE, MODIFY_VPC, VPC_QUERY_ONE,
                             DESCRIBE_SECURITY_GROUPS_ONLY_DEFAULT)
    vpc = aws_network.AwsVpc(REGION)
    vpc.Create()
    self.assertEqual(VPC_ID, vpc.id)
    self.assertEqual(SG_DEFAULT, vpc.default_security_group_id)
    self.assertCommandsCalled()


class AwsRouteTableTest(BaseAwsTest):

  def setUp(self):
    super(AwsRouteTableTest, self).setUp()
    self.route = aws_network.AwsRouteTable(REGION, VPC_ID)

  def testExistsNoRoutes(self):
    self.SetExpectedCommands(QUERY_ROUTE_NONE)
    self.assertFalse(self.route.RouteExists())
    self.assertCommandsCalled()

  def testExistsNoDefaultRoute(self):
    self.SetExpectedCommands(QUERY_ROUTE_NO_DEFAULT)
    self.assertFalse(self.route.RouteExists())
    self.assertCommandsCalled()

  def testExistsHasDefaultRoute(self):
    self.SetExpectedCommands(QUERY_ROUTE_HAS_DEFAULT)
    self.assertTrue(self.route.RouteExists())
    self.assertCommandsCalled()

  def testCreate(self):
    self.SetExpectedCommands(QUERY_ROUTE_NO_DEFAULT)
    self.route.Create()
    self.assertEqual(ROUTE_ID, self.route.id)

  def testCreateRouteNoDefault(self):
    # 'create-route' is executed
    self.SetExpectedCommands(QUERY_ROUTE_NO_DEFAULT, QUERY_ROUTE_NO_DEFAULT,
                             CREATE_ROUTE)
    self.route.Create()
    self.route.CreateRoute(GATEWAY_ID)
    self.assertCommandsCalled()

  def testCreateRouteHasDefaultRoute(self):
    # 'create-route' is not executed
    self.SetExpectedCommands(QUERY_ROUTE_HAS_DEFAULT, QUERY_ROUTE_HAS_DEFAULT)
    self.route.Create()
    self.route.CreateRoute(GATEWAY_ID)
    self.assertCommandsCalled()


class AwsInternetGatewayTest(BaseAwsTest):

  def assertQueryCommand(self, has_gw_id):
    self.assertCommandsCalled()
    gw_id_filter = AwsFilter('internet-gateway-id', GATEWAY_ID)
    gw_vpc_filter = AwsFilter('attachment.vpc-id', VPC_ID)
    if has_gw_id:
      self.assertLastCommandContains(gw_id_filter)
      self.assertLastCommandDoesNotContain(gw_vpc_filter)
    else:
      self.assertLastCommandDoesNotContain(gw_id_filter)
      self.assertLastCommandContains(gw_vpc_filter)

  def testCreateNoVpc(self):
    self.SetExpectedCommands(CREATE_GATEWAY, GATEWAY_QUERY_ONE)
    gw = aws_network.AwsInternetGateway(REGION)
    gw.Create()
    self.assertFalse(gw.attached)
    self.assertEqual(GATEWAY_ID, gw.id)
    self.assertCommandsCalled()

  def testCreateWithVpc(self):
    # the query does find an attached gateway
    self.SetExpectedCommands(GATEWAY_QUERY_ONE)
    gw = aws_network.AwsInternetGateway(REGION, VPC_ID)
    self.assertCommandsCalled()
    self.assertTrue(gw.attached)
    self.assertEqual(GATEWAY_ID, gw.id)
    self.assertQueryCommand(False)  # should do a query with attached.vpc-id
    self.mock_aws.reset_mock()
    gw.Create()
    self.mock_aws.assert_not_called()

  def testCreateWithVpcButNotFound(self):
    # the query does not find an attached gateway
    self.SetExpectedCommands(GATEWAY_QUERY_NONE)
    gw = aws_network.AwsInternetGateway(REGION, VPC_ID)
    self.assertCommandsCalled()
    self.assertFalse(gw.attached)
    self.assertIsNone(gw.id)

  def testAttachNoVpc(self):
    self.SetExpectedCommands(CREATE_GATEWAY, GATEWAY_QUERY_ONE)
    gw = aws_network.AwsInternetGateway(REGION)
    gw.Create()
    self.SetExpectedCommands(ATTACH_GATEWAY)
    gw.Attach(VPC_ID)
    self.assertCommandsCalled()
    self.assertTrue(gw.attached)
    self.assertEqual(VPC_ID, gw.vpc_id)
    self.SetExpectedCommands(GATEWAY_QUERY_ONE)
    self.assertTrue(gw._Exists())
    self.assertQueryCommand(True)

  def testDetach(self):
    self.SetExpectedCommands(CREATE_GATEWAY, GATEWAY_QUERY_ONE)
    gw = aws_network.AwsInternetGateway(REGION)
    gw.Create()
    self.SetExpectedCommands(ATTACH_GATEWAY)
    gw.Attach(VPC_ID)
    self.SetExpectedCommands(DETACH_GATEWAY)
    gw.Detach()
    self.assertCommandsCalled()
    self.SetExpectedCommands(GATEWAY_QUERY_ONE)
    self.assertTrue(gw._Exists())
    self.assertQueryCommand(True)

  def testNoVpcNoIdThrowsExeception(self):
    gw = aws_network.AwsInternetGateway(REGION)
    with self.assertRaises(errors.Error):
      gw.GetDict()


class AwsFirewallTest(BaseAwsTest):

  def testHappyPath(self):
    self.SetExpectedCommands(DESCRIBE_FW_NONE, CREATE_FW, CREATE_FW)
    fw = aws_network.AwsFirewall()
    fw.AllowPortInSecurityGroup(REGION, SG_DEFAULT, 22)
    self.assertCommandsCalled()

  def testSerialLocking(self):
    self.SetExpectedCommands(DESCRIBE_FW_NONE, CREATE_FW, CREATE_FW)
    fw = aws_network.AwsFirewall()
    fw.AllowPortInSecurityGroup(REGION, SG_DEFAULT, 22)
    self.assertCommandsCalled()
    # show that the describe and create commands are not called
    self.SetExpectedCommands()
    fw.AllowPortInSecurityGroup(REGION, SG_DEFAULT, 22)
    self.assertCommandsCalled()

  def testAddMultipleSources(self):
    self.SetExpectedCommands(DESCRIBE_FW_NONE, CREATE_FW, CREATE_FW)
    fw = aws_network.AwsFirewall()
    fw.AllowPortInSecurityGroup(REGION, SG_DEFAULT, 22, source_range=['a'])
    self.SetExpectedCommands(DESCRIBE_FW_NONE, CREATE_FW, CREATE_FW)
    fw.AllowPortInSecurityGroup(REGION, SG_DEFAULT, 22, source_range=['a', 'b'])
    self.assertCommandsCalled()


if __name__ == '__main__':
  unittest.main()
