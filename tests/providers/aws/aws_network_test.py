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
import mock
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
_COMPONENT = 'test_component'

REGION = 'us-west-1'
VPC_ID = 'vpc-1234'
SG_DEFAULT = 'sg-1234'


def AwsResponse(json_obj):
  return (json.dumps(json_obj), '', 0)


DESCRIBE_SECURITY_GROUPS_ONLY_DEFAULT = ('describe-security-groups', {
    'SecurityGroups': [{
        'GroupId': SG_DEFAULT
    }]
})

VPC_QUERY_NONE = ('describe-vpcs', {'Vpcs': []})
VPC_QUERY_ONE = ('describe-vpcs', {'Vpcs': [{'VpcId': VPC_ID}]})
VPC_CREATE = ('create-vpc', {'Vpc': {'VpcId': VPC_ID}})
MODIFY_VPC = ('modify-vpc-attribute', {})


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

  def XtestInitWithVpc(self):
    self.SetExpectedCommands(DESCRIBE_SECURITY_GROUPS_ONLY_DEFAULT)
    vpc = aws_network.AwsVpc(REGION, VPC_ID)
    self.assertEqual(VPC_ID, vpc.id)
    self.assertEqual(SG_DEFAULT, vpc.default_security_group_id)
    self.assertCommandsCalled()

  def XtestExistsVpcsNone(self):
    self.SetExpectedCommands(VPC_QUERY_NONE)
    vpc = aws_network.AwsVpc(REGION)
    self.assertFalse(vpc._Exists())
    self.assertCommandsCalled()

  def XtestExistsVpcsOne(self):
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


if __name__ == '__main__':
  unittest.main()
