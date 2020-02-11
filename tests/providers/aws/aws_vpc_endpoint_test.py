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
"""Tests for perfkitbenchmarker.providers.aws.aws_vpc_endpoint."""

import unittest
import mock
from perfkitbenchmarker import flags
from perfkitbenchmarker.providers.aws import aws_vpc_endpoint
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

SERVICE_NAME = 's3'
REGION = 'us-west-1'
FULL_SERVICE_NAME = 'com.amazonaws.{}.s3'.format(REGION)
VPC_ID = 'vpc-1234'
ENDPOINT_ID = 'vpce-1234'
ROUTE_TABLE_ID = 'rtb-1234'
CREATE_RES = {'VpcEndpoint': {'VpcEndpointId': ENDPOINT_ID}}
DELETE_RES = {'Unsuccessful': []}

QUERY_ENDPOINTS_CMD = [
    'describe-vpc-endpoints', '--filters',
    'Name=vpc-id,Values={}'.format(VPC_ID), '--filters',
    'Name=service-name,Values={}'.format(FULL_SERVICE_NAME), '--query',
    'VpcEndpoints[].VpcEndpointId'
]
DESCRIBE_ROUTES_CMD = [
    'describe-route-tables', '--filters',
    'Name=vpc-id,Values={}'.format(VPC_ID), '--query',
    'RouteTables[].RouteTableId'
]
CREATE_ENDPOINT_CMD = [
    'create-vpc-endpoint', '--vpc-endpoint-type', 'Gateway', '--vpc-id', VPC_ID,
    '--service-name', FULL_SERVICE_NAME, '--route-table-ids', ROUTE_TABLE_ID
]
DELETE_ENDPOINT_CMD = [
    'delete-vpc-endpoints', '--vpc-endpoint-ids', ENDPOINT_ID
]


class AwsVpcS3EndpointTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsVpcS3EndpointTest, self).setUp()
    self.mock_vpc = mock.Mock()
    self.mock_vpc.region = REGION
    self.mock_run_cmd = mock.patch.object(aws_vpc_endpoint.AwsVpcS3Endpoint,
                                          '_RunCommand').start()

  def testDown(self):
    super(AwsVpcS3EndpointTest, self).tearDown()
    mock.patch.stopall()

  def _InitEndpoint(self, vpc_id):
    self.mock_vpc.id = vpc_id
    return aws_vpc_endpoint.CreateEndpointService(SERVICE_NAME, self.mock_vpc)

  def testEndPointIdNoVpc(self):
    # initialize with no VPC means no immediate lookups done
    endpoint = self._InitEndpoint(None)
    self.assertIsNone(endpoint.id)
    endpoint._RunCommand.assert_not_called()

  def testEndPointIdHasVpc(self):
    # initialize with a VPC does an immediate call to find existing endpoints
    endpoint = self._InitEndpoint(VPC_ID)
    self.assertIsNone(endpoint.id, 'Endpoint id always None on initialization')
    self.mock_run_cmd.reset_mock()
    self.mock_run_cmd.side_effect = [[ENDPOINT_ID]]
    self.assertEqual(ENDPOINT_ID, endpoint.endpoint_id)
    endpoint._RunCommand.assert_called_with(QUERY_ENDPOINTS_CMD)

  def testCreate(self):
    # shows that a call to .Create() will get the routing table info followed
    # by the create-vpc-endpoint call
    endpoint = self._InitEndpoint(VPC_ID)
    self.mock_run_cmd.reset_mock()
    self.mock_run_cmd.side_effect = [
        [],  # query for endpoint id
        [ROUTE_TABLE_ID],  # query for route tables
        CREATE_RES,  # _Create()
        [ENDPOINT_ID],  # _Exists()
    ]
    endpoint.Create()
    calls = endpoint._RunCommand.call_args_list
    self.assertEqual(mock.call(QUERY_ENDPOINTS_CMD), calls[0])
    self.assertEqual(mock.call(DESCRIBE_ROUTES_CMD), calls[1])
    self.assertEqual(mock.call(CREATE_ENDPOINT_CMD), calls[2])
    self.assertEqual(mock.call(QUERY_ENDPOINTS_CMD), calls[3])
    self.assertEqual(ENDPOINT_ID, endpoint.id)

  def testDelete(self):
    endpoint = self._InitEndpoint(VPC_ID)
    self.mock_run_cmd.reset_mock()
    endpoint.id = ENDPOINT_ID
    self.mock_run_cmd.side_effect = [DELETE_RES, []]
    endpoint.Delete()
    calls = endpoint._RunCommand.call_args_list
    self.assertEqual(mock.call(DELETE_ENDPOINT_CMD), calls[0])
    self.assertEqual(mock.call(QUERY_ENDPOINTS_CMD), calls[1])


if __name__ == '__main__':
  unittest.main()
