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
"""An AWS VPC Endpoint.

See https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints.html

A VPC Endpoint provides routing between a VPC and an AWS service without using
the internet connection interface.

For example when an S3 Endpoint is created for region us-west-1 the route table
for that VPC is updated so that requests for the IP addresses associated with
com.amazonaws.us-west-1.s3 now go through this new interface and not out through
the original internet gateway.
"""

import json
from perfkitbenchmarker import providers
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.aws import util



def GetAwsVpcEndpointClass(aws_service):
  """Returns the AwsVpcEndpoint class for the given service."""
  return resource.GetResourceClass(
      AwsVpcEndpoint, CLOUD=providers.AWS, AWS_SERVICE=aws_service)


def CreateEndpointService(aws_service, vpc):
  """Creates the named VPC endpoint in the given VPC.

  Args:
    aws_service: The AWS service to use.
    vpc: The VPC to launch the endpoint service in.

  Returns:
    The resource.BaseResource of the endpoint service.
  """
  service_class = GetAwsVpcEndpointClass(aws_service)
  return service_class(vpc)


class AwsVpcEndpoint(resource.BaseResource):
  """An AWS Endpoint.

  Attributes:
    region: The AWS region of the VPC
    vpc: The aws_network.AwsVpc object to make the connection in.  The VPC does
      not initially need an ID but does when Create() is called.
  """
  REQUIRED_ATTRS = ['CLOUD', 'AWS_SERVICE']
  RESOURCE_TYPE = 'AwsVpcEndpoint'
  CLOUD = providers.AWS

  def __init__(self, vpc):
    super(AwsVpcEndpoint, self).__init__()
    assert vpc, 'Must have a VPC object (does not require an id).'
    self._vpc = vpc
    self.region = self._vpc.region
    assert self.region, 'VPC region must be set'
    self._service_name = 'com.amazonaws.{}.{}'.format(self.region,
                                                      self.AWS_SERVICE)
    # in the Create() method query to see if an endpoint already defined
    self.id = None

  @property
  def vpc_id(self):
    """Returns the VPC id.  Can be None."""
    return self._vpc.id

  @property
  def endpoint_id(self):
    """Returns the endpoint id for the defined VPC."""
    if not self.vpc_id:
      # When creating an SDDC there will not be a VPC to have an endpoint
      return None
    ids = self._RunCommand(['describe-vpc-endpoints'] +
                           util.AwsFilter('vpc-id', self.vpc_id) +
                           util.AwsFilter('service-name', self._service_name) +
                           ['--query', 'VpcEndpoints[].VpcEndpointId'])
    if not ids:
      # There is a VPC but no endpoint
      return None
    assert len(ids) == 1, 'More than 1 VPC endpoint found: {}'.format(ids)
    return ids[0]

  @property
  def route_table_id(self):
    """Returns the route table id for the VPC.

    Raises:
      AssertionError: If no VPC is defined or if there are 0 or more than 1
        routing tables found.
    """
    assert self.vpc_id, 'No defined VPC id.'
    table_ids = self._RunCommand(['describe-route-tables'] +
                                 util.AwsFilter('vpc-id', self.vpc_id) +
                                 ['--query', 'RouteTables[].RouteTableId'])
    assert len(table_ids) == 1, 'Only want 1 route table: {}'.format(table_ids)
    return table_ids[0]

  def _Create(self):
    """See base class.

    Raises:
      AssertionError: If no VPC is defined.
    """
    assert self.vpc_id, 'No defined VPC id.'
    self.id = self.endpoint_id
    if self.id:
      # Endpoint already created
      return
    create_response = self._RunCommand([
        'create-vpc-endpoint', '--vpc-endpoint-type', 'Gateway', '--vpc-id',
        self.vpc_id, '--service-name', self._service_name, '--route-table-ids',
        self.route_table_id
    ])
    self.id = create_response['VpcEndpoint']['VpcEndpointId']

  def _PostCreate(self):
    """See base class."""
    util.AddDefaultTags(self.id, self.region)

  def _Exists(self):
    """See base class."""
    return bool(self.endpoint_id)

  def _Delete(self):
    """See base class."""
    endpoint_id = self.id or self.endpoint_id
    if endpoint_id:
      self._RunCommand(
          ['delete-vpc-endpoints', '--vpc-endpoint-ids', endpoint_id])

  def _RunCommand(self, cmds):
    """Runs the AWS ec2 command in the defined region.

    Args:
      cmds: List of AWS ec2 commands to run, example: ['describe-route-tables']

    Returns:
      Dict of the AWS response.
    """
    cmd = util.AWS_PREFIX + ['ec2', '--region=%s' % self.region] + list(cmds)
    stdout, _ = util.IssueRetryableCommand(cmd)
    return json.loads(stdout)


class AwsVpcS3Endpoint(AwsVpcEndpoint):
  """An AWS VPC S3 Endpoint.

  Attributes:
    region: The AWS region of the VPC
    vpc: The aws_network.AwsVpc object to make the connection in.  The VPC does
      not initially need an ID but does when Create() is called.
  """
  AWS_SERVICE = 's3'
