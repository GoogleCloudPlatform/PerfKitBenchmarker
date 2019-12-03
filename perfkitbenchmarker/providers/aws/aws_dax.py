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
"""Module containing class for AWS' DAX cluster.

DAX cluster can be created and deleted.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_iam_role
from perfkitbenchmarker.providers.aws import util

_DAX_CLUSTER_NAME_TEMPLATE = 'pkb-dax-{uid}'
_DAX_SUBNET_GROUP_TEMPLATE = 'pkb-subnet-group-{uid}'
_DAX_SERVICE = 'dax.amazonaws.com'
_DAX_ACTION = 'dynamodb:*'
_DAX_TCP_PORT = 8011
_DAX_ROLE_NAME_TEMPLATE = 'PkbDaxServiceRole{uid}'
_DAX_POLICY_NAME_TEMPLATE = 'PolicyForPkbDaxServiceRole{uid}'
_DAX_STATUS_AVAILABLE = 'available'
_DYNAMODB_RESOURCE_TEMPLATE = 'arn:aws:dynamodb:{region}:{account}:*'

FLAGS = flags.FLAGS


class AwsDax(resource.BaseResource):
  """Class representing an AWS Dax cluster."""

  def __init__(self, benchmark_uid, zone, network):
    super(AwsDax, self).__init__()
    self.benchmark_uid = benchmark_uid
    self.zone = zone
    self.region = util.GetRegionFromZone(self.zone)
    self.vpc = network.regional_network.vpc
    self.subnet_id = network.subnet.id
    self.account = util.GetAccount()
    self.iam_role = aws_iam_role.AwsIamRole(
        self.account, _DAX_ROLE_NAME_TEMPLATE.format(uid=self.benchmark_uid),
        _DAX_POLICY_NAME_TEMPLATE.format(uid=self.benchmark_uid), _DAX_SERVICE,
        _DAX_ACTION,
        _DYNAMODB_RESOURCE_TEMPLATE.format(
            region=self.region, account=self.account))
    self.cluster_endpoint = None
    self.subnet_group_name = _DAX_SUBNET_GROUP_TEMPLATE.format(
        uid=self.benchmark_uid)
    self.cluster_name = _DAX_CLUSTER_NAME_TEMPLATE.format(
        uid=self.benchmark_uid)

  def _CreateDependencies(self):
    """See base class.

    Creates the IAM role and subnet group used by the DAX cluster.
    """

    self.iam_role.Create()
    cmd = util.AWS_PREFIX + [
        'dax', 'create-subnet-group', '--subnet-group-name',
        self.subnet_group_name, '--subnet-ids', self.subnet_id
    ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=True)
    if retcode != 0:
      logging.warn('Failed to create subnet group! %s', stderror)

  def _Create(self):
    """See base class."""
    cmd = util.AWS_PREFIX + [
        'dax', 'create-cluster', '--cluster-name', self.cluster_name,
        '--node-type', FLAGS.aws_dax_node_type, '--replication-factor',
        str(FLAGS.aws_dax_replication_factor), '--iam-role-arn',
        self.iam_role.GetRoleArn(), '--subnet-group', self.subnet_group_name,
        '--sse-specification', 'Enabled=true', '--region', self.region
    ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=True)
    if retcode != 0:
      logging.warn('Failed to create dax cluster! %s', stderror)

  def _DeleteDependencies(self):
    """See base class."""
    cmd = util.AWS_PREFIX + [
        'dax', 'delete-subnet-group', '--subnet-group-name',
        self.subnet_group_name
    ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warn('Failed to delete subnet group! %s', stderror)

    self.iam_role.Delete()

  def _Delete(self):
    """See base class."""
    cmd = util.AWS_PREFIX + [
        'dax', 'delete-cluster', '--cluster-name', self.cluster_name
    ]
    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warn('Failed to delete dax cluster! %s', stderror)

  def _Exists(self):
    """See base class."""
    cmd = util.AWS_PREFIX + [
        'dax', 'describe-clusters', '--cluster-names', self.cluster_name
    ]
    _, _, retcode = vm_util.IssueCommand(
        cmd, suppress_warning=True, raise_on_failure=False)
    return not retcode

  def _IsReady(self):
    """See base class.

    Returns:
      True if the DAX cluster is ready.
    """
    cmd = util.AWS_PREFIX + [
        'dax', 'describe-clusters', '--cluster-names', self.cluster_name
    ]

    stdout, _, retcode = vm_util.IssueCommand(
        cmd, suppress_warning=True, raise_on_failure=False)
    if retcode != 0 or not stdout:
      return False
    result = json.loads(stdout)
    status = result['Clusters'][0]['Status']
    if not status:
      return False

    if status == _DAX_STATUS_AVAILABLE and not self.cluster_endpoint:
      endpoint = result['Clusters'][0]['ClusterDiscoveryEndpoint']
      self.cluster_endpoint = '{}:{}'.format(endpoint['Address'],
                                             endpoint['Port'])
    return status == _DAX_STATUS_AVAILABLE

  def _PostCreate(self):
    """See base class.

    Enables the Dax Port on the security group's inbound rule.
    """
    for security_group in self.vpc.GetSecurityGroups():
      if security_group['GroupName'] == 'default':
        cmd = util.AWS_PREFIX + [
            'ec2', 'authorize-security-group-ingress', '--group-id',
            security_group['GroupId'], '--protocol', 'tcp', '--port',
            str(_DAX_TCP_PORT)
        ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=True)
    if retcode != 0:
      logging.warn('Failed to config Dax port! %s', stderror)

  def GetClusterEndpoint(self):
    """Returns the DAX cluster's endpoint."""
    if not self._IsReady():
      raise errors.Benchmarks.PrepareException(
          'GetEndpoint when preparing dax cluster: cluster not ready yet.')
    return self.cluster_endpoint
