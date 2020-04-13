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
"""Tests for perfkitbenchmarker.providers.aws.redshift."""

import copy
import unittest
from absl import flags
import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import redshift
from tests import pkb_common_test_case


CLUSTER_PARAMETER_GROUP = 'fake_redshift_cluster_parameter_group'
CLUSTER_SUBNET_GROUP = 'fake_redshift_cluster_subnet_group'
PKB_CLUSTER = 'pkb-cluster'
PKB_CLUSTER_DATABASE = 'pkb-database'
REDSHIFT_NODE_TYPE = 'dc2.large'
USERNAME = 'pkb-username'
PASSWORD = 'pkb-password'
TEST_RUN_URI = 'fakeru'

AWS_ZONE_US_EAST_1A = 'us-east-1a'

BASE_REDSHIFT_SPEC = {
    'cluster_identifier': PKB_CLUSTER,
    'db': PKB_CLUSTER_DATABASE,
    'user': USERNAME,
    'password': PASSWORD,
    'node_type': REDSHIFT_NODE_TYPE,
}

FLAGS = flags.FLAGS


class FakeRedshiftClusterSubnetGroup(object):

  def __init__(self):
    self.name = CLUSTER_SUBNET_GROUP


class FakeRedshiftClusterParameterGroup(object):

  def __init__(self):
    self.name = CLUSTER_PARAMETER_GROUP


class RedshiftTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(RedshiftTestCase, self).setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.zones = [AWS_ZONE_US_EAST_1A]

  def testInvalidClusterCreationError(self):
    kwargs = copy.copy(BASE_REDSHIFT_SPEC)
    kwargs['node_count'] = None
    with self.assertRaises(errors.Config.InvalidValue):
      benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)

  def testSingleNodeClusterCreation(self):
    kwargs = copy.copy(BASE_REDSHIFT_SPEC)
    kwargs['node_count'] = 1
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    redshift_local = redshift.Redshift(spec)
    self.assertIsNone(redshift_local.snapshot)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      redshift_local.Initialize(redshift_local.cluster_identifier,
                                redshift_local.node_type,
                                redshift_local.node_count, redshift_local.user,
                                redshift_local.password,
                                FakeRedshiftClusterParameterGroup(),
                                FakeRedshiftClusterSubnetGroup())
      mock_issue.assert_called_once()
      mock_issue.assert_called_with([
          'aws', '--output', 'json', '--region', 'us-east-1', 'redshift',
          'create-cluster', '--cluster-identifier', PKB_CLUSTER,
          '--cluster-type', 'single-node', '--node-type', REDSHIFT_NODE_TYPE,
          '--master-username', USERNAME, '--master-user-password', PASSWORD,
          '--cluster-parameter-group-name',
          'fake_redshift_cluster_parameter_group',
          '--cluster-subnet-group-name', 'fake_redshift_cluster_subnet_group',
          '--publicly-accessible', '--automated-snapshot-retention-period=0'
      ], raise_on_failure=False)

  def testMultiNodeClusterCreation(self):
    kwargs = copy.copy(BASE_REDSHIFT_SPEC)
    kwargs['node_count'] = 2
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    redshift_local = redshift.Redshift(spec)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      redshift_local.Initialize(redshift_local.cluster_identifier,
                                redshift_local.node_type,
                                redshift_local.node_count, redshift_local.user,
                                redshift_local.password,
                                FakeRedshiftClusterParameterGroup(),
                                FakeRedshiftClusterSubnetGroup())
      mock_issue.assert_called_once()
      mock_issue.assert_called_with([
          'aws', '--output', 'json', '--region', 'us-east-1', 'redshift',
          'create-cluster', '--cluster-identifier', PKB_CLUSTER,
          '--number-of-nodes', '2', '--node-type', REDSHIFT_NODE_TYPE,
          '--master-username', USERNAME, '--master-user-password', PASSWORD,
          '--cluster-parameter-group-name',
          'fake_redshift_cluster_parameter_group',
          '--cluster-subnet-group-name', 'fake_redshift_cluster_subnet_group',
          '--publicly-accessible', '--automated-snapshot-retention-period=0'
      ], raise_on_failure=False)


if __name__ == '__main__':
  unittest.main()
