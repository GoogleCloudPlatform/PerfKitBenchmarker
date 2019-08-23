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

import unittest

import mock

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_cluster_subnet_group
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'
CLUSTER_SUBNET_ID = 'fake_redshift_cluster_subnet_id'
AWS_ZONE_US_EAST_1A = 'us-east-1a'

FLAGS = flags.FLAGS


class RedshiftClusterSubnetGroupTestCase(
    pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(RedshiftClusterSubnetGroupTestCase, self).setUp()
    FLAGS.zones = [AWS_ZONE_US_EAST_1A]
    FLAGS.run_uri = TEST_RUN_URI

  def testValidClusterParameterGroupCreation(self):
    csg = aws_cluster_subnet_group.RedshiftClusterSubnetGroup(
        list(util.AWS_PREFIX))
    csg.subnet_id = CLUSTER_SUBNET_ID
    self.assertEqual(csg.name, 'pkb-%s' % TEST_RUN_URI)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      csg._Create()
      mock_issue.assert_called_once()
      mock_issue.assert_called_with([
          'aws', '--output', 'json', 'redshift', 'create-cluster-subnet-group',
          '--cluster-subnet-group-name', 'pkb-%s' % TEST_RUN_URI,
          '--description', 'Cluster Subnet Group for run uri %s' % TEST_RUN_URI,
          '--subnet-ids', CLUSTER_SUBNET_ID])

  def testValidClusterParameterGroupDeletion(self):
    csg = aws_cluster_subnet_group.RedshiftClusterSubnetGroup(
        list(util.AWS_PREFIX))
    self.assertEqual(csg.name, 'pkb-%s' % TEST_RUN_URI)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      csg._Delete()
      mock_issue.assert_called_once()
      mock_issue.assert_called_with([
          'aws', '--output', 'json', 'redshift', 'delete-cluster-subnet-group',
          '--cluster-subnet-group-name', 'pkb-%s' % TEST_RUN_URI],
                                    raise_on_failure=False)


if __name__ == '__main__':
  unittest.main()
