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
from absl import flags
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_cluster_parameter_group
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'

AWS_ZONE_US_EAST_1A = 'us-east-1a'

FLAGS = flags.FLAGS


class RedshiftClusterParameterGroupTestCase(
    pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(RedshiftClusterParameterGroupTestCase, self).setUp()
    FLAGS.zones = [AWS_ZONE_US_EAST_1A]
    FLAGS.run_uri = TEST_RUN_URI

  def testValidClusterParameterGroupCreation(self):
    cpg = aws_cluster_parameter_group.RedshiftClusterParameterGroup(
        1, list(util.AWS_PREFIX))
    self.assertEqual(cpg.name, 'pkb-%s' % TEST_RUN_URI)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      cpg._Create()
      self.assertEqual(mock_issue.call_count, 2)
      mock_issue.assert_called_with([
          'aws', '--output', 'json', 'redshift',
          'modify-cluster-parameter-group', '--parameter-group-name',
          'pkb-%s' % TEST_RUN_URI, '--parameters',
          ('[{"ParameterName":"wlm_json_configuration","ParameterValue":"'
           '[{\\"query_concurrency\\":1}]","ApplyType":"dynamic"}]')
      ])

  def testValidClusterParameterGroupDeletion(self):
    cpg = aws_cluster_parameter_group.RedshiftClusterParameterGroup(
        1, list(util.AWS_PREFIX))
    self.assertEqual(cpg.name, 'pkb-%s' % TEST_RUN_URI)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      cpg._Delete()
      mock_issue.assert_called_once()
      mock_issue.assert_called_with([
          'aws', '--output', 'json', 'redshift',
          'delete-cluster-parameter-group', '--parameter-group-name',
          'pkb-%s' % TEST_RUN_URI
      ], raise_on_failure=False)


if __name__ == '__main__':
  unittest.main()
