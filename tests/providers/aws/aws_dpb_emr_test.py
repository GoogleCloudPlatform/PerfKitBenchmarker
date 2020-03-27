# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_dpb_emr."""

import unittest

import mock
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_dpb_emr
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'
AWS_ZONE_US_EAST_1A = 'us-east-1a'
FLAGS = flags.FLAGS


class LocalAwsDpbEmr(aws_dpb_emr.AwsDpbEmr):

  def __init__(self):
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(
        util.GetRegionFromZone(FLAGS.dpb_service_zone))


class AwsDpbEmrTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsDpbEmrTestCase, self).setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = AWS_ZONE_US_EAST_1A
    FLAGS.zones = [AWS_ZONE_US_EAST_1A]

  def testCreateLogBucket(self):
    local_emr = LocalAwsDpbEmr()
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      local_emr._CreateLogBucket()
      self.assertEqual(mock_issue.call_count, 2)
      call_arg_list, _ = mock_issue.call_args
      self.assertListEqual([
          'aws', 's3api', 'put-bucket-tagging', '--bucket',
          'pkb-{0}-emr'.format(
              FLAGS.run_uri), '--tagging', 'TagSet=[]', '--region=us-east-1'
      ], call_arg_list[0])

  def testDeleteLogBucket(self):
    local_emr = LocalAwsDpbEmr()
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      local_emr._DeleteLogBucket()
      self.assertEqual(mock_issue.call_count, 1)
      call_arg_list, _ = mock_issue.call_args
      self.assertListEqual([
          'aws', 's3', 'rb',
          's3://%s' % 'pkb-{0}-emr'.format(FLAGS.run_uri), '--region',
          util.GetRegionFromZone(FLAGS.dpb_service_zone), '--force'
      ], call_arg_list[0])

  def testCreateBucket(self):
    local_emr = LocalAwsDpbEmr()
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      local_emr.CreateBucket('foo')
      self.assertEqual(mock_issue.call_count, 2)
      call_arg_list, _ = mock_issue.call_args
      self.assertListEqual([
          'aws', 's3api', 'put-bucket-tagging', '--bucket', 'foo', '--tagging',
          'TagSet=[]', '--region=us-east-1'
      ], call_arg_list[0])

  def testDeleteBucket(self):
    local_emr = LocalAwsDpbEmr()
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      local_emr.DeleteBucket('foo')
      self.assertEqual(mock_issue.call_count, 1)
      call_arg_list, _ = mock_issue.call_args
      self.assertListEqual([
          'aws', 's3', 'rb', 's3://{}'.format('foo'), '--region',
          util.GetRegionFromZone(FLAGS.dpb_service_zone), '--force'
      ], call_arg_list[0])


if __name__ == '__main__':
  unittest.main()
