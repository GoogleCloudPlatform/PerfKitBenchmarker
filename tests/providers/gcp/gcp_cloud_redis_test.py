# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.gcp.gcp_cloud_redis."""
import unittest
from absl import flags
import mock

from perfkitbenchmarker.providers.gcp import gcp_cloud_redis
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GcpCloudRedisTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcpCloudRedisTestCase, self).setUp()
    FLAGS.project = 'project'
    FLAGS.zones = ['us-central1-a']
    mock_spec = mock.Mock()
    self.redis = gcp_cloud_redis.CloudRedis(mock_spec)

  def testCreate(self):
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=('{}', '', 0)) as gcloud:
      self.redis._Create()
      gcloud.assert_called_once_with(timeout=600)
      self.assertTrue(self.redis._Exists())

  def testDelete(self):
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=('{}', '', 0)) as gcloud:
      self.redis._Delete()
      gcloud.assert_called_once_with(raise_on_failure=False, timeout=600)

  def testExistTrue(self):
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=('{}', '', 0)):
      self.assertTrue(self.redis._Exists())

  def testExistFalse(self):
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=('{}', '', 1)):
      self.assertFalse(self.redis._Exists())

if __name__ == '__main__':
  unittest.main()
