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
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)):
      FLAGS.project = 'project'
      FLAGS.zone = ['us-central1-a']
      mock_spec = mock.Mock()
      self.redis = gcp_cloud_redis.CloudRedis(mock_spec)

  def testCreate(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)) as gcloud:
      self.redis._Create()
      gcloud.assert_called_once_with(timeout=600)
      self.assertTrue(self.redis._Exists())

  def testDelete(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)) as gcloud:
      self.redis._Delete()
      gcloud.assert_called_with(raise_on_failure=False, timeout=600)

  def testExistTrue(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)):
      self.assertTrue(self.redis._Exists())

  def testExistFalse(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 1)):
      self.assertFalse(self.redis._Exists())

  def testReadableVersion(self):
    self.assertEqual(self.redis.ParseReadableVersion('redis_6_x'), '6.x')
    self.assertEqual(self.redis.ParseReadableVersion('redis_5_0'), '5.0')

  def testReadableVersionExtraneous(self):
    self.assertEqual(self.redis.ParseReadableVersion('redis_8'), 'redis_8')
    self.assertEqual(
        self.redis.ParseReadableVersion('redis 9.7.5'), 'redis 9.7.5')

  class TimeSeries():

    def __init__(self, points):
      self.points = [self.TimeSeriesValue(value) for value in points]

    class TimeSeriesValue():

      def __init__(self, value):
        self.value = self.TimeSeriesDoubleValue(value)

      class TimeSeriesDoubleValue():

        def __init__(self, value):
          self.double_value = value

  def testParseMonitoringTimeSeriesShort(self):
    avg_cpu = self.redis._ParseMonitoringTimeSeries([
        self.TimeSeries([1, 2]),
        self.TimeSeries([2, 1]),
        self.TimeSeries([0, 0]),
        self.TimeSeries([3, 3])
    ])
    self.assertEqual(avg_cpu, 0.1)

  def testParseMonitoringTimeSeriesMedium(self):
    avg_cpu = self.redis._ParseMonitoringTimeSeries([
        self.TimeSeries([4.05, 2.12, 3.21, 1.58]),
        self.TimeSeries([2.83, 2.27, 4.71, 5.11]),
        self.TimeSeries([0, 0, 0, 0]),
        self.TimeSeries([3.91, 3.11, 4.00, 1.65])
    ])
    self.assertEqual(avg_cpu, 0.160625)

  def testParseMonitoringTimeSeriesLong(self):
    avg_cpu = self.redis._ParseMonitoringTimeSeries([
        self.TimeSeries([12, 32, 62, 51, 12, 103, 54, 85]),
        self.TimeSeries([81, 32, 84, 91, 25, 62, 31, 1]),
        self.TimeSeries([12, 93, 101, 70, 32, 58, 18, 10]),
        self.TimeSeries([77, 34, 29, 83, 11, 8, 38, 68])
    ])
    self.assertEqual(avg_cpu, 3.25)


if __name__ == '__main__':
  unittest.main()
