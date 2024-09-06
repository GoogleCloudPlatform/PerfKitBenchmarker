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

import inspect
import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker.providers.gcp import gcp_cloud_redis
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class GcpCloudRedisTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)
    ):
      FLAGS.project = 'project'
      FLAGS.zone = ['us-central1-a']
      mock_spec = mock.Mock()
      self.redis = gcp_cloud_redis.CloudRedis(mock_spec)

  def testCreate(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)
    ) as gcloud:
      self.redis._Create()
      gcloud.assert_called_once_with(timeout=1200)
      self.assertTrue(self.redis._Exists())

  def testDelete(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)
    ) as gcloud:
      self.redis._Delete()
      gcloud.assert_called_with(raise_on_failure=False, timeout=1200)

  def testExistTrue(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 0)
    ):
      self.assertTrue(self.redis._Exists())

  def testExistFalse(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('{}', '', 1)
    ):
      self.assertFalse(self.redis._Exists())

  def testReadableVersion(self):
    self.assertEqual(self.redis.ParseReadableVersion('redis_6_x'), '6.x')
    self.assertEqual(self.redis.ParseReadableVersion('redis_5_0'), '5.0')

  def testReadableVersionExtraneous(self):
    self.assertEqual(self.redis.ParseReadableVersion('redis_8'), 'redis_8')
    self.assertEqual(
        self.redis.ParseReadableVersion('redis 9.7.5'), 'redis 9.7.5'
    )

  class TimeSeries:

    def __init__(self, points):
      self.points = [self.TimeSeriesValue(value) for value in points]

    class TimeSeriesValue:

      def __init__(self, value):
        self.value = self.TimeSeriesDoubleValue(value)

      class TimeSeriesDoubleValue:

        def __init__(self, value):
          self.double_value = value

  def testParseMonitoringTimeSeriesShort(self):
    avg_cpu = self.redis._ParseMonitoringTimeSeries([
        self.TimeSeries([1, 2]),
        self.TimeSeries([2, 1]),
        self.TimeSeries([0, 0]),
        self.TimeSeries([3, 3]),
    ])
    self.assertEqual(avg_cpu, 0.1)

  def testParseMonitoringTimeSeriesMedium(self):
    avg_cpu = self.redis._ParseMonitoringTimeSeries([
        self.TimeSeries([4.05, 2.12, 3.21, 1.58]),
        self.TimeSeries([2.83, 2.27, 4.71, 5.11]),
        self.TimeSeries([0, 0, 0, 0]),
        self.TimeSeries([3.91, 3.11, 4.00, 1.65]),
    ])
    self.assertEqual(avg_cpu, 0.160625)

  def testParseMonitoringTimeSeriesLong(self):
    avg_cpu = self.redis._ParseMonitoringTimeSeries([
        self.TimeSeries([12, 32, 62, 51, 12, 103, 54, 85]),
        self.TimeSeries([81, 32, 84, 91, 25, 62, 31, 1]),
        self.TimeSeries([12, 93, 101, 70, 32, 58, 18, 10]),
        self.TimeSeries([77, 34, 29, 83, 11, 8, 38, 68]),
    ])
    self.assertEqual(avg_cpu, 3.25)

  def testShardAndNodeCount(self):
    self.assertEqual(self.redis.shard_count, 1)
    self.assertEqual(self.redis.replicas_per_shard, 0)
    self.assertEqual(self.redis.node_count, 1)


class GcpCloudRedisClusterTestCase(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(
      managed_memory_store_cluster=True,
      managed_memory_store_shard_count=2,
      managed_memory_store_replicas_per_shard=2,
      zone=['us-central1-a'],
  )
  def testShardAndNodeCount(self):
    test_instance = gcp_cloud_redis.CloudRedis(mock.Mock())
    self.assertEqual(test_instance.shard_count, 2)
    self.assertEqual(test_instance.replicas_per_shard, 2)
    self.assertEqual(test_instance.node_count, 6)


class ConstructCloudRedisTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testInitialization(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        service_type: memorystore
        memory_store_type: {managed_memory_store.REDIS}
        version: redis_6_x
    """)
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='cloud_redis_memtier'
    )
    self.test_bm_spec.vm_groups = {'clients': [mock.MagicMock()]}

    self.test_bm_spec.ConstructMemoryStore()

    instance = self.test_bm_spec.memory_store
    with self.subTest('service_type'):
      self.assertEqual(instance.SERVICE_TYPE, 'memorystore')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('redis_version'):
      self.assertEqual(instance.redis_version, 'redis_6_x')

  def testInitializationFlagOverrides(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        service_type: elasticache
        memory_store_type: {managed_memory_store.REDIS}
        version: redis_3_2
    """)
    FLAGS['managed_memory_store_service_type'].parse('memorystore')
    FLAGS['managed_memory_store_version'].parse('redis_7_0')
    FLAGS['gcp_redis_gb'].parse(100)
    FLAGS['cloud_redis_region'].parse('us-central1')
    FLAGS['gcp_redis_zone_distribution'].parse('multi-zone')
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='cloud_redis_memtier'
    )
    self.test_bm_spec.vm_groups = {'clients': [mock.MagicMock()]}

    self.test_bm_spec.ConstructMemoryStore()

    instance = self.test_bm_spec.memory_store
    with self.subTest('service_type'):
      self.assertEqual(instance.SERVICE_TYPE, 'memorystore')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('redis_version'):
      self.assertEqual(instance.redis_version, 'redis_7_0')
    with self.subTest('size'):
      self.assertEqual(instance.size, 100)
    with self.subTest('redis_region'):
      self.assertEqual(instance.redis_region, 'us-central1')
    with self.subTest('zone_distribution'):
      self.assertEqual(instance.zone_distribution, 'multi-zone')


if __name__ == '__main__':
  unittest.main()
