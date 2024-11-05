# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_elasticache_redis."""

import inspect
import unittest

from absl import flags
import mock
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_elasticache_redis
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class AwsElasticacheRedisTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.project = 'project'
    FLAGS.zones = ['us-east-1a']
    FLAGS.cloud_redis_region = 'us-east-1'
    FLAGS.run_uri = 'run12345'
    mock_spec = mock.Mock()
    mock_spec.version = 'redis_4_0'
    mock_vm = mock.Mock()
    mock_vm.zone = FLAGS.zones[0]
    self.redis = aws_elasticache_redis.ElastiCacheRedis(mock_spec)
    self.enter_context(
        mock.patch.object(self.redis, '_GetClientVm', return_value=mock_vm)
    )
    self.mock_command = mock.patch.object(vm_util, 'IssueCommand').start()

  def testCreate(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'aws',
        'elasticache',
        'create-replication-group',
        '--engine',
        'redis',
        '--engine-version',
        '4.0.10',
        '--replication-group-id',
        'pkb-run12345',
        '--replication-group-description',
        'pkb-run12345',
        '--region',
        'us-east-1',
        '--cache-node-type',
        'cache.m4.large',
        '--cache-subnet-group-name',
        'subnet-pkb-run12345',
        '--preferred-cache-cluster-a-zs',
        'us-east-1a',
        '--no-transit-encryption-enabled',
        '--tags',
    ]
    self.redis._Create()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False
    )

  def testDelete(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'aws',
        'elasticache',
        'delete-replication-group',
        '--region',
        'us-east-1',
        '--replication-group-id',
        'pkb-run12345',
    ]
    self.redis._Delete()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False
    )

  def testExistTrue(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'aws',
        'elasticache',
        'describe-replication-groups',
        '--region',
        'us-east-1',
        '--replication-group-id',
        'pkb-run12345',
    ]
    self.redis._Exists()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False
    )

  def testReadableVersion(self):
    """Test normal cases work as they should."""
    with self.subTest('6.x'):
      self.redis.version = '6.x'
      self.assertEqual(self.redis.GetReadableVersion(), '6.x')
    with self.subTest('4.0.10'):
      self.redis.version = '4.0.10'
      self.assertEqual(self.redis.GetReadableVersion(), '4.0')

  def testReadableVersionExtraneous(self):
    """Test weird cases just return the version number as is."""
    with self.subTest('redis.8'):
      self.redis.version = 'redis.8'
      self.assertEqual(self.redis.GetReadableVersion(), 'redis.8')
    with self.subTest('redis 9_7_5'):
      self.redis.version = 'redis 9_7_5'
      self.assertEqual(self.redis.GetReadableVersion(), 'redis 9_7_5')


class ConstructElasticacheRedisTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testInitialization(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        cloud: {provider_info.AWS}
        service_type: elasticache
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
      self.assertEqual(instance.SERVICE_TYPE, 'elasticache')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('redis_version'):
      self.assertEqual(instance.version, '6.x')

  def testInitializationFlagOverrides(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        service_type: memorystore
        memory_store_type: {managed_memory_store.REDIS}
        version: redis_3_2
    """)
    FLAGS['cloud'].parse('AWS')
    FLAGS['managed_memory_store_service_type'].parse('elasticache')
    FLAGS['managed_memory_store_version'].parse('redis_7_x')
    FLAGS['elasticache_node_type'].parse('cache.m4.large')
    FLAGS['cloud_redis_region'].parse('us-east-1')
    FLAGS['elasticache_failover_zone'].parse('us-east-1a')
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='cloud_redis_memtier'
    )
    self.test_bm_spec.vm_groups = {'clients': [mock.MagicMock()]}

    self.test_bm_spec.ConstructMemoryStore()

    instance = self.test_bm_spec.memory_store
    with self.subTest('service_type'):
      self.assertEqual(instance.SERVICE_TYPE, 'elasticache')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('version'):
      self.assertEqual(instance.version, '7.1')
    with self.subTest('node_type'):
      self.assertEqual(instance.node_type, 'cache.m4.large')
    with self.subTest('redis_region'):
      self.assertEqual(instance.redis_region, 'us-east-1')
    with self.subTest('failover_zone'):
      self.assertEqual(instance.failover_zone, 'us-east-1a')


if __name__ == '__main__':
  unittest.main()
