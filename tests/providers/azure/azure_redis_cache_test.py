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
"""Tests for perfkitbenchmarker.providers.azure.azure_redis_cache."""
import inspect
import unittest
from absl import flags
import mock
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import azure_redis_cache
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class AzureRedisCacheTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.project = 'project'
    FLAGS.zones = ['eastus']
    FLAGS.cloud_redis_region = 'eastus'
    mock_spec = mock.Mock()
    mock_spec.version = 'redis_6_x'
    mock_resource_group = mock.Mock()
    self.resource_group_patch = mock.patch.object(
        azure_network, 'GetResourceGroup'
    ).start()
    self.resource_group_patch.return_value = mock_resource_group
    mock_resource_group.name = 'az_resource'
    self.redis = azure_redis_cache.AzureRedisCache(mock_spec)
    self.mock_command = mock.patch.object(vm_util, 'IssueCommand').start()

  def testCreate(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'az',
        'redis',
        'create',
        '--resource-group',
        'az_resource',
        '--location',
        'eastus',
        '--name',
        'pkb-None',
        '--sku',
        'Basic',
        '--vm-size',
        'C3',
        '--enable-non-ssl-port',
        '--redis-version',
        '6.0',
    ]
    self.redis._Create()
    self.mock_command.assert_called_once_with(expected_output, timeout=900)

  def testDelete(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'az',
        'redis',
        'delete',
        '--resource-group',
        'az_resource',
        '--name',
        'pkb-None',
        '--yes',
    ]
    self.redis._Delete()
    self.mock_command.assert_called_once_with(expected_output, timeout=900)

  def testExistTrue(self):
    self.mock_command.return_value = (None, '', 0)
    expected_output = [
        'az',
        'redis',
        'show',
        '--resource-group',
        'az_resource',
        '--name',
        'pkb-None',
    ]
    self.redis._Exists()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False
    )


class ConstructAzureRedisCacheTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testInitialization(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        cloud: {provider_info.AZURE}
        service_type: cache
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
      self.assertEqual(instance.SERVICE_TYPE, 'cache')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('redis_version'):
      self.assertEqual(instance.version, '6.0')

  def testInitializationFlagOverrides(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        service_type: memorystore
        memory_store_type: {managed_memory_store.REDIS}
        version: redis_3_2
    """)
    FLAGS['cloud'].parse(provider_info.AZURE)
    FLAGS['managed_memory_store_service_type'].parse('cache')
    FLAGS['managed_memory_store_version'].parse('redis_6_x')
    FLAGS['cloud_redis_region'].parse('eastus2')
    FLAGS['azure_redis_size'].parse('P4')
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='cloud_redis_memtier'
    )
    self.test_bm_spec.vm_groups = {'clients': [mock.MagicMock()]}

    self.test_bm_spec.ConstructMemoryStore()

    instance = self.test_bm_spec.memory_store
    with self.subTest('service_type'):
      self.assertEqual(instance.SERVICE_TYPE, 'cache')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('version'):
      self.assertEqual(instance.version, '6.0')
    with self.subTest('redis_region'):
      self.assertEqual(instance.redis_region, 'eastus2')
    with self.subTest('size'):
      self.assertEqual(instance.azure_redis_size, 'P4')


if __name__ == '__main__':
  unittest.main()
