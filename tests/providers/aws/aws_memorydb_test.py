# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

import inspect
import unittest

from absl import flags
import mock
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class ConstructMemoryDbTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testInitialization(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        cloud: {provider_info.AWS}
        service_type: memorydb
        memory_store_type: {managed_memory_store.REDIS}
        version: redis_7_1
    """)
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='cloud_redis_memtier'
    )
    self.test_bm_spec.vm_groups = {'clients': [mock.MagicMock()]}

    self.test_bm_spec.ConstructMemoryStore()

    instance = self.test_bm_spec.memory_store
    with self.subTest('service_type'):
      self.assertEqual(instance.SERVICE_TYPE, 'memorydb')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('redis_version'):
      self.assertEqual(instance.version, '7.1')

  def testInitializationFlagOverrides(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        service_type: memorystore
        memory_store_type: {managed_memory_store.REDIS}
        version: redis_3_2
    """)
    FLAGS['cloud'].parse('AWS')
    FLAGS['managed_memory_store_service_type'].parse('memorydb')
    FLAGS['managed_memory_store_version'].parse('redis_7_1')
    FLAGS['aws_memorydb_node_type'].parse('db.m4.large')
    FLAGS['cloud_redis_region'].parse('us-east-1')
    FLAGS['aws_memorydb_failover_zone'].parse('us-east-1a')
    self.test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='cloud_redis_memtier'
    )
    self.test_bm_spec.vm_groups = {'clients': [mock.MagicMock()]}

    self.test_bm_spec.ConstructMemoryStore()

    instance = self.test_bm_spec.memory_store
    with self.subTest('service_type'):
      self.assertEqual(instance.SERVICE_TYPE, 'memorydb')
    with self.subTest('memory_store_type'):
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.REDIS)
    with self.subTest('version'):
      self.assertEqual(instance.version, '7.1')
    with self.subTest('node_type'):
      self.assertEqual(instance.node_type, 'db.m4.large')
    with self.subTest('redis_region'):
      self.assertEqual(instance.redis_region, 'us-east-1')
    with self.subTest('failover_zone'):
      self.assertEqual(instance.failover_zone, 'us-east-1a')


if __name__ == '__main__':
  unittest.main()
