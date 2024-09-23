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

import inspect
import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker.providers.gcp import gcp_cloud_valkey
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class GcpCloudValkeyClusterTestCase(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(
      managed_memory_store_cluster=True,
      managed_memory_store_shard_count=2,
      managed_memory_store_replicas_per_shard=2,
      zone=['us-central1-a'],
  )
  def testShardAndNodeCount(self):
    test_instance = gcp_cloud_valkey.CloudValkey(mock.Mock())
    self.assertEqual(test_instance.shard_count, 2)
    self.assertEqual(test_instance.replicas_per_shard, 2)
    self.assertEqual(test_instance.node_count, 6)


class ConstructCloudValkeyTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testInitialization(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        service_type: memorystore
        memory_store_type: {managed_memory_store.VALKEY}
        version: {managed_memory_store.VALKEY_7_2}
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
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.VALKEY)
    with self.subTest('redis_version'):
      self.assertEqual(instance.version, 'VALKEY_7_2')

  def testInitializationFlagOverrides(self):
    test_spec = inspect.cleandoc(f"""
    cloud_redis_memtier:
      memory_store:
        service_type: elasticache
        memory_store_type: {managed_memory_store.REDIS}
        version: redis_3_2
    """)
    FLAGS['managed_memory_store_type'].parse('VALKEY')
    FLAGS['managed_memory_store_service_type'].parse('memorystore')
    FLAGS['managed_memory_store_version'].parse('VALKEY_7_2')
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
      self.assertEqual(instance.MEMORY_STORE, managed_memory_store.VALKEY)
    with self.subTest('version'):
      self.assertEqual(instance.version, 'VALKEY_7_2')
    with self.subTest('redis_region'):
      self.assertEqual(instance.location, 'us-central1')
    with self.subTest('zone_distribution'):
      self.assertEqual(instance.zone_distribution, 'multi-zone')


if __name__ == '__main__':
  unittest.main()
