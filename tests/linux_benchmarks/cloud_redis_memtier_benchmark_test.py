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
"""Tests for cloud_redis_memtier_benchmark."""

import pathlib
import unittest
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import cloud_redis_memtier_benchmark
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.providers.aws import aws_elasticache_redis  # pylint:disable=unused-import
from perfkitbenchmarker.providers.gcp import gcp_cloud_redis  # pylint:disable=unused-import
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_CLUSTER_SHARDS_OUTPUT = 'redis_cluster_shards.txt'
_DESCRIBE_CLUSTER_OUTPUT = 'elasticache_describe_cluster.txt'


def _ReadFile(filename):
  path = pathlib.Path(__file__).parents[1] / 'data' / filename
  with open(path) as f:
    return f.read()


def _GetTestRedisSpec():
  spec_args = {'cloud': 'AWS', 'redis_version': 'redis_6_x'}
  return benchmark_config_spec._CloudRedisSpec(
      'test_component', flag_values=FLAGS, **spec_args
  )


def _GetTestRedisInstance():
  test_spec = _GetTestRedisSpec()
  mock_bm_spec = mock.Mock()
  mock_bm_spec.config.cloud_redis = test_spec
  redis_class = cloud_redis_memtier_benchmark._GetManagedMemoryStoreClass()
  instance = redis_class(mock_bm_spec)  # pytype: disable=not-instantiable
  instance._ip = '0.0.0.0'
  instance._port = 1234
  return instance


def _GetTestVm(ip_address):
  vm = pkb_common_test_case.TestLinuxVirtualMachine(
      pkb_common_test_case.CreateTestVmSpec()
  )
  vm.ip_address = ip_address
  return vm


class CloudRedisMemtierBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetConfigNoFlag(self):
    config = cloud_redis_memtier_benchmark.GetConfig({})
    self.assertEqual('redis_6_x', config['cloud_redis']['redis_version'])

  def testGetConfigFlag(self):
    redis_version = 'redis_4_0'
    FLAGS.managed_memory_store_version = redis_version
    FLAGS['managed_memory_store_version'].present = 1
    config = cloud_redis_memtier_benchmark.GetConfig({})
    self.assertEqual(redis_version, config['cloud_redis']['redis_version'])

  def testPrepare(self):
    vm = mock.Mock()
    benchmark_spec = mock.Mock()
    redis_instance = mock.Mock()
    benchmark_spec.vm_groups = {'clients': [vm]}
    memory_store_patch = self.enter_context(
        mock.patch.object(
            cloud_redis_memtier_benchmark, '_GetManagedMemoryStore'
        )
    )
    memory_store_patch.return_value = redis_instance

    ip_patch = self.enter_context(
        mock.patch.object(redis_instance, 'GetMemoryStoreIp')
    )
    ip_patch.return_value = '0.0.0'
    port_patch = self.enter_context(
        mock.patch.object(redis_instance, 'GetMemoryStorePort')
    )
    port_patch.return_value = '1234'
    password_patch = self.enter_context(
        mock.patch.object(redis_instance, 'GetMemoryStorePassword')
    )
    password_patch.return_value = 'password'
    load_patch = self.enter_context(mock.patch.object(memtier, 'Load'))

    cloud_redis_memtier_benchmark.Prepare(benchmark_spec)
    redis_instance.Create.assert_called_once_with()
    load_patch.assert_called_once_with([vm], '0.0.0', '1234', 'password')

  def testRun(self):
    vm = mock.Mock()
    benchmark_spec = mock.Mock()
    benchmark_spec.vm_groups = {'clients': [vm]}
    samples = self.enter_context(
        mock.patch.object(memtier, 'RunOverAllThreadsPipelinesAndClients')
    )
    samples.return_value = []
    self.assertEqual([], cloud_redis_memtier_benchmark.Run(benchmark_spec))

  def testRunLatencyAtGivenCpu(self):
    FLAGS.memtier_run_mode = memtier.MemtierMode.MEASURE_CPU_LATENCY
    client_vm = mock.Mock()
    measure_latency_vm = mock.Mock()
    benchmark_spec = mock.Mock()
    benchmark_spec.vm_groups = {'clients': [client_vm, measure_latency_vm]}
    samples = self.enter_context(
        mock.patch.object(memtier, 'RunGetLatencyAtCpu')
    )
    samples.return_value = []
    self.assertEqual([], cloud_redis_memtier_benchmark.Run(benchmark_spec))

  def testDelete(self):
    benchmark_spec = mock.Mock()
    redis_instance = mock.Mock()
    benchmark_spec.cloud_redis_instance = redis_instance
    cloud_redis_memtier_benchmark.Cleanup(benchmark_spec)
    redis_instance.Delete.assert_called_once_with()

  @flagsaver.flagsaver(cloud='AWS')
  def testGetConnectionsMultiVm(self):
    test_redis_instance = _GetTestRedisInstance()
    test_redis_instance.name = 'pkb-cbf06969'
    vm1 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    vm1.ip_address = 'vm1'
    vm2 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    vm2.ip_address = 'vm2'
    self.enter_context(
        mock.patch.object(
            vm1,
            'RemoteCommand',
            return_value=(_ReadFile(_CLUSTER_SHARDS_OUTPUT), ''),
        )
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            return_value=(_ReadFile(_DESCRIBE_CLUSTER_OUTPUT), '', 0),
        )
    )

    connections = cloud_redis_memtier_benchmark._GetConnections(
        [vm1, vm2], test_redis_instance
    )

    self.assertCountEqual(
        connections,
        [
            memtier.MemtierConnection(vm1, '10.0.1.117', 6379),
            memtier.MemtierConnection(vm1, '10.0.2.104', 6379),
            memtier.MemtierConnection(vm1, '10.0.3.217', 6379),
            memtier.MemtierConnection(vm1, '10.0.1.09', 6379),
            memtier.MemtierConnection(vm2, '10.0.2.177', 6379),
            memtier.MemtierConnection(vm2, '10.0.1.174', 6379),
            memtier.MemtierConnection(vm2, '10.0.3.6', 6379),
        ],
    )

  @flagsaver.flagsaver(cloud='AWS')
  def testGetConnectionsSingleVm(self):
    test_redis_instance = _GetTestRedisInstance()
    test_redis_instance.name = 'pkb-cbf06969'
    vm1 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    vm1.ip_address = 'vm1'
    self.enter_context(
        mock.patch.object(
            vm1,
            'RemoteCommand',
            return_value=(_ReadFile(_CLUSTER_SHARDS_OUTPUT), ''),
        )
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            return_value=(_ReadFile(_DESCRIBE_CLUSTER_OUTPUT), '', 0),
        )
    )

    connections = cloud_redis_memtier_benchmark._GetConnections(
        [vm1], test_redis_instance
    )

    self.assertCountEqual(
        connections,
        [
            memtier.MemtierConnection(vm1, '0.0.0.0', 1234),
        ],
    )

  def testShardConnections(self):
    test_redis_instance = _GetTestRedisInstance()
    test_redis_instance.name = 'test-instance'
    vm1 = _GetTestVm('vm1')
    vm2 = _GetTestVm('vm2')
    vm3 = _GetTestVm('vm3')

    shards = [
        managed_memory_store.RedisShard('', 'shard0', 0, 'zone_a'),
        managed_memory_store.RedisShard('', 'shard1', 0, 'zone_a'),
        managed_memory_store.RedisShard('', 'shard2', 0, 'zone_a'),
        managed_memory_store.RedisShard('', 'shard3', 0, 'zone_b'),
        managed_memory_store.RedisShard('', 'shard4', 0, 'zone_b'),
        managed_memory_store.RedisShard('', 'shard5', 0, 'zone_b'),
        managed_memory_store.RedisShard('', 'shard6', 0, 'zone_c'),
        managed_memory_store.RedisShard('', 'shard7', 0, 'zone_c'),
        managed_memory_store.RedisShard('', 'shard8', 0, 'zone_c'),
        managed_memory_store.RedisShard('', 'shard9', 0, 'zone_c'),
    ]
    self.enter_context(
        mock.patch.object(
            test_redis_instance, 'GetShardEndpoints', return_value=shards
        )
    )

    connections = cloud_redis_memtier_benchmark._GetConnections(
        [vm1, vm2, vm3], test_redis_instance
    )

    self.assertCountEqual(
        connections,
        [
            memtier.MemtierConnection(vm1, 'shard0', 0),
            memtier.MemtierConnection(vm1, 'shard3', 0),
            memtier.MemtierConnection(vm1, 'shard6', 0),
            memtier.MemtierConnection(vm1, 'shard9', 0),
            memtier.MemtierConnection(vm2, 'shard1', 0),
            memtier.MemtierConnection(vm2, 'shard4', 0),
            memtier.MemtierConnection(vm2, 'shard7', 0),
            memtier.MemtierConnection(vm3, 'shard2', 0),
            memtier.MemtierConnection(vm3, 'shard5', 0),
            memtier.MemtierConnection(vm3, 'shard8', 0),
        ],
    )

  def testShardConnectionsNoZone(self):
    test_redis_instance = _GetTestRedisInstance()
    test_redis_instance.name = 'test-instance'
    vm1 = _GetTestVm('vm1')
    vm2 = _GetTestVm('vm2')
    vm3 = _GetTestVm('vm3')

    shards = [
        managed_memory_store.RedisShard('', 'shard0', 0, None),
        managed_memory_store.RedisShard('', 'shard1', 0, None),
        managed_memory_store.RedisShard('', 'shard2', 0, None),
        managed_memory_store.RedisShard('', 'shard3', 0, None),
        managed_memory_store.RedisShard('', 'shard4', 0, None),
        managed_memory_store.RedisShard('', 'shard5', 0, None),
    ]
    self.enter_context(
        mock.patch.object(
            test_redis_instance, 'GetShardEndpoints', return_value=shards
        )
    )

    connections = cloud_redis_memtier_benchmark._GetConnections(
        [vm1, vm2, vm3], test_redis_instance
    )

    self.assertCountEqual(
        connections,
        [
            memtier.MemtierConnection(vm1, 'shard0', 0),
            memtier.MemtierConnection(vm1, 'shard3', 0),
            memtier.MemtierConnection(vm2, 'shard1', 0),
            memtier.MemtierConnection(vm2, 'shard4', 0),
            memtier.MemtierConnection(vm3, 'shard2', 0),
            memtier.MemtierConnection(vm3, 'shard5', 0),
        ],
    )

  def testShardConnectionsOnePerVm(self):
    test_redis_instance = _GetTestRedisInstance()
    test_redis_instance.name = 'test-instance'
    vm1 = _GetTestVm('vm1')
    vm2 = _GetTestVm('vm2')
    vm3 = _GetTestVm('vm3')

    shards = [
        managed_memory_store.RedisShard('', 'shard0', 0, 'zone_a'),
        managed_memory_store.RedisShard('', 'shard1', 0, 'zone_b'),
        managed_memory_store.RedisShard('', 'shard2', 0, 'zone_c'),
    ]
    self.enter_context(
        mock.patch.object(
            test_redis_instance, 'GetShardEndpoints', return_value=shards
        )
    )

    connections = cloud_redis_memtier_benchmark._GetConnections(
        [vm1, vm2, vm3], test_redis_instance
    )

    self.assertCountEqual(
        connections,
        [
            memtier.MemtierConnection(vm1, 'shard0', 0),
            memtier.MemtierConnection(vm2, 'shard1', 0),
            memtier.MemtierConnection(vm3, 'shard2', 0),
        ],
    )


if __name__ == '__main__':
  unittest.main()
