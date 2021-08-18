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

import unittest
from absl import flags
import mock

from perfkitbenchmarker.linux_benchmarks import cloud_redis_memtier_benchmark
from perfkitbenchmarker.linux_packages import memtier
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


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
        mock.patch.object(cloud_redis_memtier_benchmark,
                          '_GetManagedMemoryStore'))
    memory_store_patch.return_value = redis_instance

    ip_patch = self.enter_context(
        mock.patch.object(redis_instance, 'GetMemoryStoreIp'))
    ip_patch.return_value = '0.0.0'
    port_patch = self.enter_context(
        mock.patch.object(redis_instance, 'GetMemoryStorePort'))
    port_patch.return_value = '1234'
    password_patch = self.enter_context(
        mock.patch.object(redis_instance, 'GetMemoryStorePassword'))
    password_patch.return_value = 'password'
    load_patch = self.enter_context(mock.patch.object(memtier, 'Load'))

    cloud_redis_memtier_benchmark.Prepare(benchmark_spec)
    redis_instance.Create.assert_called_once_with()
    load_patch.assert_called_once_with(vm, '0.0.0', '1234', 'password')

  def testRun(self):
    vm = mock.Mock()
    benchmark_spec = mock.Mock()
    benchmark_spec.vm_groups = {'clients': [vm]}
    samples = self.enter_context(
        mock.patch.object(memtier, 'RunOverAllThreadsPipelinesAndClients'))
    samples.return_value = []
    self.assertEqual([], cloud_redis_memtier_benchmark.Run(benchmark_spec))

  def testRunLatencyAtGivenCpu(self):
    FLAGS.memtier_run_mode = memtier.MemtierMode.MEASURE_CPU_LATENCY
    client_vm = mock.Mock()
    measure_latency_vm = mock.Mock()
    benchmark_spec = mock.Mock()
    benchmark_spec.vm_groups = {'clients': [client_vm, measure_latency_vm]}
    samples = self.enter_context(
        mock.patch.object(memtier, 'RunGetLatencyAtCpu'))
    samples.return_value = []
    self.assertEqual([], cloud_redis_memtier_benchmark.Run(benchmark_spec))

  def testDelete(self):
    benchmark_spec = mock.Mock()
    redis_instance = mock.Mock()
    benchmark_spec.cloud_redis_instance = redis_instance
    cloud_redis_memtier_benchmark.Cleanup(benchmark_spec)
    redis_instance.Delete.assert_called_once_with()


if __name__ == '__main__':
  unittest.main()
