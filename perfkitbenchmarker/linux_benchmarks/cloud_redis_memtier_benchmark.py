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
"""Runs the memtier benchmark against managed Redis services.

Spins up a cloud redis instance, runs memtier against it, then spins it down.
"""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memtier

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'cloud_redis_memtier'

BENCHMARK_CONFIG = """
cloud_redis_memtier:
  description: Run memtier against cloud redis
  cloud_redis:
    redis_version: redis_6_x
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: 1
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  # TODO(user) Remove version from config and make it a flag only
  if FLAGS['managed_memory_store_version'].present:
    config['cloud_redis']['redis_version'] = FLAGS.managed_memory_store_version
  if memtier.MEMTIER_RUN_MODE.value == memtier.MemtierMode.MEASURE_CPU_LATENCY:
    config['vm_groups']['clients']['vm_count'] += 1
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: benchmark_config
  """
  _GetManagedMemoryStoreClass().CheckPrerequisites(benchmark_config)


def _GetManagedMemoryStoreClass():
  """Gets the cloud-specific redis memory store class."""
  return (managed_memory_store.GetManagedMemoryStoreClass(
      FLAGS.cloud, managed_memory_store.REDIS))


def _GetManagedMemoryStore(benchmark_spec):
  """Get redis instance from the shared class."""
  return _GetManagedMemoryStoreClass()(benchmark_spec)


def Prepare(benchmark_spec):
  """Prepare the cloud redis instance for memtier tasks.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  memtier_vms = benchmark_spec.vm_groups['clients']
  vm_util.RunThreaded(_Install, memtier_vms)

  benchmark_spec.cloud_redis_instance = _GetManagedMemoryStore(benchmark_spec)
  benchmark_spec.cloud_redis_instance.Create()
  memory_store_ip = benchmark_spec.cloud_redis_instance.GetMemoryStoreIp()
  memory_store_port = benchmark_spec.cloud_redis_instance.GetMemoryStorePort()
  password = benchmark_spec.cloud_redis_instance.GetMemoryStorePassword()

  for vm in memtier_vms:
    memtier.Load(vm, memory_store_ip, memory_store_port, password)


def Run(benchmark_spec):
  """Run benchmark and collect samples.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  memtier_vms = benchmark_spec.vm_groups['clients']
  samples = []
  if memtier.MEMTIER_RUN_MODE.value == memtier.MemtierMode.MEASURE_CPU_LATENCY:
    samples = memtier.RunGetLatencyAtCpu(benchmark_spec.cloud_redis_instance,
                                         memtier_vms)
  elif memtier.MEMTIER_LATENCY_CAPPED_THROUGHPUT.value:
    samples = memtier.MeasureLatencyCappedThroughput(
        memtier_vms[0], benchmark_spec.cloud_redis_instance.GetMemoryStoreIp(),
        benchmark_spec.cloud_redis_instance.GetMemoryStorePort(),
        benchmark_spec.cloud_redis_instance.GetMemoryStorePassword())
  else:
    samples = memtier.RunOverAllThreadsPipelinesAndClients(
        memtier_vms[0], benchmark_spec.cloud_redis_instance.GetMemoryStoreIp(),
        benchmark_spec.cloud_redis_instance.GetMemoryStorePort(),
        benchmark_spec.cloud_redis_instance.GetMemoryStorePassword())

  for sample in samples:
    sample.metadata.update(
        benchmark_spec.cloud_redis_instance.GetResourceMetadata())

  return samples


def Cleanup(benchmark_spec):
  """Cleanup and delete redis instance.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.cloud_redis_instance.Delete()


def _Install(vm):
  vm.Install('memtier')
