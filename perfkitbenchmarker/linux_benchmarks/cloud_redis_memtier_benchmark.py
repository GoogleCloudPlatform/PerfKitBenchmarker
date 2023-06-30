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

import collections
import itertools
from absl import flags
from absl import logging
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import sample
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

_LinuxVm = linux_virtual_machine.BaseLinuxVirtualMachine
_ManagedRedis = managed_memory_store.BaseManagedMemoryStore


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
  _GetManagedMemoryStoreClass().CheckPrerequisites(benchmark_config)  # pytype: disable=attribute-error


def _GetManagedMemoryStoreClass():
  """Gets the cloud-specific redis memory store class."""
  return managed_memory_store.GetManagedMemoryStoreClass(
      FLAGS.cloud, managed_memory_store.REDIS
  )


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
  background_tasks.RunThreaded(_Install, memtier_vms)

  benchmark_spec.cloud_redis_instance = _GetManagedMemoryStore(benchmark_spec)
  benchmark_spec.cloud_redis_instance.Create()
  memory_store_ip = benchmark_spec.cloud_redis_instance.GetMemoryStoreIp()
  memory_store_port = benchmark_spec.cloud_redis_instance.GetMemoryStorePort()
  password = benchmark_spec.cloud_redis_instance.GetMemoryStorePassword()

  for vm in memtier_vms:
    memtier.Load(vm, memory_store_ip, memory_store_port, password)


def _GetConnections(
    vms: list[_LinuxVm], redis_instance: _ManagedRedis
) -> list[memtier.MemtierConnection]:
  """Gets a list of connections mapping client VMs to shards."""
  if len(vms) == 1:
    return [
        memtier.MemtierConnection(
            vms[0],
            redis_instance.GetMemoryStoreIp(),
            redis_instance.GetMemoryStorePort(),
        )
    ]
  # Spread shards by client VM (evenly distributed by zone) such that each
  # client VM gets an equal number of shards in each zone.
  connections = []
  shards = redis_instance.GetShardEndpoints(vms[0])
  shards_by_zone = collections.defaultdict(list)
  for shard in shards:
    shards_by_zone[shard.zone].append(shard)
  shards_by_vm = collections.defaultdict(list)
  # List shards alternating by zone and then distribute them to VMs. Example:
  # shards_by_zone = {
  #   'zone_a': [1, 2, 3]
  #   'zone_b': [4, 5, 6, 7]
  # } -> shards_list = [1, 2, 3, 4, 5, 6, 7]
  # vm1 gets [1, 3, 5, 7], vm2 gets [2, 4, 6]
  for shard_index, shard in enumerate(
      itertools.chain(*shards_by_zone.values())
  ):
    vm_index = shard_index % len(vms)
    vm = vms[vm_index]
    connections.append(memtier.MemtierConnection(vm, shard.ip, shard.port))
    shards_by_vm[vm].append(shard)
  logging.info('Shards by VM: %s', shards_by_vm)
  return connections


def _MeasureMemtierDistribution(
    redis_instance: _ManagedRedis,
    vms: list[_LinuxVm],
) -> list[sample.Sample]:
  """Runs and reports stats across a series of memtier runs."""
  connections = _GetConnections(vms, redis_instance)
  return memtier.MeasureLatencyCappedThroughputDistribution(
      connections,
      redis_instance.GetMemoryStoreIp(),
      redis_instance.GetMemoryStorePort(),
      vms,
      redis_instance.node_count,
      redis_instance.GetMemoryStorePassword(),
  )


def _Run(vms: list[_LinuxVm], redis_instance: _ManagedRedis):
  """Runs memtier based on provided flags."""
  if memtier.MEMTIER_RUN_MODE.value == memtier.MemtierMode.MEASURE_CPU_LATENCY:
    return memtier.RunGetLatencyAtCpu(redis_instance, vms)
  if memtier.MEMTIER_LATENCY_CAPPED_THROUGHPUT.value:
    if memtier.MEMTIER_DISTRIBUTION_ITERATIONS.value:
      return _MeasureMemtierDistribution(redis_instance, vms)
    return memtier.MeasureLatencyCappedThroughput(
        vms[0],
        redis_instance.node_count,
        redis_instance.GetMemoryStoreIp(),
        redis_instance.GetMemoryStorePort(),
        redis_instance.GetMemoryStorePassword(),
    )
  return memtier.RunOverAllThreadsPipelinesAndClients(
      vms,
      redis_instance.GetMemoryStoreIp(),
      [redis_instance.GetMemoryStorePort()],
      redis_instance.GetMemoryStorePassword(),
  )


def Run(benchmark_spec):
  """Run benchmark and collect samples.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  memtier_vms = benchmark_spec.vm_groups['clients']
  redis_instance: _ManagedRedis = benchmark_spec.cloud_redis_instance
  samples = _Run(memtier_vms, redis_instance)
  for s in samples:
    s.metadata.update(benchmark_spec.cloud_redis_instance.GetResourceMetadata())

  return samples


def Cleanup(benchmark_spec):
  """Cleanup and delete redis instance.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.cloud_redis_instance.Delete()


def _Install(vm):
  """Installs necessary client packages."""
  vm.Install('memtier')
  vm.Install('redis_cli')
