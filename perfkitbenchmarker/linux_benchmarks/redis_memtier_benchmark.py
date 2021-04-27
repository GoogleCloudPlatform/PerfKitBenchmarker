# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run memtier_benchmark against Redis.

memtier_benchmark is a load generator created by RedisLabs to benchmark
Redis.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
"""

from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server

FLAGS = flags.FLAGS
flags.DEFINE_string('redis_memtier_client_machine_type', None,
                    'If provided, overrides the memtier client machine type.')
flags.DEFINE_string('redis_memtier_server_machine_type', None,
                    'If provided, overrides the redis server machine type.')
BENCHMARK_NAME = 'redis_memtier'
BENCHMARK_CONFIG = """
redis_memtier:
  description: >
      Run memtier_benchmark against Redis.
      Specify the number of client VMs with --redis_clients.
  vm_groups:
    servers:
      vm_spec: *default_dual_core
      vm_count: 1
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
"""

_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.redis_memtier_client_machine_type:
    vm_spec = config['vm_groups']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.redis_memtier_client_machine_type)
  if FLAGS.redis_memtier_server_machine_type:
    vm_spec = config['vm_groups']['servers']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.redis_memtier_server_machine_type)
  return config


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Install Redis on one VM and memtier_benchmark on another."""
  server_count = len(bm_spec.vm_groups['servers'])
  if server_count != 1:
    raise errors.Benchmarks.PrepareException(
        f'Expected servers vm count to be 1, got {server_count}')
  client_vms = bm_spec.vm_groups['clients']
  server_vm = bm_spec.vm_groups['servers'][0]

  # Install memtier
  vm_util.RunThreaded(lambda client: client.Install('memtier'),
                      client_vms + [server_vm])

  # Install redis on the 1st machine.
  server_vm.Install('redis_server')
  redis_server.Start(server_vm)
  memtier.Load(server_vm, 'localhost', str(redis_server.DEFAULT_PORT))


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run memtier_benchmark against Redis."""
  client_vms = bm_spec.vm_groups['clients']
  server_vm = bm_spec.vm_groups['servers'][0]
  results = memtier.RunOverAllThreadsPipelinesAndClients(
      client_vms[0], server_vm.internal_ip, str(redis_server.DEFAULT_PORT))
  redis_metadata = redis_server.GetMetadata()
  for result_sample in results:
    result_sample.metadata.update(redis_metadata)
  return results


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  del bm_spec
