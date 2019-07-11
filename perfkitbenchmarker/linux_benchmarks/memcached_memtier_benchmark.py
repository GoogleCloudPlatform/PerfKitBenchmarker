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

"""Runs memtier benchmark against memcached on cloud virtual machines.

Memcached is an in-memory key-value store for small chunks of arbitrary
data (strings, objects) from results of database calls, API calls, or page
rendering.
Memcached homepage: https://memcached.org/

Memtier_benchmark is a load generator created by RedisLabs to benchmark
NoSQL key-value databases.

Memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
Memtier_benchmark usage:
https://redislabs.com/blog/memtier_benchmark-a-high-throughput-benchmarking-tool-for-redis-memcached/
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import memcached_server
from perfkitbenchmarker.linux_packages import memtier

FLAGS = flags.FLAGS


flags.DEFINE_string('memcached_memtier_client_machine_type', None,
                    'Machine type to use for the memtier client if different '
                    'from memcached server machine type.')
flags.DEFINE_string('memcached_memtier_server_machine_type', None,
                    'Machine type to use for the memtier server if different '
                    'from memcached client machine type.')


BENCHMARK_NAME = 'memcached_memtier'
BENCHMARK_CONFIG = """
memcached_memtier:
  description: Run memtier against a memcached installation.
  vm_groups:
    server:
      vm_spec: *default_single_core
      vm_count: 1
    client:
      vm_spec: *default_dual_core
      vm_count: 1
"""


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.memcached_memtier_client_machine_type:
    vm_spec = config['vm_groups']['client']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.memcached_memtier_client_machine_type)
  if FLAGS.memcached_memtier_server_machine_type:
    vm_spec = config['vm_groups']['server']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.memcached_memtier_server_machine_type)
  return config


def _InstallMemtier(vm):
  vm.Install('memtier')


def _InstallMemcached(vm):
  vm.Install('memcached_server')


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run memtier against memcached.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  client = benchmark_spec.vm_groups['client'][0]
  server = benchmark_spec.vm_groups['server'][0]

  _InstallMemtier(client)
  _InstallMemcached(server)
  memcached_server.ConfigureAndStart(server)
  memtier.Load(client, server.internal_ip, memcached_server.MEMCACHED_PORT)


def Run(benchmark_spec):
  """Runs memtier against memcached and gathers the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  client = benchmark_spec.vm_groups['client'][0]
  server = benchmark_spec.vm_groups['server'][0]
  server_ip = server.internal_ip
  metadata = {'memcached_version': memcached_server.GetVersion(server),
              'memcached_server_size': FLAGS.memcached_size_mb}

  logging.info('Start benchmarking memcached using memtier.')
  samples = memtier.Run(client, server_ip, memcached_server.MEMCACHED_PORT)
  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(unused_benchmark_spec):
  pass
