# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs multilate benchmark against memcached on cloud virtual machines.

Memcached is an in-memory key-value store for small chunks of arbitrary
data (strings, objects) from results of database calls, API calls, or page
rendering.
Memcached homepage: https://memcached.org/

Mutilate: https://github.com/leverich/mutilate
Basic Usage:
https://github.com/leverich/mutilate#basic-usage

Mutilate is a load generator for benchmarking memcached.
Compared to memtier_memcached benchmark, this benchmark
added following additional features provided by mutilate:
- Support of multiple client vms (remote agents).
- Measure_* options, allowing taking latency measurements of the
memcached server without incurring significant client-side queuing
delay.
- Setting keysize.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memcached_server
from perfkitbenchmarker.linux_packages import mutilate

FLAGS = flags.FLAGS


flags.DEFINE_string('memcached_mutilate_server_machine_type', None,
                    'Machine type to use for the memcached server if different '
                    'from memcached client machine type.')
flags.DEFINE_string('memcached_mutilate_client_machine_type', None,
                    'Machine type to use for the mutilate client if different '
                    'from memcached server machine type.')
flags.DEFINE_integer('memcached_mutilate_num_client_vms', 1,
                     'Number of mutilate client machines to use.')
flags.DEFINE_boolean('set_smp_affinity', False,
                     'Manually set smp affinity.')

BENCHMARK_NAME = 'memcached_mutilate'
BENCHMARK_CONFIG = """
memcached_mutilate:
  description: Run mutilate against a memcached installation.
  vm_groups:
    server:
      vm_spec: *default_single_core
      vm_count: 1
    client:
      vm_spec: *default_dual_core
      vm_count: 1
"""


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: The benchmark configuration.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config
  mutilate.CheckPrerequisites()


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.memcached_mutilate_client_machine_type:
    vm_spec = config['vm_groups']['client']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.memcached_mutilate_client_machine_type)
  if FLAGS.memcached_mutilate_server_machine_type:
    vm_spec = config['vm_groups']['server']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.memcached_mutilate_server_machine_type)
  if FLAGS['memcached_mutilate_num_client_vms'].present:
    config['vm_groups']['client']['vm_count'] = (
        FLAGS.memcached_mutilate_num_client_vms)
  return config


def _InstallMutilate(vm):
  vm.Install('mutilate')


def _InstallMemcached(vm):
  vm.Install('memcached_server')


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run mutilate against memcached.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  clients = benchmark_spec.vm_groups['client']
  master = clients[0]
  server = benchmark_spec.vm_groups['server'][0]
  client_install_fns = [
      functools.partial(vm.Install, 'mutilate') for vm in clients]
  server_install_fns = [functools.partial(server.Install, 'memcached_server')]
  vm_util.RunThreaded(lambda f: f(), client_install_fns + server_install_fns)

  memcached_server.ConfigureAndStart(
      server, smp_affinity=FLAGS.set_smp_affinity)
  mutilate.Load(master, server.internal_ip, memcached_server.MEMCACHED_PORT)


def Run(benchmark_spec):
  """Runs mutilate against memcached and gathers the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  clients = benchmark_spec.vm_groups['client']
  server = benchmark_spec.vm_groups['server'][0]
  server_ip = server.internal_ip
  metadata = {'memcached_version': memcached_server.GetVersion(server),
              'memcached_server_size': FLAGS.memcached_size_mb,
              'memcached_server_threads': FLAGS.memcached_num_threads,
              'smp_affinity': FLAGS.set_smp_affinity}

  samples = mutilate.Run(
      clients, server_ip, memcached_server.MEMCACHED_PORT)
  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(unused_benchmark_spec):
  pass
