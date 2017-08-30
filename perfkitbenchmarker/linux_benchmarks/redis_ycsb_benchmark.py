# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run YCSB against Redis.

Redis homepage: http://redis.io/
"""
import functools
import math
import posixpath

from itertools import repeat
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import redis_server
from perfkitbenchmarker.linux_packages import ycsb

flags.DEFINE_integer('redis_ycsb_processes', 1,
                     'Number of total ycsb processes across all clients.')

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'redis_ycsb'
BENCHMARK_CONFIG = """
redis_ycsb:
  description: >
      Run YCSB against a single Redis server.
      Specify the number of client VMs with --ycsb_client_vms.
  vm_groups:
    workers:
      vm_spec: *default_single_core
    clients:
      vm_spec: *default_single_core
"""


REDIS_PID_FILE = posixpath.join(redis_server.REDIS_DIR, 'redis.pid')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def PrepareLoadgen(load_vm):
  load_vm.Install('ycsb')


def PrepareServer(redis_vm):
  redis_vm.Install('redis_server')
  redis_server.Configure(redis_vm)
  redis_server.Start(redis_vm)


def Prepare(benchmark_spec):
  """Install Redis on one VM and memtier_benchmark on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  groups = benchmark_spec.vm_groups
  redis_vm = groups['workers'][0]
  ycsb_vms = groups['clients']

  prepare_fns = ([functools.partial(PrepareServer, redis_vm)] +
                 [functools.partial(vm.Install, 'ycsb') for vm in ycsb_vms])

  vm_util.RunThreaded(lambda f: f(), prepare_fns)

  num_ycsb = FLAGS.redis_ycsb_processes
  num_server = FLAGS.redis_total_num_processes
  # Each redis process use different ports, number of ycsb processes should
  # be at least as large as number of server processes, use round-robin
  # to assign target server process to each ycsb process
  server_metadata = [
      {'redis.port': redis_server.REDIS_FIRST_PORT + i % num_server}
      for i in range(num_ycsb)]

  benchmark_spec.executor = ycsb.YCSBExecutor(
      'redis', **{
          'shardkeyspace': True,
          'redis.host': redis_vm.internal_ip,
          'perclientparam': server_metadata})
  vm_util.SetupSimulatedMaintenance(redis_vm)



def Run(benchmark_spec):
  """Run YCSB against Redis.

  This method can run with multiple number of redis processes (server) on a
  single vm. Since redis is single threaded, there is no need to run on
  multiple server instances. When running with multiple redis processes, key
  space is sharded.

  This method can also run with muliple number of ycsb processes (client) on
  multiple client instances. Each ycsb process can only run against a single
  server process. The number of ycsb processes should be no smaller than
  the number of server processes or the number of client vms.

  To avoid having multiple ycsb processes on the same client vm
  targeting the same server process, this method hash ycsb processes to server
  processes and ycsb processes to client vms differently.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  groups = benchmark_spec.vm_groups
  ycsb_vms = groups['clients']
  num_ycsb = FLAGS.redis_ycsb_processes
  num_server = FLAGS.redis_total_num_processes
  num_client = FLAGS.ycsb_client_vms
  metadata = {'ycsb_client_vms': num_client,
              'ycsb_processes': num_ycsb,
              'redis_total_num_processes': num_server}

  # Matching client vms and ycsb processes sequentially:
  # 1st to xth ycsb clients are assigned to client vm 1
  # x+1th to 2xth ycsb clients are assigned to client vm 2, etc.
  # Duplicate VirtualMachine objects passed into YCSBExecutor to match
  # corresponding ycsb clients.
  duplicate = int(math.ceil(num_ycsb / float(num_client)))
  client_vms = [
      vm for item in ycsb_vms for vm in repeat(item, duplicate)][:num_ycsb]

  samples = list(benchmark_spec.executor.Load(client_vms,
                                              load_kwargs={'threads': 4}))
  vm_util.StartSimulatedMaintenance()
  samples += list(benchmark_spec.executor.Run(client_vms))

  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Remove Redis and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  redis_server.Cleanup(benchmark_spec.vm_groups['workers'][0])
