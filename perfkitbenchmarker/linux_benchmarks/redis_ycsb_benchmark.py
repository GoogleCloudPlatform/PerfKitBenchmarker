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
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import redis_server
from perfkitbenchmarker.linux_packages import ycsb

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

  # Do not persist to disk
  sed_cmd = (r"sed -i -e '/^save /d' -e 's/# *save \"\"/save \"\"/' "
             "{0}/redis.conf").format(redis_server.REDIS_DIR)
  redis_vm.RemoteCommand(sed_cmd)

  # Start the server
  redis_vm.RemoteCommand(
      ('nohup sudo {0}/src/redis-server {0}/redis.conf '
       '&> /dev/null & echo $! > {1}').format(
          redis_server.REDIS_DIR, REDIS_PID_FILE))


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


def Run(benchmark_spec):
  """Run YCSB against Redis.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  groups = benchmark_spec.vm_groups
  redis_vm = groups['workers'][0]
  ycsb_vms = groups['clients']
  executor = ycsb.YCSBExecutor('redis', **{'redis.host': redis_vm.internal_ip})

  metadata = {'ycsb_client_vms': FLAGS.ycsb_client_vms}

  # This thread count gives reasonably fast load time.
  samples = list(executor.LoadAndRun(ycsb_vms, load_kwargs={'threads': 4}))

  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Remove Redis and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  (benchmark_spec.vm_groups['workers'][0]
   .RemoteCommand('kill $(cat {0})'.format(REDIS_PID_FILE),
                  ignore_failure=True))
