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
"""Runs the YCSB benchmark against managed Redis services.

Spins up a cloud redis instance, runs YCSB against it, then spins it down.
"""


from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'cloud_redis_ycsb'

BENCHMARK_CONFIG = f"""
cloud_redis_ycsb:
  description: Run YCSB against cloud redis
  memory_store:
    redis_version: redis_3_2
    service_type: memorystore
    memory_store_type: {managed_memory_store.REDIS}
  vm_groups:
    clients:
      os_type: ubuntu2204  # Python 2
      vm_spec: *default_single_core
      vm_count: 2
"""

CLOUD_REDIS_CLASS_NAME = 'CloudRedis'


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def Prepare(benchmark_spec):
  """Prepare the cloud redis instance to YCSB tasks.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  ycsb_vms = benchmark_spec.vm_groups['clients']
  background_tasks.RunThreaded(_Install, ycsb_vms)

  cloud_redis_instance = benchmark_spec.memory_store
  redis_args = {
      'shardkeyspace': True,
      'redis.host': cloud_redis_instance.GetMemoryStoreIp(),
      'redis.port': cloud_redis_instance.GetMemoryStorePort(),
  }
  password = cloud_redis_instance.GetMemoryStorePassword()
  if password:
    redis_args['redis.password'] = password
  benchmark_spec.executor = ycsb.YCSBExecutor('redis', **redis_args)


def Run(benchmark_spec):
  """Doc will be updated when implemented.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  ycsb_vms = benchmark_spec.vm_groups['clients']
  return benchmark_spec.executor.LoadAndRun(ycsb_vms)


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec


def _Install(vm):
  vm.Install('ycsb')
