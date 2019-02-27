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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS
flags.DEFINE_string('redis_region',
                    'us-central1',
                    'The region to spin up cloud redis in.')

BENCHMARK_NAME = 'cloud_redis_ycsb'

BENCHMARK_CONFIG = """
cloud_redis_ycsb:
  description: Run YCSB against cloud redis
  cloud_redis:
    redis_version: redis_3_2
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: 2
"""

CLOUD_REDIS_CLASS_NAME = 'CloudRedis'


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  if FLAGS['redis_version'].present:
    config['cloud_redis']['redis_version'] = FLAGS.redis_version
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: benchmark_config
  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  # TODO(ruwa): This CheckPrerequisites call checks the prerequisites
  # on the resource. Ideally, the benchmark is not responsible for this task.
  # Instead, BaseResource should check prerequisites as part of creation and
  # child resources can override CheckPrerequisites and benefit from it.
  cloud_redis_class = (
      managed_memory_store.GetManagedMemoryStoreClass(
          FLAGS.cloud,
          managed_memory_store.REDIS))
  cloud_redis_class.CheckPrerequisites(benchmark_config)


def Prepare(benchmark_spec):
  """Prepare the cloud redis instance to YCSB tasks.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  ycsb_vms = benchmark_spec.vm_groups['clients']
  vm_util.RunThreaded(_Install, ycsb_vms)

  cloud_redis_class = (
      managed_memory_store.GetManagedMemoryStoreClass(
          FLAGS.cloud,
          managed_memory_store.REDIS))
  benchmark_spec.cloud_redis_instance = (cloud_redis_class(benchmark_spec))
  benchmark_spec.cloud_redis_instance.Create()

  instance_details = benchmark_spec.cloud_redis_instance.GetInstanceDetails()
  redis_args = {
      'shardkeyspace': True,
      'redis.host': instance_details['host'],
      'redis.port': instance_details['port']
  }
  if 'password' in instance_details:
    redis_args['redis.password'] = instance_details['password']
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
  samples = benchmark_spec.executor.LoadAndRun(ycsb_vms)

  for sample in samples:
    sample.metadata.update(
        benchmark_spec.cloud_redis_instance.GetResourceMetadata())

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.cloud_redis_instance.Delete()
  logging.info('Instance %s deleted successfully',
               benchmark_spec.cloud_redis_instance.name)


def _Install(vm):
  vm.Install('ycsb')
