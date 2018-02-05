"""Runs the YCSB benchmark against managed Redis services.

Spins up a cloud redis instance, runs YCSB against it, then spins it down.
"""

import logging
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS
flags.DEFINE_string('redis_region',
                    'us-central1',
                    'The region to spin up cloud redis in')


BENCHMARK_NAME = 'cloud_redis_ycsb'

BENCHMARK_CONFIG = """
cloud_redis_ycsb:
  description: Run YCSB against cloud redis
  cloud_redis:
    redis_version: REDIS_3_2
    redis_size_gb: 5
    redis_tier: STANDARD
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: 2
"""

CLOUD_REDIS_CLASS_NAME = 'CloudRedis'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepare the cloud redis instance to YCSB tasks.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  ycsb_vms = benchmark_spec.vm_groups['clients']
  vm_util.RunThreaded(_Install, ycsb_vms)
  instance_details = benchmark_spec.cloud_redis.GetInstanceDetails()
  benchmark_spec.executor = ycsb.YCSBExecutor(
      'redis', **{
          'shardkeyspace': True,
          'redis.host': instance_details['host'],
          'redis.port': instance_details['port']})


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

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.cloud_redis.Delete()
  logging.info('Instance %s deleted successfully',
               benchmark_spec.cloud_redis.spec.redis_name)


def _Install(vm):
  vm.Install('ycsb')
