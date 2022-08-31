# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Benchmark for Redis Enterprise database.

This benchmark finds the maximum throughput (ops/sec) while keeping the avg
latency of requests to less than a cap (1ms by default).

To run this benchmark:
1) Ensure your Redis Enterprise keyfile is at
   'perfkitbenchmarker/data/enterprise_redis_license', or specify its location
   with --enterprise_redis_license_path.
2) Ensure your Redis Enterprise software installation package is under the data
   directory as well, or under the cloud-specific preprovisioned data bucket.
   The package name and version must match the version specified by
   --enterprise_redis_version.
3) If using a different version, update the PREPROVISIONED_DATA checksum at
   perfkitbenchmarker/linux_packages/redis_enterprise by running
   `sha256sum <redislabs_tarfile>`.
"""
import logging

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import redis_enterprise

FLAGS = flags.FLAGS

_OPTIMIZE_THROUGHPUT = flags.DEFINE_boolean(
    'enterprise_redis_optimize_throughput', False,
    'If True, the benchmark will find the optimal throughput under 1ms latency '
    'for the machine type by optimizing the number of shards and proxy threads.'
    'Requires one or none of --enterprise_redis_proxy_threads and '
    '--enterprise_redis_shard_count to be set. If only one is set, keeps that '
    'value constant for the run and optimizes the other.'
)

BENCHMARK_NAME = 'redis_enterprise'
REDIS_PORT = 12006
REDIS_UI_PORT = 8443
REDIS_MANAGEMENT_PORT = 9443


BENCHMARK_CONFIG = """
redis_enterprise:
  description: Run memtier_benchmark against Redis Enterprise.
  vm_groups:
    servers:
      vm_spec:
        GCP:
          machine_type: c2-standard-30
          zone: us-east1-a
        AWS:
          machine_type: c5.9xlarge
          zone: us-east-1d
      vm_count: 1
    clients:
      vm_spec:
        GCP:
          machine_type: c2-standard-30
          zone: us-east1-a
        AWS:
          machine_type: c5.9xlarge
          zone: us-east-1d
      vm_count: 2
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def CheckPrerequisites(_):
  """Validates flag configuration."""
  if _OPTIMIZE_THROUGHPUT.value:
    if (FLAGS.enterprise_redis_proxy_threads and
        FLAGS.enterprise_redis_shard_count):
      raise errors.Setup.InvalidFlagConfigurationError(
          'Running with --enterprise_redis_optimize_throughput, expected one '
          'of --enterprise_redis_proxy_threads or '
          '--enterprise_redis_shard_count to be set, not both.')
  else:
    if not (FLAGS.enterprise_redis_proxy_threads and
            FLAGS.enterprise_redis_shard_count):
      raise errors.Setup.InvalidFlagConfigurationError(
          'Expected both --enterprise_redis_proxy_threads and '
          '--enterprise_redis_shard_count must be set, but they were not.')


def _InstallRedisEnterprise(vm):
  """Download and install enterprise redis on a vm."""
  vm.Install('redis_enterprise')


def Prepare(benchmark_spec):
  """Install Redis on one VM and memtier_benchmark on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  client_vms = benchmark_spec.vm_groups['clients']
  server_vms = benchmark_spec.vm_groups['servers']
  vm_util.RunThreaded(_InstallRedisEnterprise, client_vms + server_vms)
  vm_util.RunThreaded(lambda vm: vm.AuthenticateVm(), client_vms + server_vms)

  server_vm = server_vms[0]
  server_vm.AllowPort(REDIS_PORT)
  server_vm.AllowPort(REDIS_UI_PORT)
  server_vm.AllowPort(REDIS_MANAGEMENT_PORT)

  redis_enterprise.OfflineCores(server_vms)
  redis_enterprise.CreateCluster(server_vms)

  # Skip preparing the database if we're optimizing throughput. The database
  # will be prepared on each individual run.
  if _OPTIMIZE_THROUGHPUT.value:
    return

  client = redis_enterprise.HttpClient(server_vms)
  redis_enterprise.TuneProxy(server_vm)
  client.CreateDatabases()
  redis_enterprise.PinWorkers(server_vms)
  redis_enterprise.LoadDatabases(
      server_vms, client_vms, client.GetEndpoints())


def Run(benchmark_spec):
  """Run memtier against enterprise redis and measure latency and throughput.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  load_vms = benchmark_spec.vm_groups['clients']
  redis_vms = benchmark_spec.vm_groups['servers']
  redis_vm = redis_vms[0]

  numa_pages_migrated, _ = redis_vm.RemoteCommand(
      'cat /proc/vmstat | grep numa_pages_migrated')
  numa_pages_migrated = numa_pages_migrated.split(' ')[1]
  numa_balancing, _ = redis_vm.RemoteCommand(
      'cat /proc/sys/kernel/numa_balancing')
  setup_metadata = {
      'numa_pages_migrated': numa_pages_migrated.rstrip(),
      'numa_balancing': numa_balancing.rstrip(),
  }

  if _OPTIMIZE_THROUGHPUT.value:
    optimizer = redis_enterprise.ThroughputOptimizer(redis_vms, load_vms)
    optimal_throughput, results = optimizer.GetOptimalThroughput()
    if not optimal_throughput:
      raise errors.Benchmarks.RunError(
          'Did not get a throughput under 1ms metric. Try decreasing the '
          '--enterprise_redis_min_threads value.')
    logging.info('Found optimal throughput %s', optimal_throughput)
  else:
    _, results = redis_enterprise.Run(redis_vms, load_vms)

  for result in results:
    result.metadata.update(setup_metadata)

  return results


def Cleanup(benchmark_spec):
  del benchmark_spec
