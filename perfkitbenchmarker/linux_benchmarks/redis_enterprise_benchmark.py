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

"""Benchmark for Redis Enterprise database."""

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import redis_enterprise

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'redis_enterprise'
REDIS_PORT = 12006
REDIS_UI_PORT = 8443


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
  server_vm = benchmark_spec.vm_groups['servers']
  args = [((vm,), {}) for vm in client_vms + server_vm]
  vm_util.RunThreaded(_InstallRedisEnterprise, args)

  server_vm = server_vm[0]
  server_vm.AllowPort(REDIS_PORT)
  server_vm.AllowPort(REDIS_UI_PORT)

  redis_enterprise.CreateCluster(server_vm)
  redis_enterprise.TuneProxy(server_vm)
  redis_enterprise.SetUpCluster(server_vm, REDIS_PORT)
  # TODO(ruwa): Add task binding and offlining
  redis_enterprise.WaitForClusterUp(server_vm, REDIS_PORT)
  redis_enterprise.LoadCluster(server_vm, REDIS_PORT)


def Run(benchmark_spec):
  """Run memtier against enterprise redis and measure latency and throughput.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  load_vms = benchmark_spec.vm_groups['clients']
  redis_vm = benchmark_spec.vm_groups['servers'][0]

  numa_pages_migrated, _ = redis_vm.RemoteCommand(
      'cat /proc/vmstat | grep numa_pages_migrated')
  numa_pages_migrated = numa_pages_migrated.split(' ')[1]
  numa_balancing, _ = redis_vm.RemoteCommand(
      'cat /proc/sys/kernel/numa_balancing')
  setup_metadata = {
      'numa_pages_migrated': numa_pages_migrated.rstrip(),
      'numa_balancing': numa_balancing.rstrip(),
  }

  results = redis_enterprise.Run(redis_vm, load_vms, REDIS_PORT)

  for result in results:
    result.metadata.update(setup_metadata)

  return results


def Cleanup(benchmark_spec):
  del benchmark_spec
