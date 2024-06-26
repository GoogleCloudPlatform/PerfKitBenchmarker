# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

"""Sysbench Benchmark for unmanaged MySQL db on a VM.

This benchmark measures performance of Sysbench Databases on unmanaged MySQL.
"""

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import mysql80

FLAGS = flags.FLAGS


BENCHMARK_NAME = 'unmanaged_mysql_sysbench'
BENCHMARK_CONFIG = """
unmanaged_mysql_sysbench:
  description: Mysql on a VM benchmarked using Sysbench.
  vm_groups:
    client:
      os_type: centos_stream9
      vm_spec:
        GCP:
          machine_type: c3-standard-22
          zone: us-central1-b
    server:
      os_type: centos_stream9
      vm_spec:
        GCP:
          machine_type: c3-highmem-22
          zone: us-central1-b
      disk_spec:
        GCP:
          disk_size: 500
          disk_type: hyperdisk-balanced
          provisioned_iops: 80000
          provisioned_throughput: 2400
          num_striped_disks: 2
  flags:
    disk_fs_type: xfs
    # for now we only have mysql supported
    db_engine: mysql
"""

# There are 2 main customer scenarios:
# 1: 100G data set and all of that fits in memory,
# therefore only logging accesses disks.
# 2: 100G data and only 8G fits in memory,
# so both data access and logging access disks.
# Percona claims this is consistent with the alternative approach of
# increasing buffer pool and dataset to larger sizes
DEFAULT_BUFFER_POOL_SIZE = 8


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  # Instead of changing the default data dir of database in (multiple) configs,
  # Force the scratch disk as database default dir (simpler code).
  if (
      FLAGS.db_engine == 'mysql'
      or config['flags'].get('db_engine', None) == 'mysql'
  ):
    disk_spec = config['vm_groups']['server']['disk_spec']
    for cloud in disk_spec:
      disk_spec[cloud]['mount_point'] = '/var/lib/mysql'
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepare the servers and clients for the benchmark run.

  Args:
    benchmark_spec:
  """
  vms = benchmark_spec.vms
  # TODO(ruwa): add metadata for system settings.
  background_tasks.RunThreaded(mysql80.ConfigureSystemSettings, vms)
  background_tasks.RunThreaded(lambda vm: vm.Install('mysql80'), vms)

   # TODO(ruwa): add metadata for buffer pool size.
  buffer_pool_size = f'{DEFAULT_BUFFER_POOL_SIZE}G'
  if FLAGS.innodb_buffer_pool_size:
    buffer_pool_size = f'{FLAGS.innodb_buffer_pool_size}G'

  servers = benchmark_spec.vm_groups['server']
  for index, server in enumerate(servers):
    # mysql server ids needs to be positive integers.
    mysql80.ConfigureAndRestart(server, buffer_pool_size, index+1)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  del benchmark_spec
  # this is a work in progress.
  results = []
  return results


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  del benchmark_spec
