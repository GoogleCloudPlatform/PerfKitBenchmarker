# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

"""Sysbench Benchmark for unmanaged PostgreSQL db on a VM with LSSD prefilling.

This benchmark measures performance of Sysbench Databases on unmanaged
PostgreSQL, with FIO prefilling of the disk.
"""

import logging
from absl import flags
from perfkitbenchmarker.linux_benchmarks import lssd_workloads_util
from perfkitbenchmarker.linux_benchmarks import unmanaged_postgresql_sysbench_benchmark as base_benchmark


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'unmanaged_postgresql_sysbench_lssd'
BENCHMARK_CONFIG = """
unmanaged_postgresql_sysbench_lssd:
  description: PostgreSQL on a VM with Local SSDs benchmarked using Sysbench.
  vm_groups:
    client:
      vm_spec:
        GCP:
          machine_type: c3-highmem-22
    server:
      vm_spec:
        GCP:
          machine_type: z3-highmem-16-highlssd
          zone: us-east1-b
      disk_spec:
        GCP:
          disk_type: local # LSSD Default
          interface: NVME
          num_striped_disks: 2
  flags:
    sysbench_version: df89d34c410a2277e19f77e47e535d0890b2029b
    disk_fs_type: ext4 # LSSD standard
    db_engine: postgresql
    os_type: ubuntu2204
    skip_system_config: true
    sysbench_tables: 200
    sysbench_table_size: 60000000
    sysbench_report_interval: 1
    sysbench_run_threads: 128
    sysbench_run_seconds: 21600  # 6 hrs tests
"""


def GetConfig(user_config):
  """Get the benchmark config, applying user overrides."""
  return base_benchmark.GetConfig(
      user_config,
      benchmark_config=BENCHMARK_CONFIG,
      benchmark_name=BENCHMARK_NAME,
  )


def PrepareSystem(benchmark_spec):
  """Prepares the system for the benchmark.

  This includes calling the base PrepareSystem.

  Args:
    benchmark_spec: The benchmark specification.
  """
  # Call base PrepareSystem first, which applies settings and reboots the VM.
  base_benchmark.PrepareSystem(benchmark_spec)


def InstallPackages(benchmark_spec):
  """Install packages for the benchmark run."""
  base_benchmark.InstallPackages(benchmark_spec)
  server = benchmark_spec.vm_groups['server'][0]
  if lssd_workloads_util.LSSD_WORKLOAD_PRECONDITION_DISK.value:
    server.InstallPackages('fio')


def StartServices(benchmark_spec):
  """Start services for the benchmark run."""
  # Precondition the disk if required.
  server = benchmark_spec.vm_groups['server'][0]
  if lssd_workloads_util.LSSD_WORKLOAD_PRECONDITION_DISK.value:
    lssd_workloads_util.PreconditionDisk(server)
    lssd_workloads_util.CreateFileSystemAndMount(
        server, fs_type=FLAGS.disk_fs_type
    )

  # Call base StartServices to load DB data and start services
  base_benchmark.StartServices(benchmark_spec)
  logging.info('Checking disk space after database load...')
  data_dir = server.scratch_disks[0].mount_point
  stdout, _ = server.RemoteCommand(f'df -h {data_dir}')
  logging.info('Disk space usage for %s:\n%s', data_dir, stdout)


def Run(benchmark_spec):
  """Run the benchmark and publish results."""
  return base_benchmark.Run(benchmark_spec)


def Cleanup(benchmark_spec):
  """Cleanup the benchmark."""
  base_benchmark.Cleanup(benchmark_spec)
