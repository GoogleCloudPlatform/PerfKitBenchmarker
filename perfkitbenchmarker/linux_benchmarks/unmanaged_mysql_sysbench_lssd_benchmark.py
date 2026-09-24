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

"""Sysbench Benchmark for unmanaged MySQL db on a VM with LSSD prefilling.

This benchmark measures performance of Sysbench Databases on unmanaged MySQL,
with FIO prefilling of the disk.
"""

import logging
from absl import flags
from perfkitbenchmarker.linux_benchmarks import lssd_workloads_util
from perfkitbenchmarker.linux_benchmarks import unmanaged_mysql_sysbench_benchmark as base_benchmark
from perfkitbenchmarker.linux_packages import mysql80

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'unmanaged_mysql_sysbench_lssd'
BENCHMARK_CONFIG = (
    base_benchmark.BENCHMARK_CONFIG.replace(
        'unmanaged_mysql_sysbench:', 'unmanaged_mysql_sysbench_lssd:'
    )
    .replace(
        'sysbench_run_threads: 1,64,128,256,512,1024,2048',
        'sysbench_run_threads: 256',
    )
    .replace('sysbench_run_seconds: 300', 'sysbench_run_seconds: 21600')
)
MYSQL_DATA_DIR = '/var/lib/mysql'


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

  config_path = mysql80.GetOSDependentDefaults(server.OS_TYPE)[  # pyrefly: ignore[missing-argument]
      mysql80.MYSQL_CONFIG_PATH
  ]
  server.RemoteCommand(f'sudo mkdir -p {MYSQL_DATA_DIR}/tmp')
  server.RemoteCommand(f'sudo chown mysql:mysql {MYSQL_DATA_DIR}/tmp')
  server.RemoteCommand(
      f"sudo sed -i 's|^tmpdir.*|tmpdir = {MYSQL_DATA_DIR}/tmp\\ninnodb_tmpdir"
      f" = {MYSQL_DATA_DIR}/tmp|' {config_path}"
  )


def StartServices(benchmark_spec):
  """Start services for the benchmark run."""
  server = benchmark_spec.vm_groups['server'][0]
  if lssd_workloads_util.LSSD_WORKLOAD_PRECONDITION_DISK.value:
    lssd_workloads_util.PreconditionDisk(server)
    lssd_workloads_util.CreateFileSystemAndMount(
        server, fs_type=FLAGS.disk_fs_type
    )

  # Call base StartServices to load DB data and start services
  base_benchmark.StartServices(benchmark_spec)
  logging.info('Checking disk space after database load...')
  stdout, _ = server.RemoteCommand(f'df -h {MYSQL_DATA_DIR}')
  logging.info('Disk space usage for %s:\n%s', MYSQL_DATA_DIR, stdout)


def Run(benchmark_spec):
  """Run the benchmark and publish results."""
  return base_benchmark.Run(benchmark_spec)


def Cleanup(benchmark_spec):
  """Cleanup the benchmark."""
  base_benchmark.Cleanup(benchmark_spec)
