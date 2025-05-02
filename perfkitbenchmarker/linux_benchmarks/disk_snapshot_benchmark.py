# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs disk snapshot benchmarks."""

import logging
from typing import Any, Mapping

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import unmanaged_mysql_sysbench_benchmark
from perfkitbenchmarker.linux_packages import mysql80
from perfkitbenchmarker.linux_packages import sysbench


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'disk_snapshot'
BENCHMARK_CONFIG = """
disk_snapshot:
  description: Runs disk snapshot.
  vm_groups:
    server:
      vm_spec: *default_dual_core
      disk_spec:
        GCP:
          disk_size: 500
          disk_type: hyperdisk-balanced
          provisioned_iops: 3000
          provisioned_throughput: 750
          mount_point: /var/lib/mysql
        AWS:
          disk_size: 500
          disk_type: gp3
          provisioned_iops: 3000
          provisioned_throughput: 750
          mount_point: /var/lib/mysql
        Azure:
          disk_size: 500
          disk_type: PremiumV2_LRS
          provisioned_iops: 3000
          provisioned_throughput: 750
          mount_point: /var/lib/mysql
  flags:
    sysbench_table_size: 50000000
    sysbench_tables: 8
    sysbench_version: df89d34c410a2277e19f77e47e535d0890b2029b
    disk_fs_type: xfs
    # for now we only have mysql supported
    db_engine: mysql
    sysbench_report_interval: 1
    sysbench_ssl_mode: required
    sysbench_run_threads: 1,64,128,256,512,1024,2048
    sysbench_run_seconds: 300
"""


def GetConfig(user_config: Mapping[Any, Any]) -> Mapping[Any, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Install mysql and sysbench client to fill database on VM.

  Args:
    benchmark_spec: The benchmarks specification.
  """
  # Taken from unmanaged_mysql_sysbench_benchmark.Prepare()
  server_vm = benchmark_spec.vm_groups['server'][0]
  mysql80.ConfigureSystemSettings(server_vm)
  server_vm.Install('mysql80')
  buffer_pool_size = unmanaged_mysql_sysbench_benchmark.GetBufferPoolSize()

  new_password = FLAGS.run_uri + '_P3rfk1tbenchm4rker#'
  # mysql server ids needs to be positive integers.
  mysql80.ConfigureAndRestart(server_vm, buffer_pool_size, 1)
  mysql80.UpdatePassword(server_vm, new_password)
  mysql80.CreateDatabase(server_vm, new_password, 'sysbench')

  server_vm.InstallPackages('git')
  server_vm.Install('sysbench')
  sysbench_parameters = (
      unmanaged_mysql_sysbench_benchmark.GetSysbenchParameters(
          server_vm.internal_ip, new_password
      )
  )
  cmd = sysbench.BuildLoadCommand(sysbench_parameters)
  logging.info('%s load command: %s', FLAGS.sysbench_testname, cmd)
  server_vm.RemoteCommand(cmd)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the benchmark using the runner VM to manage snapshots."""
  vms = benchmark_spec.vm_groups['server']
  all_samples = []
  vm_samples = background_tasks.RunThreaded(CreateSnapshotOnVM, vms)
  for samples in vm_samples:
    all_samples.extend(samples)
  return all_samples


def CreateSnapshotOnVM(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
) -> list[sample.Sample]:
  """Create a snapshot on the given vm."""
  used_disk_size, _ = vm.RemoteCommand(
      'df -h | grep ".*/var/lib/mysql" | awk "{print $3}"'
  )
  logging.info('Create snapshot on %s', vm)
  background_tasks.RunThreaded(
      lambda disk: disk.CreateSnapshot(),
      vm.scratch_disks,
  )
  background_tasks.RunThreaded(
      lambda disk: disk.snapshot.Restore(), vm.scratch_disks
  )
  vm_samples = []
  for disk in vm.scratch_disks:
    vm_samples.append(
        sample.Sample(
            'snapshot_storage_compression_ratio',
            disk.snapshot.storage_gb / float(used_disk_size.strip().strip('G')),
            'ratio',
            {},
        )
    )
    vm_samples.append(
        sample.Sample(
            'snapshot_storage_gb',
            disk.snapshot.storage_gb,
            'GB',
            {},
        )
    )
    vm_samples.append(
        sample.Sample(
            'snapshot_creation_time',
            disk.snapshot.creation_end_time - disk.snapshot.creation_start_time,
            'seconds',
            {},
        )
    )
    vm_samples.append(
        sample.Sample(
            'snapshot_restore_time',
            disk.snapshot.restore_disk.create_disk_end_time
            - disk.snapshot.restore_disk.create_disk_start_time,
            'seconds',
            {},
        )
    )
  return vm_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup the benchmark."""
  del benchmark_spec
