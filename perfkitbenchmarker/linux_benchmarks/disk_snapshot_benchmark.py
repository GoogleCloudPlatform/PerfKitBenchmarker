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
          provisioned_iops: 16000
          provisioned_throughput: 800
          mount_point: /var/lib/mysql
        AWS:
          disk_size: 500
          disk_type: gp3
          provisioned_iops: 16000
          provisioned_throughput: 800
          mount_point: /var/lib/mysql
        Azure:
          disk_size: 500
          disk_type: PremiumV2_LRS
          provisioned_iops: 16000
          provisioned_throughput: 800
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
  benchmark_spec.vm_groups['client'] = benchmark_spec.vm_groups['server']
  unmanaged_mysql_sysbench_benchmark.Prepare(benchmark_spec)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the benchmark using the runner VM to manage snapshots."""
  vms = benchmark_spec.vm_groups['server']
  all_samples = []
  all_vm_samples_before_run = background_tasks.RunThreaded(
      lambda vm: CreateSnapshotOnVM(vm, 1), vms
  )
  for samples in all_vm_samples_before_run:
    all_samples.extend(samples)

  benchmark_spec.vm_groups['client'] = benchmark_spec.vm_groups['server']
  unmanaged_mysql_sysbench_benchmark.Run(benchmark_spec)
  all_vm_samples_after_run = background_tasks.RunThreaded(
      lambda vm: CreateSnapshotOnVM(vm, 2), vms
  )
  for samples in all_vm_samples_after_run:
    all_samples.extend(samples)

  return all_samples


def CreateSnapshotOnVM(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
    snapshot_num: int = 1,
) -> list[sample.Sample]:
  """Create a snapshot on the given vm."""
  used_disk_size, _ = vm.RemoteCommand(
      'df -h | grep ".*/var/lib/mysql" | awk \'{print $3}\''
  )
  logging.info('Create snapshot on %s', vm)
  background_tasks.RunThreaded(
      lambda disk: disk.CreateSnapshot(),
      vm.scratch_disks,
  )
  background_tasks.RunThreaded(
      lambda disk: disk.snapshots[-1].Restore(), vm.scratch_disks
  )
  vm_samples = []
  metadata = {'snapshot_number': snapshot_num}
  if 'G' not in used_disk_size.strip()[-1]:
    raise ValueError('Used disk size is not in GB.')
  full_snapshot_size_gb = float(used_disk_size.strip().strip('G'))
  for disk in vm.scratch_disks:
    snapshot_size = disk.snapshots[-1].storage_gb or full_snapshot_size_gb
    if snapshot_num > 1:
      snapshot_size = disk.GetLastIncrementalSnapshotSize()
    if snapshot_size:
      vm_samples.append(
          sample.Sample(
              'snapshot_storage_compression_ratio',
              snapshot_size / full_snapshot_size_gb,
              'ratio',
              metadata,
          )
      )
      # TODO(andytzhu) - Find incremental snapshot sizes on Azure.
      vm_samples.append(
          sample.Sample(
              'snapshot_storage_gb',
              snapshot_size,
              'GB',
              metadata,
          )
      )
    vm_samples.append(
        sample.Sample(
            'full_snapshot_size_gb',
            full_snapshot_size_gb,
            'GB',
            metadata,
        )
    )
    vm_samples.append(
        sample.Sample(
            'snapshot_creation_time',
            disk.snapshots[-1].creation_end_time
            - disk.snapshots[-1].creation_start_time,
            'seconds',
            metadata,
        )
    )
    vm_samples.append(
        sample.Sample(
            'snapshot_restore_time',
            disk.snapshots[-1].restore_disks[-1].create_disk_end_time
            - disk.snapshots[-1].restore_disks[-1].create_disk_start_time,
            'seconds',
            metadata,
        )
    )
  return vm_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup the benchmark."""
  del benchmark_spec
