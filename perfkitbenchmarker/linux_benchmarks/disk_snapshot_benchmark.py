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
import time
from typing import Any, Mapping

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import unmanaged_mysql_sysbench_benchmark
from perfkitbenchmarker.linux_benchmarks.fio import utils as fio_utils
from perfkitbenchmarker.linux_packages import fio


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
          disk_size: 200
          disk_type: hyperdisk-balanced
          provisioned_iops: 16000
          provisioned_throughput: 800
          mount_point: /var/lib/mysql
        AWS:
          disk_size: 200
          disk_type: gp3
          provisioned_iops: 16000
          provisioned_throughput: 800
          mount_point: /var/lib/mysql
        Azure:
          disk_size: 200
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
    fio_target_mode: against_device_without_fill
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
  background_tasks.RunThreaded(
      lambda vm: vm.Install('fio'), benchmark_spec.vm_groups['client']
  )


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


def MeasureRestoreSnapshotTime(
    source_disk: disk.BaseDisk,
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
) -> list[sample.Sample]:
  """Measure snapshot restore time to first byte and full restore time."""

  source_disk.snapshots[-1].Restore()
  latest_restore_disk = source_disk.snapshots[-1].restore_disks[-1]
  latest_restore_disk.Attach(vm)

  # TODO(andytzhu) - Refactor GetDevicePath disk methods.
  latest_restore_disk_device_path = latest_restore_disk.GetDevicePath() or ''
  if vm.CLOUD == 'AWS':
    nvme_devices = vm.GetNVMEDeviceInfo()
    for device in nvme_devices:
      if device['ModelNumber'] == 'Amazon Elastic Block Store':
        if device['DevicePath'] > latest_restore_disk_device_path:
          latest_restore_disk_device_path = device['DevicePath']

  vm.RemoteCommand(
      'sudo dd'
      f' if={latest_restore_disk_device_path} of=/dev/null'
      ' bs=4k count=1'
  )
  latest_restore_disk.restore_first_byte_end_time = time.time()

  latency_samples = MeasureLatestRestoreDiskLatency(vm)

  # Detach latest restore disk and attach another restore disk.
  latest_restore_disk.Detach()
  source_disk.snapshots[-1].num_detached_restore_disks += 1
  source_disk.snapshots[-1].Restore()
  latest_restore_disk = source_disk.snapshots[-1].restore_disks[-1]
  latest_restore_disk.Attach(vm)

  HydrateLatestRestoreDisk(vm)
  latest_restore_disk.Detach()
  source_disk.snapshots[-1].num_detached_restore_disks += 1

  return latency_samples


def HydrateLatestRestoreDisk(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
):
  """Hydrate the latest restore disk by reading all the data.

  Args:
    vm: The vm to run the fio command.
  """
  latest_restore_disk = vm.scratch_disks[-1].snapshots[-1].restore_disks[-1]
  fio_exe = fio.GetFioExec()
  latest_restore_disk_device_path = latest_restore_disk.GetDevicePath() or ''
  if vm.CLOUD == 'AWS':
    nvme_devices = vm.GetNVMEDeviceInfo()
    for device in nvme_devices:
      if device['ModelNumber'] == 'Amazon Elastic Block Store':
        if device['DevicePath'] > latest_restore_disk_device_path:
          latest_restore_disk_device_path = device['DevicePath']
  vm.RemoteCommand(
      f'{fio_exe} --filename={latest_restore_disk_device_path} --rw=read'
      ' --bs=1M --iodepth=64 --numjobs=4 --size=25% --offset_increment=25%'
      ' --ioengine=libaio --direct=1 --name=volume-initialize'
      ' --group_reporting'
  )
  latest_restore_disk.create_disk_full_end_time = time.time()


def MeasureLatestRestoreDiskLatency(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
) -> list[sample.Sample]:
  """Measure the latency of the latest restore disk.

  Args:
    vm: The vm to run the fio command.

  Returns:
    A list of sample.Sample.
  """
  latest_restore_disk = vm.scratch_disks[-1].snapshots[-1].restore_disks[-1]
  latest_restore_disk_device_path = latest_restore_disk.GetDevicePath() or ''
  if vm.CLOUD == 'AWS':
    nvme_devices = vm.GetNVMEDeviceInfo()
    for device in nvme_devices:
      if device['ModelNumber'] == 'Amazon Elastic Block Store':
        if device['DevicePath'] > latest_restore_disk_device_path:
          latest_restore_disk_device_path = device['DevicePath']

  benchmark_params = {
      'runtime': 30,
      'ramptime': 0,
  }
  job_file_string = fio_utils.GenerateJobFile(
      [latest_restore_disk],
      ['rand_4k_read_100%_iodepth-1_numjobs-1'],
      benchmark_params,
      device_path=latest_restore_disk_device_path,
  )
  samples = fio_utils.RunTest(
      vm,
      fio.GetFioExec(),
      job_file_string,
  )
  return samples


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

  vm_samples = []
  metadata = {'snapshot_number': snapshot_num}
  latency_samples = MeasureRestoreSnapshotTime(vm.scratch_disks[-1], vm)
  for latency_sample in latency_samples:
    latency_sample.metadata.update(metadata)
    vm_samples.append(latency_sample)

  if 'G' not in used_disk_size.strip()[-1]:
    raise ValueError('Used disk size is not in GB.')
  full_snapshot_size_gb = float(used_disk_size.strip().strip('G'))
  for scratch_disk in vm.scratch_disks:
    snapshot_size = (
        scratch_disk.snapshots[-1].storage_gb or full_snapshot_size_gb
    )
    if snapshot_num > 1:
      snapshot_size = scratch_disk.GetLastIncrementalSnapshotSize()
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
            scratch_disk.snapshots[-1].creation_end_time
            - scratch_disk.snapshots[-1].creation_start_time,
            'seconds',
            metadata,
        )
    )
    first_restore_disk = scratch_disk.snapshots[-1].restore_disks[-2]
    vm_samples.append(
        sample.Sample(
            'snapshot_restore_time_to_first_byte',
            first_restore_disk
            .restore_first_byte_end_time
            - first_restore_disk
            .create_disk_start_time,
            'seconds',
            metadata,
        )
    )
    latest_restore_disk = scratch_disk.snapshots[-1].restore_disks[-1]
    vm_samples.append(
        sample.Sample(
            'snapshot_full_restore_time',
            latest_restore_disk
            .create_disk_full_end_time
            - latest_restore_disk
            .create_disk_start_time,
            'seconds',
            metadata,
        )
    )
  return vm_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup the benchmark."""
  del benchmark_spec
