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
from perfkitbenchmarker.linux_benchmarks import unmanaged_mysql_sysbench_benchmark as base_benchmark

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
  return base_benchmark.GetConfig(
      user_config,
      benchmark_config=BENCHMARK_CONFIG,
      benchmark_name=BENCHMARK_NAME,
  )


def PrepareSystem(benchmark_spec):
  """Prepares the system for the benchmark.

  This includes calling the base PrepareSystem, installing FIO, prefilling the
  disk, and reformatting it without discard.

  Args:
    benchmark_spec: The benchmark specification.
  """
  # 1. Call base PrepareSystem first, which applies settings and reboots the VM.
  base_benchmark.PrepareSystem(benchmark_spec)

  server = benchmark_spec.vm_groups['server'][0]
  logging.info('installing FIO on the server in PrepareSystem...')
  server.InstallPackages('fio')

  logging.info('Prefilling disk with FIO...')
  logging.info('Unmounting disk for raw FIO prefill...')
  server.RemoteCommand(f'sudo umount {MYSQL_DATA_DIR}', ignore_failure=True)

  disk = server.scratch_disks[0]

  # Use disk.GetDevicePath() to handle both single and striped disks correctly.
  fio_cmd = (
      'sudo fio --ioengine=libaio --direct=1 --rw=write --bs=1M '
      '--iodepth=64 --size=100% --group_reporting '
      f'--name=precondition --filename={disk.GetDevicePath()}'
  )
  logging.info('Running FIO command: %s', fio_cmd)
  stdout, stderr = server.RobustRemoteCommand(fio_cmd)
  logging.info('FIO Prefill Summary:\n%s', stdout)
  if stderr:
    logging.warning('FIO Prefill Warnings/Errors:\n%s', stderr)

  logging.info('Formatting disk %s without discard...', disk.GetDevicePath())
  # -K in mkfs.xfs means do not discard blocks at mkfs time
  server.RemoteCommand(f'sudo mkfs.xfs -f -K {disk.GetDevicePath()}')
  logging.info('Remounting disk with nodiscard...')
  server.MountDisk(
      disk.GetDevicePath(), MYSQL_DATA_DIR, mount_options='defaults,nodiscard'
  )


def InstallPackages(benchmark_spec):
  base_benchmark.InstallPackages(benchmark_spec)


def StartServices(benchmark_spec):
  # Call base StartServices to load DB data and start services
  base_benchmark.StartServices(benchmark_spec)
  server = benchmark_spec.vm_groups['server'][0]
  logging.info('Checking disk space after database load...')
  stdout, _ = server.RemoteCommand(f'df -h {MYSQL_DATA_DIR}')
  logging.info('Disk space usage for %s:\n%s', MYSQL_DATA_DIR, stdout)


def Run(benchmark_spec):
  return base_benchmark.Run(benchmark_spec)


def Cleanup(benchmark_spec):
  base_benchmark.Cleanup(benchmark_spec)
