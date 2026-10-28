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
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import vm_util
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
_NOFILE_LIMIT = 500000

# LSSD-specific MySQL and InnoDB tuning parameters applied to [mysqld] so
# PD-based benchmarks sharing ha.cnf.j2 remain unaffected.
_LSSD_MYSQLD_OVERRIDES = {
    'tmpdir': f'{MYSQL_DATA_DIR}/tmp',
    'innodb_tmpdir': f'{MYSQL_DATA_DIR}/tmp',
    'open_files_limit': str(_NOFILE_LIMIT),
    'table_open_cache': '250000',
    'table_definition_cache': '20000',
    'innodb_io_capacity': '20000',
    'innodb_io_capacity_max': '40000',
    'innodb_flush_neighbors': '0',
    'innodb_change_buffering': 'all',
    'innodb_change_buffer_max_size': '25',
    'innodb_buffer_pool_load_at_startup': '1',
    'innodb_buffer_pool_dump_at_shutdown': '1',
}


def GetConfig(user_config):
  """Get the benchmark config, applying user overrides."""
  return base_benchmark.GetConfig(
      user_config,
      benchmark_config=BENCHMARK_CONFIG,
      benchmark_name=BENCHMARK_NAME,
  )


def _ConfigureMysqlForLssd(
    server: linux_virtual_machine.BaseLinuxVirtualMachine, config_path: str
) -> None:
  """Applies LSSD-specific MySQL settings to mysqld.cnf."""
  server.RemoteCommand(f'sudo mkdir -p {MYSQL_DATA_DIR}/tmp')
  server.RemoteCommand(f'sudo chown mysql:mysql {MYSQL_DATA_DIR}/tmp')

  keys_pattern = '|'.join(_LSSD_MYSQLD_OVERRIDES.keys())
  server.RemoteCommand(
      f"sudo sed -i -E '/^({keys_pattern})\\s*=/d' {config_path}"
  )
  overrides_block = '\\n'.join(
      f'{k} = {v}' for k, v in _LSSD_MYSQLD_OVERRIDES.items()
  )
  server.RemoteCommand(
      f"sudo sed -i 's|^\\[mysql\\]|{overrides_block}\\n\\n[mysql]|'"
      f' {config_path}'
  )
  if not FLAGS.mysql_disable_log_bin:
    # Disable binary logging during bulk data loading so prepare-phase binlogs
    # do not exhaust local SSD capacity.
    server.RemoteCommand(
        "sudo sed -i 's|^enforce-gtid-consistency.*|"
        f"enforce-gtid-consistency\\ndisable_log_bin|' {config_path}"
    )


def _EnableBinlogAndRestartMysql(
    server: linux_virtual_machine.BaseLinuxVirtualMachine, config_path: str
) -> None:
  """Re-enables binary logging, raises nofile limit, and restarts mysqld."""
  logging.info('Enabling binary logging and restarting mysqld...')
  password = FLAGS.run_uri + '_P3rfk1tbenchm4rker#'
  socket_path = f'{MYSQL_DATA_DIR}/mysql.sock'
  if not FLAGS.mysql_disable_log_bin:
    server.RemoteCommand(f"sudo sed -i '/^disable_log_bin/d' {config_path}")
  server.RemoteCommand(
      f"sudo mysqladmin -u root -p'{password}' -S {socket_path} shutdown"
  )

  @vm_util.Retry(
      poll_interval=1,
      timeout=300,
      retryable_exceptions=(mysql80.MysqldFailedToStopError,),
  )
  def _WaitUntilMysqlStopped() -> None:
    _, _, retcode = server.RemoteCommandWithReturnCode(
        'pgrep -x mysqld', ignore_failure=True
    )
    if retcode == 0:
      raise mysql80.MysqldFailedToStopError('mysqld is still running.')

  _WaitUntilMysqlStopped()
  server.RemoteCommand(
      f"sudo sed -i 's/nofile.*/nofile {_NOFILE_LIMIT}/'"
      ' /etc/security/limits.conf'
  )
  server.RemoteCommand('sudo -g mysql -u mysql nohup mysqld &> /dev/null &')

  @vm_util.Retry(
      poll_interval=1,
      timeout=300,
      retryable_exceptions=(mysql80.MysqldFailedToStartError,),
  )
  def _WaitUntilMysqlReady() -> None:
    _, _, retcode = server.RemoteCommandWithReturnCode(
        f"sudo mysqladmin -u root -p'{password}' -S {socket_path} ping",
        ignore_failure=True,
    )
    if retcode != 0:
      raise mysql80.MysqldFailedToStartError('mysqld is not ready yet.')

  _WaitUntilMysqlReady()


def Prepare(benchmark_spec):
  """Prepares the system, installs packages and starts services.

  This benchmark deliberately implements a single Prepare instead of the
  split PrepareSystem / InstallPackages / StartServices phases, so it does
  NOT support building a reusable VM or container image from the prepare
  stage. The local SSDs must be preconditioned with FIO and reformatted
  before the mysql package is installed: the package creates its data
  directory on the scratch disk at install time and
  mysql80.ConfigureAndStartServer does not re-initialize the default datadir,
  so preconditioning afterwards wipes it and mysqld fails to start. A
  prebuilt image cannot guarantee that the disk it later runs on has been
  preconditioned, so the whole sequence has to run in one prepare phase on
  the VM under test.

  Args:
    benchmark_spec: The benchmark specification.
  """
  server = benchmark_spec.vm_groups['server'][0]

  # Apply system settings (and reboot the VM) first.
  base_benchmark.PrepareSystem(benchmark_spec)
  if lssd_workloads_util.LSSD_WORKLOAD_PRECONDITION_DISK.value:
    server.InstallPackages('fio')
    lssd_workloads_util.PreconditionDisk(server)
    lssd_workloads_util.CreateFileSystemAndMount(
        server, fs_type=FLAGS.disk_fs_type
    )

  # Install mysql on the final filesystem and apply LSSD configuration.
  base_benchmark.InstallPackages(benchmark_spec)
  config_path = mysql80.GetOSDependentDefaults(server.OS_TYPE)[
      mysql80.MYSQL_CONFIG_PATH
  ]
  _ConfigureMysqlForLssd(server, config_path)

  # Start mysqld and load the sysbench data.
  base_benchmark.StartServices(benchmark_spec)
  _EnableBinlogAndRestartMysql(server, config_path)

  logging.info('Checking disk space after database load...')
  stdout, _ = server.RemoteCommand(f'df -h {MYSQL_DATA_DIR}')
  logging.info('Disk space usage for %s:\n%s', MYSQL_DATA_DIR, stdout)


def Run(benchmark_spec):
  """Run the benchmark and publish results."""
  return base_benchmark.Run(benchmark_spec)


def Cleanup(benchmark_spec):
  """Cleanup the benchmark."""
  base_benchmark.Cleanup(benchmark_spec)
