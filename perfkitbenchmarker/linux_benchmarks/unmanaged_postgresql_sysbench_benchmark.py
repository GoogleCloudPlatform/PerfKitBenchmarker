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

"""Sysbench Benchmark for unmanaged PostgreSQL db on a VM.

This benchmark measures performance of Sysbench Databases on unmanaged
postgreSQL.
"""

import copy
import logging
import re
import time

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import postgresql
from perfkitbenchmarker.linux_packages import sysbench


FLAGS = flags.FLAGS


BENCHMARK_NAME = 'unmanaged_postgresql_sysbench'
BENCHMARK_CONFIG = """
unmanaged_postgresql_sysbench:
  description: PostgreSQL on a VM benchmarked using Sysbench.
  vm_groups:
    client:
      vm_spec:
        GCP:
          machine_type: c3-standard-22
          zone: us-east1-b
        AWS:
          machine_type: m7i.4xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_D16s_v5
          zone: eastus
    server:
      vm_spec:
        GCP:
          machine_type: c3-standard-22
          zone: us-east1-b
        AWS:
          machine_type: r7i.4xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_E20s_v5
          zone: eastus
      disk_spec:
        GCP:
          disk_size: 500
          disk_type: hyperdisk-balanced
          provisioned_iops: 160000
          provisioned_throughput: 2400
          num_striped_disks: 1
        AWS:
          disk_size: 500
          disk_type: gp3
          provisioned_iops: 16000
          provisioned_throughput: 1000
          num_striped_disks: 5
        Azure:
          disk_size: 200
          disk_type: PremiumV2_LRS
          provisioned_iops: 40000
          provisioned_throughput: 800
          num_striped_disks: 2
  flags:
    sysbench_version: df89d34c410a2277e19f77e47e535d0890b2029b
    disk_fs_type: xfs
    db_engine: postgresql
    sysbench_report_interval: 1
    sysbench_ssl_mode: required
    sysbench_run_threads: 1,64,128,256,512,1024,2048
    sysbench_run_seconds: 300
    sysbench_load_threads: 128
    enable_transparent_hugepages: False
"""

# The database name is used to create a database on the server.
_DATABASE_TYPE = 'pgsql'
_DATABASE_NAME = 'sysbench'

# test names
_TPCC = 'percona_tpcc'
_OLTP_READ_WRITE = 'oltp_read_write'
_OLTP_READ_ONLY = 'oltp_read_only'
_OLTP_WRITE_ONLY = 'oltp_write_only'
_OLTP = [_OLTP_READ_WRITE, _OLTP_READ_ONLY, _OLTP_WRITE_ONLY]

_SHARED_BUFFER_SIZE = flags.DEFINE_string(
    'postgresql_shared_buffer_size',
    '10G',
    'Size of the shared buffer in the postgresql cluster.'
    'Format: <size>[<unit>], where <unit> is one of (B, K, M, G). '
    'Example: 16G, 512M. If no unit is specified, G is assumed by default.',
)


def _ValidateSharedBufferSizeFlagValue(value: str) -> bool:
  """Validates the shared buffer size flag's format."""
  # Checks for one or more digits, optionally followed by B, K, M, or G.
  return bool(re.fullmatch(r'^\d+[BKMG]?$', value))

flags.register_validator(
    _SHARED_BUFFER_SIZE,
    _ValidateSharedBufferSizeFlagValue,
    message=(
        '--postgresql_shared_buffer_size must be in the format <size>[<unit>] '
        'where <unit> is one of (B, K, M, G). Example: 16G, 512M, 1024K, 2048B.'
    )
)
_MEASURE_MAX_QPS = flags.DEFINE_bool(
    'postgresql_measure_max_qps',
    False,
    'Measure Max QPS of all the thread counts. Please set to'
    " false if you don't want to measure max qps.",
)
_CONF_TEMPLATE_PATH = flags.DEFINE_string(
    'postgresql_conf_template_path',
    'postgresql/postgresql-custom.conf.j2',
    'Path to the postgresql conf template file.',
)


def GetBufferSize() -> str:
  """Returns the buffer key for the given buffer size."""
  buffer_size = _SHARED_BUFFER_SIZE.value
  if buffer_size.endswith(
      ('B', 'K', 'M', 'G')
  ):
    return buffer_size
  return f'{buffer_size}G'


def GetConfig(user_config):
  """Get the benchmark config, applying user overrides.

  Args:
    user_config:

  Returns:
    Benchmark config.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  # Instead of changing the default data dir of database in (multiple) configs,
  # Force the scratch disk as database default dir (simpler code).
  disk_spec = config['vm_groups']['server']['disk_spec']
  for cloud in disk_spec:
    disk_spec[cloud]['mount_point'] = postgresql.GetOSDependentDefaults(
        FLAGS.os_type
    )['disk_mount_point']
  # Update machine type for server/client.
  if FLAGS.db_machine_type or FLAGS.db_zone:
    vm_spec = config['vm_groups']['server']['vm_spec']
    for cloud in vm_spec:
      if FLAGS.db_zone:
        vm_spec[cloud]['zone'] = FLAGS.db_zone[0]
      if FLAGS.db_machine_type:
        vm_spec[cloud]['machine_type'] = FLAGS.db_machine_type
  if FLAGS.client_vm_machine_type or FLAGS.client_vm_zone:
    vm_spec = config['vm_groups']['client']['vm_spec']
    for cloud in vm_spec:
      if FLAGS.client_vm_zone:
        vm_spec[cloud]['zone'] = FLAGS.client_vm_zone
      if FLAGS.client_vm_machine_type:
        vm_spec[cloud]['machine_type'] = FLAGS.client_vm_machine_type

  # Add replica servers if configured.
  if FLAGS.db_high_availability:
    for index, zone in enumerate(FLAGS.db_replica_zones):
      replica = copy.deepcopy(config['vm_groups']['server'])
      for cloud in replica['vm_spec']:
        replica['vm_spec'][cloud]['zone'] = zone
      config['vm_groups'][f'replica_{index}'] = replica
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepare the servers and clients for the benchmark run.

  Args:
    benchmark_spec:
  """
  vms = benchmark_spec.vms
  replica_servers = []
  for vm in benchmark_spec.vm_groups:
    if vm.startswith('replica'):
      replica_servers += benchmark_spec.vm_groups[vm]
  background_tasks.RunThreaded(postgresql.ConfigureSystemSettings, vms)
  background_tasks.RunThreaded(lambda vm: vm.Install('postgresql'), vms)

  primary_server = benchmark_spec.vm_groups['server'][0]
  postgresql.InitializeDatabase(primary_server)
  postgresql.ConfigureAndRestart(
      primary_server,
      FLAGS.run_uri,
      GetBufferSize(),
      _CONF_TEMPLATE_PATH.value,
  )
  for index, replica in enumerate(replica_servers):
    postgresql.SetupReplica(
        primary_server,
        replica,
        index,
        FLAGS.run_uri,
        GetBufferSize(),
        _CONF_TEMPLATE_PATH.value,
    )
  clients = benchmark_spec.vm_groups['client']
  for client in clients:
    client.InstallPackages('git')
    InstallSysbench(client)
    if FLAGS.sysbench_testname == _TPCC:
      client.RemoteCommand(
          'cd /opt && sudo rm -fr sysbench-tpcc && '
          f'sudo git clone {sysbench.SYSBENCH_TPCC_REPRO}'
      )
  loader_vm = benchmark_spec.vm_groups['client'][0]
  sysbench_parameters = _GetSysbenchParameters(
      primary_server.internal_ip,
      postgresql.GetPsqlUserPassword(FLAGS.run_uri),
  )
  cmd = sysbench.BuildLoadCommand(sysbench_parameters)
  logging.info('%s load command: %s', FLAGS.sysbench_testname, cmd)
  loader_vm.RemoteCommand(cmd)


def InstallSysbench(vm):
  args = {'db_driver': _DATABASE_TYPE}
  if vm.BASE_OS_TYPE == os_types.RED_HAT:
    sysbench.YumInstall(vm, args=args)
  else:
    sysbench.AptInstall(vm, args=args)


def _GetSysbenchParameters(primary_server_ip: str | None, password: str):
  """Get sysbench parameters from flags."""
  sysbench_parameters = sysbench.SysbenchInputParameters(
      db_driver=_DATABASE_TYPE,
      tables=FLAGS.sysbench_tables,
      threads=FLAGS.sysbench_load_threads,
      report_interval=FLAGS.sysbench_report_interval,
      db_user=_DATABASE_NAME,
      db_password=password,
      db_name=_DATABASE_NAME,
      host_ip=primary_server_ip,
  )
  sysbench_parameters.port = 5432
  test = FLAGS.sysbench_testname
  if test in _OLTP:
    sysbench_parameters.built_in_test = True
    sysbench_parameters.test = f'{sysbench.LUA_SCRIPT_PATH}{test}.lua'
    sysbench_parameters.db_ps_mode = 'disable'
    sysbench_parameters.skip_trx = True
    sysbench_parameters.table_size = FLAGS.sysbench_table_size

  elif test == _TPCC:
    sysbench_parameters.custom_lua_packages_path = '/opt/sysbench-tpcc/?.lua'
    sysbench_parameters.built_in_test = False
    sysbench_parameters.test = '/opt/sysbench-tpcc/tpcc.lua'
    sysbench_parameters.scale = FLAGS.sysbench_scale
    sysbench_parameters.use_fk = FLAGS.sysbench_use_fk
    sysbench_parameters.trx_level = FLAGS.sysbench_txn_isolation_level

  else:
    raise errors.Setup.InvalidConfigurationError(
        f'Test --sysbench_testname={FLAGS.sysbench_testname} is not supported.'
    )

  return sysbench_parameters


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the sysbench benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    Results.
  """
  primary_server = benchmark_spec.vm_groups['server'][0]
  client = benchmark_spec.vm_groups['client'][0]
  sysbench_parameters = _GetSysbenchParameters(
      primary_server.internal_ip,
      postgresql.GetPsqlUserPassword(FLAGS.run_uri),
  )
  results = []
  # a map of transaction metric name (tps/qps) to current sample with max value
  max_transactions = {}
  sorted_threads = sorted(FLAGS.sysbench_run_threads)
  previous_qps = 0
  reached_peak = False
  for i, thread_count in enumerate(sorted_threads):
    sysbench_parameters.threads = thread_count
    cmd = sysbench.BuildRunCommand(sysbench_parameters)
    logging.info('%s run command: %s', FLAGS.sysbench_testname, cmd)
    try:
      stdout, _ = client.RemoteCommand(
          cmd, timeout=2*FLAGS.sysbench_run_seconds,)
    except errors.VirtualMachine.RemoteCommandError as e:
      logging.exception('Failed to run sysbench command: %s', e)
      raise errors.Benchmarks.RunError(f'Error running sysbench command: {e}')
    metadata = sysbench.GetMetadata(sysbench_parameters)
    metadata.update({
        'shared_buffer_size': f'{GetBufferSize()}',
    })
    results += sysbench.ParseSysbenchTimeSeries(stdout, metadata)
    results += sysbench.ParseSysbenchLatency([stdout], metadata)
    current_transactions = sysbench.ParseSysbenchTransactions(stdout, metadata)
    results += current_transactions
    # max transactions stores the max tps/qps for all the thread counts.
    # update the max tps/qps in max_transactions.
    for item in current_transactions:
      metric = item.metric
      metric_value = item.value
      current_max_sample = max_transactions.get(metric, None)
      if not current_max_sample or current_max_sample.value < metric_value:
        max_transactions[metric] = item
    # store QPS at max threads
    # current_transactions is an array of two samples, tps and qps.
    current_qps = current_transactions[1].value
    if not reached_peak and current_qps < previous_qps:
      reached_peak = True
    # Sleep between runs if specified and not the last run
    if (
        sysbench.SYSBENCH_SLEEP_BETWEEN_RUNS_SEC.value > 0
        and i < len(sorted_threads) - 1
    ):
      logging.info(
          'Sleeping for %d seconds before the next run.',
          sysbench.SYSBENCH_SLEEP_BETWEEN_RUNS_SEC.value,
      )
      time.sleep(sysbench.SYSBENCH_SLEEP_BETWEEN_RUNS_SEC.value)
  # if we get max_qps at max thread_count, there is a possibility of a higher
  # qps at increased thread count. if --postgresql_measure_max_qps is set to
  # true, we want to make sure we achieve max QPS.
  if (
      _MEASURE_MAX_QPS.value
      and not reached_peak
  ):
    raise errors.Benchmarks.RunError(
        f'Max achieved at {sorted_threads[-1]} threads, possibility'
        ' of not enough client load. Consider using'
        ' --postgresql_measure_max_qps flag if you want to disable this check.'
    )
  if not results:
    raise errors.Benchmarks.RunError(
        'None of the sysbench tests were successful.'
    )
  # report the max tps/qps as a new metric.
  for item in max_transactions.values():
    metadata = copy.deepcopy(item.metadata)
    metadata['searched_thread_counts'] = FLAGS.sysbench_run_threads
    results.append(
        sample.Sample(
            'max_' + item.metric, item.value, item.unit, metadata=metadata
        )
    )
  # Copy client and server logs to the scratch directory.
  for _, vms in benchmark_spec.vm_groups.items():
    for vm in vms:
      vm.CopyLogs('/var/log')
  return results


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  del benchmark_spec
