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

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import postgresql16
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
          disk_type: pd-ssd
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
          disk_type: Premium_LRS_V2
          provisioned_iops: 40000
          provisioned_throughput: 800
          num_striped_disks: 2
  flags:
    sysbench_version: df89d34c410a2277e19f77e47e535d0890b2029b
    disk_fs_type: xfs
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

SHARED_BUFFER_SIZE = flags.DEFINE_integer(
    'postgresql_shared_buffer_size',
    10,
    'Size of the shared buffer in the postgresql cluster (in Gb).',
)


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
    disk_spec[cloud]['mount_point'] = postgresql16.GetOSDependentDefaults(
        FLAGS.os_type
    )['disk_mount_point']
  # Update machine type for server/client.
  if FLAGS.db_machine_type:
    vm_spec = config['vm_groups']['server']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.db_machine_type
  if FLAGS.client_vm_machine_type:
    vm_spec = config['vm_groups']['client']['vm_spec']
    for cloud in vm_spec:
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
  background_tasks.RunThreaded(postgresql16.ConfigureSystemSettings, vms)
  background_tasks.RunThreaded(lambda vm: vm.Install('postgresql16'), vms)

  primary_server = benchmark_spec.vm_groups['server'][0]
  postgresql16.InitializeDatabase(primary_server)
  postgresql16.ConfigureAndRestart(primary_server, FLAGS.run_uri)
  for index, replica in enumerate(replica_servers):
    postgresql16.SetupReplica(primary_server, replica, index, FLAGS.run_uri)
  clients = benchmark_spec.vm_groups['client']
  for client in clients:
    client.db_driver = _DATABASE_TYPE
    client.InstallPackages('git')
    client.Install('sysbench')
    if FLAGS.sysbench_testname == _TPCC:
      client.RemoteCommand(
          'cd /opt && sudo rm -fr sysbench-tpcc && '
          f'sudo git clone {sysbench.SYSBENCH_TPCC_REPRO}'
      )
  loader_vm = benchmark_spec.vm_groups['client'][0]
  sysbench_parameters = _GetSysbenchParameters(
      primary_server.internal_ip,
      postgresql16.GetPsqlUserPassword(FLAGS.run_uri),
  )
  cmd = sysbench.BuildLoadCommand(sysbench_parameters)
  logging.info('%s load command: %s', FLAGS.sysbench_testname, cmd)
  loader_vm.RemoteCommand(cmd)


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
      postgresql16.GetPsqlUserPassword(FLAGS.run_uri),
  )
  results = []
  # a map of transaction metric name (tps/qps) to current sample with max value
  max_transactions = {}
  for thread_count in FLAGS.sysbench_run_threads:
    sysbench_parameters.threads = thread_count
    stdout = sysbench.Run(client, sysbench_parameters)
    if not stdout:
      continue
    metadata = sysbench.GetMetadata(sysbench_parameters)
    metadata.update({
        'shared_buffer_size': f'{SHARED_BUFFER_SIZE.value}GB',
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
  return results


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  del benchmark_spec
