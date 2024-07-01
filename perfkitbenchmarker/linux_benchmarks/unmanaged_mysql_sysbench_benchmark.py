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

import copy
import logging
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import mysql80
from perfkitbenchmarker.linux_packages import sysbench

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

# The database name is used to create a database on the server.
_DATABASE_TYPE = 'mysql'
_DATABASE_NAME = 'sysbench'

# test names
_TPCC = 'percona_tpcc'
_OLTP = 'oltp_read_write'


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
    disk_spec[cloud]['mount_point'] = '/var/lib/mysql'
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
  # TODO(ruwa): add metadata for system settings.
  background_tasks.RunThreaded(mysql80.ConfigureSystemSettings, vms)
  background_tasks.RunThreaded(lambda vm: vm.Install('mysql80'), vms)

   # TODO(ruwa): add metadata for buffer pool size.
  buffer_pool_size = f'{DEFAULT_BUFFER_POOL_SIZE}G'
  if FLAGS.innodb_buffer_pool_size:
    buffer_pool_size = f'{FLAGS.innodb_buffer_pool_size}G'

  primary_server = benchmark_spec.vm_groups['server'][0]
  replica_servers = []
  for vm in benchmark_spec.vm_groups:
    if vm.startswith('replica'):
      replica_servers += benchmark_spec.vm_groups[vm]

  servers = [primary_server] + replica_servers
  new_password = FLAGS.run_uri + '_P3rfk1tbenchm4rker#'
  for index, server in enumerate(servers):
    # mysql server ids needs to be positive integers.
    mysql80.ConfigureAndRestart(server, buffer_pool_size, index + 1)
    mysql80.UpdatePassword(server, new_password)
    mysql80.CreateDatabase(server, new_password, _DATABASE_NAME)

  assert primary_server.internal_ip
  for replica in replica_servers:
    mysql80.SetupReplica(replica, new_password, primary_server.internal_ip)

  clients = benchmark_spec.vm_groups['client']
  for client in clients:
    client.InstallPackages('git')
    client.Install('sysbench')
    if FLAGS.sysbench_testname == _TPCC:
      client.RemoteCommand(
          'cd /opt && sudo rm -fr sysbench-tpcc && '
          f'sudo git clone {sysbench.SYSBENCH_TPCC_REPRO}')

  loader_vm = benchmark_spec.vm_groups['client'][0]
  if FLAGS.sysbench_testname == _OLTP:
    cmd = sysbench.BuildLoadCommand(
        built_in_test=True,
        test=f'{sysbench.LUA_SCRIPT_PATH}{_OLTP}.lua',
        db_driver=_DATABASE_TYPE,
        db_ps_mode='disable',  # prepared statements usage mode.
        tables=FLAGS.sysbench_tables,
        table_size=FLAGS.sysbench_table_size,
        report_interval=FLAGS.sysbench_report_interval,
        threads=FLAGS.sysbench_load_threads,
        db_user=_DATABASE_NAME,
        db_password=new_password,
        db_name=_DATABASE_NAME,
        host_ip=primary_server.internal_ip,
        ssl_setting=sysbench.SYSBENCH_SSL_MODE.value)
    logging.info('OLTP load command: %s', cmd)
    loader_vm.RemoteCommand(cmd)
  elif FLAGS.sysbench_testname == _TPCC:
    cmd = sysbench.BuildLoadCommand(
        custom_lua_packages_path='/opt/sysbench-tpcc/?.lua',
        built_in_test=False,
        test='/opt/sysbench-tpcc/tpcc.lua',
        db_driver=_DATABASE_TYPE,
        tables=FLAGS.sysbench_tables,
        scale=FLAGS.sysbench_scale,
        report_interval=FLAGS.sysbench_report_interval,
        threads=FLAGS.sysbench_load_threads,
        use_fk=FLAGS.sysbench_use_fk,
        trx_level=FLAGS.sysbench_txn_isolation_level,
        db_user=_DATABASE_NAME,
        db_password=new_password,
        db_name=_DATABASE_NAME,
        host_ip=primary_server.internal_ip,
        ssl_setting=sysbench.SYSBENCH_SSL_MODE.value)
    logging.info('TPCC load command: %s', cmd)
    loader_vm.RemoteCommand(cmd)
  else:
    raise errors.Setup.InvalidConfigurationError(
        f'Test --sysbench_testname={FLAGS.sysbench_testname} is not supported.'
    )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  del benchmark_spec
  # this is a work in progress.
  results = []
  return results


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  del benchmark_spec
