# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Sysbench Benchmark.

This is a set of benchmarks that measures performance of Sysbench Databases on
  managed MySQL or Postgres.

As other cloud providers deliver a managed MySQL service, we will add it here.
"""

import datetime
import logging
import time
from typing import List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import sysbench

FLAGS = flags.FLAGS

# The default values for flags and BENCHMARK_CONFIG are not a recommended
# configuration for comparing sysbench performance.  Rather these values
# are set to provide a quick way to verify functionality is working.
# A broader set covering different permuations on much larger data sets
# is prefereable for comparison.
flags.DEFINE_string(
    'sysbench_testname',
    'oltp_read_write',
    'The built in oltp lua script to run',
)
flags.DEFINE_integer(
    'sysbench_tables', 4, 'The number of tables used in sysbench oltp.lua tests'
)

flags.DEFINE_integer(
    'sysbench_table_size',
    100000,
    'The number of rows of each table used in the oltp tests',
)
flags.DEFINE_integer(
    'sysbench_scale', 100, 'Scale parameter as used by TPCC benchmark.'
)
_SLEEP_SEC = flags.DEFINE_integer(
    'sysbench_sleep_after_load_sec', 0, 'The time to sleep after loading data.'
)
flags.DEFINE_integer(
    'sysbench_warmup_seconds',
    10,
    'The duration of the warmup run in which results are '
    'discarded, in seconds.',
)
_RUN_DURATION = flags.DEFINE_integer(
    'sysbench_run_seconds',
    10,
    'The duration of the actual run in which results are '
    'collected, in seconds.',
)
_LOAD_CLIENTS = flags.DEFINE_integer(
    'sysbench_load_client_vms',
    None,
    'The number of client vms used in the prepare phase',
)

_LOAD_THREADS = flags.DEFINE_integer(
    'sysbench_load_threads',
    64,
    'Number of threads (per client VM) to use for loading.',
)
flag_util.DEFINE_integerlist(
    'sysbench_run_threads',
    flag_util.IntegerList([64]),
    'array of thread counts passed to sysbench, one at a time',
    module_name=__name__,
)
flags.DEFINE_integer(
    'sysbench_latency_percentile',
    100,
    'The latency percentile we ask sysbench to compute.',
)

flags.DEFINE_integer(
    'sysbench_report_interval',
    2,
    'The interval, in seconds, we ask sysbench to report results.',
)
flags.DEFINE_boolean(
    'sysbench_use_fk', True, 'Use foreign keys. This is used by TPCC benchmark.'
)
_SPANNER_PG_COMPAT_MODE = flags.DEFINE_boolean(
    'sysbench_spanner_pg_compat_mode',
    True,
    'If true, uses postgres-compatible benchmark script. Only used if'
    ' --sysbench_testname=spanner_tpcc.',
)
_TXN_ISOLATION_LEVEL = flags.DEFINE_enum(
    'sysbench_txn_isolation_level',
    'SER',
    ['SER', 'RR', 'RC'],
    'If true, uses postgres-compatible benchmark script. Only used if'
    ' --sysbench_testname=spanner_tpcc.',
)
_SKIP_LOAD_STAGE = flags.DEFINE_boolean(
    'sysbench_skip_load_stage',
    False,
    'If true, skips the loading stage of the benchmark. Useful for when '
    'testing on a long-lived or static instance where the database has already '
    'been loaded on a previous run.',
)

_AUTO_INCREMENT = flags.DEFINE_boolean(
    'sysbench_auto_inc', True, 'Auto increment for sysbench'
)

_SCALE_UP_MAX_CPU_UTILIZATION = flags.DEFINE_float(
    'sysbench_scale_up_max_cpu_utilization',
    0.95,
    'Stop the auto scale up experiment when we reach this cpu utilization',
)

# See https://github.com/Percona-Lab/sysbench-tpcc/releases for the most
# up to date version.
_SYSBENCH_TPCC_TAR = 'sysbench-tpcc.tar.gz'
BENCHMARK_DATA = {
    _SYSBENCH_TPCC_TAR: (
        '564600d6c296ef1cd88a07eeaf40bbc58688dbdc7c58fc1a0d28bb2b41c30611'
    ),
}

_SCALEUP_CLIENTS_TEST = flags.DEFINE_boolean(
    'sysbench_scaleup_clients_test',
    False,
    'Scale up the number of clients running the benchmark. This is a benchmark'
    ' mode to test how the server reacts when the load increases gradually by'
    ' having more clients over time. Need to override'
    ' sysbench.relational_db.vm_groups.clients.vm_count and the number have to'
    ' be creater than sysbench_scaleup_clients_test_num_clients for this'
    ' benchmark mode to work',
)

_SCALEUP_CLIENTS_TEST_NUM_CLIENTS = flags.DEFINE_integer(
    'sysbench_scaleup_clients_test_num_clients',
    1,
    'This defines the number of client we scale up to. Have to be smaller than'
    ' the number of existing clients.',
)

SPANNER_TPCC = 'spanner-tpcc'

# Parameters are defined in oltp_common.lua file
# https://github.com/akopytov/sysbench
_MAP_WORKLOAD_TO_VALID_UNIQUE_PARAMETERS = {
    'tpcc': set(['scale']),
    SPANNER_TPCC: set(['scale']),
    'oltp_write_only': set(['table_size', 'auto-inc']),
    'oltp_read_only': set(['table_size', 'auto-inc']),
    'oltp_read_write': set(['table_size', 'auto-inc']),
    'oltp_insert': set(['table_size', 'auto-inc']),
}


BENCHMARK_NAME = 'sysbench'
BENCHMARK_CONFIG = """
sysbench:
  description: Sysbench OLTP benchmarks.
  relational_db:
    engine: mysql
    enable_freeze_restore: True
    db_spec:
      GCP:
        machine_type: db-n1-standard-16
        zone: us-central1-c
      AWS:
        machine_type: db.m4.4xlarge
        zone: us-west-1a
      Azure:
        machine_type: GP_Gen5_2
        zone: westus
    db_disk_spec:
      GCP:
        disk_size: 100
        disk_type: pd-ssd
      AWS:
        disk_size: 6144
        disk_type: gp2
      Azure:
        #From AZ command line:
        #Valid storage sizes range from minimum of 128000 MB and additional
        #increments of 128000 MB up to maximum of 1024000 MB.
        disk_size: 128
    vm_groups:
      clients:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-west-1a
          Azure:
            machine_type: Standard_B4ms
            zone: westus
        disk_spec:
          GCP:
            disk_size: 500
            disk_type: pd-ssd
          AWS:
            disk_size: 500
            disk_type: gp2
          Azure:
            disk_size: 500
            disk_type: Premium_LRS
      servers:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-west-1a
          Azure:
            machine_type: Standard_B4ms
            zone: westus
        disk_spec: *default_500_gb
      replications:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-b
          AWS:
            machine_type: m4.4xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_B4ms
            zone: eastus
        disk_spec: *default_500_gb
"""

# Constants defined for Sysbench tests.
DISABLE = 'disable'
UNIFORM = 'uniform'

SECONDS_UNIT = 'seconds'


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  vm_count = config['relational_db']['vm_groups']['clients'].get('vm_count', 1)
  if vm_count > 1 and FLAGS.sysbench_testname != SPANNER_TPCC:
    raise errors.Setup.InvalidConfigurationError(
        f'Test --sysbench_testname={FLAGS.sysbench_testname} only supports 1'
        f' client VM, got {vm_count}. Currently only {SPANNER_TPCC} is'
        ' supported.'
    )
  return config


# TODO(chunla) Move this to engine specific module
def _GetCommonSysbenchOptions(db: relational_db.BaseRelationalDb):
  """Get Sysbench options."""
  engine_type = db.engine_type
  result = []

  if engine_type == sql_engine_utils.MYSQL:
    result.append('--db-driver=mysql')
    # Ignore possible mysql errors when running OLTP
    # https://github.com/actiontech/dble/issues/458
    # https://callisto.digital/posts/tools/using-sysbench-to-benchmark-mysql-5-7/
    if _GetSysbenchTestParameter() != 'tpcc':
      result += [
          '--db-ps-mode=%s' % DISABLE,
          # Error 1205: Lock wait timeout exceeded
          # Could happen when we overload the database
          '--mysql-ignore-errors=1213,1205,1020,2013',
      ]
  elif engine_type in [
      sql_engine_utils.POSTGRES,
      sql_engine_utils.SPANNER_POSTGRES,
  ]:
    result += [
        '--db-driver=pgsql',
    ]

  result += [db.client_vm_query_tools.GetSysbenchConnectionString()]
  return result


def CreateMetadataFromFlags():
  """Create meta data with all flags for sysbench."""
  metadata = {
      'sysbench_testname': FLAGS.sysbench_testname,
      'sysbench_tables': FLAGS.sysbench_tables,
      'sysbench_table_size': FLAGS.sysbench_table_size,
      'sysbench_scale': FLAGS.sysbench_scale,
      'sysbench_warmup_seconds': FLAGS.sysbench_warmup_seconds,
      'sysbench_run_seconds': FLAGS.sysbench_run_seconds,
      'sysbench_latency_percentile': FLAGS.sysbench_latency_percentile,
      'sysbench_report_interval': FLAGS.sysbench_report_interval,
  }
  if FLAGS.sysbench_testname == SPANNER_TPCC:
    metadata['sysbench_use_fk'] = FLAGS.sysbench_use_fk
  return metadata


def _GetSysbenchTestParameter() -> str:
  return (
      'tpcc'
      if FLAGS.sysbench_testname == SPANNER_TPCC
      else FLAGS.sysbench_testname
  )


def _GetDatabaseName(db: relational_db.BaseRelationalDb) -> str:
  """Returns the database name to use in this test."""
  return db.database if hasattr(db, 'database') else 'sbtest'


def _InstallLuaScriptsIfNecessary(vm):
  """Installs the lua scripts if necessary."""
  if _GetSysbenchTestParameter() == 'tpcc':
    vm.InstallPreprovisionedBenchmarkData(
        BENCHMARK_NAME, [_SYSBENCH_TPCC_TAR], '~'
    )
    vm.RemoteCommand(
        f'tar -zxvf {_SYSBENCH_TPCC_TAR} --strip-components 1'
        f' -C {sysbench.SYSBENCH_DIR}'
    )

    vm.PushDataFile(
        'sysbench/default_tpcc_common.lua',
        f'{sysbench.SYSBENCH_DIR}/tpcc_common.lua',
    )
  if FLAGS.sysbench_testname == SPANNER_TPCC:
    vm.PushDataFile(
        'sysbench/spanner_pg_tpcc_common.lua',
        f'{sysbench.SYSBENCH_DIR}/tpcc_common.lua',
    )
    vm.PushDataFile(
        'sysbench/spanner_pg_tpcc_run.lua',
        f'{sysbench.SYSBENCH_DIR}/tpcc_run.lua',
    )
    vm.PushDataFile(
        'sysbench/spanner_pg_tpcc.lua', f'{sysbench.SYSBENCH_DIR}/tpcc.lua'
    )


def _IsValidFlag(flag):
  return (
      flag in _MAP_WORKLOAD_TO_VALID_UNIQUE_PARAMETERS[FLAGS.sysbench_testname]
  )


def _GetSysbenchPrepareCommand(
    db: relational_db.BaseRelationalDb, num_vms: int, vm_index: int
) -> str:
  """Returns the sysbench command used to load the database."""
  data_load_cmd_tokens = [
      'cd ~/sysbench/ && nice',  # run with a niceness of lower priority
      '-15',  # to encourage cpu time for ssh commands
      'sysbench',
      _GetSysbenchTestParameter(),
      '--tables=%d' % FLAGS.sysbench_tables,
      (
          '--table_size=%d' % FLAGS.sysbench_table_size
          if _IsValidFlag('table_size')
          else ''
      ),
      ('--scale=%d' % FLAGS.sysbench_scale if _IsValidFlag('scale') else ''),
      '--threads=%d' % _LOAD_THREADS.value,
  ]

  if _IsValidFlag('auto-inc'):
    if _AUTO_INCREMENT.value:
      data_load_cmd_tokens.append('--auto-inc=on')
    else:
      data_load_cmd_tokens.append('--auto-inc=off')
  if FLAGS.sysbench_testname == SPANNER_TPCC:
    # Supports loading through multiple VMs
    scale = FLAGS.sysbench_scale
    start_scale = (scale / num_vms) * vm_index + 1
    end_scale = (scale / num_vms) * (vm_index + 1)
    data_load_cmd_tokens.extend([
        '--use_fk=%d' % (1 if FLAGS.sysbench_use_fk else 0),
        '--enable_cluster=%d' % (0 if num_vms == 1 else 1),
        '--start_scale=%d' % start_scale,
        '--end_scale=%d' % end_scale,
        '--enable_pg_compat_mode=%d'
        % (1 if _SPANNER_PG_COMPAT_MODE.value else 0),
    ])
  if (
      db.engine_type == sql_engine_utils.SPANNER_POSTGRES
      and FLAGS.sysbench_testname != SPANNER_TPCC
  ):
    table_size = FLAGS.sysbench_table_size
    fill_table_size = table_size / num_vms
    start_index = fill_table_size * vm_index + 1

    data_load_cmd_tokens.extend([
        '--start_index=%d' % start_index,
        '--fill_table_size=%d' % fill_table_size,
        '--create_secondary=false',
        '--create_tables=false',
    ])
  return ' '.join(
      data_load_cmd_tokens + _GetCommonSysbenchOptions(db) + ['prepare']
  )


def _LoadDatabase(
    command: str, vm: virtual_machine.BaseVirtualMachine
) -> tuple[str, str]:
  stdout, stderr = vm.RobustRemoteCommand(command)
  for output in (stdout, stderr):
    if 'FATAL' in output:
      raise errors.Benchmarks.RunError(
          f'Error while running prepare command: {command}\n{output}'
      )
  return stdout, stderr


def _LoadDatabaseInParallel(
    db: relational_db.BaseRelationalDb,
    client_vms: list[virtual_machine.VirtualMachine],
) -> list[sample.Sample]:
  """Loads the database using the sysbench prepare command."""
  if _LOAD_CLIENTS.value:
    client_vms = client_vms[: _LOAD_CLIENTS.value]
  db.UpdateCapacityForLoad()

  if (
      FLAGS.sysbench_testname != SPANNER_TPCC
      and db.engine_type == sql_engine_utils.SPANNER_POSTGRES
  ):
    client_vms[0].RobustRemoteCommand(
        f'cd ~/sysbench/ && nice -15 sysbench {FLAGS.sysbench_testname}'
        f' --tables={FLAGS.sysbench_tables} --table_size=0 '
        ' --threads=20 --auto-inc=off '
        '--create_secondary=false --db-driver=pgsql'
        ' --pgsql-host=/tmp prepare'
    )
  _UpdateSessions(db, _LOAD_THREADS.value)
  # Provision the Sysbench test based on the input flags (load data into DB)
  # Could take a long time if the data to be loaded is large.
  data_load_start_time = time.time()

  # Sysbench output is in stdout, but we also get stderr just in case
  # something went wrong.
  command_vm_pairs = [
      (_GetSysbenchPrepareCommand(db, len(client_vms), index), vm)
      for index, vm in enumerate(client_vms)
  ]
  args = [(command_vm_pair, {}) for command_vm_pair in command_vm_pairs]
  results = background_tasks.RunThreaded(_LoadDatabase, args)
  load_duration = time.time() - data_load_start_time
  logging.info(
      'It took %d seconds to finish the data loading step', load_duration
  )
  for _, stderr in results:
    if 'FATAL' in stderr:
      raise errors.Benchmarks.RunError('Error while running prepare phase')

  if (
      FLAGS.sysbench_testname != SPANNER_TPCC
      and db.engine_type == sql_engine_utils.SPANNER_POSTGRES
  ):
    # This command update the secondary index
    # Run all index update in parallel.
    client_vms[0].RobustRemoteCommand(
        'cd ~/sysbench/ && nice -15 sysbench oltp_read_only'
        f' --tables={FLAGS.sysbench_tables} --table_size=0 '
        f' --threads={FLAGS.sysbench_tables} --auto-inc=off '
        '--create_secondary=true --db-driver=pgsql'
        ' --pgsql-host=/tmp prepare'
    )

  db.UpdateCapacityForRun()

  return [
      sample.Sample(
          'sysbench data load time',
          load_duration,
          SECONDS_UNIT,
          CreateMetadataFromFlags(),
      )
  ]


def _PrepareClients(
    db: relational_db.BaseRelationalDb,
    client_vms: list[virtual_machine.VirtualMachine],
) -> None:
  """Installs the relevant packages on the clients."""
  # Setup common test tools required on the client VM
  # Run app install to force reinstalling sysbench.
  spanner_oltp = (
      db.engine_type == sql_engine_utils.SPANNER_POSTGRES
      and FLAGS.sysbench_testname != SPANNER_TPCC
  )
  if spanner_oltp:
    background_tasks.RunThreaded(
        lambda vm: sysbench.AptInstall(
            vm,
            spanner_oltp=spanner_oltp,
        ),
        client_vms,
    )
  else:
    background_tasks.RunThreaded(
        lambda vm: vm.Install('sysbench'),
        client_vms,
    )
  background_tasks.RunThreaded(_InstallLuaScriptsIfNecessary, client_vms)
  if (
      db.engine_type == sql_engine_utils.SPANNER_POSTGRES
      and FLAGS.sysbench_testname != SPANNER_TPCC
  ):
    background_tasks.RunThreaded(
        lambda client_query_tools: client_query_tools.InstallPackages(),
        db.client_vms_query_tools,
    )

  # Some databases install these query tools during _PostCreate, which is
  # skipped if the database is user managed / restored.
  if db.user_managed or db.restored:
    background_tasks.RunThreaded(
        lambda client_query_tools: client_query_tools.InstallPackages(),
        db.client_vms_query_tools,
    )


def _PrepareDatabase(db: relational_db.BaseRelationalDb) -> None:
  """Creates the actual database used for the test, sbtest by default."""
  db_name = _GetDatabaseName(db)

  # Recreate the DB if needed. Not applicable on a fresh run, but helps with
  # manual development.
  try:
    db.DeleteDatabase(db_name)
  except (
      errors.VirtualMachine.RemoteCommandError,
      errors.VmUtil.IssueCommandError,
  ):
    logging.warning('Error dropping database, it may not exist.')

  stdout, stderr = db.CreateDatabase(db_name)
  logging.info(
      '%s db created, stdout is %s, stderr is %s', db_name, stdout, stderr
  )


def Prepare(benchmark_spec) -> List[sample.Sample]:
  """Prepares the DB instance and configures it.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of load samples.
  """
  client_vms = benchmark_spec.vm_groups['clients']
  db: relational_db.BaseRelationalDb = benchmark_spec.relational_db

  _PrepareClients(db, client_vms)

  if _SKIP_LOAD_STAGE.value or db.restored:
    logging.info('Skipping the load stage')
    return []

  _PrepareDatabase(db)
  load_samples = _LoadDatabaseInParallel(db, client_vms)
  if _SLEEP_SEC.value:
    logging.info(
        'Sleeping for %d seconds now that loading has finished.',
        _SLEEP_SEC.value,
    )
  return load_samples


def _GetDatabaseSize(db):
  """Get the size of the database in MB."""
  db_engine_type = db.engine_type
  if db_engine_type == sql_engine_utils.MYSQL:
    stdout, _ = db.client_vm_query_tools.IssueSqlCommand(
        "SELECT table_schema AS 'Database', "
        'ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) '
        "AS 'Size (MB)' "
        'FROM information_schema.TABLES '
        'GROUP BY table_schema; '
    )
    logging.info('Query database size results: \n%s', stdout)
    # example stdout is tab delimited but shown here with spaces:
    # Database  Size (MB)
    # information_schema  0.16
    # mysql 5.53
    # performance_schema  0.00
    # sbtest  0.33
    size_mb = 0
    for line in stdout.splitlines()[1:]:
      _, word_size_mb = line.split()
      size_mb += float(word_size_mb)

  elif db_engine_type == sql_engine_utils.POSTGRES:
    stdout, _ = db.client_vm_query_tools.IssueSqlCommand(
        r'SELECT pg_database_size(' "'sbtest'" ')/1024/1024'
    )
    size_mb = int(stdout.split()[2])

  # Spanner doesn't yet support pg_database_size.
  # See https://cloud.google.com/spanner/quotas#instance_limits. Spanner
  # supports 4TB per node, so use that number for now.
  elif db_engine_type == sql_engine_utils.SPANNER_POSTGRES:
    size_mb = 4096000 * db.nodes
  else:
    raise errors.Benchmarks.RunError(
        'Unsupported engine type, please update'
        ' sysbench_benchmark._GetDatabaseSize.'
    )
  return size_mb


def _GetSysbenchRunCommand(
    duration: int,
    db: relational_db.BaseRelationalDb,
    sysbench_thread_count: int,
):
  """Returns the sysbench command as a string."""
  if duration <= 0:
    raise ValueError('Duration must be greater than zero.')

  run_cmd_tokens = [
      'nice',  # run with a niceness of lower priority
      '-15',  # to encourage cpu time for ssh commands
      'sysbench',
      _GetSysbenchTestParameter(),
      '--tables=%d' % FLAGS.sysbench_tables,
      (
          '--table_size=%d' % FLAGS.sysbench_table_size
          if _IsValidFlag('table_size')
          else ''
      ),
      ('--scale=%d' % FLAGS.sysbench_scale if _IsValidFlag('scale') else ''),
      '--rand-type=%s' % UNIFORM,
      '--threads=%d' % sysbench_thread_count,
      '--percentile=%d' % FLAGS.sysbench_latency_percentile,
      '--report-interval=%d' % FLAGS.sysbench_report_interval,
      '--max-requests=0',
      '--time=%d' % duration,
  ]
  if _GetSysbenchTestParameter() == 'tpcc':
    run_cmd_tokens.append('--trx_level=%s' % _TXN_ISOLATION_LEVEL.value)
  run_cmd = ' '.join(run_cmd_tokens + _GetCommonSysbenchOptions(db) + ['run'])
  run_cmd = 'cd ~/sysbench/ && ' + run_cmd
  return run_cmd


def _IssueSysbenchCommand(vm, duration, benchmark_spec, sysbench_thread_count):
  """Issues a sysbench run command given a vm and a duration.

      Does nothing if duration is <= 0

  Args:
    vm: The test VM to issue command to.
    duration: the duration of the sysbench run.
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
    sysbench_thread_count: count of number of threads to use in --threads
      parameter to sysbench.

  Returns:
    stdout, stderr: the result of the command.
  """
  stdout = ''
  stderr = ''
  if duration > 0:
    run_cmd = _GetSysbenchRunCommand(
        duration, benchmark_spec.relational_db, sysbench_thread_count
    )
    stdout, stderr = vm.RobustRemoteCommand(run_cmd, timeout=duration + 60)
    logging.info(
        'Sysbench results: \n stdout is:\n%s\nstderr is\n%s', stdout, stderr
    )

  return stdout, stderr


def _UpdateSessions(
    db: relational_db.BaseRelationalDb, thread_count: int
) -> None:
  """Updates the sessions used for more connection parallelism."""
  if (
      db.client_vms_query_tools[0].ENGINE_TYPE
      == sql_engine_utils.SPANNER_POSTGRES
  ):
    background_tasks.RunThreaded(
        lambda client: client.Connect(thread_count * 2),
        db.client_vms_query_tools,
    )


def _RunSysbench(vms, metadata, benchmark_spec, sysbench_thread_count):
  """Runs the Sysbench OLTP test.

  Args:
    vms: The VMs that will issue the sysbench test.
    metadata: The PKB metadata to be passed along to the final results.
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
    sysbench_thread_count: The number of client threads that will connect.

  Returns:
    Results: A list of results of this run.
  """
  # Now run the sysbench OLTP test and parse the results.
  # First step is to run the test long enough to cover the warmup period
  # as requested by the caller. Second step is the 'real' run where the results
  # are parsed and reported.
  _UpdateSessions(benchmark_spec.relational_db, sysbench_thread_count)
  vm = vms[0]

  warmup_seconds = FLAGS.sysbench_warmup_seconds
  if warmup_seconds > 0:
    logging.info('Sysbench warm-up run, duration is %d', warmup_seconds)
    _IssueSysbenchCommand(
        vm, warmup_seconds, benchmark_spec, sysbench_thread_count
    )

  run_seconds = FLAGS.sysbench_run_seconds
  logging.info('Sysbench real run, duration is %d', run_seconds)

  if _SCALEUP_CLIENTS_TEST.value:
    return _RunScaleUpClientsBenchmark(
        vms, run_seconds, benchmark_spec, sysbench_thread_count, metadata
    )

  stdout, _ = _IssueSysbenchCommand(
      vm, run_seconds, benchmark_spec, sysbench_thread_count
  )

  logging.info('\n Parsing Sysbench Results...\n')

  return (
      sysbench.ParseSysbenchTimeSeries(stdout, metadata)
      + sysbench.ParseSysbenchLatency([stdout], metadata)
      + sysbench.ParseSysbenchTransactions(stdout, metadata)
  )


def _RunScaleUpClientsBenchmark(
    vms, run_seconds, benchmark_spec, sysbench_thread_count, metadata
):
  """Runs the Scale Up Clients benchmark. Only TPS and QPS is supported."""
  scale_up_samples = []
  for i in range(1, _SCALEUP_CLIENTS_TEST_NUM_CLIENTS.value + 1):
    new_metadata = metadata.copy()
    new_metadata['sysbench_scale_up_client_count'] = i
    command_vm_pairs = [
        (vm, run_seconds, benchmark_spec, sysbench_thread_count)
        for vm in vms[:i]
    ]
    args = [(command_vm_pair, {}) for command_vm_pair in command_vm_pairs]
    results = background_tasks.RunThreaded(_IssueSysbenchCommand, args)
    stdouts = [i[0] for i in results]
    cpu_utilization = 0
    if hasattr(benchmark_spec.relational_db, 'GetAverageCpuUsage'):
      cpu_utilization = benchmark_spec.relational_db.GetAverageCpuUsage(
          _RUN_DURATION.value // 60, datetime.datetime.now()
      )
      new_metadata['cpu_utilization'] = cpu_utilization
    total_tps = []
    total_qps = []
    for stdout in stdouts:
      current_transactions = sysbench.ParseSysbenchTransactions(
          stdout, new_metadata
      )
      total_tps.append(current_transactions[0].value)
      total_qps.append(current_transactions[1].value)
    logging.info(
        'num_clients: %d total_tps: %d total_qps: %d', i, total_tps, total_qps
    )
    tps_metadata = new_metadata.copy()
    tps_metadata.update({'tps': total_tps})
    qps_metadata = new_metadata.copy()
    qps_metadata.update({'qps': total_qps})
    scale_up_samples += [
        sample.Sample('total_tps', sum(total_tps), 'tps', tps_metadata),
        sample.Sample('total_qps', sum(total_qps), 'qps', qps_metadata),
    ] + sysbench.ParseSysbenchLatency(stdouts, new_metadata)
    if cpu_utilization > _SCALE_UP_MAX_CPU_UTILIZATION.value:
      logging.info('cpu_utilization is over the threadshold, stopping')
      break

  return scale_up_samples


def Run(benchmark_spec):
  """Run the sysbench benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    Results.
  """
  logging.info('Start benchmarking, Cloud Provider is %s.', FLAGS.cloud)
  results = []
  client_vms = benchmark_spec.vms
  db = benchmark_spec.relational_db

  for thread_count in FLAGS.sysbench_run_threads:
    metadata = CreateMetadataFromFlags()
    metadata.update(db.GetResourceMetadata())
    metadata['sysbench_db_size_MB'] = _GetDatabaseSize(db)
    metadata['sysbench_thread_count'] = thread_count
    # The run phase is common across providers. The VMs[0] object contains all
    # information and states necessary to carry out the run.
    results += _RunSysbench(client_vms, metadata, benchmark_spec, thread_count)
  return results


def Cleanup(benchmark_spec):
  """Clean up benchmark related states on server and client.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec
