# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing sysbench installation and cleanup functions."""

import dataclasses
import logging
import re
import statistics

from absl import flags
import immutabledict
from perfkitbenchmarker import os_types
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

_IGNORE_CONCURRENT = flags.DEFINE_bool(
    'sysbench_ignore_concurrent_modification',
    False,
    'If true, ignores concurrent modification P0001 exceptions thrown by '
    'some databases.',
)

_SYSBENCH_SPANNER_OLTP_COMMIT_DELAY = flags.DEFINE_integer(
    'sysbench_max_commit_delay', None, 'Max commit delay for spanner oltp in ms'
)

# release 1.0.20; committed Apr 24, 2020. When updating this, also update the
# correct line for CONCURRENT_MODS, as it may have changed in between releases.
DEFAULT_RELEASE_TAG = '1.0.20'
RELEASE_TAGS = [DEFAULT_RELEASE_TAG]
SYSBENCH_VERSION = flags.DEFINE_string(
    'sysbench_version',
    DEFAULT_RELEASE_TAG,
    'Sysbench version to use. Can be a release tag or a git tag.',
)
SYSBENCH_SSL_MODE = flags.DEFINE_string(
    'sysbench_ssl_mode',
    None,
    'Sets the ssl mode to connect to the database. '
)
SYSBENCH_SLEEP_BETWEEN_RUNS_SEC = flags.DEFINE_integer(
    'sysbench_sleep_between_runs_sec',
    0,
    'The time in seconds to sleep between runs with different thread counts.',
)
SYSBENCH_VERBOSE_MODE = flags.DEFINE_integer(
    'sysbench_verbose_mode',
    None,
    'Sets the logging verbosity for sysbench command.',
)
flags.register_validator(
    SYSBENCH_VERBOSE_MODE,
    lambda value: value is None or value in [0, 1, 2, 3, 4, 5],
    'When specified, sysbench_verbose_mode must be between 0 (critical'
    ' messages) and 5 (debug)',
)

GIT_REPO = 'https://github.com/akopytov/sysbench'
SYSBENCH_DIR = '~/sysbench'
# default lua path
LUA_SCRIPT_PATH = f'{SYSBENCH_DIR}/src/lua/'

# Inserts this error code on line 534.
CONCURRENT_MODS = (
    '534 i !strcmp(con->sql_state, "P0001")/* concurrent modification */ ||'
)

# Sysbench TPCC-addon script
SYSBENCH_TPCC_REPRO = 'https://github.com/Percona-Lab/sysbench-tpcc.git'
SYSBENCH_COMMIT_DELAY = '{COMMIT_DELAY}'
DB_DRIVER_KEY = 'db_driver'


def _Install(vm, spanner_oltp=False, args=immutabledict.immutabledict()):
  """Installs the sysbench package on the VM."""
  vm.RemoteCommand(f'sudo rm -rf {SYSBENCH_DIR}')
  if SYSBENCH_VERSION.value in RELEASE_TAGS:
    vm.RemoteCommand(
        f'git clone {GIT_REPO} {SYSBENCH_DIR} --branch {SYSBENCH_VERSION.value}'
    )
  else:
    vm.RemoteCommand(
        f'git clone {GIT_REPO} {SYSBENCH_DIR} && cd {SYSBENCH_DIR} && '
        f'git checkout {SYSBENCH_VERSION.value}')

  if _IGNORE_CONCURRENT.value:
    driver_file = f'{SYSBENCH_DIR}/src/drivers/pgsql/drv_pgsql.c'
    vm.RemoteCommand(f"sed -i '{CONCURRENT_MODS}' {driver_file}")
  without_mysql = ''
  if (
      DB_DRIVER_KEY in args
      and args.get(DB_DRIVER_KEY) == 'pgsql'
  ):
    without_mysql = '--without-mysql'
  if spanner_oltp:
    vm.PushDataFile(
        'sysbench/spanner_oltp_git.diff',
        f'{SYSBENCH_DIR}/spanner_oltp_git.diff',
    )
    vm.PushDataFile(
        'sysbench/spanner_oltp_write_only.diff',
        f'{SYSBENCH_DIR}/spanner_oltp_write_only.diff',
    )
    vm.RemoteCommand(
        'cd ~/sysbench/ && git apply --reject --ignore-whitespace'
        ' spanner_oltp_git.diff'
    )

    if _SYSBENCH_SPANNER_OLTP_COMMIT_DELAY.value:
      vm.RemoteCommand(
          f'sed -i "s/{SYSBENCH_COMMIT_DELAY}/'
          f'{_SYSBENCH_SPANNER_OLTP_COMMIT_DELAY.value}/g" '
          f'{SYSBENCH_DIR}/spanner_oltp_write_only.diff'
      )
      vm.RemoteCommand(
          'cd ~/sysbench/ && git apply --reject --ignore-whitespace'
          ' spanner_oltp_write_only.diff'
      )

  vm_util.Retry(max_retries=10)(vm.RemoteCommand)(
      f'cd {SYSBENCH_DIR} && ./autogen.sh && ./configure --with-pgsql'
      f' {without_mysql}'
  )
  vm.RemoteCommand(f'cd {SYSBENCH_DIR} && make -j && sudo make install')


def Uninstall(vm):
  """Uninstalls the sysbench package on the VM."""
  vm.RemoteCommand(f'cd {SYSBENCH_DIR} && sudo make uninstall')


def YumInstall(vm, args=immutabledict.immutabledict()):
  """Installs the sysbench package on the VM."""
  mariadb_pkg_name = 'mariadb-devel'
  devel_pkg_name = 'postgresql-devel'
  if (
      DB_DRIVER_KEY in args
      and args.get(DB_DRIVER_KEY) == 'pgsql'
  ):
    if vm.OS_TYPE in os_types.AMAZONLINUX_TYPES:
      mariadb_pkg_name = ''
  elif vm.OS_TYPE in os_types.AMAZONLINUX_TYPES:
    # Use mysql-devel according to sysbench documentation.
    mariadb_pkg_name = 'mysql-devel'
  vm.InstallPackages(
      f'make automake libtool pkgconfig libaio-devel {mariadb_pkg_name} '
      f'openssl-devel {devel_pkg_name}'
  )
  _Install(vm, args=args)


def AptInstall(vm, spanner_oltp=False, args=immutabledict.immutabledict()):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages(
      'make automake libtool pkg-config libaio-dev default-libmysqlclient-dev '
      'libssl-dev libpq-dev'
  )
  _Install(vm, spanner_oltp=spanner_oltp, args=args)


def ParseSysbenchTimeSeries(sysbench_output, metadata) -> list[sample.Sample]:
  """Parses sysbench output.

  Extract relevant TPS and latency numbers, and populate the final result
  collection with these information.

  Specifically, we are interested in tps and latency numbers reported by each
  reporting interval.

  Args:
    sysbench_output: The output from sysbench.
    metadata: Metadata of the benchmark

  Returns:
    Three arrays, the tps, latency and qps numbers and average latency.
  """
  tps_numbers = []
  latency_numbers = []
  qps_numbers = []
  for line in sysbench_output.split('\n'):
    # parse a line like (it's one line - broken up in the comment to fit):
    # [ 6s ] thds: 16 tps: 650.51 qps: 12938.26 (r/w/o: 9046.18/2592.05/1300.03)
    # lat (ms,99%): 40.37 err/s: 0.00 reconn/s: 0.00
    if re.match(r'^\[', line):
      match = re.search('tps: (.*?) ', line)
      if not match:
        raise ValueError(f'no tps in: {line}')
      tps_numbers.append(float(match.group(1)))
      match = re.search(r'lat \(.*?\): (.*?) ', line)
      if not match:
        raise ValueError(f'no lat in: {line}')
      latency_numbers.append(float(match.group(1)))
      match = re.search(r'qps: (.*?) \(.*?\) ', line)
      if not match:
        raise ValueError(f'no qps in: {line}')
      qps_numbers.append(float(match.group(1)))
      if line.startswith('SQL statistics:'):
        break

  tps_metadata = metadata.copy()
  tps_metadata.update({'tps': tps_numbers})
  tps_sample = sample.Sample('tps_array', -1, 'tps', tps_metadata)

  latency_metadata = metadata.copy()
  latency_metadata.update({'latency': latency_numbers})
  latency_sample = sample.Sample('latency_array', -1, 'ms', latency_metadata)

  qps_metadata = metadata.copy()
  qps_metadata.update({'qps': qps_numbers})
  qps_sample = sample.Sample('qps_array', -1, 'qps', qps_metadata)

  return [tps_sample, latency_sample, qps_sample]


def ParseSysbenchLatency(
    sysbench_outputs: list[str], metadata
) -> list[sample.Sample]:
  """Parse sysbench latency results."""
  min_latency_array = []
  average_latency_array = []
  max_latency_array = []
  for sysbench_output in sysbench_outputs:
    min_latency_array.append(
        regex_util.ExtractFloat('min: *([0-9]*[.]?[0-9]+)', sysbench_output)
    )

    average_latency_array.append(
        regex_util.ExtractFloat('avg: *([0-9]*[.]?[0-9]+)', sysbench_output)
    )
    max_latency_array.append(
        regex_util.ExtractFloat('max: *([0-9]*[.]?[0-9]+)', sysbench_output)
    )
  min_latency_meta = metadata.copy()
  average_latency_meta = metadata.copy()
  max_latency_meta = metadata.copy()
  if len(sysbench_outputs) > 1:
    min_latency_meta.update({'latency_array': min_latency_array})
    average_latency_meta.update({'latency_array': average_latency_array})
    max_latency_meta.update({'latency_array': max_latency_array})
  return [
      sample.Sample(
          'min_latency',
          min(min_latency_array),
          'ms',
          min_latency_meta,
      ),
      sample.Sample(
          'average_latency',
          statistics.mean(average_latency_array),
          'ms',
          average_latency_meta,
      ),
      sample.Sample(
          'max_latency',
          max(max_latency_array),
          'ms',
          max_latency_meta,
      ),
  ]


def ParseSysbenchTransactions(sysbench_output, metadata) -> list[sample.Sample]:
  """Parse sysbench transaction results."""
  transactions_per_second = regex_util.ExtractFloat(
      r'transactions: *[0-9]* *\(([0-9]*[.]?[0-9]+) per sec.\)', sysbench_output
  )
  queries_per_second = regex_util.ExtractFloat(
      r'queries: *[0-9]* *\(([0-9]*[.]?[0-9]+) per sec.\)', sysbench_output
  )
  # Associate the current latencies with the current transactions.
  min_latency = regex_util.ExtractFloat(
      'min: *([0-9]*[.]?[0-9]+)', sysbench_output)
  average_latency = regex_util.ExtractFloat(
      'avg: *([0-9]*[.]?[0-9]+)', sysbench_output)
  max_latency = regex_util.ExtractFloat(
      'max: *([0-9]*[.]?[0-9]+)', sysbench_output)
  percentile_95_latency = None
  try:
    percentile_95_latency = regex_util.ExtractFloat(
        '95th percentile: *([0-9]*[.]?[0-9]+)', sysbench_output
    )
  except regex_util.NoMatchError:
    logging.info('P95 percentile not available in output.')

  sample_metadata = metadata.copy()
  sample_metadata['min_latency'] = min_latency
  sample_metadata['average_latency'] = average_latency
  sample_metadata['max_latency'] = max_latency
  if percentile_95_latency:
    sample_metadata['percentile_95_latency'] = percentile_95_latency
  return [
      sample.Sample('tps', transactions_per_second, 'tps', sample_metadata),
      sample.Sample('qps', queries_per_second, 'qps', sample_metadata),
  ]


@dataclasses.dataclass
class SysbenchInputParameters:
  """A dataclass for sysbench input flags."""
  custom_lua_packages_path: str | None = None
  built_in_test: bool | None = True  # if this test comes with sysbench
  test: str | None = None  # sysbench test path
  db_driver: str | None = None  # sysbench default mysql
  db_ps_mode: str | None = None  # sysbench default auto
  skip_trx: bool | None = False  # sysbench default off
  trx_level: str | None = None  # transaction isolation level, default RR
  tables: int | None = None  # number of tables to create, default 1
  table_size: int | None = None  # number of rows to insert, default 10000
  scale: int | None = None  # scale factor, default 100
  report_interval: int | None = None  # default 0 (disabled)
  threads: int | None = None  # number of threads, default 1
  events: int | None = None  # limit on events to run, default 0
  rate: int | None = None  # rate limit, default 0 (unlimited)
  use_fk: int | None = None  # use foreign keys, default 1 (on)
  db_user: str | None = None
  db_password: str | None = None
  db_name: str | None = None
  host_ip: str | None = None
  ssl_setting: str | None = None
  mysql_ignore_errors: str | None = None
  port: int | None = None


def _BuildGenericCommand(
    sysbench_parameters: SysbenchInputParameters,
) -> list[str]:
  """Builds a generic sysbench command."""
  cmd = []
  if sysbench_parameters.custom_lua_packages_path:
    cmd += [f'LUA_PATH={sysbench_parameters.custom_lua_packages_path}']
  if sysbench_parameters.built_in_test:
    cmd += ['sysbench']
  if sysbench_parameters.test:
    cmd += [sysbench_parameters.test]
  args = {
      'db-driver': sysbench_parameters.db_driver,
      'db-ps-mode': sysbench_parameters.db_ps_mode,
      'tables': sysbench_parameters.tables,
      'table_size': sysbench_parameters.table_size,
      'scale': sysbench_parameters.scale,
      'report-interval': sysbench_parameters.report_interval,
      'threads': sysbench_parameters.threads,
      'events': sysbench_parameters.events,
      'rate': sysbench_parameters.rate,
      'use_fk': sysbench_parameters.use_fk,
      'trx_level': sysbench_parameters.trx_level,
      'mysql-ignore-errors': sysbench_parameters.mysql_ignore_errors,
  }
  if SYSBENCH_VERBOSE_MODE.value:
    args['verbosity'] = SYSBENCH_VERBOSE_MODE.value

  for arg, value in args.items():
    if value is not None:
      cmd.extend([f'--{arg}={value}'])
  if sysbench_parameters.skip_trx:
    cmd += ['--skip_trx=on']
  cmd += GetSysbenchDatabaseFlags(
      sysbench_parameters.db_driver,
      sysbench_parameters.db_user,
      sysbench_parameters.db_password,
      sysbench_parameters.db_name,
      sysbench_parameters.host_ip,
      sysbench_parameters.ssl_setting,
      sysbench_parameters.port,
  )
  return cmd


def BuildLoadCommand(sysbench_parameters: SysbenchInputParameters) -> str:
  """Builds a sysbench load command."""
  cmd = _BuildGenericCommand(sysbench_parameters)
  cmd += ['prepare']
  return  f'cd {SYSBENCH_DIR} && ' + ' '.join(cmd)


def BuildRunCommand(sysbench_parameters: SysbenchInputParameters) -> str:
  """Builds a sysbench run command."""
  cmd = _BuildGenericCommand(sysbench_parameters)
  cmd += [f'--time={FLAGS.sysbench_run_seconds}']
  cmd += ['run']
  return  f'cd {SYSBENCH_DIR} && ' + ' '.join(cmd)


def GetSysbenchDatabaseFlags(
    db_driver: str,
    db_user: str,
    db_password: str,
    db_name: str,
    host_ip: str,
    ssl_setting: str | None = None,  # only available in sysbench ver 1.1+
    port: int | None = None,
) -> list[str]:
  """Returns the database flags for sysbench."""
  if db_driver == 'mysql':
    ssl_flag = []
    if ssl_setting:
      ssl_flag += [f'--mysql-ssl={ssl_setting}']
    return ssl_flag + [
        f'--mysql-user={db_user}',
        f'--mysql-password={db_password}',
        f'--mysql-db={db_name}',
        f'--mysql-host={host_ip}',
    ]
  elif db_driver == 'pgsql':
    cmd = []
    if ssl_setting:
      cmd += [f'--pgsql-sslmode={ssl_setting}']
    return cmd + [
        f'--pgsql-user={db_user}',
        f"--pgsql-password='{db_password}'",
        f'--pgsql-db={db_name}',
        f'--pgsql-host={host_ip}',
        f'--pgsql-port={port}',
    ]
  return []


def GetMetadata(parameters: SysbenchInputParameters) -> dict[str, str]:
  """Returns the metadata for sysbench."""
  args = {
      'sysbench_testname': parameters.test,
      'sysbench_driver': parameters.db_driver,
      'sysbench_ps_mode': parameters.db_ps_mode,
      'sysbench_skip_trx': parameters.skip_trx,
      'sysbench_trx_level': parameters.trx_level,
      'sysbench_tables': parameters.tables,
      'sysbench_table_size': parameters.table_size,
      'sysbench_scale': parameters.scale,
      'sysbench_report_interval': parameters.report_interval,
      'sysbench_threads': parameters.threads,
      'sysbench_events': parameters.events,
      'sysbench_rate': parameters.rate,
      'sysbench_use_fk': parameters.use_fk,
      'sysbench_ssl_setting': parameters.ssl_setting,
      'sysbench_mysql_ignore_errors': parameters.mysql_ignore_errors,
  }
  metadata = {}
  for arg, value in args.items():
    if value is not None:
      metadata[arg] = str(value)
  if _SYSBENCH_SPANNER_OLTP_COMMIT_DELAY.value:
    metadata['sysbench_max_commit_delay'] = str(
        _SYSBENCH_SPANNER_OLTP_COMMIT_DELAY.value
    )
  return metadata
