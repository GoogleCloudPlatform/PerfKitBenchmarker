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

import re
import statistics
from typing import Optional, List

from absl import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample


FLAGS = flags.FLAGS

_IGNORE_CONCURRENT = flags.DEFINE_bool(
    'sysbench_ignore_concurrent_modification',
    False,
    'If true, ignores concurrent modification P0001 exceptions thrown by '
    'some databases.',
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


def _Install(vm, spanner_oltp=False):
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
  if spanner_oltp:
    vm.PushDataFile(
        'sysbench/spanner_oltp_git.diff',
        f'{SYSBENCH_DIR}/spanner_oltp_git.diff',
    )
    vm.RemoteCommand(
        'cd ~/sysbench/ && git apply --reject --ignore-whitespace'
        ' spanner_oltp_git.diff'
    )

  vm.RemoteCommand(
      f'cd {SYSBENCH_DIR} && ./autogen.sh && ./configure --with-pgsql'
  )
  vm.RemoteCommand(f'cd {SYSBENCH_DIR} && make -j && sudo make install')


def Uninstall(vm):
  """Uninstalls the sysbench package on the VM."""
  vm.RemoteCommand(f'cd {SYSBENCH_DIR} && sudo make uninstall')


def YumInstall(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages(
      'make automake libtool pkgconfig libaio-devel mariadb-devel '
      'openssl-devel postgresql-devel'
  )
  _Install(vm)


def AptInstall(vm, spanner_oltp=False):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages(
      'make automake libtool pkg-config libaio-dev default-libmysqlclient-dev '
      'libssl-dev libpq-dev'
  )
  _Install(vm, spanner_oltp=spanner_oltp)


def ParseSysbenchTimeSeries(sysbench_output, metadata) -> List[sample.Sample]:
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
    sysbench_outputs: List[str], metadata
) -> List[sample.Sample]:
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


def ParseSysbenchTransactions(sysbench_output, metadata) -> List[sample.Sample]:
  """Parse sysbench transaction results."""
  transactions_per_second = regex_util.ExtractFloat(
      r'transactions: *[0-9]* *\(([0-9]*[.]?[0-9]+) per sec.\)', sysbench_output
  )
  queries_per_second = regex_util.ExtractFloat(
      r'queries: *[0-9]* *\(([0-9]*[.]?[0-9]+) per sec.\)', sysbench_output
  )
  return [
      sample.Sample('tps', transactions_per_second, 'tps', metadata),
      sample.Sample('qps', queries_per_second, 'qps', metadata),
  ]


# TODO(ruwa): wrap in a dataclass.
def BuildLoadCommand(
    custom_lua_packages_path: Optional[str] = None,
    built_in_test: Optional[bool] = True,  # if this test comes with sysbench
    test: Optional[str] = None,  # sysbench test path
    db_driver: Optional[str] = None,  # sysbench default mysql
    db_ps_mode: Optional[str] = None,  # sysbench default auto
    skip_trx: Optional[bool] = False,  # sysbench default off
    trx_level: Optional[str] = None,  # transaction isolation level, default RR
    tables: Optional[int] = None,  # number of tables to create, default 1
    table_size: Optional[int] = None,  # number of rows to insert, default 10000
    scale: Optional[int] = None,  # scale factor, default 100
    report_interval: Optional[int] = None,  # default 0 (disabled)
    threads: Optional[int] = None,  # number of threads, default 1
    events: Optional[int] = None,   # limit on events to run, default 0
    rate: Optional[int] = None,  # rate limit, default 0 (unlimited)
    use_fk: Optional[int] = None,  # use foreign keys, default 1 (on)
    db_user: Optional[str] = None,
    db_password: Optional[str] = None,
    db_name: Optional[str] = None,
    host_ip: Optional[str] = None,
    ssl_setting: Optional[str] = None,
) -> str:
  """Builds a sysbench load command."""
  cmd = []
  if custom_lua_packages_path:
    cmd += [f'LUA_PATH={custom_lua_packages_path}']
  if built_in_test:
    cmd += ['sysbench']
  if test:
    cmd += [test]
  args = {
      'db-driver': db_driver,
      'db-ps-mode': db_ps_mode,
      'tables': tables,
      'table_size': table_size,
      'scale': scale,
      'report-interval': report_interval,
      'threads': threads,
      'events': events,
      'rate': rate,
      'use_fk': use_fk,
      'trx_level': trx_level,
  }
  for arg, value in args.items():
    if value is not None:
      cmd.extend([f'--{arg}={value}'])
  if skip_trx:
    cmd += ['--skip_trx=on']
  cmd += GetSysbenchDatabaseFlags(
      db_driver, db_user, db_password, db_name, host_ip, ssl_setting)
  cmd += ['prepare']
  return  f'cd {SYSBENCH_DIR} && ' + ' '.join(cmd)


def GetSysbenchDatabaseFlags(
    db_driver: str,
    db_user: str,
    db_password: str,
    db_name: str,
    host_ip: str,
    ssl_setting: Optional[str] = None,  # only available in sysbench ver 1.1+
) -> List[str]:
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
  return []
