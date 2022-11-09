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

"""Module containing hammerdbcli benchmark installation functions."""

import collections
import functools
import posixpath
import re
import statistics
from typing import Any, FrozenSet, List, Optional

from absl import flags
from dateutil import parser
from perfkitbenchmarker import data
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine

FLAGS = flags.FLAGS

# picked to match what is used by the partner scripts
WORKING_DIRECTORY_NAME = '~/hammerdbcli'
HAMMERDB_RUN_LOCATION = '/var/lib/google/HammerDB'
TRANSACTION_COUNT_LOCATION = '/tmp/hdbtcount.log '
P3RF_CLOUD_SQL_TEST_DIR = 'hammerdbcli_tcl'

HAMMERDB_SCRIPT_TPC_H = 'tpc_h'
HAMMERDB_SCRIPT_TPC_C = 'tpc_c'

MINUTES_TO_MS = 60 * 1000
HAMMERDB_4_0 = '4.0'
HAMMERDB_4_3 = '4.3'
HAMMERDB_4_5 = '4.5'

TPCH_TABLES = [
    'customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp',
    'region', 'supplier'
]


MAP_VERSION_TO_INSTALL_FILE_NAME = {
    HAMMERDB_4_0: 'install_hammerdb_4_0.sh',
    HAMMERDB_4_3: 'install_hammerdb_4_3.sh'
}

MAP_SCRIPT_TO_DATABASE_NAME = {
    HAMMERDB_SCRIPT_TPC_C: 'tpcc',
    HAMMERDB_SCRIPT_TPC_H: 'tpch'
}

RUN_SCRIPT_TYPE = 'RUN'
BUILD_SCRIPT_TYPE = 'BUILD'

# number of queries that are expected from the TPC H Test
TPC_H_QUERY_COUNT = 22
LINUX_OPEN_FILE_LIMIT = 1000
LARGE_OPEN_FILE_LIMIT = 200000

# Constant string related to hammerdb result
QUERY_TIMES_GEOMEAN = 'query_times_geomean'
SECONDS = 'seconds'

MILLISECONDS = 'milliseconds'

TPM = 'TPM'
NOPM = 'NOPM'

# Default out of the box configuration shipped with the engine.
NON_OPTIMIZED = 'non_optimized'
MINIMUM_RECOVERY = 'minimum_recovery'
RESTORABLE = 'restorable'

HAMMERDB_VERSION = flags.DEFINE_enum(
    'hammerdbcli_version', HAMMERDB_4_0,
    [HAMMERDB_4_0, HAMMERDB_4_3, HAMMERDB_4_5],
    'Version of the hammerdb. Currently supported patched '
    'version of 4.0 and the public versions of 4.3 and 4.5.')

HAMMERDB_SCRIPT = flags.DEFINE_enum(
    'hammerdbcli_script', HAMMERDB_SCRIPT_TPC_C,
    [HAMMERDB_SCRIPT_TPC_H, HAMMERDB_SCRIPT_TPC_C],
    'The script to run for hammerdb.')

_HAMMERDB_BUILD_TIMEOUT = flags.DEFINE_integer(
    'hammerdbcli_build_timeout', 15000,
    'Timeout hammerdb build phase when exceed build timeout.')

HAMMERDB_RUN_TIMEOUT = flags.DEFINE_integer(
    'hammerdbcli_run_timeout', None,
    'Timeout when hammerdb exceed run time. No timeout when None is specified')

_HAMMERDB_SET_LINUX_OPEN_FILE_LIMIT = flags.DEFINE_integer(
    'hammerdbcli_set_linux_open_file_limit', None,
    'Override the default linux open file limit, '
    'hammerdb fails due when VU is over 1000 the open file limit is not  '
    'override.')

HAMMERDB_TPCH_DEGREE_OF_PARALLEL = flags.DEFINE_integer(
    'hammerdbcli_tpch_degree_of_parallel', 2,
    'Number of parallel worker in tpch postgres.')

HAMMERDB_TPCC_RAMPUP = flags.DEFINE_integer(
    'hammerdbcli_tpcc_rampup', 5,
    'Duration in minutes for the database to ramp up before '
    'gathering sampels.')

HAMMERDB_TPCC_DURATION = flags.DEFINE_integer(
    'hammerdbcli_tpcc_duration', 10,
    'Duration in minutes of running the benchmark.')

HAMMERDB_TPCC_NUM_WAREHOUSE = flags.DEFINE_integer(
    'hammerdbcli_tpcc_num_warehouse', 5, 'Number of warehouses set in TPCC')

HAMMERDB_TPCC_ALL_WAREHOUSE = flags.DEFINE_bool(
    'hammerdbcli_tpcc_all_warehouse', False, 'Uses all '
    'warehouse in TPCC benchmark')

NUM_RUN = flags.DEFINE_integer(
    'hammerdbcli_num_runs', 1, 'Run the benchmark '
    'n times.')

LOAD_TPCH_TABLES_TO_COLUMNAR_ENGINE = flags.DEFINE_bool(
    'hammerdbcli_load_tpch_tables_to_columnar_engine', False,
    'Load the table to columnar engine.')

HAMMERDB_NUM_VU = flags.DEFINE_integer('hammerdbcli_num_vu', None,
                                       'Number of virtual users')
HAMMERDB_BUILD_TPCC_NUM_VU = flags.DEFINE_integer(
    'hammerdbcli_build_tpcc_num_vu', None, 'Number of virtual '
    'users to build the tpcc database. This is set to '
    'hammerdb_num_vu if not set.')

HAMMERDB_BUILD_TPCH_NUM_VU = flags.DEFINE_integer(
    'hammerdbcli_build_tpch_num_vu', 8, 'Number of virtual'
    ' users when building tpch table')

HAMMERDB_TPCH_SCALE_FACTOR = flags.DEFINE_integer(
    'hammerdbcli_tpch_scale_factor', 1, 'Scaling factor for TPCH')

HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION = flags.DEFINE_enum(
    'hammerdbcli_optimized_server_configuration', NON_OPTIMIZED,
    [NON_OPTIMIZED, MINIMUM_RECOVERY, RESTORABLE],
    'Server configuration to use when '
    'benchmarking IAAS DB using HammerDB tool')

TPCC_LOG_TRANSACTIONS = flags.DEFINE_bool(
    'hammerdbcli_tpcc_log_transactions', False,
    'Gather data for TPM every seconds. Only supported on hammerdb 4.3.')

HAMMERDB_TPCC_TIME_PROFILE = flags.DEFINE_bool(
    'hammerdbcli_tpcc_time_profile', False,
    'Gather data for tpcc latency data by turning on time'
    ' profile flag in hammerdb. Might decrease the '
    'the TPM/NOPM, gathered due to extra measurements.')


def SetDefaultConfig():
  """Set the default configurations of unfilled flags."""
  if HAMMERDB_NUM_VU.value is None:
    if HAMMERDB_SCRIPT.value == HAMMERDB_SCRIPT_TPC_H:
      FLAGS.hammerdbcli_num_vu = 1
    elif HAMMERDB_SCRIPT.value == HAMMERDB_SCRIPT_TPC_C:
      FLAGS.hammerdbcli_num_vu = 4
  if HAMMERDB_BUILD_TPCC_NUM_VU.value is None:
    FLAGS.hammerdbcli_build_tpcc_num_vu = HAMMERDB_NUM_VU.value


# define Hammerdb exception
class HammerdbBenchmarkError(Exception):
  pass


class HammerDbTclScript(object):
  """Represents a TCL script that will be run inside of hammerdbcli."""

  def __init__(self, tcl_script_name: str, needed_parameters: FrozenSet[str],
               path: str, script_type: str):
    self.tcl_script_name = tcl_script_name
    self.needed_parameters = needed_parameters
    self.path = path
    self.script_type = script_type

  def Install(self, vm: virtual_machine.VirtualMachine,
              tcl_script_parameters: Any):
    PushCloudSqlTestFile(vm, self.tcl_script_name, self.path)

    for parameter in self.needed_parameters:
      tcl_script_parameters.SearchAndReplaceInScript(vm, self.tcl_script_name,
                                                     parameter)

  @classmethod
  def CheckErrorFromHammerdb(cls, stdout: str):
    """Check errors from the stdout of Hammerdb.

    Some sample errors
      Error in Virtual User 1:
      [Microsoft][ODBC Driver 17 for SQL Server][SQL Server]
      User does not have permission to perform this action.
      (executing the statement) (Error message when SQL command throws error)

      Vuser 1:FINISHED FAILED (Faied in some operation but finished)

      Virtual Users remain running (Runs terminated before finished)

    Args:
       stdout: Stdout from Hammerdb script.

    Raises:
     Exception: exception when hammerdb failed
    """
    if ('Error' in stdout or 'FAILED' in stdout or
        'Virtual Users remain running' in stdout):
      raise HammerdbBenchmarkError('Script failed during the build phase '
                                   'with stdout {0}'.format(stdout))

  def Run(self,
          vm: virtual_machine.BaseVirtualMachine,
          timeout: Optional[int] = 60*60*6) -> str:
    """Run hammerdbcli script."""
    script_location = '{0}/{1}'.format(LocalWorkingDirectory(),
                                       self.tcl_script_name)
    cmd = ''

    if TPCC_LOG_TRANSACTIONS.value:
      # Logs are appended to hdbtcount file. Remove this file in case
      # there are multiple run phase.
      vm.RemoteCommand(f'sudo rm -f {TRANSACTION_COUNT_LOCATION}')
    # When VU is over LINUX_OPEN_FILE_LIMIT the hammerdbcli will fail
    # due to exceeding the default open file limit on linux.
    # Increase the Open file limit to a large number.
    if _HAMMERDB_SET_LINUX_OPEN_FILE_LIMIT.value:
      cmd = f'ulimit -n {_HAMMERDB_SET_LINUX_OPEN_FILE_LIMIT.value} &&'
    stdout, _ = vm.RemoteCommand(
        InDir(
            HAMMERDB_RUN_LOCATION, 'PATH="$PATH:/opt/mssql-tools/bin" &&'
            + cmd + 'sudo ./hammerdbcli auto {0}'.format(script_location)),
        timeout=timeout)

    self.CheckErrorFromHammerdb(stdout)
    return stdout


SCRIPT_PARAMETER_IP = '{{DATABASE_IP}}'
SCRIPT_PARAMETER_PORT = '{{DATABASE_PORT}}'
SCRIPT_PARAMETER_PASSWORD = '{{DATABASE_PASSWORD}}'
SCRIPT_PARAMETER_USER = '{{DATABASE_USER}}'
SCRIPT_PARAMETER_BUILD_TIMEOUT = '{{BUILD_TIMEOUT}}'
SCRIPT_PARAMETER_AZURE = '{{IS_AZURE}}'
SCRIPT_PARAMETER_TPCC_BUILD_USERS = '{{BUILD_VIRTUAL_USERS_TPC_C}}'
SCRIPT_PARAMETER_TPCC_USERS = '{{VIRTUAL_USERS_TPC_C}}'
SCRIPT_PARAMETER_TPCH_USERS = '{{VIRTUAL_USERS_TPC_H}}'
SCRIPT_PARAMETER_TPCC_TIME_PROFILE = '{{TIME_PROFILE_TPC_C}}'
SCRIPT_PARAMETER_TPCC_NUM_WAREHOUSE = '{{NUM_WAREHOUSE_TPC_C}}'
SCRIPT_PARAMETER_TPCC_ALL_WAREHOUSE = '{{ALL_WAREHOUSE_TPC_C}}'
SCRIPT_PARAMETER_TPCC_RAMPUP = '{{RAMPUP_TPC_C}}'
SCRIPT_PARAMETER_TPCC_DURATION = '{{DURATION_TPC_C}}'
SCRIPT_PARAMETER_TPCH_SCALE_FACTOR = '{{SCALE_FACTOR_TPC_H}}'
SCRIPT_PARAMETER_TPCH_DEGREE_OF_PARALLEL = '{{DEGREE_OF_PARALLEL_TPC_H}}'
SCRIPT_PARAMETER_TPCC_LOG_TRANSACTIONS = '{{LOG_TRANSACTIONS}}'
SCRIPT_PARAMETER_WAIT_TO_COMPLETE = '{{WAIT_TO_COMPLETE}}'
TPCC_PARAMS = frozenset({SCRIPT_PARAMETER_IP,
                         SCRIPT_PARAMETER_PORT,
                         SCRIPT_PARAMETER_PASSWORD,
                         SCRIPT_PARAMETER_USER,
                         SCRIPT_PARAMETER_AZURE,
                         SCRIPT_PARAMETER_TPCC_USERS,
                         SCRIPT_PARAMETER_TPCC_NUM_WAREHOUSE,
                         SCRIPT_PARAMETER_TPCC_ALL_WAREHOUSE,
                         SCRIPT_PARAMETER_BUILD_TIMEOUT,
                         SCRIPT_PARAMETER_TPCC_RAMPUP,
                         SCRIPT_PARAMETER_TPCC_DURATION,
                         SCRIPT_PARAMETER_TPCC_BUILD_USERS,
                         SCRIPT_PARAMETER_TPCC_TIME_PROFILE,
                         SCRIPT_PARAMETER_TPCC_LOG_TRANSACTIONS,
                         SCRIPT_PARAMETER_WAIT_TO_COMPLETE})

TPCH_PARAMS = frozenset({SCRIPT_PARAMETER_IP,
                         SCRIPT_PARAMETER_PORT,
                         SCRIPT_PARAMETER_PASSWORD,
                         SCRIPT_PARAMETER_USER,
                         SCRIPT_PARAMETER_AZURE,
                         SCRIPT_PARAMETER_TPCH_USERS,
                         SCRIPT_PARAMETER_TPCH_SCALE_FACTOR,
                         SCRIPT_PARAMETER_TPCH_DEGREE_OF_PARALLEL,
                         SCRIPT_PARAMETER_BUILD_TIMEOUT})


class TclScriptParameters(object):
  """Handle of the parameters that may be needed by a TCL script."""

  def __init__(self, ip, port, password, user, is_managed_azure,
               hammerdb_script, script_type):
    if ':' in ip:
      ip = ip.replace(':', '\\:')
    self.map_search_to_replace = {
        SCRIPT_PARAMETER_IP: ip,
        SCRIPT_PARAMETER_PORT: port,
        SCRIPT_PARAMETER_PASSWORD: password,
        SCRIPT_PARAMETER_USER: user,
        SCRIPT_PARAMETER_AZURE: 'true' if is_managed_azure else 'false',
        SCRIPT_PARAMETER_BUILD_TIMEOUT: _HAMMERDB_BUILD_TIMEOUT.value
    }

    if hammerdb_script == HAMMERDB_SCRIPT_TPC_H:
      # If the script is TPCH and in build phase,
      # uses HAMMERDB_BUILD_TPCH_NUM_VU.value as TPCH_USERS
      tpch_user_param = None
      if script_type == BUILD_SCRIPT_TYPE:
        tpch_user_param = HAMMERDB_BUILD_TPCH_NUM_VU.value
      else:
        tpch_user_param = HAMMERDB_NUM_VU.value

      self.map_search_to_replace.update({
          SCRIPT_PARAMETER_TPCH_DEGREE_OF_PARALLEL:
              HAMMERDB_TPCH_DEGREE_OF_PARALLEL.value,
          SCRIPT_PARAMETER_TPCH_USERS:
              tpch_user_param,
          SCRIPT_PARAMETER_TPCH_SCALE_FACTOR:
              HAMMERDB_TPCH_SCALE_FACTOR.value,
      })

    elif hammerdb_script == HAMMERDB_SCRIPT_TPC_C:
      # Wait to complete forces the script to stop.
      # Set the wait time to tpcc duration plus rampup time and add extra 10
      # minutes of buffer
      wait_to_complete_seconds = (int(HAMMERDB_TPCC_DURATION.value) +
                                  int(HAMMERDB_TPCC_RAMPUP.value)) * 60 + 600
      self.map_search_to_replace.update({
          SCRIPT_PARAMETER_TPCC_DURATION:
              HAMMERDB_TPCC_DURATION.value,
          SCRIPT_PARAMETER_TPCC_RAMPUP:
              HAMMERDB_TPCC_RAMPUP.value,
          SCRIPT_PARAMETER_TPCC_BUILD_USERS:
              HAMMERDB_BUILD_TPCC_NUM_VU.value,
          SCRIPT_PARAMETER_TPCC_USERS:
              HAMMERDB_NUM_VU.value,
          SCRIPT_PARAMETER_TPCC_NUM_WAREHOUSE:
              HAMMERDB_TPCC_NUM_WAREHOUSE.value,
          SCRIPT_PARAMETER_TPCC_ALL_WAREHOUSE:
              HAMMERDB_TPCC_ALL_WAREHOUSE.value,
          SCRIPT_PARAMETER_TPCC_TIME_PROFILE:
              'true' if HAMMERDB_TPCC_TIME_PROFILE.value else 'false',
          SCRIPT_PARAMETER_TPCC_LOG_TRANSACTIONS:
              'true' if TPCC_LOG_TRANSACTIONS.value else 'false',
          SCRIPT_PARAMETER_WAIT_TO_COMPLETE: wait_to_complete_seconds
      })
    else:
      raise Exception('Unknown hammerdb_script')

  def SearchAndReplaceInScript(self, vm: virtual_machine.BaseVirtualMachine,
                               script_name: str, parameter: str):
    SearchAndReplaceTclScript(vm, parameter,
                              self.map_search_to_replace[parameter],
                              script_name)


TPC_C_SQLSERVER_BUILD_SCRIPT = HammerDbTclScript(
    'hammerdb_sqlserver_tpc_c_build.tcl', TPCC_PARAMS, P3RF_CLOUD_SQL_TEST_DIR,
    BUILD_SCRIPT_TYPE)

TPC_C_SQLSERVER_RUN_SCRIPT = HammerDbTclScript(
    'hammerdb_sqlserver_tpc_c_run.tcl', TPCC_PARAMS, P3RF_CLOUD_SQL_TEST_DIR,
    RUN_SCRIPT_TYPE)

TPC_H_SQLSERVER_BUILD_SCRIPT = HammerDbTclScript(
    'hammerdb_sqlserver_tpc_h_build.tcl', TPCH_PARAMS, P3RF_CLOUD_SQL_TEST_DIR,
    BUILD_SCRIPT_TYPE)

TPC_H_SQLSERVER_RUN_SCRIPT = HammerDbTclScript(
    'hammerdb_sqlserver_tpc_h_run.tcl', TPCH_PARAMS, P3RF_CLOUD_SQL_TEST_DIR,
    RUN_SCRIPT_TYPE)

TPC_C_MYSQL_BUILD_SCRIPT = HammerDbTclScript('hammerdb_mysql_tpc_c_build.tcl',
                                             TPCC_PARAMS,
                                             P3RF_CLOUD_SQL_TEST_DIR,
                                             BUILD_SCRIPT_TYPE)

TPC_C_MYSQL_RUN_SCRIPT = HammerDbTclScript('hammerdb_mysql_tpc_c_run.tcl',
                                           TPCC_PARAMS, P3RF_CLOUD_SQL_TEST_DIR,
                                           RUN_SCRIPT_TYPE)

TPC_H_MYSQL_BUILD_SCRIPT = HammerDbTclScript('hammerdb_mysql_tpc_h_build.tcl',
                                             TPCH_PARAMS,
                                             P3RF_CLOUD_SQL_TEST_DIR,
                                             BUILD_SCRIPT_TYPE)

TPC_H_MYSQL_RUN_SCRIPT = HammerDbTclScript('hammerdb_mysql_tpc_h_run.tcl',
                                           TPCH_PARAMS, P3RF_CLOUD_SQL_TEST_DIR,
                                           RUN_SCRIPT_TYPE)

TPC_C_POSTGRES_BUILD_SCRIPT = HammerDbTclScript(
    'hammerdb_postgres_tpc_c_build.tcl',
    TPCC_PARAMS, P3RF_CLOUD_SQL_TEST_DIR, BUILD_SCRIPT_TYPE)

TPC_C_POSTGRES_RUN_SCRIPT = HammerDbTclScript(
    'hammerdb_postgres_tpc_c_run.tcl',
    TPCC_PARAMS, P3RF_CLOUD_SQL_TEST_DIR, RUN_SCRIPT_TYPE)

TPC_H_POSTGRES_BUILD_SCRIPT = HammerDbTclScript(
    'hammerdb_postgres_tpc_h_build.tcl',
    TPCH_PARAMS, P3RF_CLOUD_SQL_TEST_DIR, BUILD_SCRIPT_TYPE)

TPC_H_POSTGRES_RUN_SCRIPT = HammerDbTclScript(
    'hammerdb_postgres_tpc_h_run.tcl',
    TPCH_PARAMS, P3RF_CLOUD_SQL_TEST_DIR, RUN_SCRIPT_TYPE)

SCRIPT_MAPPING = {
    sql_engine_utils.MYSQL: {
        HAMMERDB_SCRIPT_TPC_H: [TPC_H_MYSQL_BUILD_SCRIPT,
                                TPC_H_MYSQL_RUN_SCRIPT],
        HAMMERDB_SCRIPT_TPC_C: [TPC_C_MYSQL_BUILD_SCRIPT,
                                TPC_C_MYSQL_RUN_SCRIPT]
    },
    sql_engine_utils.SQLSERVER: {
        HAMMERDB_SCRIPT_TPC_H: [
            TPC_H_SQLSERVER_BUILD_SCRIPT, TPC_H_SQLSERVER_RUN_SCRIPT
        ],
        HAMMERDB_SCRIPT_TPC_C: [
            TPC_C_SQLSERVER_BUILD_SCRIPT, TPC_C_SQLSERVER_RUN_SCRIPT
        ]
    },
    sql_engine_utils.POSTGRES: {
        HAMMERDB_SCRIPT_TPC_H: [TPC_H_POSTGRES_BUILD_SCRIPT,
                                TPC_H_POSTGRES_RUN_SCRIPT],
        HAMMERDB_SCRIPT_TPC_C: [TPC_C_POSTGRES_BUILD_SCRIPT,
                                TPC_C_POSTGRES_RUN_SCRIPT]
    }
}

# TPCC queries are defined in the tpcc spec
# http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
TPCC_QUERY_TYPES = [
    'neword', 'payment', 'delivery', 'slev', 'ostat', 'gettimestamp'
]

# The ordering of the query metrics is used as an index to extract
# from the benchmark output.
TPCC_QUERY_METRICS = ['MIN', 'P50', 'P95', 'P99', 'MAX', 'SAMPLES']

TPCC_QUERY_METRICS_4_3 = ['CALLS', 'MIN', 'MAX', 'P99', 'P95', 'P50']


def _GetFileContent(vm: virtual_machine.BaseVirtualMachine,
                    file_path: str) -> str:
  stdout, _ = vm.RemoteCommand(f'cat {file_path}')
  return stdout


def ParseTpcCTimeProfileResultsFromFile(stdout: str) -> List[sample.Sample]:
  """Extracts latency result from time profile file."""
  tpcc_metrics = []
  # Timed profile output starts with >>>>>
  # The last 5 output  are the summary of all virtual users
  # The output have the following format
  # >>>>> PROC: NEWORD
  # CALLS: 322104 MIN: 2.269ms AVG: 9.236ms MAX: 36.366ms TOTAL: 2975206.250ms
  # P99: 14.197ms P95: 12.399ms P50: 9.087ms SD: 1808.882 RATIO: 41.115%
  stdout = stdout.split('>>>>')[-5:]
  for line in stdout:
    query_type = str.lower(
        regex_util.ExtractGroup('PROC: ([A-Z]*)', line, flags=re.M))
    for metric in TPCC_QUERY_METRICS_4_3:
      metric_regex = metric + ': ([0-9.]*)'
      metric_value = regex_util.ExtractGroup(metric_regex, line, flags=re.M)
      tpcc_metrics.append(
          sample.Sample(query_type + '_' + metric,
                        metric_value,
                        MILLISECONDS))
  return tpcc_metrics


def ParseTpcCTimeProfileResults(stdout: str) -> List[sample.Sample]:
  """Extract latency result from time profile TPC-C runs."""
  tpcc_metrics = []
  # match a string like
  # Vuser 2:|neword|MIN-35|P50%-9970.5|P95%-148|P99%-153|MAX-16816|SAMPLES-87
  percentile_dic = collections.defaultdict(list)
  percentile_regex = (r'MIN-([0-9.]*)\|P50%-([0-9.]*)\|P95%-([0-9.]*)\|'
                      r'P99%-([0-9.]*)\|MAX-([0-9.]*)\|SAMPLES-([0-9.]*)')
  for line in stdout.split('\n'):
    for query_type in TPCC_QUERY_TYPES:
      if query_type in line:
        m = re.search(percentile_regex, line)
        if m:
          for idx, query_metric in enumerate(TPCC_QUERY_METRICS):
            percentile_dic[query_type + '_' + query_metric].append(
                float(m.group(idx + 1)))

  gathered_metrics = percentile_dic.keys()
  if len(gathered_metrics) != len(TPCC_QUERY_METRICS) * len(TPCC_QUERY_TYPES):
    raise HammerdbBenchmarkError('Unexpected TPCC result count')

  # Time profile log percentile metrics every 10 seconds
  for key in gathered_metrics:
    if 'SAMPLES' not in key:
      # Metrics is in micro seconds
      tpcc_metrics.append(
          sample.Sample(key,
                        statistics.mean(percentile_dic[key]) / 1000,
                        MILLISECONDS))
  return tpcc_metrics


def ParseTpcCTPMResultsFromFile(stdout: str) -> List[sample.Sample]:
  """Parse TPCC TPM metrics per seconds."""
  tpm_metrics = []
  time_series = []
  for line in stdout.split('\n'):
    # Metrics looks as follows
    # 193290 PostgreSQL tpm @ Thu Jun 30 21:18:41 UTC 2022
    if 'tpm @' in line:
      tpm = line.split()[0]
      if tpm.isnumeric():
        date = parser.parse(line.split('tpm @ ')[-1])
        tpm_metrics.append(float(tpm))
        time_series.append(sample.ConvertDateTimeToUnixMs(date))

  ramp_up_ends = time_series[0] + HAMMERDB_TPCC_RAMPUP.value * MINUTES_TO_MS

  ramp_down_starts = ramp_up_ends + (HAMMERDB_TPCC_DURATION.value
                                     * MINUTES_TO_MS)
  tpm_sample = sample.CreateTimeSeriesSample(tpm_metrics, time_series,
                                             sample.TPM_TIME_SERIES, TPM, 1,
                                             ramp_up_ends, ramp_down_starts, {})
  return [tpm_sample]


def ParseTpcCResults(
    stdout: str, vm: virtual_machine.BaseVirtualMachine) -> List[sample.Sample]:
  """Extract results from the TPC-C script."""
  tpcc_metrics = ParseBasicTpcCResults(stdout)

  if HAMMERDB_TPCC_TIME_PROFILE.value:
    if HAMMERDB_VERSION.value == HAMMERDB_4_0:
      tpcc_metrics += ParseTpcCTimeProfileResults(stdout)
    else:
      tpcc_results = _GetFileContent(vm, '/tmp/hdbxtprofile.log')
      tpcc_metrics += ParseTpcCTimeProfileResultsFromFile(tpcc_results)

  if TPCC_LOG_TRANSACTIONS.value:
    tpcc_results = _GetFileContent(vm, '/tmp/hdbtcount.log')
    tpcc_metrics += ParseTpcCTPMResultsFromFile(tpcc_results)
  return tpcc_metrics


def ParseBasicTpcCResults(stdout: str) -> List[sample.Sample]:
  """Extract basic results from the TPC-C script."""
  # match a string like:
  # "Vuser 1:TEST RESULT : System achieved 40213 NOPM from 92856 SQL Server TPM"

  regex = (r'Vuser 1:TEST RESULT : System achieved '
           r'(\d*) NOPM from (\d*) (\w| )* TPM')

  tpm = regex_util.ExtractInt(regex, stdout, group=2)
  nopm = regex_util.ExtractInt(regex, stdout, group=1)

  tpcc_metrics = [sample.Sample(TPM, tpm, TPM), sample.Sample(NOPM, nopm, NOPM)]
  return tpcc_metrics


def GeoMean(float_array: List[float]) -> float:
  """Calculate the geomean of the numbers in float_array."""
  return functools.reduce(lambda x, y: x * y,
                          float_array)**(1.0 / len(float_array))


def ParseTpcHResults(stdout: str) -> List[sample.Sample]:
  """Extract results from the TPC-H script."""

  # parse all instances of strings like:
  # query 19 completed in 0.429 seconds
  regex = r'query (\d*) completed in (\d*.\d*) seconds'

  # matches is a list of tuples, with one tuple for each capture:
  matches = regex_util.ExtractAllMatches(regex, stdout)
  results = []
  query_times = []
  for match in matches:
    metric_name = 'Query_' + match[0]
    time_seconds = float(match[1])
    results.append(sample.Sample(metric_name, time_seconds, SECONDS))
    query_times.append(time_seconds)
  if len(results) != TPC_H_QUERY_COUNT:
    raise HammerdbBenchmarkError('Unexpected TPCH result count')

  results.append(
      sample.Sample(QUERY_TIMES_GEOMEAN, GeoMean(query_times), SECONDS))

  return results


def LocalWorkingDirectory() -> str:
  """Get the directory local on the machine for storing data.

  Returns:
    The directory on the VM
  """
  return WORKING_DIRECTORY_NAME


def InDir(directory: str, command: str) -> str:
  return 'cd {0} && {1}'.format(directory, command)


def InLocalDir(command: str) -> str:
  return InDir(LocalWorkingDirectory(), command)


def PushCloudSqlTestFile(vm: virtual_machine.BaseVirtualMachine, data_file: str,
                         path: str):
  vm.PushFile(data.ResourcePath(posixpath.join(path, data_file)),
              LocalWorkingDirectory())


def SearchAndReplaceGuestFile(vm: virtual_machine.BaseVirtualMachine,
                              directory: str, filename: str, search: str,
                              replace: str):
  vm.RemoteCommand(
      InDir(directory,
            'sed -i.bak \'s:{0}:{1}:\' {2}'.format(search, replace, filename)))


def SearchAndReplaceTclScript(vm: virtual_machine.BaseVirtualMachine,
                              search: str, replace: str, script_name: str):
  SearchAndReplaceGuestFile(vm, LocalWorkingDirectory(),
                            script_name, search, replace)


def Install(vm: virtual_machine.BaseVirtualMachine):
  """Installs hammerdbcli and dependencies on the VM."""
  vm.InstallPackages('curl')
  vm.InstallPackages('patch')
  vm.RemoteCommand('mkdir -p {0}'.format(LocalWorkingDirectory()))
  vm.RemoteCommand('sudo mkdir -p {0}'.format(HAMMERDB_RUN_LOCATION))

  install_file = MAP_VERSION_TO_INSTALL_FILE_NAME[HAMMERDB_VERSION.value]

  files_required = [install_file]
  # Push Hammerdb install files
  if HAMMERDB_VERSION.value == HAMMERDB_4_0:
    # Patches hammerdb 4.0 for Postgres on Azure and time profile frequency
    files_required += ['pgolap.tcl.patch', 'pgoltp.tcl.patch',
                       'postgresql.xml.patch', 'etprof-1.1.tm.patch']

  for file in files_required:
    PushCloudSqlTestFile(vm, file, P3RF_CLOUD_SQL_TEST_DIR)

  vm.RemoteCommand(InLocalDir(f'chmod +x {install_file}'))
  vm.RemoteCommand(InLocalDir(f'sudo ./{install_file}'))

  db_engine = sql_engine_utils.GetDbEngineType(FLAGS.db_engine)
  if db_engine == sql_engine_utils.MYSQL:
    # Install specific mysql library for hammerdb
    vm.Install('libmysqlclient21')

  vm.RemoteCommand('export LD_LIBRARY_PATH=\'/usr/lib/x86_64-linux-gnu/\'')


def SetupConfig(vm: virtual_machine.BaseVirtualMachine, db_engine: str,
                hammerdb_script: str, ip: str, port: str, password: str,
                user: str, is_managed_azure: bool):
  """Sets up the necessary scripts on the VM with the necessary parameters."""
  db_engine = sql_engine_utils.GetDbEngineType(db_engine)

  if db_engine not in SCRIPT_MAPPING:
    raise ValueError('{0} is currently not supported for running '
                     'hammerdb benchmarks.'.format(db_engine))

  if hammerdb_script not in SCRIPT_MAPPING[db_engine]:
    raise ValueError('{0} is not a known hammerdb script.'.format(
        hammerdb_script))

  scripts = SCRIPT_MAPPING[db_engine][hammerdb_script]

  for script in scripts:
    script_parameters = TclScriptParameters(
        ip, port, password, user, is_managed_azure,
        hammerdb_script, script.script_type)
    script.Install(vm, script_parameters)

  # Run all the build script or scripts before actual run phase
  for i in range(len(scripts) - 1):
    scripts[i].Run(vm)


def Run(vm: virtual_machine.BaseVirtualMachine,
        db_engine: str,
        hammerdb_script: str,
        timeout: Optional[int] = 60*60*8) -> List[sample.Sample]:
  """Run the HammerDBCli Benchmark.

  Runs Hammerdb TPCC or TPCH script.
  TPCC gathers TPM (transactions per minute) and NOPM (new order per minute).
  Definitions can be found here:

  https://www.hammerdb.com/blog/uncategorized/why-both-tpm-and-nopm-performance-metrics/

  TPCH gathers the latency of the 22 TPCH queries.

  Args:
     vm:  The virtual machine to run on that has
          Install and SetupConfig already invoked on it.
     db_engine:  The type of database that the script is running on
     hammerdb_script:  An enumeration from HAMMERDB_SCRIPT indicating which
                       script to run.  Must have been prior setup with
                       SetupConfig method on the vm to work.
    timeout: Timeout when running hammerdbcli

  Returns:
     _HammerDBCliResults object with TPM and NOPM values.
  """
  db_engine = sql_engine_utils.GetDbEngineType(db_engine)

  scripts = SCRIPT_MAPPING[db_engine][hammerdb_script]

  # Run the build scripts which contains build schema (inserts into dbs)
  # And the benchmark scripts. The last stdout is the result from the run script
  stdout = scripts[-1].Run(vm, timeout=timeout)

  if hammerdb_script == HAMMERDB_SCRIPT_TPC_H:
    return ParseTpcHResults(stdout)
  else:
    return ParseTpcCResults(stdout, vm)


def GetMetadata(db_engine: str):
  """Returns the meta data needed for hammerdb."""
  script = HAMMERDB_SCRIPT.value
  metadata = {
      'hammerdbcli_script': script,
  }

  metadata['hammerdbcli_version'] = HAMMERDB_VERSION.value
  metadata['hammerdbcli_vu'] = HAMMERDB_NUM_VU.value
  metadata['hammerdbcli_num_run'] = NUM_RUN.value
  if not FLAGS.use_managed_db and HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION.value:
    metadata['hammerdbcli_optimized_server_configuration'] = (
        HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION.value)

  if script == HAMMERDB_SCRIPT_TPC_H:
    metadata['hammerdbcli_scale_factor'] = HAMMERDB_TPCH_SCALE_FACTOR.value
    metadata['hammerdbcli_load_tpch_tables_to_columnar_engine'] = (
        LOAD_TPCH_TABLES_TO_COLUMNAR_ENGINE.value)
    metadata['hammerdbcli_build_tpch_num_vu'] = (
        HAMMERDB_BUILD_TPCH_NUM_VU.value)
    if db_engine == sql_engine_utils.POSTGRES:
      metadata['hammerdbcli_degree_of_parallel'] = (
          HAMMERDB_TPCH_DEGREE_OF_PARALLEL.value)
  elif script == HAMMERDB_SCRIPT_TPC_C:
    metadata['hammerdbcli_num_warehouse'] = HAMMERDB_TPCC_NUM_WAREHOUSE.value
    metadata['hammerdbcli_all_warehouse'] = HAMMERDB_TPCC_ALL_WAREHOUSE.value
    metadata['hammerdbcli_rampup'] = HAMMERDB_TPCC_RAMPUP.value
    metadata['hammerdbcli_duration'] = HAMMERDB_TPCC_DURATION.value
    metadata['hammerdbcli_tpcc_time_profile'] = HAMMERDB_TPCC_TIME_PROFILE.value
    metadata['hammerdbcli_tpcc_log_transactions'] = TPCC_LOG_TRANSACTIONS.value
    if HAMMERDB_BUILD_TPCC_NUM_VU.value is None:
      FLAGS.hammerdbcli_build_tpcc_num_vu = HAMMERDB_NUM_VU.value
    metadata['hammerdbcli_build_tpcc_num_vu'] = HAMMERDB_BUILD_TPCC_NUM_VU.value
  return metadata
