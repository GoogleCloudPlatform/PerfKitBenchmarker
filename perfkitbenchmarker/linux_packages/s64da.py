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
"""Module containing s64da related functions."""
import collections
import dataclasses
import datetime
from typing import List, Tuple

from perfkitbenchmarker import relational_db as r_db
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine

ALL_TYPES = 'All types'
GIT_REPO = 'https://github.com/swarm64/s64da-benchmark-toolkit'
S64DA_REPO = 's64da'
DB_NAME = 'htap'
TPM = 'TPM'
TPCC_FAILURE_RATE = 'TPCC_failure_rate'
TPCH_FAILURE_RATE = 'TPCH_failure_rate'

TPCH_NUM_QUERIES = 22

WORKING_DIRECTORY_NAME = '~/S64DA_REPO'

OLAP_RESULT_PATH = './results/olap.csv'
OLTP_RESULT_PATH = './results/oltp.csv'

S64DA_VERSION = '5.7.0'


@dataclasses.dataclass(frozen=True)
class OLTPRow:
  time: datetime.datetime
  total_transactions: float
  transaction_type: str
  successful_transactions: float


@dataclasses.dataclass(frozen=True)
class OLAPRow:
  response_time: float
  query_status: str
  query_id: str


class S64DAValueError(Exception):
  pass


def AptInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Installs the s64da package on the VM."""
  vm.Install('pip')
  cmds = [
      (
          'wget '
          f'https://github.com/swarm64/s64da-benchmark-toolkit/archive/refs/tags/{S64DA_VERSION}.tar.gz'
      ),
      f'tar -xf v{S64DA_VERSION}.tar.gz',
      f'mv s64da-benchmark-toolkit-{S64DA_VERSION} {WORKING_DIRECTORY_NAME}',
      f'python3 -m pip install -r {WORKING_DIRECTORY_NAME}/requirements.txt',
  ]
  for cmd in cmds:
    vm.RemoteCommand(cmd)


def PrepareBenchmark(
    vm: virtual_machine.BaseVirtualMachine,
    db: r_db.BaseRelationalDb,
    benchmark: str,
    schema: str,
    scale_factor: int,
    max_jobs: int,
) -> None:
  """Load a database with a dataset."""
  vm.RemoteCommand(
      InLocalDir(
          './prepare_benchmark '
          f'--dsn={db.client_vm_query_tools.GetDSNConnectionString(DB_NAME)} '
          f'--benchmark={benchmark} --schema={schema} '
          f'--scale-factor={scale_factor} '
          f'--max-jobs {max_jobs}'
      )
  )


def RunBenchmark(
    vm: virtual_machine.BaseVirtualMachine,
    db: r_db.BaseRelationalDb,
    benchmark: str,
    oltp_workers: int,
    olap_workers: int,
    duration: int,
    olap_timeout: str,
    ramp_up_duration: int,
    run_on_replica: bool,
) -> List[sample.Sample]:
  """Runs the benchmark and gathers the data."""
  olap_query_tools = db.client_vm_query_tools
  if run_on_replica:
    olap_query_tools = db.client_vm_query_tools_for_replica
  vm.RemoteCommand(
      InLocalDir(
          './run_benchmark '
          f'--dsn={db.client_vm_query_tools.GetDSNConnectionString(DB_NAME)} '
          f'{benchmark} '
          '--dont-wait-until-enough-data '
          f'--oltp-workers={oltp_workers} '
          f'--olap-workers={olap_workers} '
          f'--duration={duration} '
          '--output=csv '
          '--csv-interval=1 '
          f'--olap-dsns={olap_query_tools.GetDSNConnectionString(DB_NAME)} '
          f'--olap-timeout={olap_timeout} '
      )
  )

  olap_results, _ = vm.RemoteCommand(InLocalDir(f'cat {OLAP_RESULT_PATH}'))
  oltp_results, _ = vm.RemoteCommand(InLocalDir(f'cat {OLTP_RESULT_PATH}'))
  return ParseOLAPResults(olap_results) + ParseOLTPResults(
      oltp_results, ramp_up_duration
  )


def ParseTime(time: str) -> datetime.datetime:
  return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f')


def ParseSamples(stdout: str) -> List[List[str]]:
  """Parses the csv file into a list format."""
  return [line.split(', ') for line in stdout.strip().split('\n')]


def SplitRampUpSamples(
    rows: List[OLTPRow], ramp_up_duration: int
) -> Tuple[List[OLTPRow], List[OLTPRow]]:
  """Splits samples before ramp up and after ramp up.

  The samples returned from the s64da benchmarks are in chronological order.

  Args:
    rows: Samples returned from the s64da benchmarks
    ramp_up_duration: Time for the benchmark to rampup.

  Returns:
    Tuple of split samples.
  """
  index = 0
  start_time = rows[0].time
  for row in rows:
    time = row.time
    if (time - start_time).total_seconds() >= ramp_up_duration:
      break
    index += 1
  return (rows[:index], rows[index:])


def ParseOLAPResults(stdout: str) -> List[sample.Sample]:
  """Parses OLTP Results from s64da."""
  # The OLTP Results is in csv format with the following headers
  # time, client-id, iteration-id, query-id, query-status, response time.
  # 2021-09-07 22:26:42.618063, 0, 1, 2, OK, 1.12
  # S64da runs the query in sequence thus not all queries might finish
  # when the benchmark completes.
  # To compute the geomean of the query, find the average response time
  # of each query-id then take the geomean.
  # Failing queries is not accepted and will raise an error.
  results = []
  olap_rows = [
      OLAPRow(query_id=row[3], query_status=row[4], response_time=float(row[5]))
      for row in ParseSamples(stdout)
  ]

  queries_result = collections.defaultdict(list)
  total_queries = len(olap_rows)
  failure_queries = 0
  for row in olap_rows:
    if row.query_status == 'OK':
      queries_result[row.query_id].append(row.response_time)
    else:
      failure_queries += 1

  failure_rate = float(failure_queries) / total_queries * 100
  if len(queries_result) != TPCH_NUM_QUERIES:
    raise S64DAValueError('Not all queries completed at least once')

  averaged_result = {
      query: sum(latency) / len(latency)
      for query, latency in queries_result.items()
  }

  results += [
      sample.Sample('Query ' + query, average_latency, 's')
      for query, average_latency in averaged_result.items()
  ]

  results.append(
      sample.Sample(
          'query_times_geomean', sample.GeoMean(averaged_result.values()), 's'
      )
  )

  results.append(sample.Sample(TPCH_FAILURE_RATE, failure_rate, '%'))
  return results


def ParseOLTPResults(stdout: str, ramp_up_duration: int) -> List[sample.Sample]:
  """Parse OLAP Results from s64da."""
  # The OLAP Results is in CSV format with the following headers
  # time, transaction type, number of transactions, number of ok transaction,
  # number of err transactions,
  # ..., min_latency, avg_latency, max_latency (ms).
  # To compute the TPM we sums up the number of transations with the type
  # 'All type'.
  results = []
  oltp_rows = []
  for row in ParseSamples(stdout):
    oltp_rows.append(
        OLTPRow(
            transaction_type=row[1],
            time=ParseTime(row[0]),
            total_transactions=float(row[2]),
            successful_transactions=float(row[3]),
        )
    )

  oltp_rows = [row for row in oltp_rows if row.transaction_type == ALL_TYPES]

  before_ramp_up_rows, after_ramp_up_rows = SplitRampUpSamples(
      oltp_rows, ramp_up_duration
  )
  successful_transactions_before_ramp_up = 0
  if before_ramp_up_rows:
    # Get the number of total transaction before ramp up.
    last_metrics_before_ramp_up = before_ramp_up_rows[-1]
    successful_transactions_before_ramp_up = (
        last_metrics_before_ramp_up.successful_transactions
    )

  # Get total the transaction number
  last_metrics = after_ramp_up_rows[-1]
  failure_rate = (
      (last_metrics.total_transactions - last_metrics.successful_transactions)
      / last_metrics.total_transactions
      * 100
  )

  successful_transactions_after_ramp_up = (
      last_metrics.successful_transactions
      - successful_transactions_before_ramp_up
  )

  tpm = int(
      (successful_transactions_after_ramp_up * 60.0) / len(after_ramp_up_rows)
  )
  results.append(sample.Sample(TPM, tpm, TPM))
  results.append(sample.Sample(TPCC_FAILURE_RATE, failure_rate, '%'))
  return results


def InLocalDir(command: str) -> str:
  return f'cd {WORKING_DIRECTORY_NAME} && {command}'
