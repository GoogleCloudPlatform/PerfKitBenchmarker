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

"""Runs an ApacheBench benchmark.

See https://httpd.apache.org/docs/2.4/programs/ab.html#synopsis for more info.
"""

from __future__ import annotations

import collections
import dataclasses
import io
import logging
from typing import Any, Dict, List

from absl import flags
import pandas as pd
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import apache2_server


@dataclasses.dataclass
class ApacheBenchIpConfig:
  """Ip Config settings used for a run."""

  ip_type: str
  server_attr: str
  output_path: str
  percentile_data_path: str
  raw_request_data_path: str


@dataclasses.dataclass
class ApacheBenchRunConfig:
  """Run config settings used for a run."""

  num_requests: int
  concurrency: int
  keep_alive: bool
  http_method: str
  socket_timeout: int
  timelimit: int | None
  server_content_size: int
  client_vms: int

  def GetCommand(
      self,
      ip_config: ApacheBenchIpConfig,
      server: virtual_machine.VirtualMachine,
  ) -> str:
    """Builds Apache Bench command with class fields.

    Args:
      ip_config: Ip config object containing info for this run.
      server: The Virtual Machine that is running Apache.

    Returns:
      String representing the command to run Apache Bench.
    """
    cmd = 'ab -r '

    if self.keep_alive:
      cmd += '-k '
    if self.timelimit:
      cmd += f'-t {self.timelimit} '

    cmd += (
        f'-n {self.num_requests} '
        f'-c {self.concurrency} '
        f'-m {self.http_method} '
        f'-s {self.socket_timeout} '
        f'-e {ip_config.percentile_data_path} '
        f'-g {ip_config.raw_request_data_path} '
        f'http://{getattr(server, ip_config.server_attr)}:{_PORT}/ '
        f'1> {ip_config.output_path}'
    )

    return cmd


@dataclasses.dataclass
class ApacheBenchResults:
  """Data related to the ApacheBench output."""

  complete_requests: int
  failed_requests: int
  requests_per_second: float
  requests_per_second_unit: str
  time_per_request: float
  time_per_request_unit: str
  time_per_request_concurrent: float
  time_per_request_concurrent_unit: str
  transfer_rate: float
  transfer_rate_unit: str
  time_taken_for_tests: float
  time_taken_for_tests_unit: str
  total_transferred: int
  total_transferred_unit: str
  html_transferred: int
  html_transferred_unit: str
  histogram: sample._Histogram
  raw_results: Dict[int, List[float]]
  cpu_seconds: float | None = None

  def GetSamples(self, metadata: Dict[str, Any]) -> List[sample.Sample]:
    """Generate a list of samples based on the data stored in this object."""
    return [
        sample.Sample('Failed requests', self.failed_requests, '', metadata),
        sample.Sample(
            'Requests per second',
            self.requests_per_second,
            self.requests_per_second_unit,
            metadata,
        ),
        sample.Sample(
            'Time per request',
            self.time_per_request,
            self.time_per_request_unit,
            metadata,
        ),
        sample.Sample(
            'Time per request concurrent',
            self.time_per_request_concurrent,
            self.time_per_request_concurrent_unit,
            metadata,
        ),
        sample.Sample(
            'Transfer rate',
            self.transfer_rate,
            self.transfer_rate_unit,
            metadata,
        ),
        sample.Sample(
            'Total transferred',
            self.total_transferred,
            self.total_transferred_unit,
            metadata,
        ),
        sample.Sample(
            'HTML transferred',
            self.html_transferred,
            self.html_transferred_unit,
            metadata,
        ),
        sample.Sample('CPU Seconds', self.cpu_seconds, 'seconds', metadata),
        sample.CreateHistogramSample(
            self.histogram, '', '', 'ms', metadata, 'ApacheBench Histogram'
        ),
        sample.Sample(
            'Raw Request Times',
            0,
            '',
            {**metadata, **{'raw_requests': self.raw_results}},
        ),
    ]


class ApacheBenchRunMode:
  """Enum of options for --apachebench_run_mode."""

  MAX_THROUGHPUT = 'MAX_THROUGHPUT'
  STANDARD = 'STANDARD'

  ALL = (MAX_THROUGHPUT, STANDARD)


BENCHMARK_NAME = 'apachebench'
BENCHMARK_CONFIG = """
apachebench:
  description: Runs apachebench benchmark.
  vm_groups:
    server:
      os_type: ubuntu2004
      vm_spec: *default_single_core
    client:
      os_type: ubuntu2004
      vm_spec: *default_single_core
      vm_count: 1
"""
FLAGS = flags.FLAGS

# Default port for Apache
_PORT = 80
# Max number of concurrent requests each VM can make (limited by sockets)
_MAX_CONCURRENCY_PER_VM = 1024
# Constants used to differentiate between runs using internal or external ip
_INTERNAL_IP_CONFIG = ApacheBenchIpConfig(
    'internal-ip',
    'internal_ip',
    'internal_results.txt',
    'internal_ip_percentiles.csv',
    'internal_ip_request_times.tsv',
)
_EXTERNAL_IP_CONFIG = ApacheBenchIpConfig(
    'external-ip',
    'ip_address',
    'external_results.txt',
    'external_ip_percentiles.csv',
    'external_ip_request_times.tsv',
)
# Map from ip_addresses flag enum to list of ip configs to use
_IP_ADDRESSES_TO_IP_CONFIGS = {
    vm_util.IpAddressSubset.INTERNAL: [_INTERNAL_IP_CONFIG],
    vm_util.IpAddressSubset.EXTERNAL: [_EXTERNAL_IP_CONFIG],
    vm_util.IpAddressSubset.BOTH: [_INTERNAL_IP_CONFIG, _EXTERNAL_IP_CONFIG],
    vm_util.IpAddressSubset.REACHABLE: [
        _INTERNAL_IP_CONFIG,
        _EXTERNAL_IP_CONFIG,
    ],
}

_NUM_REQUESTS = flags.DEFINE_integer(
    'apachebench_num_requests',
    default=10000,
    help='Number of requests to perform for the benchmarking session.',
)
_CONCURRENCY = flags.DEFINE_integer(
    'apachebench_concurrency',
    default=1,
    help='Number of multiple requests to perform at a time.',
)
_KEEP_ALIVE = flags.DEFINE_boolean(
    'apachebench_keep_alive',
    default=True,
    help='Enable the HTTP KeepAlive feature.',
)
_HTTP_METHOD = flags.DEFINE_enum(
    'apachebench_http_method',
    default='GET',
    enum_values=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
    help='Custom HTTP method for the requests.',
)
_SOCKET_TIMEOUT = flags.DEFINE_integer(
    'apachebench_socket_timeout',
    default=30,
    help='Maximum number of seconds to wait before the socket times out.',
)
_TIMELIMIT = flags.DEFINE_integer(
    'apachebench_timelimit',
    default=None,
    help=(
        'Maximum number of seconds to spend for benchmarking. '
        'After the timelimit is reached, additional requests will not be sent.'
    ),
)
_APACHE_SERVER_CONTENT_SIZE = flags.DEFINE_integer(
    'apachebench_server_content_size',
    default=2070000,  # Default based on average website size of 2.07 MB
    help='The size of the content the Apache server will serve (in bytes).',
)
_CLIENT_VMS = flags.DEFINE_integer(
    'apachebench_client_vms', default=1, help='The number of client VMs to use.'
)
flags.register_validator(
    'apachebench_client_vms',
    lambda value: value == 1,
    message=(
        'The number of client VMs should be 1 as '
        'metric combination logic is not yet implemented.'
    ),
)
_RUN_MODE = flags.DEFINE_enum(
    'apachebench_run_mode',
    default=ApacheBenchRunMode.STANDARD,
    enum_values=ApacheBenchRunMode.ALL,
    help=(
        'Specify which run mode to use.'
        'MAX_THROUGHPUT: Searches for concurrency level with max requests per '
        'second while keeping number of failed requests at 0. '
        'STANDARD: Runs Apache Bench with specified flags.'
    ),
)
_MAX_CONCURRENCY = flags.DEFINE_integer(
    'apachebench_max_concurrency',
    default=1000,
    upper_bound=_MAX_CONCURRENCY_PER_VM,
    help=(
        'The maximum number of concurrent requests to use when searching for '
        'max throughput (when --apachebench_run_mode=MAX_THROUGHPUT).'
    ),
)


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Returns config for ApacheBench benchmark."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['vm_groups']['client']['vm_count'] = _CLIENT_VMS.value

  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares the apachebench benchmark."""
  client_vms = benchmark_spec.vm_groups['client']
  server = benchmark_spec.vm_groups['server'][0]

  background_tasks.RunThreaded(
      lambda vm: vm.Install('apache2_utils'), client_vms
  )
  server.Install('apache2_server')

  server.AllowPort(_PORT)
  apache2_server.SetupServer(server, _APACHE_SERVER_CONTENT_SIZE.value)
  apache2_server.StartServer(server)


def _ParseRawRequestData(
    vm: virtual_machine.VirtualMachine, path: str, column: str = 'ttime'
) -> Dict[int, List[float]]:
  """Loads column of tsv file created by ApacheBench into a list.

  Args:
    vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    path: The path to the tsv file output inside of the VM which ran AB.
    column: The column with numeric data to create the list from.

  Returns:
    Dict where the keys represents the second a request was started (starting
    at 0) and the values are lists of data (specified by the column argument)
    from requests started at the time specified by the corresponding key.

  Column meanings:
    ctime: Time to establish a connection with the server.
    dtime: Time from connection established to response recieved.
    ttime: ctime + dtime
    wait: Time from request written to response from server.

  Example ApacheBench percentile output.

  starttime                       seconds     ctime   dtime   ttime   wait
  Fri Jun 24 14:16:49 2022        1656105409      2       2       4       2
  Fri Jun 24 14:16:54 2022        1656105414      2       2       5       2
  Fri Jun 24 14:16:54 2022        1656105414      2       2       5       2
  Fri Jun 24 14:16:54 2022        1656105414      3       2       5       2
  """
  tsv, _ = vm.RemoteCommand(f'cat {path}')
  data_frame = pd.read_csv(io.StringIO(tsv), sep='\t')

  data_frame['seconds'] = pd.to_numeric(data_frame['seconds'])
  data_frame[column] = pd.to_numeric(data_frame[column])

  result = {}
  min_seconds = data_frame['seconds'].min()

  for _, row in data_frame.iterrows():
    second = int(row['seconds'] - min_seconds)
    if second not in result.keys():
      result[second] = []

    result[second].append(float(row[column]))

  return result


def _ParseHistogramFromFile(
    vm: virtual_machine.VirtualMachine, path: str, successful_requests: int
) -> sample._Histogram:
  """Creates a histogram from csv file created by ApacheBench.

  Args:
    vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    path: The path to the csv file output inside of the VM which ran AB.
    successful_requests: The number of successful requests.

  Returns:
    sample._Histogram where each bucket represents a unique request time and
    the count for a bucket represents the number of requests that match the
    bucket's request time.

  Example ApacheBench percentile output.

  percent, response time
  0,10.21
  1,12.12
  2,15.10
  """
  csv, _ = vm.RemoteCommand(f'cat {path}')
  csv = csv.strip()

  # Skip header line
  rows = csv.split('\n')[1:]
  result = collections.OrderedDict()

  # 0th percentile represents 1 request (the quickest request)
  percentile, response_time = list(map(float, rows[0].split(',')))
  result[response_time] = 1

  last_unique_percentile = percentile
  prev_percentile, prev_response_time = percentile, response_time

  # Skip 0th percentile
  for row in rows[1:]:
    percentile, response_time = list(map(float, row.split(',')))

    if response_time != prev_response_time:
      last_unique_percentile = prev_percentile

    # Calculate number of requests in this bucket
    result[response_time] = int(
        successful_requests * (percentile - last_unique_percentile) / 100
    )

    prev_percentile = percentile
    prev_response_time = response_time

  return result


def _ParseResults(
    vm: virtual_machine.VirtualMachine, ip_config: ApacheBenchIpConfig
) -> ApacheBenchResults:
  """Parse results of ApacheBench into an ApacheBenchResults object.

  Args:
    vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    ip_config: The ApacheBenchIpConfig used.

  Returns:
    ApacheBenchResults object populated with the results read from path.

  Example ApacheBench output.

  Server Software: Apache/2.4.7

  Document Path: /
  Document Length: 58769 bytes

  Concurrency Level: 10
  Time taken for tests: 1.004 seconds
  Complete requests: 100
  Failed requests: 0
  Total transferred: 5911100 bytes
  HTML transferred: 5876900 bytes
  Requests per second: 99.56 [#/sec] (mean)
  Time per request: 100.444 [ms] (mean)
  Time per request: 10.044 [ms] (mean, across all concurrent requests)
  Transfer rate: 5747.06 [Kbytes/sec] received
  """
  output, _ = vm.RemoteCommand(f'cat {ip_config.output_path}')

  complete_requests = regex_util.ExtractExactlyOneMatch(
      r'Complete requests:\s+(\d+)', output
  )

  failed_requests = regex_util.ExtractExactlyOneMatch(
      r'Failed requests:\s+(\d+)', output
  )

  requests_per_second, requests_per_second_unit = (
      regex_util.ExtractExactlyOneMatch(
          r'Requests per second:\s+(\d*\.?\d*) \[([^\s]+)\] \(\w+\)', output
      )
  )

  time_per_request, time_per_request_unit = regex_util.ExtractExactlyOneMatch(
      r'Time per request:\s+(\d*\.?\d*) \[([^\s]+)\] \(mean\)', output
  )

  time_per_request_concurrent, time_per_request_concurrent_unit = (
      regex_util.ExtractExactlyOneMatch(
          r'Time per request:\s+(\d*\.?\d*) \[([^\s]+)\] \(mean,', output
      )
  )

  transfer_rate, transfer_rate_unit = regex_util.ExtractExactlyOneMatch(
      r'Transfer rate:\s+(\d*\.?\d*) \[([^\s]+)\] received', output
  )

  time_taken_for_tests, time_taken_for_tests_unit = (
      regex_util.ExtractExactlyOneMatch(
          r'Time taken for tests:\s+(\d*\.?\d*) ([^\s]+)', output
      )
  )

  total_transferred, total_transferred_unit = regex_util.ExtractExactlyOneMatch(
      r'Total transferred:\s+(\d*\.?\d*) ([^\s]+)', output
  )

  html_transferred, html_transferred_unit = regex_util.ExtractExactlyOneMatch(
      r'HTML transferred:\s+(\d*\.?\d*) ([^\s]+)', output
  )

  return ApacheBenchResults(
      complete_requests=int(complete_requests),
      failed_requests=int(failed_requests),
      requests_per_second=float(requests_per_second),
      requests_per_second_unit=requests_per_second_unit,
      time_per_request=float(time_per_request),
      time_per_request_unit=time_per_request_unit,
      time_per_request_concurrent=float(time_per_request_concurrent),
      time_per_request_concurrent_unit=time_per_request_concurrent_unit,
      transfer_rate=float(transfer_rate),
      transfer_rate_unit=transfer_rate_unit,
      time_taken_for_tests=float(time_taken_for_tests),
      time_taken_for_tests_unit=time_taken_for_tests_unit,
      total_transferred=int(total_transferred),
      total_transferred_unit=total_transferred_unit,
      html_transferred=int(html_transferred),
      html_transferred_unit=html_transferred_unit,
      histogram=_ParseHistogramFromFile(
          vm,
          ip_config.percentile_data_path,
          int(complete_requests) - int(failed_requests),
      ),
      raw_results=_ParseRawRequestData(vm, ip_config.raw_request_data_path),
  )


def GetMetadata(
    results: ApacheBenchResults,
    run_config: ApacheBenchRunConfig,
    ip_config: ApacheBenchIpConfig,
) -> Dict[str, Any]:
  """Returns the apachebench metadata as a dictonary.

  Args:
    results: The dictionary returned by _Run.
    run_config: The ApacheBenchRunConfig passed to _Run.
    ip_config: The ApacheBenchIpConfig passed to _Run.

  Returns:
    A dictonary of metadata to be included in all ApacheBench samples.
  """

  return {
      'apachebench_requests': run_config.num_requests,
      'apachebench_concurrency_level': run_config.concurrency,
      'apachebench_keep_alive': run_config.keep_alive,
      'apachebench_http_method': run_config.http_method,
      'apachebench_socket_timeout': run_config.socket_timeout,
      'apachebench_timelimit': run_config.timelimit,
      'apachebench_server_content_size': run_config.server_content_size,
      'apachebench_ip_type': ip_config.ip_type,
      'apachebench_client_vms': run_config.client_vms,
      'apachebench_complete_requests': results.complete_requests,
      'apachebench_time_taken_for_tests': results.time_taken_for_tests,
      'apachebench_run_mode': _RUN_MODE.value,
  }


def _Run(
    client_vms: List[virtual_machine.VirtualMachine],
    server_vm: virtual_machine.VirtualMachine,
    run_config: ApacheBenchRunConfig,
    ip_config: ApacheBenchIpConfig,
) -> ApacheBenchResults:
  """Runs apachebench benchmark."""
  apache2_server.StartServer(server_vm)
  cmd = run_config.GetCommand(ip_config, server_vm)
  background_tasks.RunThreaded(lambda vm: vm.RemoteCommand(cmd), client_vms)

  result = background_tasks.RunThreaded(
      lambda vm: _ParseResults(vm, ip_config), client_vms
  )[0]

  result.cpu_seconds = apache2_server.GetApacheCPUSeconds(server_vm)
  return result


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs apachebench and reports the results."""
  server_vm = benchmark_spec.vm_groups['server'][0]
  client_vms = benchmark_spec.vm_groups['client']
  samples = []

  run_config = ApacheBenchRunConfig(
      num_requests=_NUM_REQUESTS.value,
      concurrency=_CONCURRENCY.value,
      keep_alive=_KEEP_ALIVE.value,
      http_method=_HTTP_METHOD.value,
      socket_timeout=_SOCKET_TIMEOUT.value,
      timelimit=_TIMELIMIT.value,
      server_content_size=_APACHE_SERVER_CONTENT_SIZE.value,
      client_vms=_CLIENT_VMS.value,
  )

  if _RUN_MODE.value == ApacheBenchRunMode.STANDARD:
    for ip_config in _IP_ADDRESSES_TO_IP_CONFIGS[FLAGS.ip_addresses]:
      clients = client_vms
      result = _Run(clients, server_vm, run_config, ip_config)
      metadata = GetMetadata(result, run_config, ip_config)

      samples += result.GetSamples(metadata)

  if _RUN_MODE.value == ApacheBenchRunMode.MAX_THROUGHPUT:
    for ip_config in _IP_ADDRESSES_TO_IP_CONFIGS[FLAGS.ip_addresses]:
      results = {}
      min_val, max_val = 1, _MAX_CONCURRENCY.value
      # Ternary search for max number of concurrent requests
      while min_val < max_val:
        logging.info(
            'Searching for max throughput in concurrency range [%d, %d]',
            min_val,
            max_val,
        )
        mid1 = min_val + (max_val - min_val) // 3
        mid2 = max_val - (max_val - min_val) // 3

        mid1_result = results.get(mid1)
        if not mid1_result:
          run_config = dataclasses.replace(run_config, concurrency=mid1)
          mid1_result = _Run(client_vms, server_vm, run_config, ip_config)

        logging.info(
            'mid1 concurrency: %d, failed requests: %d, requests/second: %f',
            mid1,
            mid1_result.failed_requests,
            mid1_result.requests_per_second,
        )
        if mid1_result.failed_requests > 0:
          max_val = mid1 - 1
          continue

        mid2_result = results.get(mid2)
        if not mid2_result:
          run_config = dataclasses.replace(run_config, concurrency=mid2)
          mid2_result = _Run(client_vms, server_vm, run_config, ip_config)

        logging.info(
            'mid2 concurrency: %d, failed requests: %d, requests/second: %f',
            mid2,
            mid2_result.failed_requests,
            mid2_result.requests_per_second,
        )
        if mid2_result.failed_requests > 0:
          max_val = mid2 - 1
          continue

        if mid1_result.requests_per_second > mid2_result.requests_per_second:
          max_val = mid2 - 1
        else:
          min_val = mid1 + 1

        results[mid1] = mid1_result
        results[mid2] = mid2_result

      run_config = dataclasses.replace(run_config, concurrency=max_val)
      best_result = results.get(max_val)
      if not best_result:
        best_result = _Run(client_vms, server_vm, run_config, ip_config)

      metadata = GetMetadata(best_result, run_config, ip_config)
      samples += best_result.GetSamples(metadata)
      samples.append(
          sample.Sample('Max Concurrent Requests', max_val, '', metadata)
      )

  return samples


def Cleanup(unused_benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  pass
