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

import dataclasses
import io
from typing import Any, Dict, List

from absl import flags
import pandas as pd
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import apache2_server


@dataclasses.dataclass
class ApacheBenchConfig:
  """Config settings used for a run."""
  ip_type: str
  server_attr: str
  output_path: str
  percentile_output_path: str
  raw_request_data_path: str


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

  def GetSamples(self, metadata: Dict[str, Any]) -> List[sample.Sample]:
    """Generate a list of samples based on the data stored in this object."""
    return [
        sample.Sample('Failed requests', self.failed_requests, '', metadata),
        sample.Sample('Requests per second', self.requests_per_second,
                      self.requests_per_second_unit, metadata),
        sample.Sample('Time per request', self.time_per_request,
                      self.time_per_request_unit, metadata),
        sample.Sample('Time per request concurrent',
                      self.time_per_request_concurrent,
                      self.time_per_request_concurrent_unit, metadata),
        sample.Sample('Transfer rate', self.transfer_rate,
                      self.transfer_rate_unit, metadata),
        sample.Sample('Total transferred', self.total_transferred,
                      self.total_transferred_unit, metadata),
        sample.Sample('HTML transferred', self.html_transferred,
                      self.html_transferred_unit, metadata)
    ]


BENCHMARK_NAME = 'apachebench'
BENCHMARK_CONFIG = """
apachebench:
  description: Runs apachebench benchmark.
  vm_groups:
    client:
      os_type: ubuntu1804
      vm_spec: *default_single_core
      vm_count: 1
    server:
      os_type: ubuntu1804
      vm_spec: *default_single_core
"""
FLAGS = flags.FLAGS

# Default port for Apache
_PORT = 80
# Constants used to differentiate between runs using internal or external ip
_INTERNAL_IP_CONFIG = ApacheBenchConfig(
        'internal-ip',
        'internal_ip',
        'internal_results.txt',
        'internal_ip_percentiles.csv',
        'internal_ip_request_times.tsv')
_EXTERNAL_IP_CONFIG = ApacheBenchConfig(
        'external-ip',
        'ip_address',
        'external_results.txt',
        'external_ip_percentiles.csv',
        'external_ip_request_times.tsv')
# Map from ip_addresses flag enum to list of ip configs to use
_IP_ADDRESSES_TO_IP_CONFIGS = {
    vm_util.IpAddressSubset.INTERNAL: [_INTERNAL_IP_CONFIG],
    vm_util.IpAddressSubset.EXTERNAL: [_EXTERNAL_IP_CONFIG],
    vm_util.IpAddressSubset.BOTH: [_INTERNAL_IP_CONFIG, _EXTERNAL_IP_CONFIG],
    vm_util.IpAddressSubset.REACHABLE: [
        _INTERNAL_IP_CONFIG, _EXTERNAL_IP_CONFIG
    ]
}

_NUM_REQUESTS = flags.DEFINE_integer(
    'apachebench_num_requests', default=10000,
    help='Number of requests to perform for the benchmarking session.')
_CONCURRENCY = flags.DEFINE_integer(
    'apachebench_concurrency', default=1,
    help='Number of multiple requests to perform at a time.')
_KEEP_ALIVE = flags.DEFINE_boolean(
    'apachebench_keep_alive', default=True,
    help='Enable the HTTP KeepAlive feature.')
_HTTP_METHOD = flags.DEFINE_enum(
    'apachebench_http_method', default='GET',
    enum_values=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
    help='Custom HTTP method for the requests.')
_SOCKET_TIMEOUT = flags.DEFINE_integer(
    'apachebench_socket_timeout', default=30,
    help='Maximum number of seconds to wait before the socket times out.')
_TIMELIMIT = flags.DEFINE_integer(
    'apachebench_timelimit',
    default=None,
    help='Maximum number of seconds to spend for benchmarking.'
    'After the timelimit is reached, additional requests will not be sent.')
_APACHE_SERVER_CONTENT_SIZE = flags.DEFINE_integer(
    'apachebench_server_content_size',
    default=2070000,  # Default based on average website size of 2.07 MB
    help='The size of the content the Apache server will serve (in bytes).')
_CLIENT_VMS = flags.DEFINE_integer(
    'apachebench_client_vms',
    default=1,
    help='The number of client VMs to use.')


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['vm_groups']['client']['vm_count'] = _CLIENT_VMS.value
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares the apachebench benchmark."""
  client_vms = benchmark_spec.vm_groups['client']
  server = benchmark_spec.vm_groups['server'][0]

  vm_util.RunThreaded(lambda vm: vm.Install('apache2_utils'), client_vms)
  server.Install('apache2_server')

  server.AllowPort(_PORT)
  apache2_server.SetupServer(server, _APACHE_SERVER_CONTENT_SIZE.value)
  apache2_server.StartServer(server)


def _ParseHistogramFromFile(vm: virtual_machine.VirtualMachine, path: str,
                            column: str = 'ttime') -> sample._Histogram:
  """Loads tsv file created by ApacheBench at path in the VM into a histogram.

  The tsv located by path inside of the virtual machine vm will be loaded and
  used to create a sample._Histogram.

  Args:
    vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    path: The path to the tsv file output inside of the VM which ran AB.
    column: The column with numeric data to create the Histogram from.
  Returns:
    sample._Histogram created from the data in the column of the tsv file
    specified.

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

  return sample.MakeHistogram(data_frame[column].tolist())


def _ParsePercentilesFromFile(vm: virtual_machine.VirtualMachine,
                              path: str) -> Dict[str, str]:
  """Loads the csv file created by ApacheBench at path in the VM into a dict.

  The csv located by path inside of the virtual machine vm will be loaded. For
  each percentage served, a set of key/value pairs is created.

  Args:
    vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    path: The path to the csv file output inside of the VM which ran AB.
  Returns:
    Dictonary where each key represents a request percentile
    and the value for 'x' represents the time in ms it took for a request in the
    xth percentile to run.

  Example ApacheBench percentile output.

  percent, response time
  0,10
  1,12
  2,15
  """
  csv, _ = vm.RemoteCommand(f'cat {path}')
  csv = csv.strip()
  result = {}

  # Skip header line
  for row in csv.split('\n')[1:]:
    percent, response_time = row.split(',')
    result[percent] = response_time

  return result


def _ParseResultsFromFile(
    vm: virtual_machine.VirtualMachine,
    path: str,) -> ApacheBenchResults:
  """Loads the txt file created by ApacheBench at path in the VM into a dict.

  The txt located by path inside of the virtual machine vm will be loaded. For
  each row of relevant results, a set of key/value pairs is created. The keys
  will all be prepended with `apachebench` or similar.

  Args:
    vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    path: The path to the output file inside of the VM which ran AB.
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
  output, _ = vm.RemoteCommand(f'cat {path}')

  complete_requests = regex_util.ExtractExactlyOneMatch(
      r'Complete requests:\s+(\d+)', output)

  failed_requests = regex_util.ExtractExactlyOneMatch(
      r'Failed requests:\s+(\d+)', output)

  requests_per_second, requests_per_second_unit = regex_util.ExtractExactlyOneMatch(
      r'Requests per second:\s+(\d*\.?\d*) \[([^\s]+)\] \(\w+\)', output)

  time_per_request, time_per_request_unit = regex_util.ExtractExactlyOneMatch(
      r'Time per request:\s+(\d*\.?\d*) \[([^\s]+)\] \(mean\)', output)

  time_per_request_concurrent, time_per_request_concurrent_unit = regex_util.ExtractExactlyOneMatch(
      r'Time per request:\s+(\d*\.?\d*) \[([^\s]+)\] \(mean,', output)

  transfer_rate, transfer_rate_unit = regex_util.ExtractExactlyOneMatch(
      r'Transfer rate:\s+(\d*\.?\d*) \[([^\s]+)\] received', output)

  time_taken_for_tests, time_taken_for_tests_unit = regex_util.ExtractExactlyOneMatch(
      r'Time taken for tests:\s+(\d*\.?\d*) ([^\s]+)', output)

  total_transferred, total_transferred_unit = regex_util.ExtractExactlyOneMatch(
      r'Total transferred:\s+(\d*\.?\d*) ([^\s]+)', output)

  html_transferred, html_transferred_unit = regex_util.ExtractExactlyOneMatch(
      r'HTML transferred:\s+(\d*\.?\d*) ([^\s]+)', output)

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
      html_transferred_unit=html_transferred_unit)


def GetMetadata(results: ApacheBenchResults,
                config: ApacheBenchConfig) -> Dict[str, Any]:
  """Returns the apachebench metadata as a dictonary.

  Args:
    results: The dictionary returned by _Run.
    config: The ApacheBenchConfig passed to _Run.
  Returns:
    A dictonary of metadata to be included in all ApacheBench samples.
  """

  metadata = {
      'apachebench_requests': _NUM_REQUESTS.value,
      'apachebench_concurrency_level': _CONCURRENCY.value,
      'apachebench_keep_alive': _KEEP_ALIVE.value,
      'apachebench_http_method': _HTTP_METHOD.value,
      'apachebench_socket_timeout': _SOCKET_TIMEOUT.value,
      'apachebench_timelimit': _TIMELIMIT.value,
      'apachebench_ip_type': config.ip_type,
      'apachebench_client_vms': _CLIENT_VMS.value,
      'apachebench_complete_requests': results.complete_requests,
      'apachebench_time_taken_for_tests':
          f'{results.time_taken_for_tests} {results.time_taken_for_tests_unit}'
  }

  return metadata


def _Run(benchmark_spec: bm_spec.BenchmarkSpec,
         config: ApacheBenchConfig) -> List[ApacheBenchResults]:
  """Runs apachebench benchmark."""
  vm_groups = benchmark_spec.vm_groups
  client_vms = vm_groups['client']
  server = vm_groups['server'][0]

  # Building the ApacheBench command with appropriate flags
  cmd = 'ab -r '

  if _KEEP_ALIVE.value:
    cmd += '-k '
  if _TIMELIMIT.value:
    cmd += f'-t {_TIMELIMIT.value} '

  cmd += (
      f'-n {_NUM_REQUESTS.value} '
      f'-c {_CONCURRENCY.value} '
      f'-m {_HTTP_METHOD.value} '
      f'-s {_SOCKET_TIMEOUT.value} '
      f'-e {config.percentile_output_path} '
      f'-g {config.raw_request_data_path} '
      f'http://{getattr(server, config.server_attr)}:{_PORT}/ '
      f'1> {config.output_path}')

  vm_util.RunThreaded(lambda vm: vm.RemoteCommand(cmd), client_vms)

  return vm_util.RunThreaded(
      lambda vm: _ParseResultsFromFile(vm, config.output_path), client_vms)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs apachebench and reports the results."""
  client_vms = benchmark_spec.vm_groups['client']
  samples = []

  for config in _IP_ADDRESSES_TO_IP_CONFIGS[FLAGS.ip_addresses]:
    results = _Run(benchmark_spec, config)

    # Format pkb-style samples
    for vm, result in zip(client_vms, results):
      metadata = GetMetadata(result, config)
      metadata['client_vm'] = vm.hostname

      samples += result.GetSamples(metadata)

      # Add percentile data to a sample
      percentiles = _ParsePercentilesFromFile(vm, config.percentile_output_path)
      percentiles.update(metadata)
      samples.append(
          sample.Sample('ApacheBench Percentiles', 0, '', percentiles))

      # Add a total request time histogram sample
      histogram = _ParseHistogramFromFile(vm, config.raw_request_data_path)
      samples.append(sample.CreateHistogramSample(
          histogram, '', '', 'ms', metadata, 'ApacheBench Histrogram'))

  return samples


def Cleanup(unused_benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  pass
