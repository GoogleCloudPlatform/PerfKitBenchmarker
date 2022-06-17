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

from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine

BENCHMARK_NAME = 'apachebench'
BENCHMARK_CONFIG = """
apachebench:
  description: Runs apachebench benchmark.
  vm_groups:
    client:
      os_type: ubuntu1804
      vm_spec: *default_single_core
    server:
      os_type: ubuntu1804
      vm_spec: *default_single_core
"""

# Default port for Apache
_PORT = 80
# Output file for ApacheBench results
_OUTPUT = 'ab_results.txt'
# Output file for ApacheBench request percentile data
_PERCENTILE_OUTPUT = 'percentiles.csv'
_APACHEBENCH_METRICS = [
    'Failed requests',
    'Requests per second (mean)',
    'Time per request (mean)', 'Transfer rate',
    'Time per request (mean, across all concurrent requests)'
]

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
    help='Maximum number of seconds to spend for benchmarking. '
    'After the timelimit is reached, additional requests will not be sent.')


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(unused_benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares the apachebench benchmark."""
  pass


def _ParsePercentilesFromFile(unused_vm: virtual_machine.VirtualMachine,
                              unused_path: str) -> Dict[str, List[str]]:
  """Loads the csv file created by ApacheBench at path in the VM into a dict.

  The csv located by path inside of the virtual machine vm will be loaded. For
  each percentage served, a set of key/value pairs is created.
  Args:
    unused_vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    unused_path: The path to the csv file output inside of the VM which ran AB.
  Returns:
    Dictonary where each key represents a request percentile
    and the value for 'x' represents the time in ms it took for a request in the
    xth percentile to run.
  """
  return {}


def _ParseResultsFromFile(unused_vm: virtual_machine.VirtualMachine,
                          unused_path: str) -> Dict[str, str]:
  """Loads the txt file created by ApacheBench at path in the VM into a dict.

  The txt located by path inside of the virtual machine vm will be loaded. For
  each row of relevant results, a set of key/value pairs is created. The keys
  will all be prepended with `apachebench` or similar.

  Args:
    unused_vm: The Virtual Machine that has run an ApacheBench (AB) benchmark.
    unused_path:  The path to the txt file output inside of the VM which ran AB.
  Returns:
    Dictionary where each key/value pair represents a relevant result
    produced by an ApacheBench benchmark.
  """
  return {}


def GetMetadata(results: Dict[str, str]) -> Dict[str, Any]:
  """Returns the apachebench metadata as a dictonary.

  Args:
    results: The dictionary returned by _Run.
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
      'apachebench_time_taken_for_tests': results.get('Time taken for tests'),
      'apachebench_complete_requests': results.get('Complete requests'),
      'apachebench_total_transferred': results.get('Total transferred'),
      'apachebench_html_transferred': results.get('HTML transferred'),
  }

  return metadata


def _Run(benchmark_spec: bm_spec.BenchmarkSpec) -> Dict[str, str]:
  """Runs apachebench benchmark."""
  vm_groups = benchmark_spec.vm_groups
  client = vm_groups['client'][0]

  return _ParseResultsFromFile(client, _OUTPUT)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs apachebench and reports the results."""
  results = _Run(benchmark_spec)
  unused_metadata = GetMetadata(results)

  return []


def Cleanup(unused_benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  pass
