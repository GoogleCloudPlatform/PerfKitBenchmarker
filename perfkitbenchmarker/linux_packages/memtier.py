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
"""Module containing memtier installation, utilization and cleanup functions."""

import copy
import dataclasses
import json
import logging
import math
import os
import pathlib
import re
import time
from typing import Any, Dict, List, Optional, Text, Tuple, Union

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

GIT_REPO = 'https://github.com/RedisLabs/memtier_benchmark'
GIT_TAG = '793d74dbc09395dfc241342d847730a6197d7c0c'
MEMTIER_DIR = '%s/memtier_benchmark' % linux_packages.INSTALL_DIR
APT_PACKAGES = ('build-essential autoconf automake libpcre3-dev '
                'libevent-dev pkg-config zlib1g-dev libssl-dev')
YUM_PACKAGES = (
    'zlib-devel pcre-devel libmemcached-devel libevent-devel openssl-devel')
MEMTIER_RESULTS = '/tmp/memtier_results'

_LOAD_NUM_PIPELINES = 100  # Arbitrarily high for loading
_WRITE_ONLY = '1:0'
CPU_TOLERANCE = 0.05
WARM_UP_SECONDS = 360
JSON_OUT_FILE = '/tmp/json_data'
# upper limit to pipelines when binary searching for latency-capped throughput.
# arbitrarily chosen for large latency.
MAX_PIPELINES_COUNT = 5000
# upper limit to clients when binary searching for latency-capped throughput
# arbitrarily chosen for large latency.
MAX_CLIENTS_COUNT = 1000

MemtierHistogram = List[Dict[str, Union[float, int]]]

FLAGS = flags.FLAGS


class MemtierMode(object):
  """Enum of options for --memtier_run_mode."""
  MEASURE_CPU_LATENCY = 'MEASURE_CPU_LATENCY'
  NORMAL_RUN = 'NORMAL_RUN'
  ALL = (MEASURE_CPU_LATENCY, NORMAL_RUN)


MEMTIER_PROTOCOL = flags.DEFINE_enum(
    'memtier_protocol', 'memcache_binary',
    ['memcache_binary', 'redis', 'memcache_text'],
    'Protocol to use. Supported protocols are redis, '
    'memcache_text, and memcache_binary. '
    'Defaults to memcache_binary.')
MEMTIER_RUN_COUNT = flags.DEFINE_integer(
    'memtier_run_count', 1, 'Number of full-test iterations to perform. '
    'Defaults to 1.')
MEMTIER_RUN_DURATION = flags.DEFINE_integer(
    'memtier_run_duration', None, 'Mutually exclusive with memtier_requests.'
    'Duration for each client count in seconds. '
    'By default, test length is set '
    'by memtier_requests, the number of requests sent by each '
    'client. By specifying run_duration, key space remains '
    'the same (from 1 to memtier_requests), but test stops '
    'once run_duration is passed. '
    'Total test duration = run_duration * runs * '
    'len(memtier_clients).')
MEMTIER_REQUESTS = flags.DEFINE_integer(
    'memtier_requests', 10000, 'Mutually exclusive with memtier_run_duration. '
    'Number of total requests per client. Defaults to 10000.')
flag_util.DEFINE_integerlist(
    'memtier_clients', [50],
    'Comma separated list of number of clients per thread. '
    'Specify more than 1 value to vary the number of clients. '
    'Defaults to [50].')
flag_util.DEFINE_integerlist('memtier_threads', [4],
                             'Number of threads. Defaults to 4.')
MEMTIER_RATIO = flags.DEFINE_string(
    'memtier_ratio', '1:9', 'Set:Get ratio. Defaults to 1:9 Sets:Gets.')
MEMTIER_DATA_SIZE = flags.DEFINE_integer(
    'memtier_data_size', 32, 'Object data size. Defaults to 32 bytes.')
MEMTIER_KEY_PATTERN = flags.DEFINE_string(
    'memtier_key_pattern', 'R:R',
    'Set:Get key pattern. G for Gaussian distribution, R for '
    'uniform Random, S for Sequential. Defaults to R:R.')
MEMTIER_LOAD_KEY_MAXIMUM = flags.DEFINE_integer(
    'memtier_load_key_maximum', None, 'Key ID maximum value to load. '
    'The range of keys will be from 1 (min) to this specified max key value. '
    'If not set, defaults to memtier_key_maximum. Setting this different from '
    'memtier_key_maximum allows triggering of eviction behavior.')
MEMTIER_KEY_MAXIMUM = flags.DEFINE_integer(
    'memtier_key_maximum', 10000000, 'Key ID maximum value. The range of keys '
    'will be from 1 (min) to this specified max key value.')
MEMTIER_LATENCY_CAPPED_THROUGHPUT = flags.DEFINE_bool(
    'latency_capped_throughput', False,
    'Measure latency capped throughput. Use in conjunction with '
    'memtier_latency_cap. Defaults to False. ')
MEMTIER_LATENCY_CAP = flags.DEFINE_float(
    'memtier_latency_cap', 1.0, 'Latency cap in ms. Use in conjunction with '
    'latency_capped_throughput. Defaults to 1ms.')
MEMTIER_RUN_MODE = flags.DEFINE_enum(
    'memtier_run_mode', MemtierMode.NORMAL_RUN, MemtierMode.ALL,
    'Mode that the benchmark is set to. NORMAL_RUN measures latency and '
    'throughput, MEASURE_CPU_LATENCY measures single threaded latency at '
    'memtier_cpu_target. When measuring CPU latency flags for '
    'clients, threads, and pipelines are ignored and '
    'memtier_cpu_target and memtier_cpu_duration must not '
    'be None.')
MEMTIER_CPU_TARGET = flags.DEFINE_float(
    'memtier_cpu_target', 0.5,
    'The target CPU utilization when running memtier and trying to get the '
    'latency at variable CPU metric. The target can range from 1%-100% and '
    'represents the percent CPU utilization (e.g. 0.5 -> 50% CPU utilization)')
MEMTIER_CPU_DURATION = flags.DEFINE_integer(
    'memtier_cpu_duration', 300, 'Number of seconds worth of data taken '
    'to measure the CPU utilization of an instance. When MEASURE_CPU_LATENCY '
    'mode is on, memtier_run_duration is set to memtier_cpu_duration '
    '+ WARM_UP_SECONDS.')
flag_util.DEFINE_integerlist(
    'memtier_pipeline', [1],
    'Number of pipelines to use for memtier. Defaults to 1, '
    'i.e. no pipelining.')
MEMTIER_CLUSTER_MODE = flags.DEFINE_bool(
    'memtier_cluster_mode', False, 'Passthrough for --cluster-mode flag')
MEMTIER_TIME_SERIES = flags.DEFINE_bool(
    'memtier_time_series', False, 'Include per second time series output '
    'for ops and max latency. This greatly increase the number of samples.')


def YumInstall(vm):
  """Installs the memtier package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(YUM_PACKAGES)

  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, MEMTIER_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(MEMTIER_DIR, GIT_TAG))
  pkg_config = 'PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:${PKG_CONFIG_PATH}'
  vm.RemoteCommand('cd {0} && autoreconf -ivf && {1} ./configure && '
                   'sudo make install'.format(MEMTIER_DIR, pkg_config))


def AptInstall(vm):
  """Installs the memtier package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(APT_PACKAGES)
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, MEMTIER_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(MEMTIER_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && autoreconf -ivf && ./configure && '
                   'sudo make install'.format(MEMTIER_DIR))


def _Uninstall(vm):
  """Uninstalls the memtier package on the VM."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(MEMTIER_DIR))


def YumUninstall(vm):
  """Uninstalls the memtier package on the VM."""
  _Uninstall(vm)


def AptUninstall(vm):
  """Uninstalls the memtier package on the VM."""
  _Uninstall(vm)


def BuildMemtierCommand(
    server: Optional[str] = None,
    port: Optional[str] = None,
    protocol: Optional[str] = None,
    clients: Optional[int] = None,
    threads: Optional[int] = None,
    ratio: Optional[str] = None,
    data_size: Optional[int] = None,
    pipeline: Optional[int] = None,
    key_minimum: Optional[int] = None,
    key_maximum: Optional[int] = None,
    key_pattern: Optional[str] = None,
    requests: Optional[Union[str, int]] = None,
    run_count: Optional[int] = None,
    random_data: Optional[bool] = None,
    test_time: Optional[int] = None,
    outfile: Optional[pathlib.PosixPath] = None,
    password: Optional[str] = None,
    cluster_mode: Optional[bool] = None,
    json_out_file: Optional[pathlib.PosixPath] = None,
) -> str:
  """Returns command arguments used to run memtier."""
  # Arguments passed with a parameter
  args = {
      'server': server,
      'port': port,
      'authenticate': password,
      'protocol': protocol,
      'clients': clients,
      'threads': threads,
      'ratio': ratio,
      'data-size': data_size,
      'pipeline': pipeline,
      'key-minimum': key_minimum,
      'key-maximum': key_maximum,
      'key-pattern': key_pattern,
      'requests': requests,
      'run-count': run_count,
      'test-time': test_time,
      'out-file': outfile,
      'json-out-file': json_out_file,
      'print-percentile': '50,90,95,99,99.9',
  }
  # Arguments passed without a parameter
  no_param_args = {'random-data': random_data, 'cluster-mode': cluster_mode}
  # Build the command
  cmd = ['memtier_benchmark']
  for arg, value in args.items():
    if value is not None:
      cmd.extend([f'--{arg}', str(value)])
  for no_param_arg, value in no_param_args.items():
    if value:
      cmd.append(f'--{no_param_arg}')
  return ' '.join(cmd)


def Load(client_vm,
         server_ip: str,
         server_port: str,
         server_password: Optional[str] = None) -> None:
  """Preload the server with data."""
  load_key_maximum = (
      MEMTIER_LOAD_KEY_MAXIMUM.value
      if MEMTIER_LOAD_KEY_MAXIMUM.value else MEMTIER_KEY_MAXIMUM.value)
  cmd = BuildMemtierCommand(
      server=server_ip,
      port=server_port,
      protocol=MEMTIER_PROTOCOL.value,
      clients=1,
      threads=1,
      ratio=_WRITE_ONLY,
      data_size=MEMTIER_DATA_SIZE.value,
      pipeline=_LOAD_NUM_PIPELINES,
      key_minimum=1,
      key_maximum=load_key_maximum,
      requests='allkeys',
      password=server_password)
  client_vm.RemoteCommand(cmd)


def RunOverAllThreadsPipelinesAndClients(
    client_vm,
    server_ip: str,
    server_port: str,
    password: Optional[str] = None) -> List[sample.Sample]:
  """Runs memtier over all pipeline and thread combinations."""
  samples = []
  for pipeline in FLAGS.memtier_pipeline:
    for client_thread in FLAGS.memtier_threads:
      for client in FLAGS.memtier_clients:
        logging.info(
            'Start benchmarking redis/memcached using memtier:\n'
            '\tmemtier client: %s'
            '\tmemtier threads: %s'
            '\tmemtier pipeline, %s', client, client_thread, pipeline)
        results = _Run(
            vm=client_vm,
            server_ip=server_ip,
            server_port=server_port,
            threads=client_thread,
            pipeline=pipeline,
            clients=client,
            password=password)
        metadata = GetMetadata(
            clients=client, threads=client_thread, pipeline=pipeline)
        samples.extend(results.GetSamples(metadata))
  return samples


@dataclasses.dataclass(frozen=True)
class MemtierBinarySearchParameters:
  """Parameters to aid binary search of memtier."""
  lower_bound: float
  upper_bound: float
  pipelines: int
  threads: int
  clients: int


def MeasureLatencyCappedThroughput(
    client_vm,
    server_ip: str,
    server_port: str,
    password: Optional[str] = None) -> List[sample.Sample]:
  """Runs memtier to find the maximum throughput under a latency cap."""
  samples = []

  for modify_load_func in [_ModifyPipelines, _ModifyClients]:
    parameters = MemtierBinarySearchParameters(
        lower_bound=0,
        upper_bound=math.inf,
        pipelines=1,
        threads=1,
        clients=1)
    current_max_result = MemtierResult(0, 0, 0, 0, 0, 0, [], [], [], [])
    current_metadata = None
    while parameters.lower_bound < (parameters.upper_bound - 1):
      result = _Run(
          vm=client_vm,
          server_ip=server_ip,
          server_port=server_port,
          threads=parameters.threads,
          pipeline=parameters.pipelines,
          clients=parameters.clients,
          password=password)
      logging.info('Binary search for latency capped throughput.\n'
                   '\tMemtier ops throughput: %s'
                   '\tmemtier 95th percentile latency: %s'
                   '\tupper bound: %s'
                   '\tlower bound: %s',
                   result.ops_per_sec, result.p95_latency,
                   parameters.lower_bound, parameters.upper_bound)
      if result.ops_per_sec > current_max_result.ops_per_sec:
        current_max_result = result
        current_metadata = GetMetadata(
            clients=parameters.clients,
            threads=parameters.threads,
            pipeline=parameters.pipelines)
      # 95 percentile used to decide latency cap
      parameters = modify_load_func(parameters, result.p95_latency)
    samples.extend(current_max_result.GetSamples(current_metadata))
  return samples


def _ModifyPipelines(current_parameters: 'MemtierBinarySearchParameters',
                     latency: float) -> 'MemtierBinarySearchParameters':
  """Modify pipelines count for next iteration of binary search."""
  if latency <= MEMTIER_LATENCY_CAP.value:
    lower_bound = current_parameters.pipelines
    upper_bound = min(current_parameters.upper_bound, MAX_PIPELINES_COUNT)
  else:
    lower_bound = current_parameters.lower_bound
    upper_bound = current_parameters.pipelines

  pipelines = lower_bound + math.ceil((upper_bound - lower_bound) / 2)
  return MemtierBinarySearchParameters(
      lower_bound=lower_bound,
      upper_bound=upper_bound,
      pipelines=pipelines,
      threads=1,
      clients=1)


def _ModifyClients(current_parameters: 'MemtierBinarySearchParameters',
                   latency: float) -> 'MemtierBinarySearchParameters':
  """Modify clients count for next iteration of binary search."""
  if latency <= MEMTIER_LATENCY_CAP.value:
    lower_bound = current_parameters.clients * current_parameters.threads
    upper_bound = min(current_parameters.upper_bound, MAX_CLIENTS_COUNT)
  else:
    lower_bound = current_parameters.lower_bound
    upper_bound = current_parameters.clients * current_parameters.threads

  total_clients = lower_bound + math.ceil((upper_bound - lower_bound) / 2)
  threads = _FindFactor(total_clients)
  clients = total_clients // threads
  return MemtierBinarySearchParameters(
      lower_bound=lower_bound,
      upper_bound=upper_bound,
      pipelines=1,
      threads=threads,
      clients=clients)


def _FindFactor(number):
  """Find any factor of the given number. Returns 1 for primes."""
  i = round(math.sqrt(number))
  while i > 0:
    if number % i == 0:
      return i
    i -= 1


def RunGetLatencyAtCpu(cloud_instance, client_vms):
  """Run a modified binary search to find latency at a given CPU.

  Args:
    cloud_instance: A managed cloud instance. Only works on managed cloud
      instances but could extend to vms.
    client_vms: Need at least two client vms, one to hold the CPU utilization
      load and the other to get the single threaded latency.

  Returns:
    A list of sample.Sample instances.
  """
  samples = []
  server_ip = cloud_instance.GetMemoryStoreIp()
  server_port = cloud_instance.GetMemoryStorePort()
  password = cloud_instance.GetMemoryStorePassword()
  load_vm = client_vms[0]
  latency_measurement_vm = client_vms[-1]

  # Implement modified binary search to find optimal client count +/- tolerance
  # of target CPU usage
  target = MEMTIER_CPU_TARGET.value

  # Larger clusters need two threads to get to maximum CPU
  threads = 1 if cloud_instance.GetInstanceSize() < 150 or target < 0.5 else 2
  pipeline = 1

  # Set maximum of the binary search based off size of the instance
  upper_bound = cloud_instance.GetInstanceSize() // 2 + 10
  lower_bound = 1
  current_clients = 1
  while lower_bound < upper_bound:
    current_clients = (upper_bound + lower_bound) // 2
    _Run(
        vm=load_vm,
        server_ip=server_ip,
        server_port=server_port,
        threads=threads,
        pipeline=pipeline,
        clients=current_clients,
        password=password)

    cpu_percent = cloud_instance.MeasureCpuUtilization(
        MEMTIER_CPU_DURATION.value)

    if not cpu_percent:
      raise errors.Benchmarks.RunError(
          'Could not measure CPU utilization for the instance.')
    logging.info(
        'Tried %s clients and got %s%% CPU utilization for the last run with '
        'the target CPU being %s%%', current_clients, cpu_percent, target)

    if cpu_percent < target - CPU_TOLERANCE:
      lower_bound = current_clients + 1
    elif cpu_percent > target + CPU_TOLERANCE:
      upper_bound = current_clients - 1
    else:
      logging.info('Finished binary search and the current client count is %s',
                   current_clients)
      process_args = [
          (_Run, [
              load_vm, server_ip, server_port, threads, pipeline,
              current_clients, password
          ], {}),
          (_GetSingleThreadedLatency,
           [latency_measurement_vm, server_ip, server_port, password], {})
      ]
      results = vm_util.RunParallelThreads(process_args, len(process_args))
      metadata = GetMetadata(
          clients=current_clients, threads=threads, pipeline=pipeline)
      metadata['measured_cpu_percent'] = cloud_instance.MeasureCpuUtilization(
          MEMTIER_CPU_DURATION.value)
      samples.extend(results[1].GetSamples(metadata))
      return samples

  # If complete binary search without finding a client count,
  # it's not possible on this configuration.
  raise errors.Benchmarks.RunError(
      'Completed binary search and did not find a client count that worked for '
      'this configuration and CPU utilization.')


def _GetSingleThreadedLatency(client_vm, server_ip: str, server_port: str,
                              password: str) -> 'MemtierResult':
  """Wait for background run to stabilize then send single threaded request."""
  time.sleep(300)
  return _Run(
      vm=client_vm,
      server_ip=server_ip,
      server_port=server_port,
      threads=1,
      pipeline=1,
      clients=1,
      password=password)


def _Run(vm,
         server_ip: str,
         server_port: str,
         threads: int,
         pipeline: int,
         clients: int,
         password: Optional[str] = None) -> 'MemtierResult':
  """Runs the memtier benchmark on the vm."""
  results_file = pathlib.PosixPath(f'{MEMTIER_RESULTS}_{server_port}')
  vm.RemoteCommand(f'rm -f {results_file}')
  json_results_file = (pathlib.PosixPath(f'{JSON_OUT_FILE}_{server_port}')
                       if MEMTIER_TIME_SERIES.value else None)
  vm.RemoteCommand(f'rm -f {json_results_file}')
  # Specify one of run requests or run duration.
  requests = (
      MEMTIER_REQUESTS.value if MEMTIER_RUN_DURATION.value is None else None)
  test_time = (
      MEMTIER_RUN_DURATION.value
      if MEMTIER_RUN_MODE.value == MemtierMode.NORMAL_RUN else WARM_UP_SECONDS +
      MEMTIER_CPU_DURATION.value)
  cmd = BuildMemtierCommand(
      server=server_ip,
      port=server_port,
      protocol=MEMTIER_PROTOCOL.value,
      run_count=MEMTIER_RUN_COUNT.value,
      clients=clients,
      threads=threads,
      ratio=MEMTIER_RATIO.value,
      data_size=MEMTIER_DATA_SIZE.value,
      key_pattern=MEMTIER_KEY_PATTERN.value,
      pipeline=pipeline,
      key_minimum=1,
      key_maximum=MEMTIER_KEY_MAXIMUM.value,
      random_data=True,
      test_time=test_time,
      requests=requests,
      password=password,
      outfile=results_file,
      cluster_mode=MEMTIER_CLUSTER_MODE.value,
      json_out_file=json_results_file)
  vm.RemoteCommand(cmd)

  output_path = os.path.join(
      vm_util.GetTempDir(), f'memtier_results_{server_port}')
  vm_util.IssueCommand(['rm', '-f', output_path])
  vm.PullFile(vm_util.GetTempDir(), results_file)

  time_series_json = None
  if json_results_file:
    json_path = os.path.join(
        vm_util.GetTempDir(), f'json_data_{server_port}')
    vm_util.IssueCommand(['rm', '-f', json_path])
    vm.PullFile(vm_util.GetTempDir(), json_results_file)
    with open(json_path, 'r') as ts_json:
      time_series_json = ts_json.read()

  with open(output_path, 'r') as output:
    summary_data = output.read()
  return MemtierResult.Parse(summary_data, time_series_json)


def GetMetadata(clients: int, threads: int, pipeline: int) -> Dict[str, Any]:
  """Metadata for memtier test."""
  meta = {
      'memtier_protocol': MEMTIER_PROTOCOL.value,
      'memtier_run_count': MEMTIER_RUN_COUNT.value,
      'memtier_requests': MEMTIER_REQUESTS.value,
      'memtier_threads': threads,
      'memtier_clients': clients,
      'memtier_ratio': MEMTIER_RATIO.value,
      'memtier_key_maximum': MEMTIER_KEY_MAXIMUM.value,
      'memtier_data_size': MEMTIER_DATA_SIZE.value,
      'memtier_key_pattern': MEMTIER_KEY_PATTERN.value,
      'memtier_pipeline': pipeline,
      'memtier_version': GIT_TAG,
      'memtier_run_mode': MEMTIER_RUN_MODE.value,
      'memtier_cluster_mode': MEMTIER_CLUSTER_MODE.value,
  }
  if MEMTIER_RUN_DURATION.value:
    meta['memtier_run_duration'] = MEMTIER_RUN_DURATION.value
  if MEMTIER_RUN_MODE.value == MemtierMode.MEASURE_CPU_LATENCY:
    meta['memtier_cpu_target'] = MEMTIER_CPU_TARGET.value
    meta['memtier_cpu_duration'] = MEMTIER_CPU_DURATION.value
  if MEMTIER_LOAD_KEY_MAXIMUM.value:
    meta['memtier_load_key_maximum'] = MEMTIER_LOAD_KEY_MAXIMUM.value
  return meta


@dataclasses.dataclass
class MemtierResult:
  """Class that represents memtier results."""
  ops_per_sec: float
  kb_per_sec: float
  latency_ms: float
  p90_latency: float
  p95_latency: float
  p99_latency: float
  get_latency_histogram: MemtierHistogram
  set_latency_histogram: MemtierHistogram
  ops_time_series: List[Tuple[int, int]]
  max_latency_time_series: List[Tuple[int, int]]

  @classmethod
  def Parse(cls, memtier_results: Text,
            time_series_json: Optional[Text]) -> 'MemtierResult':
    """Parse memtier_benchmark result textfile and return results.

    Args:
      memtier_results: Text output of running Memtier benchmark.
      time_series_json: Time series data of the results in json format.

    Returns:
      MemtierResult object.

    Example memtier_benchmark output.

    4         Threads
    50        Connections per thread
    20        Seconds
    Type        Ops/sec     Hits/sec   Misses/sec   Avg. Latency  ...    KB/sec
    ----------------------------------------------------------------------------
    Sets        4005.50          ---          ---        4.50600  ...    308.00
    Gets       40001.05         0.00     40001.05        4.54300  ...    1519.00
    Totals     44006.55         0.00     40001.05        4.54000  ...    1828.00

    Request Latency Distribution
    Type        <= msec      Percent
    ------------------------------------------------------------------------
    SET               0         9.33
    SET               1        71.07
    ...
    SET              33       100.00
    SET              36       100.00
    ---
    GET               0        10.09
    GET               1        70.88
    ..
    GET              40       100.00
    GET              41       100.00
    """
    aggregated_result = _ParseTotalThroughputAndLatency(memtier_results)
    set_histogram, get_histogram = _ParseHistogram(memtier_results)
    ops_time_series = []
    max_latency_time_series = []
    if time_series_json:
      ops_time_series, max_latency_time_series = _ParseTimeSeries(
          time_series_json)
    return cls(
        ops_per_sec=aggregated_result.ops_per_sec,
        kb_per_sec=aggregated_result.kb_per_sec,
        latency_ms=aggregated_result.latency_ms,
        p90_latency=aggregated_result.p90_latency,
        p95_latency=aggregated_result.p95_latency,
        p99_latency=aggregated_result.p99_latency,
        get_latency_histogram=get_histogram,
        set_latency_histogram=set_histogram,
        ops_time_series=ops_time_series,
        max_latency_time_series=max_latency_time_series
        )

  def GetSamples(self, metadata: Dict[str, Any]) -> List[sample.Sample]:
    """Return this result as a list of samples."""
    metadata['avg_latency'] = self.latency_ms
    metadata['p90_latency'] = self.p90_latency
    metadata['p95_latency'] = self.p95_latency
    metadata['p99_latency'] = self.p99_latency
    samples = [
        sample.Sample('Ops Throughput', self.ops_per_sec, 'ops/s', metadata),
        sample.Sample('KB Throughput', self.kb_per_sec, 'KB/s', metadata),
        sample.Sample('Latency', self.latency_ms, 'ms', metadata),
    ]
    for name, histogram in [('get', self.get_latency_histogram),
                            ('set', self.set_latency_histogram)]:
      hist_meta = copy.deepcopy(metadata)
      hist_meta.update({'histogram': json.dumps(histogram)})
      samples.append(
          sample.Sample(f'{name} latency histogram', 0, '', hist_meta))
    for interval, count in self.ops_time_series:
      time_series_meta = copy.deepcopy(metadata)
      time_series_meta.update({
          'time_series_sec': interval,
          'time_series_ops': count,
      })
      samples.append(
          sample.Sample('Ops Time Series', count, 'ops', time_series_meta))
    for interval, latency in self.max_latency_time_series:
      time_series_meta = copy.deepcopy(metadata)
      time_series_meta.update({
          'time_series_sec': interval,
          'time_series_max_latency': latency,
      })
      samples.append(
          sample.Sample('Max Latency Time Series', latency, 'ms',
                        time_series_meta))
    return samples


def _ParseHistogram(
    memtier_results: Text) -> Tuple[MemtierHistogram, MemtierHistogram]:
  """Parses the 'Request Latency Distribution' section of memtier output."""
  set_histogram = []
  get_histogram = []
  total_requests = MEMTIER_REQUESTS.value
  sets = int(MEMTIER_RATIO.value.split(':')[0])
  gets = int(MEMTIER_RATIO.value.split(':')[1])
  approx_total_sets = round(float(total_requests) / (sets + gets) * sets)
  last_total_sets = 0
  approx_total_gets = total_requests - approx_total_sets
  last_total_gets = 0
  for raw_line in memtier_results.splitlines():
    line = raw_line.strip()
    last_total_sets = _ParseLine(r'^SET', line, approx_total_sets,
                                 last_total_sets, set_histogram)
    last_total_gets = _ParseLine(r'^GET', line, approx_total_gets,
                                 last_total_gets, get_histogram)
  return set_histogram, get_histogram


@dataclasses.dataclass(frozen=True)
class MemtierAggregateResult:
  """Parsed aggregated memtier results."""
  ops_per_sec: float
  kb_per_sec: float
  latency_ms: float
  p90_latency: float
  p95_latency: float
  p99_latency: float


def _ParseTotalThroughputAndLatency(
    memtier_results: Text) -> 'MemtierAggregateResult':
  """Parses the 'TOTALS' output line and return throughput and latency."""
  columns = None
  for raw_line in memtier_results.splitlines():
    line = raw_line.strip()
    if re.match(r'^Type', line):
      columns = re.split(r' \s+', line)
    if re.match(r'^Totals', line):
      if not columns:
        raise errors.Benchmarks.RunError(
            'No "Type" line preceding "Totals" in memtier output.')
      totals = line.split()
      if len(totals) != len(columns):
        raise errors.Benchmarks.RunError(
            'Length mismatch between "Type" and "Totals" lines:'
            f'\nType: {columns}\n Totals: {totals}')

      def _FetchStat(key):
        key_index = columns.index(key)
        if key_index == -1:
          raise errors.Benchmarks.RunError(
              f'Stats table does not contain "{key}" column.')
        return float(totals[columns.index(key)])  # pylint: disable=cell-var-from-loop
      return MemtierAggregateResult(
          ops_per_sec=_FetchStat('Ops/sec'),
          kb_per_sec=_FetchStat('KB/sec'),
          latency_ms=_FetchStat('Avg. Latency'),
          p90_latency=_FetchStat('p90 Latency'),
          p95_latency=_FetchStat('p95 Latency'),
          p99_latency=_FetchStat('p99 Latency')
          )
  raise errors.Benchmarks.RunError('No "Totals" line in memtier output.')


def _ParseLine(pattern: str, line: str, approx_total: int, last_total: int,
               histogram: MemtierHistogram) -> float:
  """Helper function to parse an output line."""
  if not re.match(pattern, line):
    return last_total

  _, msec, percent = line.split()
  counts = _ConvertPercentToAbsolute(approx_total, float(percent))
  bucket_counts = int(round(counts - last_total))
  if bucket_counts > 0:
    histogram.append({'microsec': float(msec) * 1000, 'count': bucket_counts})
  return counts


def _ConvertPercentToAbsolute(total_value: int, percent: float) -> float:
  """Given total value and a 100-based percentage, returns the actual value."""
  return percent / 100 * total_value


def _ParseTimeSeries(time_series_json: Text
                     ) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
  """Parse time series ops throughput from json output."""
  ops_series = []
  max_latency_series = []
  raw = json.loads(time_series_json)
  time_series = raw['ALL STATS']['Totals']['Time-Serie']
  for interval, data_dict in time_series.items():
    ops_series.append((interval, data_dict['Count']))
    max_latency_series.append((interval, data_dict['Max Latency']))
  return ops_series, max_latency_series
