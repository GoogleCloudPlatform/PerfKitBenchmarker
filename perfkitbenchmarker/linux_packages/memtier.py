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

import abc
import collections
import copy
import dataclasses
import json
import math
import os
import pathlib
import random
import re
import statistics
import time
from typing import Any, Dict, List, Tuple, Union

from absl import flags
from absl import logging
import numpy as np
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
import seaborn as sns

GIT_REPO = 'https://github.com/RedisLabs/memtier_benchmark'
GIT_TAG = '2.1.3'
MEMTIER_DIR = '%s/memtier_benchmark' % linux_packages.INSTALL_DIR
APT_PACKAGES = (
    'build-essential autoconf automake libpcre3-dev '
    'libevent-dev pkg-config zlib1g-dev libssl-dev'
)
YUM_PACKAGES = (
    'zlib-devel pcre-devel libmemcached-devel libevent-devel openssl-devel '
    'gcc-c++ libtool autoconf automake'
)
MEMTIER_RESULTS = 'memtier_results'
TMP_FOLDER = '/tmp'
_LOAD_NUM_PIPELINES = 100  # Arbitrarily high for loading
_WRITE_ONLY = '1:0'
CPU_TOLERANCE = 0.05
WARM_UP_SECONDS = 360
RAMP_DOWN_TIME = 10
JSON_OUT_FILE = 'json_data'
# upper limit to pipelines when binary searching for latency-capped throughput.
# arbitrarily chosen for large latency.
MAX_PIPELINES_COUNT = 5000
MAX_CLIENTS_COUNT = 30

# Metrics aggregated for --memtier_distribution_binary_search
MEMTIER_DISTRIBUTION_METRICS = {
    'ops_per_sec': 'ops/s',
    'kb_per_sec': 'KB/s',
    'latency_ms': 'ms',
    '90': 'ms',
    '95': 'ms',
    '99': 'ms',
}

MemtierHistogram = List[Dict[str, Union[float, int]]]

FLAGS = flags.FLAGS

PACKAGE_NAME = 'memtier'
_LARGE_CLUSTER_TAR = 'memtier_large_cluster.tar.gz'
PREPROVISIONED_DATA = {
    _LARGE_CLUSTER_TAR: (
        '4b7364c484001a94e4b8bcd61602c8fbdc8a75d84c751f9c4cfb694942b64052'
    )
}
MEMTIER_LARGE_CLUSTER = flags.DEFINE_bool(
    'memtier_large_cluster',
    False,
    'If true, uses the large cluster binary for memtier.',
)


class MemtierMode:
  """Enum of options for --memtier_run_mode."""

  MEASURE_CPU_LATENCY = 'MEASURE_CPU_LATENCY'
  NORMAL_RUN = 'NORMAL_RUN'
  ALL = (MEASURE_CPU_LATENCY, NORMAL_RUN)


MEMTIER_PROTOCOL = flags.DEFINE_enum(
    'memtier_protocol',
    'memcache_binary',
    ['memcache_binary', 'redis', 'memcache_text'],
    (
        'Protocol to use. Supported protocols are redis, memcache_text, and'
        ' memcache_binary. Defaults to memcache_binary.'
    ),
)
MEMTIER_RUN_COUNT = flags.DEFINE_integer(
    'memtier_run_count',
    1,
    'Number of full-test iterations to perform. Defaults to 1.',
)
MEMTIER_RUN_DURATION = flags.DEFINE_integer(
    'memtier_run_duration',
    None,
    (
        'Mutually exclusive with memtier_requests. Duration for each client'
        ' count in seconds. By default, test length is set by memtier_requests,'
        ' the number of requests sent by each client. By specifying'
        ' run_duration, key space remains the same (from 1 to'
        ' memtier_requests), but test stops once run_duration is passed. Total'
        ' test duration = run_duration * runs * len(memtier_clients).'
    ),
)
MEMTIER_REQUESTS = flags.DEFINE_integer(
    'memtier_requests',
    10000,
    (
        'Mutually exclusive with memtier_run_duration. Number of total requests'
        ' per client. Defaults to 10000.'
    ),
)
MEMTIER_EXPIRY_RANGE = flags.DEFINE_string(
    'memtier_expiry_range',
    None,
    (
        'Use random expiry values from the specified range. '
        'Must be expressed as [0-n]-[1-n]. Applies for keys created when '
        'loading the db and when running the memtier benchmark. '
    ),
)
flag_util.DEFINE_integerlist(
    'memtier_clients',
    [50],
    (
        'Comma separated list of number of clients per thread. Specify more'
        ' than 1 value to vary the number of clients. Defaults to [50].'
    ),
)
flag_util.DEFINE_integerlist(
    'memtier_threads', [4], 'Number of threads. Defaults to 4.'
)
MEMTIER_RATIO = flags.DEFINE_string(
    'memtier_ratio', '1:9', 'Set:Get ratio. Defaults to 1:9 Sets:Gets.'
)
MEMTIER_DATA_SIZE = flags.DEFINE_integer(
    'memtier_data_size', 32, 'Object data size. Defaults to 32 bytes.'
)
MEMTIER_DATA_SIZE_LIST = flags.DEFINE_string(
    'memtier_data_size_list',
    None,
    (
        'Mutually exclusive with memtier_data_size. Object data size list'
        ' specified as [size1:weight1],...[sizeN:weightN].'
    ),
)
MEMTIER_KEY_PATTERN = flags.DEFINE_string(
    'memtier_key_pattern',
    'R:R',
    (
        'Set:Get key pattern. G for Gaussian distribution, R for uniform'
        ' Random, S for Sequential. Defaults to R:R.'
    ),
)
MEMTIER_LOAD_KEY_MAXIMUM = flags.DEFINE_integer(
    'memtier_load_key_maximum',
    None,
    (
        'Key ID maximum value to load. The range of keys will be from 1 (min)'
        ' to this specified max key value. If not set, defaults to'
        ' memtier_key_maximum. Setting this different from memtier_key_maximum'
        ' allows triggering of eviction behavior.'
    ),
)
MEMTIER_KEY_MAXIMUM = flags.DEFINE_integer(
    'memtier_key_maximum',
    10000000,
    (
        'Key ID maximum value. The range of keys will be from 1 (min) to this'
        ' specified max key value.'
    ),
)
MEMTIER_LATENCY_CAPPED_THROUGHPUT = flags.DEFINE_bool(
    'latency_capped_throughput',
    False,
    (
        'Measure latency capped throughput. Use in conjunction with'
        ' memtier_latency_cap. Defaults to False. '
    ),
)
MEMTIER_DISTRIBUTION_ITERATIONS = flags.DEFINE_integer(
    'memtier_distribution_iterations',
    None,
    (
        'If set, measures the distribution of latency capped throughput across'
        ' multiple iterations. Will run a set number of iterations for the'
        ' benchmark test and  calculate mean/stddev for metrics. Note that this'
        ' is different from memtier_run_count which is a passthrough to the'
        ' actual memtier benchmark tool which reports different aggregate'
        ' stats.'
    ),
)
MEMTIER_DISTRIBUTION_BINARY_SEARCH = flags.DEFINE_bool(
    'memtier_distribution_binary_search',
    True,
    (
        'If true, uses a binary search to measure the optimal client and thread'
        ' count needed for max throughput under latency cap. Else, uses'
        ' --memtier_clients, --memtier_threads, and --memtier_pipelines for the'
        ' iterations.'
    ),
)
MEMTIER_LATENCY_CAP = flags.DEFINE_float(
    'memtier_latency_cap',
    1.0,
    (
        'Latency cap in ms. Use in conjunction with latency_capped_throughput.'
        ' Defaults to 1ms.'
    ),
)
MEMTIER_RUN_MODE = flags.DEFINE_enum(
    'memtier_run_mode',
    MemtierMode.NORMAL_RUN,
    MemtierMode.ALL,
    (
        'Mode that the benchmark is set to. NORMAL_RUN measures latency and'
        ' throughput, MEASURE_CPU_LATENCY measures single threaded latency at'
        ' memtier_cpu_target. When measuring CPU latency flags for clients,'
        ' threads, and pipelines are ignored and memtier_cpu_target and'
        ' memtier_cpu_duration must not be None.'
    ),
)
MEMTIER_CPU_TARGET = flags.DEFINE_float(
    'memtier_cpu_target',
    0.5,
    (
        'The target CPU utilization when running memtier and trying to get the'
        ' latency at variable CPU metric. The target can range from 1%-100% and'
        ' represents the percent CPU utilization (e.g. 0.5 -> 50% CPU'
        ' utilization)'
    ),
)
MEMTIER_CPU_DURATION = flags.DEFINE_integer(
    'memtier_cpu_duration',
    300,
    (
        'Number of seconds worth of data taken to measure the CPU utilization'
        ' of an instance. When MEASURE_CPU_LATENCY mode is on,'
        ' memtier_run_duration is set to memtier_cpu_duration +'
        ' WARM_UP_SECONDS.'
    ),
)
flag_util.DEFINE_integerlist(
    'memtier_pipeline',
    [1],
    (
        'Number of pipelines to use for memtier. Defaults to 1, i.e. no'
        ' pipelining.'
    ),
)
MEMTIER_CLUSTER_MODE = flags.DEFINE_bool(
    'memtier_cluster_mode', False, 'Passthrough for --cluster-mode flag'
)
MEMTIER_TIME_SERIES = flags.DEFINE_bool(
    'memtier_time_series',
    False,
    (
        'Include per second time series output for ops and max latency. This'
        ' greatly increase the number of samples.'
    ),
)

MEMTIER_SERVER_SELECTION = flags.DEFINE_enum(
    'memtier_server_selection',
    'uniform',
    ['uniform', 'random'],
    (
        'Distribution pattern for server instance port to redis memtier'
        ' clients.Supported distributions are uniform and random. Defaults to'
        ' uniform.'
    ),
)
MEMTIER_TLS = flags.DEFINE_bool('memtier_tls', False, 'Whether to enable TLS.')
MEMTIER_DISTINCT_CLIENT_SEED = flags.DEFINE_bool(
    'memtier_distinct_client_seed',
    True,
    'If true, each client will use a distinct seed.',
)
MEMTIER_COMMAND = flags.DEFINE_string(
    'memtier_command',
    None,
    'Custom memtier command to execute. Use __key__ and __data__ as'
    ' placeholders.',
)
MEMTIER_KEY_PREFIX = flags.DEFINE_string(
    'memtier_key_prefix', '', 'Prefix for keys used in memtier.'
)
MEMTIER_RANDOM_DATA = flags.DEFINE_bool(
    'memtier_random_data', True, 'Use random data for memtier.'
)


class BuildFailureError(Exception):
  pass


class RunFailureError(Exception):
  pass


class RetryableRunError(Exception):
  pass


def YumInstall(vm: virtual_machine.VirtualMachine) -> None:
  """Installs the memtier_benchmark package on the VM."""
  vm.RemoteCommand('sudo dnf update -y')
  vm.Install('build_tools')
  vm.InstallPackages(YUM_PACKAGES)
  _Install(vm)


def AptInstall(vm):
  """Installs the memtier package on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(APT_PACKAGES)
  _Install(vm)


def _Install(vm):
  """Installs the memtier package on the VM."""
  vm.RemoteCommand('git clone {} {}'.format(GIT_REPO, MEMTIER_DIR))
  vm.RemoteCommand('cd {} && git checkout {}'.format(MEMTIER_DIR, GIT_TAG))
  if MEMTIER_LARGE_CLUSTER.value:
    vm.RemoteCommand(f'rm -rf {MEMTIER_DIR}')
    vm.InstallPreprovisionedPackageData(
        PACKAGE_NAME, [_LARGE_CLUSTER_TAR], MEMTIER_DIR
    )
    vm.RemoteCommand(
        f'tar -C {MEMTIER_DIR} -xvzf {MEMTIER_DIR}/{_LARGE_CLUSTER_TAR}'
    )
  # autoreconf is segfaulting on some runs. so retry.
  vm_util.Retry(max_retries=10)(vm.RemoteCommand)(
      f'cd {MEMTIER_DIR} && autoreconf -ifvvvvvvvvv'
  )
  vm.RemoteCommand(
      f'cd {MEMTIER_DIR} && ./configure && make && sudo make install'
  )


def Uninstall(vm):
  """Uninstalls the memtier package on the VM."""
  vm.RemoteCommand('cd {} && sudo make uninstall'.format(MEMTIER_DIR))


def BuildMemtierCommand(
    server: str | None = None,
    port: int | None = None,
    protocol: str | None = None,
    clients: int | None = None,
    threads: int | None = None,
    ratio: str | None = None,
    data_size: int | None = None,
    data_size_list: str | None = None,
    pipeline: int | None = None,
    key_minimum: int | None = None,
    key_maximum: int | None = None,
    key_pattern: str | None = None,
    requests: Union[str, int] | None = None,
    run_count: int | None = None,
    random_data: bool | None = None,
    distinct_client_seed: bool | None = None,
    test_time: int | None = None,
    outfile: pathlib.PosixPath | None = None,
    password: str | None = None,
    cluster_mode: bool | None = None,
    shard_addresses: str | None = None,
    tls: bool | None = None,
    expiry_range: str | None = None,
    json_out_file: pathlib.PosixPath | None = None,
    command: str | None = None,
    key_prefix: str | None = None,
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
      'pipeline': pipeline,
      'key-minimum': key_minimum,
      'key-maximum': key_maximum,
      'requests': requests,
      'run-count': run_count,
      'test-time': test_time,
      'out-file': outfile,
      'json-out-file': json_out_file,
      'print-percentile': '50,90,95,99,99.5,99.9,99.95,99.99',
      'shard-addresses': shard_addresses,
      'key-prefix': key_prefix,
  }
  # The `--ratio` and `--key-pattern' flags are exclusive with `--command`.
  if command:
    args['command'] = command
  else:
    args['key-pattern'] = key_pattern
    args['ratio'] = ratio
  if data_size_list:
    args['data-size-list'] = data_size_list
  else:
    args['data-size'] = data_size
  if expiry_range:
    args['expiry-range'] = expiry_range
  # Arguments passed without a parameter
  no_param_args = {
      'random-data': random_data,
      'distinct-client-seed': distinct_client_seed,
      'cluster-mode': cluster_mode,
      'tls': tls,
      # Don't skip certificate verification by default. keydb_memtier_benchmark
      # does this by hacking in ca args into the server ip.
      'tls-skip-verify': tls and FLAGS.cloud_redis_tls,
  }
  # Build the command
  cmd = []
  if cluster_mode:
    cmd += ['ulimit -n 32758 &&']
  cmd += ['memtier_benchmark']
  for arg, value in args.items():
    if value is not None:
      if arg == 'command':
        cmd.append(f'--{arg} "{value}"')
      else:
        cmd.extend([f'--{arg}', str(value)])
  for no_param_arg, value in no_param_args.items():
    if value:
      cmd.append(f'--{no_param_arg}')
  return ' '.join(cmd)


@dataclasses.dataclass(frozen=True)
class LoadRequest:
  key_minimum: int
  key_maximum: int
  server_port: int
  server_ip: str
  server_password: str


def _LoadSingleVM(
    load_vm: virtual_machine.VirtualMachine, request: LoadRequest
) -> None:
  """Loads the DB from a single VM."""
  cmd = BuildMemtierCommand(
      server=request.server_ip,
      port=request.server_port,
      protocol=MEMTIER_PROTOCOL.value,
      clients=1,
      threads=1,
      ratio=_WRITE_ONLY,
      data_size=MEMTIER_DATA_SIZE.value,
      data_size_list=MEMTIER_DATA_SIZE_LIST.value,
      pipeline=_LOAD_NUM_PIPELINES,
      key_minimum=request.key_minimum,
      key_maximum=request.key_maximum,
      requests='allkeys',
      cluster_mode=MEMTIER_CLUSTER_MODE.value,
      password=request.server_password,
      tls=MEMTIER_TLS.value,
      expiry_range=MEMTIER_EXPIRY_RANGE.value,
  )
  load_vm.RemoteCommand(cmd)


def Load(
    vms: list[virtual_machine.VirtualMachine],
    server_ip: str,
    server_port: int,
    server_password: str | None = None,
) -> None:
  """Loads the database before performing tests."""
  load_requests = []
  load_key_maximum = (
      MEMTIER_LOAD_KEY_MAXIMUM.value
      if MEMTIER_LOAD_KEY_MAXIMUM.value
      else MEMTIER_KEY_MAXIMUM.value
  )
  load_records_per_vm = load_key_maximum // len(vms)
  for i, _ in enumerate(vms):
    load_requests.append((
        vms[i],
        LoadRequest(
            key_minimum=max(i * load_records_per_vm, 1),
            key_maximum=(i + 1) * load_records_per_vm,
            server_port=server_port,
            server_ip=server_ip,
            server_password=server_password,
        ),
    ))

  background_tasks.RunThreaded(
      _LoadSingleVM, [(arg, {}) for arg in load_requests]
  )


def RunOverAllClientVMs(
    client_vms,
    server_ip: str,
    ports: List[int],
    pipeline,
    threads,
    clients,
    password: str | None = None,
) -> 'List[MemtierResult]':
  """Run redis memtier on all client vms.

  Run redis memtier on all client vms based on given ports.

  Args:
    client_vms: A list of client vms.
    server_ip: Ip address of the server.
    ports: List of ports to run against the server.
    pipeline: Number of pipeline to use in memtier.
    threads: Number of threads to use in memtier.
    clients: Number of clients to use in memtier.
    password: Password of the server.

  Returns:
   List of memtier results.
  """

  def DistributeClientsToPorts(port_index):
    client_index = port_index % len(client_vms)

    if MEMTIER_SERVER_SELECTION.value == 'uniform':
      port = ports[port_index]
    else:
      port = random.choice(ports)

    vm = client_vms[client_index]
    return _Run(
        vm=vm,
        server_ip=server_ip,
        server_port=port,
        threads=threads,
        pipeline=pipeline,
        clients=clients,
        password=password,
        unique_id=str(port_index),
    )

  results = background_tasks.RunThreaded(
      DistributeClientsToPorts, list(range(len(ports)))
  )

  return results


def RunOverAllThreadsPipelinesAndClients(
    client_vms,
    server_ip: str,
    server_ports: List[int],
    password: str | None = None,
) -> List[sample.Sample]:
  """Runs memtier over all pipeline and thread combinations."""
  samples = []
  for pipeline in FLAGS.memtier_pipeline:
    for threads in FLAGS.memtier_threads:
      for clients in FLAGS.memtier_clients:
        results = RunOverAllClientVMs(
            client_vms=client_vms,
            server_ip=server_ip,
            ports=server_ports,
            threads=threads,
            pipeline=pipeline,
            clients=clients,
            password=password,
        )
        metadata = GetMetadata(
            clients=clients, threads=threads, pipeline=pipeline
        )

        for result in results:
          samples.extend(result.GetSamples(metadata))
        samples.extend(AggregateMemtierResults(results, metadata))
  return samples


@dataclasses.dataclass(frozen=True)
class MemtierBinarySearchParameters:
  """Parameters to aid binary search of memtier."""

  lower_bound: float = 0
  upper_bound: float = math.inf
  pipelines: int = 1
  threads: int = 1
  clients: int = 1


@dataclasses.dataclass(frozen=True)
class MemtierConnection:
  """Parameters mapping client to server endpoint."""

  client_vm: virtual_machine.BaseVirtualMachine
  address: str
  port: int


def _RunParallelConnections(
    connections: list[MemtierConnection],
    server_ip: str,
    server_port: int,
    threads: int,
    clients: int,
    pipelines: int,
    password: str | None = None,
) -> list['MemtierResult']:
  """Runs memtier in parallel with the given connections."""
  run_args = []
  base_args = {
      'server_ip': server_ip,
      'server_port': server_port,
      'threads': threads,
      'clients': clients,
      'pipeline': pipelines,
      'password': password,
  }

  connections_by_vm = collections.defaultdict(list)
  for conn in connections:
    connections_by_vm[conn.client_vm].append(conn)

  # Currently more than one client VM will cause shards to be distributed
  # evenly between them. This behavior could be customized later with a flag.
  if len(connections_by_vm) > 1:
    for vm, conns in connections_by_vm.items():
      shard_addresses = ','.join(
          f'{conn.address}:{conn.port}' for conn in conns
      )
      args = copy.deepcopy(base_args)
      args.update({
          'vm': vm,
          'shard_addresses': shard_addresses,
          'unique_id': vm.ip_address,
      })
      run_args.append(((), args))
  else:
    for connection in connections:
      args = copy.deepcopy(base_args)
      args.update({
          'vm': connection.client_vm,
          'unique_id': connection.client_vm.ip_address,
      })
      run_args.append(((), args))
  logging.info('Connections: %s', connections_by_vm)
  logging.info('Running with args: %s', run_args)
  return background_tasks.RunThreaded(_Run, run_args)


class _LoadModifier(abc.ABC):
  """Base class for load modification in binary search."""

  @abc.abstractmethod
  def GetInitialParameters(self) -> MemtierBinarySearchParameters:
    """Returns the initial parameters used in the binary search."""

  @abc.abstractmethod
  def ModifyLoad(
      self, parameters: MemtierBinarySearchParameters, latency: float
  ) -> MemtierBinarySearchParameters:
    """Returns new search parameters."""


class _PipelineModifier(_LoadModifier):
  """Modifies pipelines in single-client binary search."""

  def GetInitialParameters(self) -> MemtierBinarySearchParameters:
    return MemtierBinarySearchParameters(
        upper_bound=MAX_PIPELINES_COUNT, pipelines=MAX_PIPELINES_COUNT // 2
    )

  def ModifyLoad(
      self, parameters: MemtierBinarySearchParameters, latency: float
  ) -> MemtierBinarySearchParameters:
    if latency <= MEMTIER_LATENCY_CAP.value:
      lower_bound = parameters.pipelines
      upper_bound = min(parameters.upper_bound, MAX_PIPELINES_COUNT)
    else:
      lower_bound = parameters.lower_bound
      upper_bound = parameters.pipelines

    pipelines = lower_bound + math.ceil((upper_bound - lower_bound) / 2)
    return MemtierBinarySearchParameters(
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        pipelines=pipelines,
        threads=1,
        clients=1,
    )


def _FindFactor(number: int, max_threads: int, max_clients: int) -> int:
  """Find a factor of the given number (or close to it if it's prime)."""
  for i in reversed(range(1, max_threads + 1)):
    if number % i == 0 and number // i <= max_clients:
      return i
  return _FindFactor(number - 1, max_threads, max_clients)


@dataclasses.dataclass
class _ClientModifier(_LoadModifier):
  """Modifies clients in single-pipeline binary search."""

  max_clients: int
  max_threads: int

  def GetInitialParameters(self) -> MemtierBinarySearchParameters:
    return MemtierBinarySearchParameters(
        upper_bound=self.max_clients * self.max_threads,
        threads=max(self.max_threads // 2, 1),
        clients=self.max_clients,
    )

  def ModifyLoad(
      self, parameters: MemtierBinarySearchParameters, latency: float
  ) -> MemtierBinarySearchParameters:
    if latency <= MEMTIER_LATENCY_CAP.value:
      lower_bound = parameters.clients * parameters.threads + 1
      upper_bound = min(
          parameters.upper_bound, self.max_clients * self.max_threads
      )
    else:
      lower_bound = parameters.lower_bound
      upper_bound = parameters.clients * parameters.threads - 1

    total_clients = lower_bound + math.ceil((upper_bound - lower_bound) / 2)
    threads = _FindFactor(total_clients, self.max_threads, self.max_clients)
    clients = total_clients // threads
    return MemtierBinarySearchParameters(
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        pipelines=1,
        threads=threads,
        clients=clients,
    )


def _CombineResults(results: list['MemtierResult']) -> 'MemtierResult':
  """Combines multiple MemtierResults into a single aggregate."""
  ops_per_sec = sum([result.ops_per_sec for result in results])
  kb_per_sec = sum([result.kb_per_sec for result in results])
  latency_ms = sum([result.latency_ms for result in results]) / len(results)
  latency_dic = collections.defaultdict(int)
  for result in results:
    for k, v in result.latency_dic.items():
      latency_dic[k] += v
  for k in latency_dic:
    latency_dic[k] /= len(results)
  return MemtierResult(
      ops_per_sec=ops_per_sec,
      kb_per_sec=kb_per_sec,
      latency_ms=latency_ms,
      latency_dic=latency_dic,
      metadata=results[0].metadata,
      parameters=results[0].parameters,
  )


def _BinarySearchForLatencyCappedThroughput(
    connections: list[MemtierConnection],
    load_modifiers: list[_LoadModifier],
    server_ip: str,
    server_port: int,
    password: str | None = None,
) -> list['MemtierResult']:
  """Runs memtier to find the maximum throughput under a latency cap."""
  results = []
  for modifier in load_modifiers:
    parameters = modifier.GetInitialParameters()
    current_max_result = MemtierResult(
        latency_dic={
            '50': 0,
            '90': 0,
            '95': 0,
            '99': 0,
            '99.5': 0,
            '99.9': 0,
            '99.950': 0,
            '99.990': 0,
        },
    )
    while parameters.lower_bound < (parameters.upper_bound - 1):
      parallel_results = _RunParallelConnections(
          connections,
          server_ip,
          server_port,
          parameters.threads,
          parameters.clients,
          parameters.pipelines,
          password,
      )
      result = _CombineResults(parallel_results)
      logging.info(
          (
              'Binary search for latency capped throughput.'
              '\nMemtier ops throughput: %s qps'
              '\nmemtier 95th percentile latency: %s ms'
              '\n%s'
          ),
          result.ops_per_sec,
          result.latency_dic['95'],
          parameters,
      )
      if (
          result.ops_per_sec > current_max_result.ops_per_sec
          and result.latency_dic['95'] <= MEMTIER_LATENCY_CAP.value
      ):
        current_max_result = result
        current_max_result.parameters = parameters
        current_max_result.metadata.update(
            GetMetadata(
                clients=parameters.clients,
                threads=parameters.threads,
                pipeline=parameters.pipelines,
            )
        )
      # 95 percentile used to decide latency cap
      new_parameters = modifier.ModifyLoad(parameters, result.latency_dic['95'])
      if new_parameters == parameters:
        break
      parameters = new_parameters
    results.append(current_max_result)
    logging.info(
        'Found optimal parameters %s for throughput %s and p95 latency %s',
        current_max_result.parameters,
        current_max_result.ops_per_sec,
        current_max_result.latency_dic['95'],
    )
  return results


def MeasureLatencyCappedThroughput(
    client_vm: virtual_machine.VirtualMachine,
    server_shard_count: int,
    server_ip: str,
    server_port: int,
    password: str | None = None,
) -> List[sample.Sample]:
  """Runs memtier to find the maximum throughput under a latency cap."""
  max_threads = client_vm.NumCpusForBenchmark(report_only_physical_cpus=True)
  max_clients = MAX_CLIENTS_COUNT // server_shard_count
  samples = []
  for result in _BinarySearchForLatencyCappedThroughput(
      [MemtierConnection(client_vm, server_ip, server_port)],
      [_PipelineModifier(), _ClientModifier(max_clients, max_threads)],
      server_ip,
      server_port,
      password,
  ):
    samples.extend(result.GetSamples())
  return samples


def _CalculateMode(values: list[float]) -> float:
  """Calculates the mode of a distribution using kernel density estimation."""
  ax = sns.histplot(values, kde=True)
  kdeline = ax.lines[0]
  xs = kdeline.get_xdata()
  ys = kdeline.get_ydata()
  mode_idx = np.argmax(ys)
  mode = xs[mode_idx]
  return mode


def MeasureLatencyCappedThroughputDistribution(
    connections: list[MemtierConnection],
    server_ip: str,
    server_port: int,
    client_vms: list[virtual_machine.VirtualMachine],
    server_shard_count: int,
    password: str | None = None,
) -> list[sample.Sample]:
  """Measures distribution of throughput across several iterations.

  In particular, this function will first find the optimal number of threads and
  clients per thread, and then run the test with those parameters for the
  specified number of iterations. The reported samples will include mean and
  stdev of QPS and latency across the series of runs.

  Args:
    connections: list of connections from client to server.
    server_ip: Ip address of the server.
    server_port: Port of the server.
    client_vms: A list of client vms.
    server_shard_count: Number of shards in the redis cluster.
    password: Password of the server.

  Returns:
    A list of throughput and latency samples.
  """
  parameters_for_test = MemtierBinarySearchParameters(
      pipelines=FLAGS.memtier_pipeline[0],
      clients=FLAGS.memtier_clients[0],
      threads=FLAGS.memtier_threads[0],
  )
  if MEMTIER_DISTRIBUTION_BINARY_SEARCH.value:
    max_threads = client_vms[0].NumCpusForBenchmark(
        report_only_physical_cpus=True
    )
    shards_per_client = server_shard_count / len(client_vms)
    max_clients = int(MAX_CLIENTS_COUNT // shards_per_client)
    result = _BinarySearchForLatencyCappedThroughput(
        connections,
        [_ClientModifier(max_clients, max_threads)],
        server_ip,
        server_port,
        password,
    )[0]
    parameters_for_test = result.parameters

  logging.info(
      'Starting test iterations with parameters %s', parameters_for_test
  )
  results: list[MemtierResult] = []
  for _ in range(MEMTIER_DISTRIBUTION_ITERATIONS.value):
    results_for_run = _RunParallelConnections(
        connections,
        server_ip,
        server_port,
        parameters_for_test.threads,
        parameters_for_test.clients,
        parameters_for_test.pipelines,
        password,
    )
    aggregate_result = _CombineResults(results_for_run)
    logging.info('Aggregate result: %s', aggregate_result)
    results.append(aggregate_result)

  samples = []
  metadata = {
      'distribution_iterations': MEMTIER_DISTRIBUTION_ITERATIONS.value,
      'threads': parameters_for_test.threads,
      'clients': parameters_for_test.clients,
      'pipelines': parameters_for_test.pipelines,
  }
  for metric, units in MEMTIER_DISTRIBUTION_METRICS.items():
    is_latency = metric.isdigit()
    values = (
        [result.latency_dic[metric] for result in results]
        if is_latency
        else [getattr(result, metric) for result in results]
    )
    if is_latency:
      metric = f'p{metric} latency'
    samples.append(
        sample.Sample(
            f'Mean {metric}', statistics.mean(values), units, metadata
        )
    )
    if len(values) > 1:
      samples.extend([
          sample.Sample(
              f'Stdev {metric}',
              statistics.stdev(values),
              units,
              metadata,
          ),
          sample.Sample(
              f'Mode {metric}',
              _CalculateMode(values),
              units,
              metadata,
          ),
      ])

  return samples


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
        password=password,
    )

    cpu_percent = cloud_instance.MeasureCpuUtilization(
        MEMTIER_CPU_DURATION.value
    )

    if not cpu_percent:
      raise errors.Benchmarks.RunError(
          'Could not measure CPU utilization for the instance.'
      )
    logging.info(
        (
            'Tried %s clients and got %s%% CPU utilization for the last run'
            ' with the target CPU being %s%%'
        ),
        current_clients,
        cpu_percent,
        target,
    )

    if cpu_percent < target - CPU_TOLERANCE:
      lower_bound = current_clients + 1
    elif cpu_percent > target + CPU_TOLERANCE:
      upper_bound = current_clients - 1
    else:
      logging.info(
          'Finished binary search and the current client count is %s',
          current_clients,
      )
      process_args = [
          (
              _Run,
              [
                  load_vm,
                  server_ip,
                  server_port,
                  threads,
                  pipeline,
                  current_clients,
                  password,
              ],
              {},
          ),
          (
              _GetSingleThreadedLatency,
              [latency_measurement_vm, server_ip, server_port, password],
              {},
          ),
      ]
      results = background_tasks.RunParallelThreads(
          process_args, len(process_args)
      )
      metadata = GetMetadata(
          clients=current_clients, threads=threads, pipeline=pipeline
      )
      metadata['measured_cpu_percent'] = cloud_instance.MeasureCpuUtilization(
          MEMTIER_CPU_DURATION.value
      )
      samples.extend(results[1].GetSamples(metadata))
      return samples

  # If complete binary search without finding a client count,
  # it's not possible on this configuration.
  raise errors.Benchmarks.RunError(
      'Completed binary search and did not find a client count that worked for '
      'this configuration and CPU utilization.'
  )


def _GetSingleThreadedLatency(
    client_vm, server_ip: str, server_port: int, password: str
) -> 'MemtierResult':
  """Wait for background run to stabilize then send single threaded request."""
  time.sleep(300)
  return _Run(
      vm=client_vm,
      server_ip=server_ip,
      server_port=server_port,
      threads=1,
      pipeline=1,
      clients=1,
      password=password,
  )


@vm_util.Retry(
    poll_interval=1,
    timeout=60,
    retryable_exceptions=(RetryableRunError),
)
def _IssueRetryableCommand(vm, cmd: str, timeout: int | None = None) -> None:
  """Issue redis command, retry connection failure."""
  _, stderr = vm.RobustRemoteCommand(cmd, timeout=timeout)
  if 'Connection error' in stderr:
    raise RetryableRunError('Redis client connection failed, retrying')
  if 'handle error response' in stderr:
    raise RunFailureError(stderr)


@vm_util.Retry(poll_interval=1, timeout=60)
def _CheckRedisReachable(vm, server_ip: str, server_port: int) -> None:
  """Checks if redis server is reachable."""
  logging.info('Checking reachability of %s:%s', server_ip, server_port)
  # Output: Connection to xxxx:yyy:zzzz:wwww:: 6379 port [tcp/redis] succeeded!
  cmd = f'nc -zv {server_ip} {server_port} 2>&1 | grep succeeded'
  vm.RemoteCommand(cmd)


def _Run(
    vm,
    server_ip: str,
    server_port: int,
    threads: int,
    pipeline: int,
    clients: int,
    password: str | None = None,
    unique_id: str | None = None,
    shard_addresses: str | None = None,
) -> 'MemtierResult':
  """Runs the memtier benchmark on the vm."""
  logging.info(
      (
          'Start benchmarking redis/memcached using memtier:\n'
          '\tmemtier client: %s'
          '\tmemtier threads: %s'
          '\tmemtier pipeline, %s'
      ),
      clients,
      threads,
      pipeline,
  )

  _CheckRedisReachable(vm, server_ip, server_port)

  file_name_suffix = '_'.join(filter(None, [str(server_port), unique_id]))
  memtier_results_file_name = (
      '_'.join([MEMTIER_RESULTS, file_name_suffix]) + '.log'
  )
  memtier_results_file = pathlib.PosixPath(
      f'{TMP_FOLDER}/{memtier_results_file_name}'
  )
  vm.RemoteCommand(f'rm -f {memtier_results_file}')

  json_results_file_name = '_'.join([JSON_OUT_FILE, file_name_suffix]) + '.log'
  json_results_file = (
      pathlib.PosixPath(f'{TMP_FOLDER}/{json_results_file_name}')
      if MEMTIER_TIME_SERIES.value
      else None
  )
  if json_results_file is not None:
    vm.RemoteCommand(f'rm -f {json_results_file}')
  # Specify one of run requests or run duration.
  requests = (
      MEMTIER_REQUESTS.value if MEMTIER_RUN_DURATION.value is None else None
  )
  test_time = (
      MEMTIER_RUN_DURATION.value
      if MEMTIER_RUN_MODE.value == MemtierMode.NORMAL_RUN
      else WARM_UP_SECONDS + MEMTIER_CPU_DURATION.value
  )
  cmd = BuildMemtierCommand(
      server=server_ip,
      port=server_port,
      protocol=MEMTIER_PROTOCOL.value,
      run_count=MEMTIER_RUN_COUNT.value,
      clients=clients,
      threads=threads,
      ratio=MEMTIER_RATIO.value,
      data_size=MEMTIER_DATA_SIZE.value,
      data_size_list=MEMTIER_DATA_SIZE_LIST.value,
      key_pattern=MEMTIER_KEY_PATTERN.value,
      pipeline=pipeline,
      key_minimum=1,
      key_maximum=MEMTIER_KEY_MAXIMUM.value,
      random_data=MEMTIER_RANDOM_DATA.value,
      distinct_client_seed=MEMTIER_DISTINCT_CLIENT_SEED.value,
      test_time=test_time,
      requests=requests,
      password=password,
      outfile=memtier_results_file,
      cluster_mode=MEMTIER_CLUSTER_MODE.value,
      shard_addresses=shard_addresses,
      json_out_file=json_results_file,
      tls=MEMTIER_TLS.value,
      command=MEMTIER_COMMAND.value,
      key_prefix=MEMTIER_KEY_PREFIX.value,
  )
  logging.info('Memtier command: %s', cmd)
  # Add a buffer to the timeout to account for command overhead.
  timeout = test_time + 100 if test_time else None
  _IssueRetryableCommand(vm, cmd, timeout=timeout)

  output_path = os.path.join(vm_util.GetTempDir(), memtier_results_file_name)
  vm_util.IssueCommand(['rm', '-f', output_path])
  vm.PullFile(vm_util.GetTempDir(), memtier_results_file)

  time_series_json = None
  if json_results_file:
    json_path = os.path.join(vm_util.GetTempDir(), json_results_file_name)
    vm_util.IssueCommand(['rm', '-f', json_path])
    vm.PullFile(vm_util.GetTempDir(), json_results_file)
    with open(json_path) as ts_json:
      time_series_json = ts_json.read()
      if not time_series_json:
        logging.warning('No metrics in time series json.')
      else:
        # If there is no thoughput the output from redis memtier might
        # return -nan which breaks the json.
        # Sample error line
        # "Average Latency": -nan
        # "Min Latency": 9223372036854776.000
        # 9223372036854776 is likely caused by rounding long long in c++
        # The new version of memtier_benchmark may output 'inf'.
        time_series_json = (
            time_series_json.replace('-nan', '0.000')
            .replace('inf', '0.000')
            .replace('9223372036854776.000', '0.000')
        )
        time_series_json = json.loads(time_series_json)

  with open(output_path) as output:
    summary_data = output.read()
    logging.info(summary_data)
  return MemtierResult.Parse(summary_data, time_series_json)


def GetMetadata(clients: int, threads: int, pipeline: int) -> Dict[str, Any]:
  """Metadata for memtier test."""
  meta = {
      'memtier_protocol': MEMTIER_PROTOCOL.value,
      'memtier_run_count': MEMTIER_RUN_COUNT.value,
      'memtier_requests': MEMTIER_REQUESTS.value,
      'memtier_expiry_range': MEMTIER_EXPIRY_RANGE.value,
      'memtier_threads': threads,
      'memtier_clients': clients,
      'memtier_ratio': MEMTIER_RATIO.value,
      'memtier_key_maximum': MEMTIER_KEY_MAXIMUM.value,
      'memtier_key_pattern': MEMTIER_KEY_PATTERN.value,
      'memtier_pipeline': pipeline,
      'memtier_version': GIT_TAG,
      'memtier_run_mode': MEMTIER_RUN_MODE.value,
      'memtier_cluster_mode': MEMTIER_CLUSTER_MODE.value,
      'memtier_tls': MEMTIER_TLS.value,
      'memtier_distinct_client_seed': MEMTIER_DISTINCT_CLIENT_SEED.value,
      'memtier_command': MEMTIER_COMMAND.value,
      'memtier_key_prefix': MEMTIER_KEY_PREFIX.value,
      'memtier_random_data': MEMTIER_RANDOM_DATA.value,
  }
  if MEMTIER_DATA_SIZE_LIST.value:
    meta['memtier_data_size_list'] = MEMTIER_DATA_SIZE_LIST.value
  else:
    meta['memtier_data_size'] = MEMTIER_DATA_SIZE.value
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

  ops_per_sec: float = 0.0
  kb_per_sec: float = 0.0

  latency_ms: float = 0.0
  latency_dic: Dict[str, float] = dataclasses.field(default_factory=dict)
  get_latency_histogram: MemtierHistogram = dataclasses.field(
      default_factory=list
  )
  set_latency_histogram: MemtierHistogram = dataclasses.field(
      default_factory=list
  )

  timestamps: List[int] = dataclasses.field(default_factory=list)
  ops_series: List[int] = dataclasses.field(default_factory=list)
  latency_series: Dict[str, List[int]] = dataclasses.field(default_factory=dict)

  runtime_info: Dict[str, str] = dataclasses.field(default_factory=dict)
  metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)
  parameters: MemtierBinarySearchParameters = MemtierBinarySearchParameters()

  @classmethod
  def Parse(
      cls, memtier_results: str, time_series_json: Dict[Any, Any] | None
  ) -> 'MemtierResult':
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
    runtime_info = {}
    ops_series = []
    latency_series = {}
    timestamps = []
    if time_series_json:
      runtime_info = _GetRuntimeInfo(time_series_json)
      timestamps, ops_series, latency_series = _ParseTimeSeries(
          time_series_json
      )
    return cls(
        ops_per_sec=aggregated_result.ops_per_sec,
        kb_per_sec=aggregated_result.kb_per_sec,
        latency_ms=aggregated_result.latency_ms,
        latency_dic=aggregated_result.latency_dic,
        get_latency_histogram=get_histogram,
        set_latency_histogram=set_histogram,
        timestamps=timestamps,
        ops_series=ops_series,
        latency_series=latency_series,
        runtime_info=runtime_info,
    )

  def GetSamples(
      self, metadata: Dict[str, Any] | None = None
  ) -> List[sample.Sample]:
    """Return this result as a list of samples."""
    if metadata:
      self.metadata.update(copy.deepcopy(metadata))
    self.metadata['avg_latency'] = self.latency_ms
    for key, value in self.latency_dic.items():
      self.metadata[f'p{key}_latency'] = value
    samples = [
        sample.Sample(
            'Ops Throughput', self.ops_per_sec, 'ops/s', self.metadata
        ),
        sample.Sample('KB Throughput', self.kb_per_sec, 'KB/s', self.metadata),
        sample.Sample('Latency', self.latency_ms, 'ms', self.metadata),
    ]
    for name, histogram in [
        ('get', self.get_latency_histogram),
        ('set', self.set_latency_histogram),
    ]:
      hist_meta = copy.deepcopy(self.metadata)
      hist_meta.update({'histogram': json.dumps(histogram)})
      samples.append(
          sample.Sample(f'{name} latency histogram', 0, '', hist_meta)
      )
    if self.runtime_info:
      samples.append(
          sample.Sample(
              'Memtier Duration',
              self.runtime_info['Total_duration'],
              'ms',
              self.runtime_info,
          )
      )
    return samples


def AlignTimeDiffMemtierResults(
    memtier_results: List[MemtierResult],
) -> None:
  """Realign the timestamps if time diff between clients are greater than 1s."""
  start_times = [result.timestamps[0] for result in memtier_results]
  min_start_time = min(start_times)
  max_start_time = max(start_times)
  diff_time = max_start_time - min_start_time
  logging.info(
      'Max difference in start time between clients is %d ms',
      max_start_time - min_start_time,
  )
  if diff_time < 1000:
    return

  # There are time diff greater than 1s
  # We add 0 padding to the start of the series and remove the end
  # based on the time diff
  for result in memtier_results:
    diff_in_seconds = (result.timestamps[0] - min_start_time) // 1000
    if diff_in_seconds < 1:
      continue

    extra_timestamps = [
        result.timestamps[0] - 1000 * t
        for t in range(diff_in_seconds + 1, 1, -1)
    ]
    empty_results = [0 for i in range(diff_in_seconds)]
    result.timestamps = extra_timestamps + result.timestamps[:-diff_in_seconds]
    for key in result.latency_series:
      result.latency_series[key] = (
          empty_results + result.latency_series[key][:-diff_in_seconds]
      )

    result.ops_series = empty_results + result.ops_series[:-diff_in_seconds]


def AggregateMemtierResults(
    memtier_results: List[MemtierResult], metadata: Dict[str, Any]
) -> List[sample.Sample]:
  """Aggregate memtier time series from all clients.

  Aggregation assume followings:
    1. All memtier clients runs with the same duration
    2. All memtier clients starts at the same time

  To aggregate the ops_series, sum all ops from each clients.
  To aggregate the max latency series, get
  the max latency from all clients.

  Args:
    memtier_results: A list of memtier result.
    metadata: Extra metadata.

  Returns:
    List of time series samples.
  """
  total_ops = 0
  total_kb = 0
  for memtier_result in memtier_results:
    total_ops += memtier_result.ops_per_sec
    total_kb += memtier_result.kb_per_sec

  samples = [
      sample.Sample(
          'Total Ops Throughput', total_ops, 'ops/s', metadata=metadata
      ),
      sample.Sample('Total KB Throughput', total_kb, 'KB/s', metadata=metadata),
  ]

  if not MEMTIER_TIME_SERIES.value:
    return samples

  non_empty_results = []
  for result in memtier_results:
    if result.timestamps:
      non_empty_results.append(result)
    else:
      logging.warning(
          'There is empty result: %s %s %s',
          str(result.ops_per_sec),
          str(result.timestamps),
          str(result.runtime_info),
      )
  AlignTimeDiffMemtierResults(non_empty_results)
  timestamps = memtier_results[0].timestamps

  # Not all clients have the same duration
  # Determine the duration based on the max length
  # or use the Memtier run duration as the max length
  series_length = len(timestamps)
  if MEMTIER_RUN_DURATION.value:
    series_length = max(MEMTIER_RUN_DURATION.value + 1, series_length)
    if series_length != len(timestamps):
      new_timestamps = [timestamps[0] + 1000 * i for i in range(series_length)]
      timestamps = new_timestamps
  ops_series = [0] * series_length
  latency_series = collections.defaultdict(list)

  for memtier_result in non_empty_results:
    for i in range(len(memtier_result.ops_series)):
      ops_series[i] += memtier_result.ops_series[i]

    for key, latencies in memtier_result.latency_series.items():
      for i, latency in enumerate(latencies):
        if len(latency_series[key]) <= i:
          latency_series[key].append([])
        latency_series[key][i].append(latency)

  aggregate_latency_series = {
      key: [max(latencies) for latencies in value]
      for key, value in latency_series.items()
  }

  ramp_down_starts = timestamps[-1]
  # Gives 10s for ramp down
  if len(timestamps) > RAMP_DOWN_TIME:
    ramp_down_starts = timestamps[-RAMP_DOWN_TIME]

  samples.append(
      sample.CreateTimeSeriesSample(
          ops_series,
          timestamps,
          sample.OPS_TIME_SERIES,
          'ops',
          1,
          ramp_down_starts=ramp_down_starts,
          additional_metadata=metadata,
      )
  )
  for key, value in aggregate_latency_series.items():
    samples.append(
        sample.CreateTimeSeriesSample(
            value,
            timestamps[0 : len(value)],
            f'{key}_time_series',
            'ms',
            1,
            additional_metadata=metadata,
        )
    )
  individual_latencies = collections.defaultdict(list)
  for metric, latency_at_timestamp in latency_series.items():
    for client_latency in latency_at_timestamp:
      for client, latency in enumerate(client_latency):
        if len(individual_latencies[metric]) <= client:
          individual_latencies[metric].append([])
        individual_latencies[metric][client].append(latency)

  for metric, client_latencies in individual_latencies.items():
    for client, latencies in enumerate(client_latencies):
      additional_metadata = {}
      additional_metadata.update(metadata)
      additional_metadata['client'] = client
      additional_metadata[sample.DISABLE_CONSOLE_LOG] = True
      samples.append(
          sample.CreateTimeSeriesSample(
              latencies,
              timestamps[0 : len(latencies)],
              f'{metric}_time_series',
              'ms',
              1,
              additional_metadata=additional_metadata,
          )
      )
  return samples


def _ParseHistogram(
    memtier_results: str,
) -> Tuple[MemtierHistogram, MemtierHistogram]:
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
    last_total_sets = _ParseLine(
        r'^SET', line, approx_total_sets, last_total_sets, set_histogram
    )
    last_total_gets = _ParseLine(
        r'^GET', line, approx_total_gets, last_total_gets, get_histogram
    )
  return set_histogram, get_histogram


@dataclasses.dataclass(frozen=True)
class MemtierAggregateResult:
  """Parsed aggregated memtier results."""

  ops_per_sec: float
  kb_per_sec: float
  latency_ms: float
  latency_dic: Dict[str, float]


def _ParseTotalThroughputAndLatency(
    memtier_results: str,
) -> 'MemtierAggregateResult':
  """Parses the 'TOTALS' output line and return throughput and latency."""
  columns = None
  for raw_line in memtier_results.splitlines():
    line = raw_line.strip()
    if re.match(r'^Type', line):
      columns = re.split(r' \s+', line.replace('Latency', 'Latency '))
    if re.match(r'^Totals', line):
      if not columns:
        raise errors.Benchmarks.RunError(
            'No "Type" line preceding "Totals" in memtier output.'
        )
      totals = line.split()
      if len(totals) != len(columns):
        raise errors.Benchmarks.RunError(
            'Length mismatch between "Type" and "Totals" lines:'
            f'\nType: {columns}\n Totals: {totals}'
        )

      def _FetchStat(key):
        key_index = columns.index(key)
        if key_index == -1:
          raise errors.Benchmarks.RunError(
              f'Stats table does not contain "{key}" column.'
          )
        return float(totals[columns.index(key)])  # pylint: disable=cell-var-from-loop

      latency_dic = {}
      for percentile in (
          '50',
          '90',
          '95',
          '99',
          '99.5',
          '99.9',
          '99.950',
          '99.990',
      ):
        latency_dic[percentile] = _FetchStat(f'p{percentile} Latency')
      return MemtierAggregateResult(
          ops_per_sec=_FetchStat('Ops/sec'),
          kb_per_sec=_FetchStat('KB/sec'),
          latency_ms=_FetchStat('Avg. Latency'),
          latency_dic=latency_dic,
      )
  raise errors.Benchmarks.RunError('No "Totals" line in memtier output.')


def _ParseLine(
    pattern: str,
    line: str,
    approx_total: int,
    last_total: int,
    histogram: MemtierHistogram,
) -> float:
  """Helper function to parse an output line."""
  if not re.match(pattern, line):
    return last_total

  # Skip cases where we have an incomplete line (not enough values to unpack).
  try:
    _, msec, percent = line.split()
  except ValueError:
    return last_total
  counts = _ConvertPercentToAbsolute(approx_total, float(percent))
  bucket_counts = int(round(counts - last_total))
  if bucket_counts > 0:
    histogram.append({'microsec': float(msec) * 1000, 'count': bucket_counts})
  return counts


def _ConvertPercentToAbsolute(total_value: int, percent: float) -> float:
  """Given total value and a 100-based percentage, returns the actual value."""
  return percent / 100 * total_value


def _ParseTimeSeries(
    time_series_json: Dict[Any, Any] | None,
) -> Tuple[List[int], List[int], Dict[str, List[int]]]:
  """Parse time series ops throughput from json output."""
  timestamps = []
  ops_series = []
  latency_series = collections.defaultdict(list)
  if time_series_json:
    time_series = time_series_json['ALL STATS']['Totals']['Time-Serie']
    start_time = int(time_series_json['ALL STATS']['Runtime']['Start time'])

    for interval, data_dict in time_series.items():
      current_time = int(interval) * 1000 + start_time
      timestamps.append(current_time)
      ops_series.append(data_dict['Count'])
      if int(data_dict['Count']) == 0:
        # When there is no throughput, max latency does not exist
        # data_dict sample
        # {
        #     'Count': 67,
        #     'Average Latency': 0.155,
        #     'Min Latency': 0.104,
        #     'Max Latency': 0.319,
        #     'p50.00': 0.151,
        #     'p90.00': 0.183,
        #     'p95.00': 0.247,
        #     'p99.00': 0.319,
        #     'p99.90': 0.319,
        # }
        for key, value in data_dict.items():
          if key == 'Count':
            continue
          latency_series[key].append(0)
      else:
        for key, value in data_dict.items():
          if key not in (
              'Average Latency',
              'Min Latency',
              'Max Latency',
              'p50.00',
              'p90.00',
              'p95.00',
              'p99.00',
              'p99.50',
              'p99.90',
              'p99.95',
              'p99.99',
          ):
            continue

          latency_series[key].append(value)
  return (
      timestamps,
      ops_series,
      latency_series,
  )


def _GetRuntimeInfo(time_series_json: Dict[Any, Any] | None):
  """Fetch runtime info (i.e start, end times and duration) from json output."""
  runtime_info = {}
  if time_series_json:
    runtime_info = time_series_json['ALL STATS']['Runtime']
    runtime_info = {
        key.replace(' ', '_'): val for key, val in runtime_info.items()
    }
  return runtime_info
