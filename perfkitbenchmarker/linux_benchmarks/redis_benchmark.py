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

"""Run memtier_benchmark against Redis.

memtier_benchmark is a load generator created by RedisLabs to benchmark
Redis.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
"""


import collections
import logging
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server

_NUM_PROCESSES = flags.DEFINE_integer(
    'redis_numprocesses', 1, 'Number of Redis processes to '
    'spawn per processor.')
_NUM_CLIENTS = flags.DEFINE_integer('redis_clients', 5,
                                    'Number of redis loadgen clients')
_SET_GET_RATIO = flags.DEFINE_string(
    'redis_setgetratio', '1:0', 'Ratio of reads to write '
    'performed by the memtier benchmark, default is '
    '\'1:0\', ie: writes only.')
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'redis'
BENCHMARK_CONFIG = """
redis:
  description: >
      Run memtier_benchmark against Redis.
      Specify the number of client VMs with --redis_clients.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

_LinuxVirtualMachine = linux_virtual_machine.BaseLinuxVirtualMachine
_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['vm_groups']['default']['vm_count'] = 1 + _NUM_CLIENTS.value
  return config


def PrepareLoadgen(load_vm: _LinuxVirtualMachine) -> None:
  load_vm.Install('memtier')


def GetNumRedisServers(redis_vm: _LinuxVirtualMachine) -> int:
  """Get the number of redis servers to install/use for this test."""
  if FLAGS.num_cpus_override:
    return FLAGS.num_cpus_override * _NUM_PROCESSES.value
  return redis_vm.NumCpusForBenchmark() * _NUM_PROCESSES.value


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Install Redis on one VM and memtier_benchmark on another."""
  vms = bm_spec.vms
  redis_vm = vms[0]
  # Install latest redis on the 1st machine.
  redis_vm.Install('redis_server')
  redis_server.Configure(redis_vm)
  redis_server.Start(redis_vm)

  vm_util.RunThreaded(PrepareLoadgen, vms)

  # Pre-populate the redis server(s) with data
  for i in range(GetNumRedisServers(redis_vm)):
    port = redis_server.REDIS_FIRST_PORT + i
    memtier.Load(redis_vm, 'localhost', port)


RedisResult = collections.namedtuple('RedisResult',
                                     ['throughput', 'average_latency'])


def RunLoad(redis_vm: _LinuxVirtualMachine, load_vm: _LinuxVirtualMachine,
            threads: int, port: str, test_id: str) -> RedisResult:
  """Spawn a memtier_benchmark on the load_vm against the redis_vm:port.

  Args:
    redis_vm: The target of the memtier_benchmark
    load_vm: The vm that will run the memtier_benchmark.
    threads: The number of threads to run in this memtier_benchmark process.
    port: the port to target on the redis_vm.
    test_id: test id to differentiate between tests.
  Returns:
    A throughput, latency tuple, or None if threads was 0.
  Raises:
    Exception:  If an invalid combination of FLAGS is specified.
  """
  if threads == 0:
    return None

  if len(FLAGS.memtier_pipeline) != 1:
    raise Exception('Only one memtier pipeline is supported.  '
                    f'Passed in {FLAGS.memtier_pipeline}.')
  memtier_pipeline = FLAGS.memtier_pipeline[0]

  measurement_cmd = memtier.BuildMemtierCommand(
      server=redis_vm.internal_ip,
      port=port,
      data_size=FLAGS.memtier_data_size,
      ratio=_SET_GET_RATIO.value,
      key_pattern=FLAGS.memtier_key_pattern,
      pipeline=memtier_pipeline,
      threads=threads,
      test_time=20,
      key_minimum=1,
      key_maximum=FLAGS.memtier_requests,
      outfile=f'outfile-{test_id}')
  no_output_cmd = memtier.BuildMemtierCommand(
      server=redis_vm.internal_ip,
      port=port,
      data_size=FLAGS.memtier_data_size,
      ratio=_SET_GET_RATIO.value,
      key_pattern=FLAGS.memtier_key_pattern,
      pipeline=memtier_pipeline,
      threads=threads,
      test_time=10,
      key_minimum=1,
      key_maximum=FLAGS.memtier_requests,
      outfile='/dev/null')
  final_cmd = f'{no_output_cmd}; {measurement_cmd}; {no_output_cmd}'

  load_vm.RemoteCommand(final_cmd)
  output, _ = load_vm.RemoteCommand(f'cat outfile-{test_id} | grep Totals | '
                                    'tr -s \' \' | cut -d \' \' -f 2')
  throughput = float(output)
  output, _ = load_vm.RemoteCommand(f'cat outfile-{test_id} | grep Totals | '
                                    'tr -s \' \' | cut -d \' \' -f 5')
  latency = float(output)

  output, _ = load_vm.RemoteCommand(f'cat outfile-{test_id}')
  logging.info(output)

  return RedisResult(throughput, latency)


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run memtier_benchmark against Redis."""
  vms = bm_spec.vms
  redis_vm = vms[0]
  load_vms = vms[1:]
  latency = 0.0
  latency_threshold = 1000000.0
  threads = 0
  results = []
  num_servers = GetNumRedisServers(redis_vm)
  max_throughput_for_completion_latency_under_1ms = 0.0

  while latency < latency_threshold:
    threads += max(1, int(threads * .15))
    num_loaders = len(load_vms) * num_servers
    args = []
    for i in range(num_loaders):
      load_vm = load_vms[i % len(load_vms)]
      run_threads = threads // num_loaders + (0 if
                                              (i + 1) > threads % num_loaders
                                              else 1)
      port = redis_server.REDIS_FIRST_PORT + i % num_servers
      test_id = i
      args.append(((redis_vm, load_vm, run_threads, port, test_id), {}))
    client_results = [
        i for i in vm_util.RunThreaded(RunLoad, args) if i is not None
    ]
    logging.info('Redis results by client: %s', client_results)
    throughput = sum(r.throughput for r in client_results)

    if not throughput:
      raise errors.Benchmarks.RunError(
          f'Zero throughput for {threads} threads: {client_results}')

    # Average latency across clients
    latency = (sum(client_latency * client_throughput
                   for client_latency, client_throughput in client_results) /
               throughput)

    if latency < 1.0:
      max_throughput_for_completion_latency_under_1ms = max(
          max_throughput_for_completion_latency_under_1ms,
          throughput)
    results.append(sample.Sample('throughput', throughput, 'req/s',
                                 {'latency': latency, 'threads': threads}))
    logging.info('Threads : %d  (%f, %f) < %f', threads, throughput, latency,
                 latency_threshold)
    if threads == 1:
      latency_threshold = latency * 20

  results.append(sample.Sample(
      'max_throughput_for_completion_latency_under_1ms',
      max_throughput_for_completion_latency_under_1ms,
      'req/s'))

  return results


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  del bm_spec
