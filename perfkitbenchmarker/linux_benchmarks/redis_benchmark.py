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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import logging
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import redis_server
from six.moves import range

flags.DEFINE_integer('redis_numprocesses', 1, 'Number of Redis processes to '
                     'spawn per processor.')
flags.DEFINE_integer('redis_clients', 5, 'Number of redis loadgen clients')
flags.DEFINE_string('redis_setgetratio', '1:0', 'Ratio of reads to write '
                    'performed by the memtier benchmark, default is '
                    '\'1:0\', ie: writes only.')

FIRST_PORT = 6379
FLAGS = flags.FLAGS
LOAD_THREAD = 1
LOAD_CLIENT = 1
LOAD_PIPELINE = 100
START_KEY = 1

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


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['vm_groups']['default']['vm_count'] = 1 + FLAGS.redis_clients
  return config


def PrepareLoadgen(load_vm):
  load_vm.Install('memtier')


def GetNumRedisServers(redis_vm):
  """Get the number of redis servers to install/use for this test."""
  if FLAGS.num_cpus_override:
    return FLAGS.num_cpus_override * FLAGS.redis_numprocesses
  return redis_vm.NumCpusForBenchmark() * FLAGS.redis_numprocesses


def Prepare(benchmark_spec):
  """Install Redis on one VM and memtier_benchmark on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  redis_vm = vms[0]
  # Install latest redis on the 1st machine.
  redis_vm.Install('redis_server')
  redis_server.Configure(redis_vm)
  redis_server.Start(redis_vm)

  # Remove snapshotting
  sed_cmd = (r"sed -i -e '/save 900/d' -e '/save 300/d' -e '/save 60/d' -e 's/#"
             "   save \"\"/save \"\"/g' %s/redis.conf")
  redis_vm.RemoteCommand(sed_cmd % redis_server.GetRedisDir())

  args = [((vm,), {}) for vm in vms]
  vm_util.RunThreaded(PrepareLoadgen, args)

  for i in range(GetNumRedisServers(redis_vm)):
    port = FIRST_PORT + i
    redis_vm.RemoteCommand(
        'cp %s/redis.conf %s/redis-%d.conf' %
        (redis_server.GetRedisDir(), redis_server.GetRedisDir(), port))
    redis_vm.RemoteCommand(
        r'sed -i -e "s/port 6379/port %d/g" %s/redis-%d.conf' %
        (port, redis_server.GetRedisDir(), port))
    redis_vm.RemoteCommand(
        'nohup sudo %s/src/redis-server %s/redis-%d.conf &> /dev/null &' %
        (redis_server.GetRedisDir(), redis_server.GetRedisDir(), port))
    # Pre-populate the redis server(s) with data
    redis_vm.RemoteCommand(
        'memtier_benchmark -s localhost -p %d -d %s -t %d -c %d '
        '--ratio 1:0 --key-pattern %s --pipeline %d '
        '--key-minimum %d --key-maximum %d -n allkeys ' %
        (port, FLAGS.memtier_data_size, LOAD_THREAD, LOAD_CLIENT,
         FLAGS.memtier_key_pattern, LOAD_PIPELINE, START_KEY,
         FLAGS.memtier_requests))


RedisResult = collections.namedtuple('RedisResult',
                                     ['throughput', 'average_latency'])


def RunLoad(redis_vm, load_vm, threads, port, test_id):
  """Spawn a memteir_benchmark on the load_vm against the redis_vm:port.

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
                    'Passed in {0}'.format(FLAGS.memtier_pipeline))
  memtier_pipeline = FLAGS.memtier_pipeline[0]

  base_cmd = ('memtier_benchmark -s %s  -p %d  -d %s '
              '--ratio %s --key-pattern %s --pipeline %d -c 1 -t %d '
              '--test-time %d --random-data --key-minimum %d '
              '--key-maximum %d > %s ;')
  final_cmd = (base_cmd % (redis_vm.internal_ip, port, FLAGS.memtier_data_size,
                           FLAGS.redis_setgetratio, FLAGS.memtier_key_pattern,
                           memtier_pipeline, threads, 10, START_KEY,
                           FLAGS.memtier_requests, '/dev/null') +
               base_cmd % (redis_vm.internal_ip, port, FLAGS.memtier_data_size,
                           FLAGS.redis_setgetratio, FLAGS.memtier_key_pattern,
                           memtier_pipeline, threads, 20, START_KEY,
                           FLAGS.memtier_requests, 'outfile-%d' % test_id) +
               base_cmd % (redis_vm.internal_ip, port, FLAGS.memtier_data_size,
                           FLAGS.redis_setgetratio, FLAGS.memtier_key_pattern,
                           memtier_pipeline, threads, 10, START_KEY,
                           FLAGS.memtier_requests, '/dev/null'))

  load_vm.RemoteCommand(final_cmd)
  output, _ = load_vm.RemoteCommand('cat outfile-%d | grep Totals | '
                                    'tr -s \' \' | cut -d \' \' -f 2' % test_id)
  throughput = float(output)
  output, _ = load_vm.RemoteCommand('cat outfile-%d | grep Totals | '
                                    'tr -s \' \' | cut -d \' \' -f 5' % test_id)
  latency = float(output)

  output, _ = load_vm.RemoteCommand('cat outfile-%d' % test_id)
  logging.info(output)

  return RedisResult(throughput, latency)


def Run(benchmark_spec):
  """Run memtier_benchmark against Redis.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
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
    args = [((redis_vm, load_vms[i % len(load_vms)], threads // num_loaders +
              (0 if (i + 1) > threads % num_loaders else 1),
              FIRST_PORT + i % num_servers, i), {}) for i in range(num_loaders)]
    client_results = [i for i in vm_util.RunThreaded(RunLoad, args)
                      if i is not None]
    logging.info('Redis results by client: %s', client_results)
    throughput = sum(r.throughput for r in client_results)

    if not throughput:
      raise errors.Benchmarks.RunError(
          'Zero throughput for {} threads: {}'.format(threads, client_results))

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


def Cleanup(benchmark_spec):
  """Remove Redis and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
