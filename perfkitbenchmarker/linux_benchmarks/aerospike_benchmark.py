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

"""Runs Aerospike (http://www.aerospike.com).

Aerospike is an opensource NoSQL solution. This benchmark runs a read/update
load test with varying numbers of client threads against an Aerospike server.

This test can be run in a variety of configurations including memory only,
remote/persistent ssd, and local ssd. The Aerospike configuration is controlled
by the "aerospike_storage_type" and "data_disk_type" flags.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import aerospike_client
from perfkitbenchmarker.linux_packages import aerospike_server
from six.moves import map
from six.moves import range


FLAGS = flags.FLAGS

flags.DEFINE_integer('aerospike_min_client_threads', 8,
                     'The minimum number of Aerospike client threads.',
                     lower_bound=1)
flags.DEFINE_integer('aerospike_max_client_threads', 128,
                     'The maximum number of Aerospike client threads.',
                     lower_bound=1)
flags.DEFINE_integer('aerospike_client_threads_step_size', 8,
                     'The number to increase the Aerospike client threads by '
                     'for each iteration of the test.',
                     lower_bound=1)
flags.DEFINE_integer('aerospike_read_percent', 90,
                     'The percent of operations which are reads.',
                     lower_bound=0, upper_bound=100)
flags.DEFINE_integer('aerospike_num_keys', 1000000,
                     'The number of keys to load Aerospike with. The index '
                     'must fit in memory regardless of where the actual '
                     'data is being stored and each entry in the '
                     'index requires 64 bytes.')

BENCHMARK_NAME = 'aerospike'
BENCHMARK_CONFIG = """
aerospike:
  description: Runs Aerospike.
  vm_groups:
    workers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      vm_count: null
      disk_count: 0
    client:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if FLAGS.aerospike_storage_type == aerospike_server.DISK:
    if FLAGS.data_disk_type == disk.LOCAL:
      # Didn't know max number of local disks, decide later.
      config['vm_groups']['workers']['disk_count'] = (
          config['vm_groups']['workers']['disk_count'] or None)
    else:
      config['vm_groups']['workers']['disk_count'] = (
          config['vm_groups']['workers']['disk_count'] or 1)

  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  aerospike_client.CheckPrerequisites()


def Prepare(benchmark_spec):
  """Install Aerospike server on one VM and Aerospike C client on the other.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """
  client = benchmark_spec.vm_groups['client'][0]
  workers = benchmark_spec.vm_groups['workers']

  def _Prepare(vm):
    if vm == client:
      vm.Install('aerospike_client')
    else:
      aerospike_server.ConfigureAndStart(vm, [workers[0].internal_ip])

  vm_util.RunThreaded(_Prepare, benchmark_spec.vms)


def Run(benchmark_spec):
  """Runs a read/update load test on Aerospike.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  client = benchmark_spec.vm_groups['client'][0]
  servers = benchmark_spec.vm_groups['workers']
  samples = []

  def ParseOutput(output):
    """Parses Aerospike output.

    Args:
      output: The stdout from running the benchmark.

    Returns:
      A tuple of average TPS and average latency.
    """
    read_latency = re.findall(
        r'read.*Overall Average Latency \(ms\) ([0-9]+\.[0-9]+)\n', output)[-1]
    write_latency = re.findall(
        r'write.*Overall Average Latency \(ms\) ([0-9]+\.[0-9]+)\n', output)[-1]
    average_latency = (
        (FLAGS.aerospike_read_percent / 100.0) * float(read_latency) +
        ((100 - FLAGS.aerospike_read_percent) / 100.0) * float(write_latency))
    tps = list(map(int, re.findall(r'total\(tps=([0-9]+) ', output)))
    return float(sum(tps)) / len(tps), average_latency

  load_command = ('./%s/benchmarks/target/benchmarks -z 32 -n test -w I '
                  '-o B:1000 -k %s -h %s' %
                  (aerospike_client.CLIENT_DIR, FLAGS.aerospike_num_keys,
                   ','.join(s.internal_ip for s in servers)))
  client.RemoteCommand(load_command, should_log=True)

  max_throughput_for_completion_latency_under_1ms = 0.0
  for threads in range(FLAGS.aerospike_min_client_threads,
                       FLAGS.aerospike_max_client_threads + 1,
                       FLAGS.aerospike_client_threads_step_size):
    load_command = ('timeout 60 ./%s/benchmarks/target/benchmarks '
                    '-z %s -n test -w RU,%s -o B:1000 -k %s '
                    '--latency 5,1 -h %s;:' %
                    (aerospike_client.CLIENT_DIR, threads,
                     FLAGS.aerospike_read_percent, FLAGS.aerospike_num_keys,
                     ','.join(s.internal_ip for s in servers)))
    stdout, _ = client.RemoteCommand(load_command, should_log=True)
    tps, latency = ParseOutput(stdout)

    metadata = {
        'Average Transactions Per Second': tps,
        'Client Threads': threads,
        'Storage Type': FLAGS.aerospike_storage_type,
        'Read Percent': FLAGS.aerospike_read_percent,
    }
    samples.append(sample.Sample('Average Latency', latency, 'ms', metadata))
    if latency < 1.0:
      max_throughput_for_completion_latency_under_1ms = max(
          max_throughput_for_completion_latency_under_1ms,
          tps)

  samples.append(sample.Sample(
                 'max_throughput_for_completion_latency_under_1ms',
                 max_throughput_for_completion_latency_under_1ms,
                 'req/s'))

  return samples


def Cleanup(benchmark_spec):
  """Cleanup Aerospike.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['workers']
  client = benchmark_spec.vm_groups['client'][0]

  client.RemoteCommand('sudo rm -rf aerospike*')

  def StopServer(server):
    server.RemoteCommand('cd %s && nohup sudo make stop' %
                         aerospike_server.AEROSPIKE_DIR)
    server.RemoteCommand('sudo rm -rf aerospike*')
  vm_util.RunThreaded(StopServer, servers)
