# Copyright 2014 Google Inc. All rights reserved.
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
by the "aerospike_storage_type" and "scratch_disk_type" flags.
"""

import re
import time

from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import aerospike_server


FLAGS = flags.FLAGS

MEMORY = 'memory'
DISK = 'disk'
flags.DEFINE_enum('aerospike_storage_type', MEMORY, [MEMORY, DISK],
                  'The type of storage to use for Aerospike data. The type of '
                  'disk is controlled by the "scratch_disk_type" flag.')
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

BENCHMARK_INFO = {'name': 'aerospike',
                  'description': 'Runs Aerospike',
                  'num_machines': 2}
AEROSPIKE_CLIENT = 'https://github.com/aerospike/aerospike-client-c.git'
CLIENT_DIR = 'aerospike-client-c'
CLIENT_VERSION = '3.0.84'
PATCH_FILE = 'aerospike.patch'


def GetInfo():
  info = BENCHMARK_INFO.copy()
  if (FLAGS.aerospike_storage_type == DISK and
      FLAGS.scratch_disk_type != disk.LOCAL):
    info['scratch_disk'] = True
  else:
    info['scratch_disk'] = False
  return info


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(PATCH_FILE)


def _PrepareClient(client):
  """Prepare the Aerospike C client on a VM."""
  client.Install('build_tools')
  client.Install('lua5_1')
  client.Install('openssl')
  clone_command = 'git clone %s'
  client.RemoteCommand(clone_command % AEROSPIKE_CLIENT)
  build_command = ('cd %s && git checkout %s && git submodule update --init '
                   '&& make')
  client.RemoteCommand(build_command % (CLIENT_DIR, CLIENT_VERSION))

  # Apply a patch to the client benchmark so we have access to average latency
  # of requests. Switching over to YCSB should obviate this.
  client.PushDataFile(PATCH_FILE)
  benchmark_dir = '%s/benchmarks/src/main' % CLIENT_DIR
  client.RemoteCommand('cp aerospike.patch %s' % benchmark_dir)
  client.RemoteCommand('cd %s && patch -p1 -f  < aerospike.patch'
                       % benchmark_dir)
  client.RemoteCommand('sed -i -e "s/lpthread/lpthread -lz/" '
                       '%s/benchmarks/Makefile' % CLIENT_DIR)
  client.RemoteCommand('cd %s/benchmarks && make' % CLIENT_DIR)


def _PrepareServer(server):
  """Prepare the Aerospike server on a VM."""
  server.Install('aerospike_server')

  if FLAGS.aerospike_storage_type == DISK:
    if FLAGS.scratch_disk_type == disk.LOCAL:
      devices = server.GetLocalDisks()
    else:
      devices = [scratch_disk.GetDevicePath()
                 for scratch_disk in server.scratch_disks]
  else:
    devices = []

  server.RenderTemplate(data.ResourcePath('aerospike.conf.j2'),
                        aerospike_server.AEROSPIKE_CONF_PATH,
                        {'devices': devices})

  for scratch_disk in server.scratch_disks:
    server.RemoteCommand('sudo umount %s' % scratch_disk.mount_point)

  server.RemoteCommand('cd %s && make init' % aerospike_server.AEROSPIKE_DIR)
  server.RemoteCommand('cd %s; nohup sudo make start &> /dev/null &' %
                       aerospike_server.AEROSPIKE_DIR)
  time.sleep(5)  # Wait for server to come up


def Prepare(benchmark_spec):
  """Install Aerospike server on one VM and Aerospike C client on the other.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """
  server, client = benchmark_spec.vms

  def _Prepare(vm):
    if vm == client:
      _PrepareClient(vm)
    else:
      _PrepareServer(vm)

  vm_util.RunThreaded(_Prepare, benchmark_spec.vms)


def Run(benchmark_spec):
  """Runs a read/update load test on Aerospike.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  server, client = benchmark_spec.vms
  samples = []

  def ParseOutput(output):
    """Parses Aerospike output.

    Args:
      output: The stdout from running the benchmark.

    Returns:
      A tuple of average TPS and average latency.
    """
    write_latency, read_latency = re.findall(
        r'Overall Average Latency \(ms\) ([0-9]+\.[0-9]+)', output)[-2:]
    average_latency = (
        (FLAGS.aerospike_read_percent / 100.0) * float(read_latency) +
        ((100 - FLAGS.aerospike_read_percent) / 100.0) * float(write_latency))
    tps = map(int, re.findall(r'total\(tps=([0-9]+)', output)[:-1])
    return float(sum(tps)) / len(tps), average_latency

  load_command = ('./%s/benchmarks/target/benchmarks -z 32 -n test -w I '
                  '-o B:1000 -k 1000000 -h %s' %
                  (CLIENT_DIR, server.internal_ip))
  client.RemoteCommand(load_command, should_log=True)

  max_throughput_for_completion_latency_under_1ms = 0.0
  for threads in range(FLAGS.aerospike_min_client_threads,
                       FLAGS.aerospike_max_client_threads + 1,
                       FLAGS.aerospike_client_threads_step_size):
    load_command = ('timeout 15 ./%s/benchmarks/target/benchmarks '
                    '-z %s -n test -w RU,%s -o B:1000 -k 1000000 '
                    '--latency 5,1 -h %s;:' %
                    (CLIENT_DIR, threads, FLAGS.aerospike_read_percent,
                     server.internal_ip))
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
  server, client = benchmark_spec.vms

  client.RemoteCommand('sudo rm -rf aerospike*')
  server.RemoteCommand('cd %s && nohup sudo make stop' %
                       aerospike_server.AEROSPIKE_DIR)
  server.RemoteCommand('sudo rm -rf aerospike*')
