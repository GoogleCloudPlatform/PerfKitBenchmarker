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

"""Runs aerospike."""

import logging
import re
import time

import gflags as flags

from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

MEMORY = 'memory'
DISK = 'disk'
flags.DEFINE_enum('aerospike_storage_type', MEMORY, [MEMORY, DISK],
                  'The type of storage to use for Aerospike data. The type of '
                  'disk is controlled by a combination of the '
                  '"scratch_disk_type" and "use_local_disk" flags.')

BENCHMARK_INFO = {'name': 'aerospike',
                  'description': 'Runs Aerospike',
                  'num_machines': 2
                 }
PACKAGES = ('build-essential git autoconf libtool libssl-dev lua5.1 '
            'liblua5.1-dev')
AEROSPIKE_SERVER = 'https://github.com/aerospike/aerospike-server.git'
AEROSPIKE_CLIENT = 'https://github.com/aerospike/aerospike-client-c.git'
CLIENT_DIR = 'aerospike-client-c'
CLIENT_VERSION = '3.0.84'
SERVER_DIR = 'aerospike-server'
SERVER_VERSION = '3.3.19'
READ_PERCENT = 90
MAX_THREADS = 128


def GetInfo():
  if FLAGS.aerospike_storage_type == DISK and not FLAGS.use_local_disk:
    BENCHMARK_INFO['scratch_disk'] = True
  else:
    BENCHMARK_INFO['scratch_disk'] = False
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install Aerospike server on one VM and Aerospike C client on the other.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """
  server, client = benchmark_spec.vms

  def InstallPackages(vm):
    vm.InstallPackage(PACKAGES)

  vm_util.RunThreaded(InstallPackages, benchmark_spec.vms)

  clone_command = 'git clone %s'
  server.RemoteCommand(clone_command % AEROSPIKE_SERVER)
  client.RemoteCommand(clone_command % AEROSPIKE_CLIENT)

  build_command = 'cd %s; git checkout %s; git submodule update --init; make'
  server.RemoteCommand(build_command % (SERVER_DIR, SERVER_VERSION))
  client.RemoteCommand(build_command % (CLIENT_DIR, CLIENT_VERSION))

  client.PushDataFile('aerospike.patch')
  benchmark_dir = '%s/benchmarks/src/main' % CLIENT_DIR
  client.RemoteCommand('cp aerospike.patch %s' % benchmark_dir)
  client.RemoteCommand('cd %s; patch -p1 -f  < aerospike.patch' % benchmark_dir)
  client.RemoteCommand('sed -i -e "s/lpthread/lpthread -lz/" '
                       '%s/benchmarks/Makefile' % CLIENT_DIR)
  client.RemoteCommand('cd %s/benchmarks; make' % CLIENT_DIR)

  if FLAGS.aerospike_storage_type == DISK:
    if FLAGS.use_local_disk:
      devices = server.GetLocalDrives()
    else:
      devices = [disk.GetDevicePath() for disk in server.scratch_disks]

    server.RenderTemplate(data.ResourcePath('aerospike.conf.j2'),
                          'aerospike-server/as/etc/aerospike_dev.conf',
                          {'devices': devices})

  for disk in server.scratch_disks:
    server.RemoteCommand('sudo umount %s' % disk.mount_point)

  server.RemoteCommand('cd %s; make init' % SERVER_DIR)
  server.RemoteCommand(
      'cd %s; nohup sudo make start &> /dev/null &' % SERVER_DIR)
  time.sleep(5)  # Wait for server to come up


def Run(benchmark_spec):
  """Run Aerospike on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
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
    average_latency = ((READ_PERCENT / 100.0) * float(read_latency) +
                       ((100 - READ_PERCENT) / 100.0) * float(write_latency))
    tps = map(int, re.findall(r'total\(tps=([0-9]+)', output)[:-1])
    return float(sum(tps) / len(tps)), average_latency

  load_command = ('./%s/benchmarks/target/benchmarks -z 32 -n test -w I '
                  '-o B:1000  -k 1000000 -h %s' %
                  (CLIENT_DIR, server.internal_ip))
  client.RemoteCommand(load_command, should_log=True)

  for threads in xrange(1,MAX_THREADS):
    load_command = ('timeout 15 ./%s/benchmarks/target/benchmarks '
                    '-z %s -n test -w RU,%s -o B:1000  -k 1000000 '
                    '--latency 5,1 -h %s;:' %
                    (CLIENT_DIR, threads, READ_PERCENT,
                     server.internal_ip))
    stdout, _ = client.RemoteCommand(load_command, should_log=True)
    tps, latency = ParseOutput(stdout)
    samples.append('%s,%s' % (tps, latency))

  logging.info('Aerospike Results:\nAverage TPS,Average Latency(ms)\n%s',
               '\n'.join(samples))
  return []

def Cleanup(benchmark_spec):
  """Cleanup Aerospike.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server, client = benchmark_spec.vms

  client.RemoteCommand('sudo rm -rf aerospike*')
  server.RemoteCommand('cd %s; nohup sudo make stop' % SERVER_DIR)
  server.RemoteCommand('sudo rm -rf aerospike*')

