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

"""Runs plain Iperf.

Docs:
http://iperf.fr/

Runs Iperf to collect network throughput.
"""

import logging
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

flags.DEFINE_integer('iperf_sending_thread_count', 1,
                     'Number of connections to make to the '
                     'server for sending traffic.',
                     lower_bound=1)
flags.DEFINE_integer('iperf_runtime_in_seconds', 60,
                     'Number of seconds to run iperf.',
                     lower_bound=1)

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'iperf',
                  'description': 'Run iperf',
                  'scratch_disk': False,
                  'num_machines': 2}

IPERF_PORT = 20000
IPERF_RETRIES = 5


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install iperf and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """

  fw = benchmark_spec.firewall
  vms = benchmark_spec.vms
  for vm in vms:
    vm.Install('iperf')
    if vm_util.ShouldRunOnExternalIpAddress():
      fw.AllowPort(vm, IPERF_PORT)
    vm.RemoteCommand('nohup iperf --server --port %s &> /dev/null &' %
                     IPERF_PORT)


@vm_util.Retry(max_retries=IPERF_RETRIES)
def _RunIperf(sending_vm, receiving_vm, receiving_ip_address, ip_type):
  """Run iperf using sending 'vm' to connect to 'ip_address'.

  Args:
    sending_vm: The VM sending traffic.
    receiving_vm: The VM receiving traffic.
    receiving_ip_address: The IP address of the iperf server (ie the receiver).
    ip_type: The IP type of 'ip_address' (e.g. 'internal', 'external')
  Returns:
    A Sample.
  """
  iperf_cmd = ('iperf --client %s --port %s --format m --time %s -P %s' %
               (receiving_ip_address, IPERF_PORT,
                FLAGS.iperf_runtime_in_seconds,
                FLAGS.iperf_sending_thread_count))
  # the additional time on top of the iperf runtime is to account for the
  # time it takes for the iperf process to start and exit
  timeout_buffer = 30 + FLAGS.iperf_sending_thread_count
  stdout, _ = sending_vm.RemoteCommand(iperf_cmd, should_log=True,
                                       timeout=FLAGS.iperf_runtime_in_seconds +
                                       timeout_buffer)

  # Example output from iperf that needs to be parsed
  # STDOUT: ------------------------------------------------------------
  # Client connecting to 10.237.229.201, TCP port 5001
  # TCP window size: 0.04 MByte (default)
  # ------------------------------------------------------------
  # [  6] local 10.76.234.115 port 53527 connected with 10.237.229.201 port 5001
  # [  3] local 10.76.234.115 port 53524 connected with 10.237.229.201 port 5001
  # [  4] local 10.76.234.115 port 53525 connected with 10.237.229.201 port 5001
  # [  5] local 10.76.234.115 port 53526 connected with 10.237.229.201 port 5001
  # [ ID] Interval       Transfer     Bandwidth
  # [  4]  0.0-60.0 sec  3730 MBytes  521.1 Mbits/sec
  # [  5]  0.0-60.0 sec  3499 MBytes   489 Mbits/sec
  # [  6]  0.0-60.0 sec  3044 MBytes   425 Mbits/sec
  # [  3]  0.0-60.0 sec  3738 MBytes   522 Mbits/sec
  # [SUM]  0.0-60.0 sec  14010 MBytes  1957 Mbits/sec

  thread_values = re.findall(r'\[SUM].*\s+(\d+\.?\d*).Mbits/sec', stdout)
  if not thread_values:
    # If there is no sum you have try and figure out an estimate
    # which happens when threads start at different times.  The code
    # below will tend to overestimate a bit.
    thread_values = re.findall('\[.*\d+\].*\s+(\d+\.?\d*).Mbits/sec', stdout)

    if len(thread_values) != FLAGS.iperf_sending_thread_count:
      raise ValueError('Only %s out of %s iperf threads reported a'
                       ' throughput value.' %
                       (len(thread_values), FLAGS.iperf_sending_thread_count))

  total_throughput = 0.0
  for value in thread_values:
    total_throughput += float(value)

  metadata = {
      # The meta data defining the environment
      'receiving_machine_type': receiving_vm.machine_type,
      'receiving_zone': receiving_vm.zone,
      'sending_machine_type': sending_vm.machine_type,
      'sending_thread_count': FLAGS.iperf_sending_thread_count,
      'sending_zone': sending_vm.zone,
      'runtime_in_seconds': FLAGS.iperf_runtime_in_seconds,
      'ip_type': ip_type
  }
  return sample.Sample('Throughput', total_throughput, 'Mbits/sec', metadata)


def Run(benchmark_spec):
  """Run iperf on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  results = []

  logging.info('Iperf Results:')

  # Send traffic in both directions
  for originator in [0, 1]:
    sending_vm = vms[originator]
    receiving_vm = vms[originator ^ 1]
    # Send using external IP addresses
    if vm_util.ShouldRunOnExternalIpAddress():
      results.append(_RunIperf(sending_vm,
                               receiving_vm,
                               receiving_vm.ip_address,
                               'external'))

    # Send using internal IP addresses
    if vm_util.ShouldRunOnInternalIpAddress(sending_vm,
                                            receiving_vm):
      results.append(_RunIperf(sending_vm,
                               receiving_vm,
                               receiving_vm.internal_ip,
                               'internal'))

  return results


def Cleanup(benchmark_spec):
  """Cleanup iperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms[1].RemoteCommand('pkill -9 iperf')
