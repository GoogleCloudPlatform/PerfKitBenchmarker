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

import re

import gflags as flags
import logging

from perfkitbenchmarker import perfkitbenchmarker_lib

FLAGS = flags.FLAGS

BENCHMARKS_INFO = {'name': 'iperf',
                   'description': 'Run iperf',
                   'scratch_disk': False,
                   'num_machines': 2}

IPERF_PORT = 20000


def GetInfo():
  return BENCHMARKS_INFO


def Prepare(benchmark_spec):
  """Install iperf on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm in vms:
    vm.InstallPackage('iperf')

  fw = benchmark_spec.firewall
  fw.AllowPort(vms[1], IPERF_PORT)

  vms[1].RemoteCommand(
      'nohup iperf --server --port %s &> /dev/null &' % IPERF_PORT)


def Run(benchmark_spec):
  """Run iperf on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  server_vm = vms[1]
  results = []

  metadata = {
      'server_machine_type': server_vm.machine_type,
      'server_zone': server_vm.zone,
      'receiving_zone': server_vm.zone,
      'client_machine_type': vm.machine_type,
      'client_zone': vm.zone,
      'sending_zone': vm.zone
  }

  def RunIperf(ip_address, ip_type):
    """Run iperf using client 'vm' to connect to 'ip_address'.

    Args:
      ip_address: The IP address of the iperf server.
      ip_type: The IP type of 'ip_address' (e.g. 'internal', 'external')
    Returns:
      A single sample (see 'Run' docstring for sample type description).
    Raises:
      ValueError: When iperf results are not found in stdout.
    """
    iperf_cmd = ('iperf --client %s --port %s --format m --time 60' %
                 (ip_address, IPERF_PORT))
    iperf_pattern = re.compile(r'(\d+\.\d+|\d+) Mbits/sec')
    stdout, _ = vm.RemoteCommand(iperf_cmd, should_log=True)
    match = iperf_pattern.search(stdout)
    if not match:
      raise ValueError('Could not find iperf result in stdout:\n\n%s' % stdout)
    value = match.group(1)
    meta = metadata.copy()
    meta['ip_type'] = ip_type
    return ('Throughput', float(value), 'Mbits/sec', meta)

  logging.info('Iperf Results:')

  if perfkitbenchmarker_lib.ShouldRunOnExternalIpAddress():
    results.append(RunIperf(server_vm.ip_address, 'external'))

  if perfkitbenchmarker_lib.ShouldRunOnInternalIpAddress(vm, server_vm):
    results.append(RunIperf(server_vm.internal_ip, 'internal'))

  return results


def Cleanup(benchmark_spec):
  """Cleanup iperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms[1].RemoteCommand('pkill -9 iperf')

  for vm in vms:
    vm.UninstallPackage('iperf')
