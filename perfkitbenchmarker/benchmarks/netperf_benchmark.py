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

"""Runs plain netperf in a few modes.

docs:
http://www.netperf.org/svn/netperf2/tags/netperf-2.4.5/doc/netperf.html#TCP_005fRR
manpage: http://manpages.ubuntu.com/manpages/maverick/man1/netperf.1.html

Runs TCP_RR, TCP_CRR, and TCP_STREAM benchmarks from netperf across two
machines.
"""

import logging
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

BENCHMARKS_INFO = {'name': 'netperf_simple',
                   'description': 'Run Netperf tcp_rr, tcp_crr, and tcp_stream',
                   'scratch_disk': False,
                   'num_machines': 2}

NETPERF_NAME = 'netperf-2.6.0.tar.gz'
NETPERF_SRC = 'netperf-2.6.0/src'
NETPERF_LOC = 'ftp://ftp.netperf.org/netperf/%s' % NETPERF_NAME
NETPERF_BENCHMARKS = ['TCP_RR', 'TCP_CRR', 'TCP_STREAM', 'UDP_RR']
COMMAND_PORT = 20000
DATA_PORT = 20001


def GetInfo():
  return BENCHMARKS_INFO


def PrepareNetperf(vm):
  """Installs netperf on a single vm."""
  logging.info('netperf prepare on %s', vm)
  vm.InstallPackage('build-essential')
  wget_cmd = '/usr/bin/wget %s' % NETPERF_LOC
  vm.RemoteCommand(wget_cmd)
  vm.RemoteCommand('tar xvfz %s' % NETPERF_NAME)
  make_cmd = 'cd netperf-2.6.0;./configure;make'
  vm.RemoteCommand(make_cmd)


def Prepare(benchmark_spec):
  """Install netperf on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms = vms[:2]
  vm_util.RunThreaded(PrepareNetperf, vms)

  fw = benchmark_spec.firewall
  # TODO(user): takes too long, change API to take range, put all in the same
  #    range.

  fw.AllowPort(vms[1], COMMAND_PORT)
  fw.AllowPort(vms[1], DATA_PORT)

  vms[1].RemoteCommand('%s/netserver -p %s' % (NETPERF_SRC, COMMAND_PORT))


def RunNetperf(vm, benchmark_name, server_ip):
  """Spawns netperf on a remove VM, parses results.

  Args:
    vm: The VM that the netperf TCP_RR benchmark will be run upon.
    benchmark_name: The netperf benchmark to run, see the documentation.
    server_ip: A machine that is running netserver.

  Returns:
    A single sample in the form of a tuple. The tuple contains
        the sample metric (string), value (float), unit (string),
        and empty metadata dictionary (to be filled out later).
  """
  netperf_cmd = ('{src}/netperf -p {command_port} -t {benchmark_name} '
                 '-H {server_ip} -- -P {data_port}').format(
                     src=NETPERF_SRC, benchmark_name=benchmark_name,
                     server_ip=server_ip, command_port=COMMAND_PORT,
                     data_port=DATA_PORT)
  logging.info('Netperf Results:')
  stdout, _ = vm.RemoteCommand(netperf_cmd, should_log=True)
  match = re.search(r'(\d+\.\d+)\s+\n', stdout).group(1)
  value = float(match)
  # TODO(user): Pull the test to metric name/unit mapping out into a dict.
  if benchmark_name == 'TCP_STREAM':
    metric = 'TCP_STREAM_Throughput'
    unit = 'Mbits/sec'
  else:
    metric = '%s_Transaction_Rate' % benchmark_name
    unit = 'transactions_per_second'
  return (metric, value, unit, {})


def Run(benchmark_spec):
  """Run netperf TCP_RR on the target vm.

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
  logging.info('TCP_RR running on %s', vm)
  results = []
  metadata = {
      'ip_type': 'external',
      'server_machine_type': server_vm.machine_type,
      'server_zone': server_vm.zone,
      'receiving_zone': server_vm.zone,
      'client_machine_type': vm.machine_type,
      'client_zone': vm.zone,
      'sending_zone': vm.zone
  }
  for netperf_benchmark in NETPERF_BENCHMARKS:

    if vm_util.ShouldRunOnExternalIpAddress():
      external_ip_result = RunNetperf(vm, netperf_benchmark,
                                      server_vm.ip_address)
      external_ip_result[3].update(metadata)
      results.append(external_ip_result)

    if vm_util.ShouldRunOnInternalIpAddress(vm, server_vm):
      internal_ip_result = RunNetperf(vm, netperf_benchmark,
                                      server_vm.internal_ip)
      metadata['ip_type'] = 'internal'
      internal_ip_result[3].update(metadata)
      results.append(internal_ip_result)

  return results


def StopNetserver(vm):
  """Stops Netserver on the specified vm.

  Args:
    vm: The VM upon which the stop command will be run.
  """
  vm.InstallPackage('psmisc')
  vm.RemoteCommand('killall netserver')
  vm.UninstallPackage('psmisc')


def Cleanup(benchmark_spec):
  """Cleanup netperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms = vms[:2]
  StopNetserver(vms[1])
  for vm in vms:
    logging.info('uninstalling netperf on %s', vm)
    vm.RemoteCommand('rm -rf netperf-2.6.0')
    vm.RemoteCommand('rm -f %s' % NETPERF_NAME)
    vm.UninstallPackage('build-essential')
