# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs plain netperf aggregate script runemomniaggdemo.sh to test packets/sec.

docs:
https://hewlettpackard.github.io/netperf/doc/netperf.html
manpage: http://manpages.ubuntu.com/manpages/maverick/man1/netperf.1.html

Runs multiple tests in script between one source machine and two target machines
to test packets per second and inbound and outbound throughput

"""

import collections
import logging
import os
import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import netperf

ALL_BENCHMARKS = ['STREAM', 'MAERTS', 'BIDIR', 'RRAGG']
flags.DEFINE_list('netperf_aggregate_benchmarks', ALL_BENCHMARKS,
                  'The netperf aggregate benchmark(s) to run. '
                  'STREAM measures outbound throughput. '
                  'MAERTS measures inbound throughput. '
                  'RRAGG measures packets per second.'
                  'BIDIR measure bidirectional bulk throughput')
flags.register_validator(
    'netperf_aggregate_benchmarks',
    lambda benchmarks: benchmarks and set(benchmarks).issubset(ALL_BENCHMARKS))

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'netperf_aggregate'
BENCHMARK_CONFIG = """
netperf_aggregate:
  description: simultaneous netperf to multiple endpoints
  vm_groups:
    servers:
      vm_spec: *default_single_core
      vm_count: 2
    client:
      vm_spec: *default_single_core
"""

TRANSACTIONS_PER_SECOND = 'transactions_per_second'

# Command ports are even (id*2), data ports are odd (id*2 + 1)
PORT_START = 12865

REMOTE_SCRIPTS_DIR = 'netperf_test_scripts'
REMOTE_SCRIPT = 'runemomniaggdemo.sh'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def PrepareNetperfAggregate(vm):
  """Installs netperf on a single vm."""

  vm.Install('python3')
  vm.Install('pip3')
  vm.RemoteCommand('sudo pip3 install --upgrade pip')
  vm.Install('texinfo')
  vm.Install('python_rrdtool')
  vm.Install('netperf')

  # Enable test types in the script runemomniaggdemo.sh
  for benchmark in FLAGS.netperf_aggregate_benchmarks:
    vm.RemoteCommand(
        f'sed -i "s/DO_{benchmark}=0;/DO_{benchmark}=1;/g" /opt/pkb/netperf-netperf-2.7.0/doc/examples/runemomniaggdemo.sh'
    )

  port_end = PORT_START

  if vm_util.ShouldRunOnExternalIpAddress():
    vm.AllowPort(PORT_START, port_end)

  netserver_cmd = ('{netserver_path} -p {port_start}').format(
      port_start=PORT_START,
      netserver_path=netperf.NETSERVER_PATH)
  vm.RemoteCommand(netserver_cmd)


def Prepare(benchmark_spec):
  """Install netperf on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """

  vms = benchmark_spec.vms
  client_vm = benchmark_spec.vm_groups['client'][0]

  vm_util.RunThreaded(PrepareNetperfAggregate, vms)
  client_vm.RemoteCommand(
      f'sudo chmod 777 {os.path.join(netperf.NETPERF_EXAMPLE_DIR, REMOTE_SCRIPT)}'
  )


def ParseNetperfAggregateOutput(stdout, test_type):
  """Parses the stdout of a single netperf process.

  Args:
    stdout: the stdout of the netperf process
    test_type: the type of test

  Returns:
    A tuple containing (throughput_sample, latency_samples, latency_histogram)
  """
  # Don't modify the metadata dict that was passed in

  logging.info('Parsing netperf aggregate output')
  metadata = {}
  aggregate_samples = []

  for line in stdout.splitlines():
    match = re.search('peak interval', line)
    if match:
      line_split = line.split()
      metric = f'{test_type} {line_split[0]} {line_split[6]}'
      value = float(line_split[5])
      unit = line_split[6]
      aggregate_samples.append(sample.Sample(
          metric, value, unit, metadata))

      # Each Transaction consists of a send and a receive packet
      # So Packets per second is Trans/s * 2
      if 'Trans/s' in metric:
        metric = metric.split()[0] + ' Packets/s'
        value = value * 2
        unit = 'Packets/s'
        aggregate_samples.append(sample.Sample(metric, value, unit, metadata))

  return aggregate_samples


def RunNetperfAggregate(vm, server_ips):
  """Spawns netperf on a remote VM, parses results.

  Args:
    vm: The VM that the netperf TCP_RR benchmark will be run upon.
    server_ips: Machines that are running netserver.

  Returns:
    A sample.Sample object with the result.
  """

  # setup remote hosts file
  vm.RemoteCommand(f'cd {netperf.NETPERF_EXAMPLE_DIR} && rm remote_hosts')
  ip_num = 0
  for ip in server_ips:
    vm.RemoteCommand(f"echo 'REMOTE_HOSTS[{ip_num}]={ip}' >> "
                     f"{netperf.NETPERF_EXAMPLE_DIR}/remote_hosts")
    ip_num += 1

  vm.RemoteCommand(f"echo 'NUM_REMOTE_HOSTS={len(server_ips)}' >> "
                   f"{netperf.NETPERF_EXAMPLE_DIR}/remote_hosts")

  vm.RemoteCommand(
      f'cd {netperf.NETPERF_EXAMPLE_DIR} && '
      'export PATH=$PATH:. && '
      'chmod +x runemomniaggdemo.sh && '
      './runemomniaggdemo.sh',
      ignore_failure=True,
      should_log=True,
      login_shell=False,
      timeout=1200)

  interval_naming = collections.namedtuple(
      'IntervalNaming', 'output_file parse_name')
  benchmark_interval_mapping = {
      'STREAM': interval_naming('netperf_outbound', 'Outbound'),
      'MAERTS': interval_naming('netperf_inbound', 'Inbound'),
      'RRAGG': interval_naming('netperf_tps', 'Request/Response Aggregate'),
      'BIDIR': interval_naming('netperf_bidirectional', 'Bidirectional')
  }

  samples = []
  for benchmark in FLAGS.netperf_aggregate_benchmarks:
    output_file = benchmark_interval_mapping[benchmark].output_file
    parse_name = benchmark_interval_mapping[benchmark].parse_name
    proc_stdout, _ = vm.RemoteCommand(
        f'cd {netperf.NETPERF_EXAMPLE_DIR} && python3 post_proc.py '
        f'--intervals {output_file}.log',
        ignore_failure=False)
    vm.RemoteCommand(f'cd {netperf.NETPERF_EXAMPLE_DIR} && rm {output_file}*')
    samples.extend(ParseNetperfAggregateOutput(proc_stdout, f'{parse_name}'))

  return samples


def Run(benchmark_spec):
  """Run netperf TCP_RR on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """

  # set client and server vms
  vm_dict = benchmark_spec.vm_groups
  client_vms = vm_dict['client']
  server_vms = vm_dict['servers']
  client_vm = client_vms[0]

  results = []

  if vm_util.ShouldRunOnExternalIpAddress():
    server_ips = list((vm.ip_address for vm in server_vms))
    external_ip_results = RunNetperfAggregate(client_vm, server_ips)
    for external_ip_result in external_ip_results:
      external_ip_result.metadata['ip_type'] = 'external'
    results.extend(external_ip_results)

  # check if all server vms internal ips are reachable
  run_internal = True
  for tmp_vm in server_vms:
    if not vm_util.ShouldRunOnInternalIpAddress(client_vm, tmp_vm):
      run_internal = False
      break

  if run_internal:
    server_ips = list((vm.internal_ip for vm in server_vms))
    internal_ip_results = RunNetperfAggregate(client_vm, server_ips)

    for internal_ip_result in internal_ip_results:
      internal_ip_result.metadata['ip_type'] = 'internal'
    results.extend(internal_ip_results)

  return results


def Cleanup(benchmark_spec):
  """Cleanup netperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm in vms:
    vm.RemoteCommand('sudo killall netserver')
