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

"""Runs mesh network benchmarks.

Runs TCP_RR, TCP_STREAM benchmarks from netperf and compute total throughput
and average latency inside mesh network.
"""


import logging
import re
import threading

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import netperf


flags.DEFINE_integer('num_connections', 1,
                     'Number of connections between each pair of vms.')

flags.DEFINE_integer('num_iterations', 1,
                     'Number of iterations for each run.')


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mesh_network'
BENCHMARK_CONFIG = """
mesh_network:
  description: >
    Measures VM to VM cross section bandwidth in
    a mesh network. Specify the number of VMs in the network
    with --num_vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

NETPERF_BENCHMARKSS = ['TCP_RR', 'TCP_STREAM']
VALUE_INDEX = 1
RESULT_LOCK = threading.Lock()


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['vm_groups']['default']['vm_count'] = FLAGS.num_vms
  if FLAGS.num_vms < 2:  # Needs at least 2 vms to run the benchmark.
    config['vm_groups']['default']['vm_count'] = 2
  return config


def PrepareVM(vm):
  """Prepare netperf on a single VM.

  Args:
    vm: The VM that needs to install netperf package.
  """
  vm.RemoteCommand('./netserver')


def Prepare(benchmark_spec):
  """Install vms with necessary softwares.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the banchmark.
  """
  vms = benchmark_spec.vms
  logging.info('Preparing netperf on %s', vms[0])
  vms[0].Install('netperf')
  for vm in vms:
    vms[0].MoveFile(vm, netperf.NETPERF_PATH)
    vms[0].MoveFile(vm, netperf.NETSERVER_PATH)
  vm_util.RunThreaded(PrepareVM, vms, len(vms))


def RunNetperf(vm, benchmark_name, servers, result):
  """Spawns netperf on a remote VM, parses results.

  Args:
    vm: The VM running netperf.
    benchmark_name: The netperf benchmark to run.
    servers: VMs running netserver.
    result: The result variable shared by all threads.
  """
  cmd = ''
  if FLAGS.duration_in_seconds:
    cmd_duration_suffix = '-l %s' % FLAGS.duration_in_seconds
  else:
    cmd_duration_suffix = ''
  for server in servers:
    if vm != server:
      cmd += ('./netperf -t '
              '{benchmark_name} -H {server_ip} -i {iterations} '
              '{cmd_suffix} & ').format(
                  benchmark_name=benchmark_name,
                  server_ip=server.internal_ip,
                  iterations=FLAGS.num_iterations,
                  cmd_suffix=cmd_duration_suffix)
  netperf_cmd = ''
  for _ in range(FLAGS.num_connections):
    netperf_cmd += cmd
  netperf_cmd += 'wait'
  output, _ = vm.RemoteCommand(netperf_cmd)
  logging.info(output)

  match = re.findall(r'(\d+\.\d+)\s+\n', output)
  value = 0
  expected_num_match = (len(servers) - 1) * FLAGS.num_connections
  if len(match) != expected_num_match:
    raise errors.Benchmarks.RunError(
        'Netserver not reachable. Expecting %s results, got %s.' %
        (expected_num_match, len(match)))
  for res in match:
    if benchmark_name == 'TCP_RR':
      value += 1.0 / float(res) * 1000.0
    else:
      value += float(res)
  with RESULT_LOCK:
    result[VALUE_INDEX] += value


def Run(benchmark_spec):
  """Run netperf on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    Total throughput, average latency in the form of tuple. The tuple contains
        the sample metric (string), value (float), unit (string).
  """
  vms = benchmark_spec.vms
  num_vms = len(vms)
  results = []
  for netperf_benchmark in NETPERF_BENCHMARKSS:
    args = []
    metadata = {
        'number_machines': num_vms,
        'number_connections': FLAGS.num_connections
    }

    if netperf_benchmark == 'TCP_STREAM':
      metric = 'TCP_STREAM_Total_Throughput'
      unit = 'Mbits/sec'
      value = 0.0
    else:
      metric = 'TCP_RR_Average_Latency'
      unit = 'ms'
      value = 0.0
    result = [metric, value, unit, metadata]
    args = [((source, netperf_benchmark, vms, result), {}) for source in vms]
    vm_util.RunThreaded(RunNetperf, args, num_vms)
    result = sample.Sample(*result)
    if netperf_benchmark == 'TCP_RR':
      denom = ((num_vms - 1) *
               num_vms *
               FLAGS.num_connections)
      result = result._replace(value=result.value / denom)

    results.append(result)
  logging.info(results)
  return results


def Cleanup(benchmark_spec):
  """Cleanup netperf on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm in vms:
    logging.info('uninstalling netperf on %s', vm)
    vm.RemoteCommand('pkill -9 netserver')
    vm.RemoteCommand('rm netserver')
    vm.RemoteCommand('rm netperf')
