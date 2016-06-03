# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs the data_serving benchmark of Cloudsuite.

More info: http://cloudsuite.ch/dataserving/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import docker

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_data_serving'
BENCHMARK_CONFIG = """
cloudsuite_data_serving:
  description: >
      Run YCSB client against Cassandra servers.
  vm_groups:
    server_seed:
      vm_spec: *default_single_core
      vm_count: 1
    servers:
      vm_spec: *default_single_core
      vm_count: 1
    client:
      vm_spec: *default_single_core
      vm_count: 1
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepare docker containers and set the dataset up.

  Install docker. Pull the required images from DockerHub.
  Create a table into server-seed and load the dataset.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server_seed = benchmark_spec.vm_groups['server_seed'][0]
  servers = benchmark_spec.vm_groups['servers']
  client = benchmark_spec.vm_groups['client'][0]

  def PrepareCommon(vm):
    if not docker.IsInstalled(vm):
      vm.Install('docker')

  def PrepareServerSeed(vm):
    PrepareCommon(vm)
    vm.RemoteCommand('sudo docker pull cloudsuite/data-serving:server')
    vm.RemoteCommand('sudo docker run -d --name cassandra-server-seed '
                     '--net host cloudsuite/data-serving:server')

  def PrepareServer(vm):
    PrepareCommon(vm)
    vm.RemoteCommand('sudo docker pull cloudsuite/data-serving:server')
    start_server_cmd = ('sudo docker run -d --name cassandra-server '
                        '-e CASSANDRA_SEEDS=%s --net host '
                        'cloudsuite/data-serving:server' %
                        server_seed.internal_ip)
    vm.RemoteCommand(start_server_cmd)

  def PrepareClient(vm):
    PrepareCommon(vm)
    vm.RemoteCommand('sudo docker pull cloudsuite/data-serving:client')

  target_arg_tuples = ([(PrepareServerSeed, [server_seed], {})] +
                       [(PrepareServer, [vm], {}) for vm in servers] +
                       [(PrepareClient, [client], {})])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))


def Run(benchmark_spec):
  """Run the data_serving benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  server_seed = benchmark_spec.vm_groups['server_seed'][0]
  servers = benchmark_spec.vm_groups['servers']
  client = benchmark_spec.vm_groups['client'][0]
  results = []

  server_ips_arr = []
  server_ips_arr.append(server_seed.internal_ip)

  for vm in servers:
    server_ips_arr.append(vm.internal_ip)

  server_ips = ','.join(server_ips_arr)

  benchmark_cmd = ('sudo docker run --rm --name cassandra-client --net host '
                   'cloudsuite/data-serving:client %s' % server_ips)
  stdout, _ = client.RemoteCommand(benchmark_cmd, should_log=True)

  def GetResults(match_str, result_label, result_metric):
    matches = re.findall(match_str, stdout)
    if len(matches) != 1:
      raise errors.Benchmarks.RunError('Expected to find result label: %s' %
                                       result_label)
    results.append(sample.Sample(result_label, float(matches[0]),
                                 result_metric))

  GetResults('\[OVERALL\], RunTime\(ms\), (\d+.?\d*)',
             'OVERALL RunTime', 'ms')
  GetResults('\[OVERALL\], Throughput\(ops\/sec\), (\d+.?\d*)',
             'OVERALL Throughput', 'ops/sec')
  GetResults('\[CLEANUP\], Operations, (\d+.?\d*)',
             'CLEANUP Operations', 'ops')
  GetResults('\[CLEANUP\], AverageLatency\(us\), (\d+.?\d*)',
             'CLEANUP AverageLatency', 'us')
  GetResults('\[CLEANUP\], MinLatency\(us\), (\d+.?\d*)',
             'CLEANUP MinLatency', 'us')
  GetResults('\[CLEANUP\], MaxLatency\(us\), (\d+.?\d*)',
             'CLEANUP MaxLatency', 'us')
  GetResults('\[CLEANUP\], 95thPercentileLatency\(ms\), (\d+.?\d*)',
             'CLEANUP 95thPercentileLatency', 'ms')
  GetResults('\[CLEANUP\], 99thPercentileLatency\(ms\), (\d+.?\d*)',
             'CLEANUP 99thPercentileLatency', 'ms')
  GetResults('\[READ\], Operations, (\d+.?\d*)',
             'READ Operations', 'ops')
  GetResults('\[READ\], AverageLatency\(us\), (\d+.?\d*)',
             'READ AverageLatency', 'us')
  GetResults('\[READ\], MinLatency\(us\), (\d+.?\d*)',
             'READ MinLatency', 'us')
  GetResults('\[READ\], MaxLatency\(us\), (\d+.?\d*)',
             'READ MaxLatency', 'us')
  GetResults('\[READ\], 95thPercentileLatency\(ms\), (\d+.?\d*)',
             'READ 95thPercentileLatency', 'ms')
  GetResults('\[READ\], 99thPercentileLatency\(ms\), (\d+.?\d*)',
             'READ 99thPercentileLatency', 'ms')
  GetResults('\[UPDATE\], Operations, (\d+.?\d*)',
             'UPDATE Operations', 'us')
  GetResults('\[UPDATE\], AverageLatency\(us\), (\d+.?\d*)',
             'UPDATE AverageLatency', 'us')
  GetResults('\[UPDATE\], MinLatency\(us\), (\d+.?\d*)',
             'UPDATE MinLatency', 'us')
  GetResults('\[UPDATE\], MaxLatency\(us\), (\d+.?\d*)',
             'UPDATE MaxLatency', 'us')
  GetResults('\[UPDATE\], 95thPercentileLatency\(ms\), (\d+.?\d*)',
             'UPDATE 95thPercentileLatency', 'ms')
  GetResults('\[UPDATE\], 99thPercentileLatency\(ms\), (\d+.?\d*)',
             'UPDATE 99thPercentileLatency', 'ms')

  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server_seed = benchmark_spec.vm_groups['server_seed'][0]
  servers = benchmark_spec.vm_groups['servers']
  client = benchmark_spec.vm_groups['client'][0]

  def CleanupServerCommon(vm):
    vm.RemoteCommand('sudo docker rm cassandra-server')
    vm.RemoteCommand('sudo docker rmi cloudsuite/data-serving:server')

  def CleanupServerSeed(vm):
    vm.RemoteCommand('sudo docker stop cassandra-server-seed')
    CleanupServerCommon(vm)

  def CleanupServer(vm):
    vm.RemoteCommand('sudo docker stop cassandra-server')
    CleanupServerCommon(vm)

  def CleanupClient(vm):
    vm.RemoteCommand('sudo docker rmi cloudsuite/data-serving:client')

  target_arg_tuples = ([(CleanupServerSeed, [server_seed], {})] +
                       [(CleanupServer, [vm], {}) for vm in servers] +
                       [(CleanupClient, [client], {})])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))
