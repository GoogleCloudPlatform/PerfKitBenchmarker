# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs the Web Search benchmark of Cloudsuite.

More info: http://cloudsuite.ch/websearch/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import docker

FLAGS = flags.FLAGS

flags.DEFINE_string('cloudsuite_web_search_server_heap_size',
                    '3g',
                    'Java heap size for Solr server in the usual java format.')
flags.DEFINE_integer('cloudsuite_web_search_ramp_up',
                     90,
                     'Benchmark ramp up time in seconds.',
                     lower_bound=1)
flags.DEFINE_integer('cloudsuite_web_search_ramp_down',
                     60,
                     'Benchmark ramp down time in seconds.',
                     lower_bound=1)
flags.DEFINE_integer('cloudsuite_web_search_steady_state',
                     60,
                     'Benchmark steady state time in seconds.',
                     lower_bound=1)
flags.DEFINE_integer('cloudsuite_web_search_scale',
                     50,
                     'Number of simulated web search users.',
                     lower_bound=1)

BENCHMARK_NAME = 'cloudsuite_web_search'
BENCHMARK_CONFIG = """
cloudsuite_web_search:
  description: >
    Run Cloudsuite Web Search benchmark. Specify the number of
    clients with --num_vms.
  vm_groups:
    servers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    clients:
      vm_spec: *default_single_core
      vm_count: 1
"""

DISK_PATH = '/scratch'


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['num_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.num_vms
  return config


def Prepare(benchmark_spec):
  """Install docker. Pull the required images from DockerHub.

  Start Solr index node and client.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['servers'][0]
  clients = benchmark_spec.vm_groups['clients']

  def PrepareCommon(vm):
    if not docker.IsInstalled(vm):
      vm.Install('docker')

  def PrepareServer(vm):
    PrepareCommon(vm)
    server_cmd = ('sudo echo \'DOCKER_OPTS="-g %s"\''
                  '| sudo tee /etc/default/docker > /dev/null' % (DISK_PATH))
    stdout, _ = vm.RemoteCommand(server_cmd, should_log=True)

    server_cmd = 'sudo service docker restart'
    stdout, _ = vm.RemoteCommand(server_cmd, should_log=True)

    vm.Install('cloudsuite/web-search:server')

    server_cmd = ('sudo docker run -d --net host '
                  '--name server cloudsuite/web-search:server %s 1' %
                  (FLAGS.cloudsuite_web_search_server_heap_size))

    stdout, _ = servers.RemoteCommand(server_cmd, should_log=True)

  def PrepareClient(vm):
    PrepareCommon(vm)
    vm.Install('cloudsuite/web-search:client')

  PrepareServer(servers)

  target_arg_tuples = ([(PrepareClient, [vm], {}) for vm in clients])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))


def Run(benchmark_spec):
  """Run the Web Search benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  clients = benchmark_spec.vm_groups['clients'][0]
  servers = benchmark_spec.vm_groups['servers'][0]

  benchmark_cmd = ('sudo docker run --rm --net host --name client '
                   'cloudsuite/web-search:client %s %d %d %d %d ' %
                   (servers.internal_ip,
                    FLAGS.cloudsuite_web_search_scale,
                    FLAGS.cloudsuite_web_search_ramp_up,
                    FLAGS.cloudsuite_web_search_steady_state,
                    FLAGS.cloudsuite_web_search_ramp_down))
  stdout, _ = clients.RemoteCommand(benchmark_cmd, should_log=True)

  ops_per_sec = re.findall(r'\<metric unit="ops/sec"\>(\d+\.?\d*)', stdout)
  num_ops_per_sec = float(ops_per_sec[0])
  p90 = re.findall(r'\<p90th\>(\d+\.?\d*)', stdout)
  num_p90 = float(p90[0])
  p99 = re.findall(r'\<p99th\>(\d+\.?\d*)', stdout)
  num_p99 = float(p99[0])

  results = []
  results.append(sample.Sample('Operations per second', num_ops_per_sec,
                               'ops/s'))
  results.append(sample.Sample('90th percentile latency', num_p90, 's'))
  results.append(sample.Sample('99th percentile latency', num_p99, 's'))
  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['servers'][0]
  clients = benchmark_spec.vm_groups['clients']

  def CleanupClient(vm):
    vm.RemoteCommand('sudo docker stop client')
    vm.RemoteCommand('sudo docker rm client')

  def CleanupServer(vm):
    vm.RemoteCommand('sudo docker stop server')
    vm.RemoteCommand('sudo docker rm server')

  target_arg_tuples = ([(CleanupClient, [vm], {}) for vm in clients] +
                       [(CleanupServer, [servers], {})])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))
