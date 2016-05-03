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
import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

flags.DEFINE_string('cs_websearch_client_heap_size',
                    '2g',
                    'Java heap size for Faban client in the usual java format.'
                    ' Default: 2g.')

flags.DEFINE_string('cs_websearch_server_heap_size',
                    '3g',
                    'Java heap size for Solr server in the usual java format.'
                    ' Default: 3g.')
flags.DEFINE_enum('cs_websearch_query_distr',
                  'Random',
                  ['Random', 'Ziphian'],
                  'Distribution of query terms. '
                  'Random and Ziphian distributions are available. '
                  'Default: Random.')
flags.DEFINE_integer('cs_websearch_num_clients',
                     1,
                     'Number of client machines.',
                     lower_bound=1)
flags.DEFINE_integer('cs_websearch_ramp_up',
                     90,
                     'Benchmark ramp up time in seconds.',
                     lower_bound=1)
flags.DEFINE_integer('cs_websearch_ramp_down',
                     60,
                     'Benchmark ramp down time in seconds.',
                     lower_bound=1)
flags.DEFINE_integer('cs_websearch_steady_state',
                     60,
                     'Benchmark steady state time in seconds.',
                     lower_bound=1)
flags.DEFINE_integer('cs_websearch_scale',
                     50,
                     'Number of simulated web search users.',
                     lower_bound=1)

BENCHMARK_NAME = 'cloudsuite_websearch'
BENCHMARK_CONFIG = """
cloudsuite_websearch:
  description: >
    Run Cloudsuite Web Search benchmark. Specify the number of worker
    VMs with --num_vms.
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


def _HasDocker(vm):
  resp, _ = vm.RemoteCommand('command -v docker',
                             ignore_failure=True,
                             suppress_warning=True)
  if resp.rstrip() == "":
    return False
  else:
    return True


def Prepare(benchmark_spec):
  """Install docker. Pull the required images from DockerHub.
  Start Solr index node and client.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  servers = benchmark_spec.vm_groups['servers'][0]
  clients = benchmark_spec.vm_groups['clients']

  for vm in vms:
    if not _HasDocker(vm):
      vm.Install('docker')

  server_cmd = ('sudo echo \'DOCKER_OPTS="-g %s"\''
                '| sudo tee /etc/default/docker > /dev/null' % (DISK_PATH))
  stdout, _ = servers.RemoteCommand(server_cmd, should_log=True)

  server_cmd = ('sudo service docker restart')
  stdout, _ = servers.RemoteCommand(server_cmd, should_log=True)

  for vm in clients:
    vm.RemoteCommand('sudo docker pull cloudsuite/web-search:client')

  servers.RemoteCommand('sudo docker pull cloudsuite/web-search:server')

  server_cmd = ('sudo docker run -d --net host '
                '--name server cloudsuite/web-search:server %s 1' %
                (FLAGS.cs_websearch_server_heap_size))
  stdout, _ = servers.RemoteCommand(server_cmd, should_log=True)

  time.sleep(60)


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
  results = []

  benchmark_cmd = ('sudo docker run --rm --net host --name client '
                   'cloudsuite/web-search:client %s %d %d %d %d ' %
                   (servers.internal_ip,
                    FLAGS.cs_websearch_scale,
                    FLAGS.cs_websearch_ramp_up,
                    FLAGS.cs_websearch_steady_state,
                    FLAGS.cs_websearch_ramp_down))
  stdout, _ = clients.RemoteCommand(benchmark_cmd, should_log=True)

  ops_per_sec = re.findall(r'\<metric unit="ops/sec"\>(\d+\.?\d*)', stdout)
  sum_ops_per_sec = 0.0
  for value in ops_per_sec:
     sum_ops_per_sec += float(value)
  p90 = re.findall(r'\<p90th\>(\d+\.?\d*)', stdout)
  sum_p90 = 0.0
  for value in p90:
     sum_p90 += float(value)
  p99 = re.findall(r'\<p99th\>(\d+\.?\d*)', stdout)
  sum_p99 = 0.0
  for value in p99:
     sum_p99 += float(value)

  results = []
  results.append(sample.Sample('Operations per second', sum_ops_per_sec,
                               'ops/s'))
  results.append(sample.Sample('90th percentile latency', sum_p90, 's'))
  results.append(sample.Sample('99th percentile latency', sum_p99, 's'))
  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['servers'][0]
  clients = benchmark_spec.vm_groups['clients']

  for vm in clients:
    vm.RemoteCommand('sudo docker stop client')
    vm.RemoteCommand('sudo docker rm client')

  servers.RemoteCommand('sudo docker stop server')
  servers.RemoteCommand('sudo docker rm server')

  for vm in clients:
    vm.RemoteCommand('sudo docker rmi cloudsuite/web-search:client')

  servers.RemoteCommand('sudo docker rmi cloudsuite/web-search:server')
