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

"""Runs the web serving benchmark of Cloudsuite.

More info: http://cloudsuite.ch/webserving/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import docker

flags.DEFINE_integer('cloudsuite_web_serving_pm_max_children', 150,
                     'The maximum number php-fpm pm children.', lower_bound=8)

flags.DEFINE_integer('cloudsuite_web_serving_load_scale', 100,
                     'The maximum number of concurrent users '
                     'that can be simulated.', lower_bound=2)
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_web_serving'
BENCHMARK_CONFIG = """
cloudsuite_web_serving:
  description: >
    Run Cloudsuite web serving benchmark.
  vm_groups:
    backend:
      vm_spec: *default_single_core
      vm_count: 1
    frontend:
      vm_spec: *default_single_core
      vm_count: 1
    client:
      vm_spec: *default_single_core
      vm_count: 1
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.
  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if FLAGS['num_vms'].present and FLAGS.num_vms < 3:
    raise ValueError('Web Serving requires at least 3 VMs')


def Prepare(benchmark_spec):
  """Install docker. Pull images. Start nginx, mysql, and memcached.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  frontend = benchmark_spec.vm_groups['frontend'][0]
  backend = benchmark_spec.vm_groups['backend'][0]
  client = benchmark_spec.vm_groups['client'][0]

  def PrepareCommon(vm):
    if not docker.IsInstalled(vm):
      vm.Install('docker')
    vm.RemoteCommand("sudo sh -c 'echo %s web_server >>/etc/hosts'" %
                     frontend.internal_ip)
    vm.RemoteCommand("sudo sh -c 'echo %s memcache_server >>/etc/hosts'" %
                     frontend.internal_ip)
    vm.RemoteCommand("sudo sh -c 'echo %s mysql_server >>/etc/hosts'" %
                     backend.internal_ip)
    vm.RemoteCommand("sudo sh -c 'echo %s faban_client >>/etc/hosts'" %
                     client.internal_ip)

  def PrepareFrontend(vm):
    PrepareCommon(vm)
    vm.Install('cloudsuite/web-serving:web_server')
    vm.Install('cloudsuite/web-serving:memcached_server')
    vm.RemoteCommand('sudo docker run -dt --net host --name web_server '
                     'cloudsuite/web-serving:web_server '
                     '/etc/bootstrap.sh mysql_server memcache_server %s' %
                     (FLAGS.cloudsuite_web_serving_pm_max_children))
    vm.RemoteCommand('sudo docker run -dt --net host --name memcache_server '
                     'cloudsuite/web-serving:memcached_server')

  def PrepareBackend(vm):
    PrepareCommon(vm)
    vm.Install('cloudsuite/web-serving:db_server')
    vm.RemoteCommand('sudo docker run -dt --net host --name mysql_server '
                     'cloudsuite/web-serving:db_server web_server')

  def PrepareClient(vm):
    PrepareCommon(vm)
    vm.Install('cloudsuite/web-serving:faban_client')

  target_arg_tuples = [(PrepareFrontend, [frontend], {}),
                       (PrepareBackend, [backend], {}),
                       (PrepareClient, [client], {})]
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))


def Run(benchmark_spec):
  """Run the web serving benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  frontend = benchmark_spec.vm_groups['frontend'][0]
  client = benchmark_spec.vm_groups['client'][0]
  results = []

  cmd = ('sudo docker run --net host --name faban_client '
         'cloudsuite/web-serving:faban_client %s %s' %
         (frontend.internal_ip, FLAGS.cloudsuite_web_serving_load_scale))
  stdout, _ = client.RemoteCommand(cmd)

  # The output contains a faban summary xml.
  # Example: http://faban.org/1.1/docs/guide/driver/samplesummary_xml.html
  match = re.search(r'\<metric unit="ops/sec"\>(.+)\</metric\>',
                    stdout, re.MULTILINE)
  if match:
    results.append(sample.Sample('Throughput', float(match.group(1)), 'ops/s'))

  matches = re.findall(r'\<avg\>(.+)\</avg\>', stdout, re.MULTILINE)
  if len(matches) > 0:
    sum_avg = 0.0
    for m in matches:
      sum_avg += float(m)
    avg_avg = 1000 * sum_avg / len(matches)
    results.append(sample.Sample('Average response time', avg_avg, 'ms'))

  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  frontend = benchmark_spec.vm_groups['frontend'][0]
  backend = benchmark_spec.vm_groups['backend'][0]
  client = benchmark_spec.vm_groups['client'][0]

  def CleanupFrontend(vm):
    vm.RemoteCommand('sudo docker stop memcache_server')
    vm.RemoteCommand('sudo docker rm memcache_server')
    vm.RemoteCommand('sudo docker stop web_server')
    vm.RemoteCommand('sudo docker rm web_server')

  def CleanupBackend(vm):
    vm.RemoteCommand('sudo docker stop mysql_server')
    vm.RemoteCommand('sudo docker rm mysql_server')

  def CleanupClient(vm):
    vm.RemoteCommand('sudo docker rm faban_client')

  target_arg_tuples = [(CleanupFrontend, [frontend], {}),
                       (CleanupBackend, [backend], {}),
                       (CleanupClient, [client], {})]
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))
