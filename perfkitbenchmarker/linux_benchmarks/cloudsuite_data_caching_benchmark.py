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

"""Runs the data caching benchmark of Cloudsuite 3.0.

More info: http://cloudsuite.ch/datacaching
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import docker

flags.DEFINE_string('cloudsuite_data_caching_memcached_flags',
                    '-t 1 -m 2048 -n 550',
                    'Flags to be given to memcached.')
flags.DEFINE_integer('cloudsuite_data_caching_rps',
                     18000,
                     'Number of requests per second.')
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_data_caching'
BENCHMARK_CONFIG = """
cloudsuite_data_caching:
  description: >
    Runs Cloudsuite3.0 Data Caching benchmark.
  vm_groups:
    server:
      vm_spec: *default_single_core
    client:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install docker. Pull the required images from DockerHub. Create datasets.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server_vm = benchmark_spec.vm_groups['server'][0]
  client_vm = benchmark_spec.vm_groups['client'][0]

  # Make sure docker is installed on all VMs.
  for vm in (server_vm, client_vm):
    if not docker.IsInstalled(vm):
      vm.Install('docker')

  # Prepare and start the server VM.
  server_vm.Install('cloudsuite/data-caching:server')
  server_vm.RemoteCommand("echo '%s    dc-client' | sudo tee -a /etc/hosts >"
                          " /dev/null" % client_vm.internal_ip)
  server_vm.RemoteCommand('sudo docker run --name dc-server --net host -d '
                          'cloudsuite/data-caching:server %s' %
                          FLAGS.cloudsuite_data_caching_memcached_flags)

  # Prepare the client.
  client_vm.Install('cloudsuite/data-caching:client')
  client_vm.RemoteCommand("echo '%s    dc-server' | sudo tee -a /etc/hosts >"
                          " /dev/null" % server_vm.internal_ip)


def _ParseOutput(output_str):
    numbers = [float(f) for f in re.findall(r"([-+]?\d*\.\d+|\d+)",
               " ".join(output_str.splitlines(1)[-4:]))]

    results = []
    results.append(sample.Sample("Requests per second",
                                 numbers[1], "req/s"))
    results.append(sample.Sample("Average latency",
                                 numbers[7], "ms"))
    results.append(sample.Sample("90th percentile latency",
                                 numbers[8], "ms"))
    results.append(sample.Sample("95th percentile latency",
                                 numbers[9], "ms"))
    results.append(sample.Sample("99th percentile latency",
                                 numbers[10], "ms"))

    req_rems = numbers[15:-1]

    results.append(sample.Sample("Average outstanding requests per requester",
                                 sum(req_rems) / len(req_rems), "reqs"))

    return results


def Run(benchmark_spec):
  """Run the data-caching benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  client_vm = benchmark_spec.vm_groups['client'][0]

  benchmark_cmd = ('sudo docker run --rm --name dc-client --net host'
                   ' cloudsuite/data-caching:client -rps %d' %
                   FLAGS.cloudsuite_data_caching_rps)

  stdout, _ = client_vm.RemoteCommand(benchmark_cmd, should_log=True)

  return _ParseOutput(stdout)


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server_vm = benchmark_spec.vm_groups['server'][0]
  client_vm = benchmark_spec.vm_groups['client'][0]

  server_vm.RemoteCommand('sudo docker stop dc-server')
  server_vm.RemoteCommand('sudo docker rm dc-server')
  client_vm.RemoteCommand('sudo docker stop dc-client')
  client_vm.RemoteCommand('sudo docker rm dc-client')
