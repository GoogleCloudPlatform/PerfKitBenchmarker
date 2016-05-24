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

"""Runs the media streaming benchmark of Cloudsuite.

More info: http://cloudsuite.ch/mediastreaming/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import docker

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_media_streaming'
BENCHMARK_CONFIG = """
cloudsuite_media_streaming:
  description: >
    Run Cloudsuite media streaming benchmark.
  vm_groups:
    server:
      vm_spec: *default_single_core
      vm_count: 1
    client:
      vm_spec: *default_single_core
      vm_count: 1
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install docker. Pull images. Create datasets. Start Nginx server.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server = benchmark_spec.vm_groups['server'][0]
  client = benchmark_spec.vm_groups['client'][0]

  def PrepareCommon(vm):
    if not docker.IsInstalled(vm):
      vm.Install('docker')
    vm.RemoteCommand('sudo docker pull cloudsuite/media-streaming:dataset')
    vm.RemoteCommand('sudo docker create --name dataset '
                     'cloudsuite/media-streaming:dataset')

  def PrepareServer(vm):
    PrepareCommon(vm)
    vm.RemoteCommand('sudo docker pull cloudsuite/media-streaming:server')
    vm.RemoteCommand('sudo docker run -d --name server --net host '
                     '--volumes-from dataset '
                     'cloudsuite/media-streaming:server')

  def PrepareClient(vm):
    PrepareCommon(vm)
    vm.RemoteCommand('sudo docker pull cloudsuite/media-streaming:client')

  target_arg_tuples = [(PrepareServer, [server], {}),
                       (PrepareClient, [client], {})]
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))


def Run(benchmark_spec):
  """Run the media streaming benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  server = benchmark_spec.vm_groups['server'][0]
  client = benchmark_spec.vm_groups['client'][0]
  results = []

  stdout, _ = client.RemoteCommand('sudo docker run --rm --name client '
                                   '--net host --volumes-from dataset '
                                   'cloudsuite/media-streaming:client %s'
                                   % server.internal_ip)

  match = re.search(r'^Requests: (.+)$', stdout, re.MULTILINE)
  if match:
    results.append(sample.Sample('Requests', float(match.group(1)), ''))
  match = re.search(r'^Replies: (.+)$', stdout, re.MULTILINE)
  if match:
    results.append(sample.Sample('Replies', float(match.group(1)), ''))
  match = re.search(r'^Reply rate: (.+)$', stdout, re.MULTILINE)
  if match:
    results.append(sample.Sample('Reply rate', float(match.group(1)),
                                 'replies/s'))
  match = re.search(r'^Reply time: (.+)$', stdout, re.MULTILINE)
  if match:
    results.append(sample.Sample('Reply time', float(match.group(1)), 'ms'))
  match = re.search(r'^Net I/O: (.+)$', stdout, re.MULTILINE)
  if match:
    results.append(sample.Sample('Net I/O', float(match.group(1)), 'KB/s'))
  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server = benchmark_spec.vm_groups['server'][0]
  client = benchmark_spec.vm_groups['client'][0]

  def CleanupCommon(vm):
    vm.RemoteCommand('sudo docker rm -v dataset')
    vm.RemoteCommand('sudo docker rmi cloudsuite/media-streaming:dataset')

  def CleanupServer(vm):
    server.RemoteCommand('sudo docker stop server')
    server.RemoteCommand('sudo docker rm server')
    server.RemoteCommand('sudo docker rmi cloudsuite/media-streaming:server')
    CleanupCommon(vm)

  def CleanupClient(vm):
    client.RemoteCommand('sudo docker rmi cloudsuite/media-streaming:client')
    CleanupCommon(vm)

  target_arg_tuples = [(CleanupServer, [server], {}),
                       (CleanupClient, [client], {})]
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))
