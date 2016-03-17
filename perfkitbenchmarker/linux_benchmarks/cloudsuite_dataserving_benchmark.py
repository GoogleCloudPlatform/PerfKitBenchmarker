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

"""Runs the dataserving benchmark of Cloudsuite.

More info: http://cloudsuite.ch/dataserving/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_dataserving'
BENCHMARK_CONFIG = """
cloudsuite_dataserving:
  description: >
      Run YCSB against Cassandra. Specify the
      Cassandra cluster size with --num_vms. Specify the number
      of YCSB VMs with --ycsb_client_vms.
  vm_groups:
    servers:
      vm_spec: *default_single_core
      vm_count: 1
    clients:
      vm_spec: *default_single_core
      vm_count: 1
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
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
        Create a table and load the dataset.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['servers']
  clients = benchmark_spec.vm_groups['clients']

  for vm in servers:
    if not _HasDocker(vm):
      vm.Install('docker')
    vm.RemoteCommand('sudo docker pull cloudsuite/data-serving:server')
    vm.RemoteCommand('sudo docker run -d --name cassandra-server --net host '
                     'cloudsuite/data-serving:server')

  for vm in clients:
    if not _HasDocker(vm):
      vm.Install('docker')
    vm.RemoteCommand('sudo docker pull cloudsuite/data-serving:client')


def Run(benchmark_spec):
  """Run the dataserving benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  client = benchmark_spec.vm_groups['clients'][0]
  server = benchmark_spec.vm_groups['servers'][0]
  results = []

  benchmark_cmd = ('sudo docker run --rm --name cassandra-client --net host '
                   'cloudsuite/data-serving:client %s' % (server.internal_ip))
  stdout, _ = client.RemoteCommand(benchmark_cmd, should_log=True)

  overall_execution_time = re.findall('\[OVERALL\], RunTime\(ms\), (\d+.?\d*)',
                                      stdout)[0]
  results.append(sample.Sample('OVERALL RunTime',
                               float(overall_execution_time), 'ms'))

  overall_throughput = re.findall('\[OVERALL\], Throughput\(ops\/sec\)'
                                  ', (\d+.?\d*)',
                                  stdout)[0]
  results.append(sample.Sample('OVERALL Throughput',
                               float(overall_throughput), 'ops/sec'))


  cleanup_ops = re.findall('\[CLEANUP\], Operations, (\d+.?\d*)',
                           stdout)[0]
  results.append(sample.Sample('CLEANUP Operations',
                               float(cleanup_ops), 'ops'))

  cleanup_ave_lat = re.findall('\[CLEANUP\], AverageLatency\(us\), (\d+.?\d*)',
                               stdout)[0]
  results.append(sample.Sample('CLEANUP AverageLatency',
                               float(cleanup_ave_lat), 'us'))

  cleanup_min_lat = re.findall('\[CLEANUP\], MinLatency\(us\), (\d+.?\d*)',
                               stdout)[0]
  results.append(sample.Sample('CLEANUP MinLatency',
                               float(cleanup_min_lat), 'us'))

  cleanup_max_lat = re.findall('\[CLEANUP\], MaxLatency\(us\), (\d+.?\d*)',
                               stdout)[0]
  results.append(sample.Sample('CLEANUP MaxLatency',
                               float(cleanup_max_lat), 'us'))

  cleanup_95th_lat = re.findall('\[CLEANUP\], 95thPercentileLatency\(ms\)'
                                ', (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('CLEANUP 95thPercentileLatency',
                               float(cleanup_95th_lat), 'ms'))

  cleanup_99th_lat = re.findall('\[CLEANUP\], 99thPercentileLatency\(ms\)'
                                ', (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('CLEANUP 99thPercentileLatency',
                               float(cleanup_99th_lat), 'ms'))


  read_ops = re.findall('\[READ\], Operations, (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('READ Operations', float(read_ops), 'ops'))

  read_ave_lat = re.findall('\[READ\], AverageLatency\(us\), (\d+.?\d*)',
                            stdout)[0]
  results.append(sample.Sample('READ AverageLatency',
                               float(read_ave_lat), 'us'))

  read_min_lat = re.findall('\[READ\], MinLatency\(us\), (\d+.?\d*)',
                            stdout)[0]
  results.append(sample.Sample('READ MinLatency',
                               float(read_min_lat), 'us'))

  read_max_lat = re.findall('\[READ\], MaxLatency\(us\), (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('READ MaxLatency', float(read_max_lat), 'us'))

  read_95th_lat = re.findall('\[READ\], 95thPercentileLatency\(ms\)'
                             ', (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('READ 95thPercentileLatency',
                               float(read_95th_lat), 'ms'))

  read_99th_lat = re.findall('\[READ\], 99thPercentileLatency\(ms\)'
                             ', (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('READ 99thPercentileLatency',
                               float(read_99th_lat), 'ms'))


  update_ops = re.findall('\[UPDATE\], Operations, (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('UPDATE Operations', float(update_ops), 'us'))

  update_ave_lat = re.findall('\[UPDATE\], AverageLatency\(us\), (\d+.?\d*)',
                              stdout)[0]
  results.append(sample.Sample('UPDATE AverageLatency',
                 float(update_ave_lat), 'us'))

  update_min_lat = re.findall('\[UPDATE\], MinLatency\(us\), (\d+.?\d*)',
                              stdout)[0]
  results.append(sample.Sample('UPDATE MinLatency',
                               float(update_min_lat), 'us'))

  update_max_lat = re.findall('\[UPDATE\], MaxLatency\(us\)'
                              ', (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('UPDATE MaxLatency',
                               float(update_max_lat), 'us'))

  update_95th_lat = re.findall('\[UPDATE\], 95thPercentileLatency\(ms\)'
                               ', (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('UPDATE 95thPercentileLatency',
                               float(update_95th_lat), 'ms'))

  update_99th_lat = re.findall('\[UPDATE\], 99thPercentileLatency\(ms\)'
                               ', (\d+.?\d*)', stdout)[0]
  results.append(sample.Sample('UPDATE 99thPercentileLatency',
                               float(update_99th_lat), 'ms'))

  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['servers']
  clients = benchmark_spec.vm_groups['clients']

  for vm in servers:
    vm.RemoteCommand('sudo docker stop cassandra-server')
    vm.RemoteCommand('sudo docker rm cassandra-server')
    vm.RemoteCommand('sudo docker rmi cloudsuite/data-serving:server')

  for vm in clients:
    vm.RemoteCommand('sudo docker rmi cloudsuite/data-serving:client')
