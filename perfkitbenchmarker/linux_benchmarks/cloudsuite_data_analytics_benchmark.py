# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs the data analytics benchmark of Cloudsuite.

More info: http://cloudsuite.ch/dataanalytics/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import docker

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_data_analytics'
BENCHMARK_CONFIG = """
cloudsuite_data_analytics:
  description: >
    Run Cloudsuite data analytics benchmark. Specify the number of slave VMs
    with --num_vms.
  vm_groups:
    master:
      vm_spec: *default_single_core
      vm_count: 1
    slaves:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['num_vms'].present:
    config['vm_groups']['slaves']['vm_count'] = FLAGS.num_vms
  return config


def Prepare(benchmark_spec):
  """Install docker. Pull images. Start the master and slaves.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
    required to run the benchmark.
  """
  master = benchmark_spec.vm_groups['master'][0]
  slaves = benchmark_spec.vm_groups['slaves']

  def PrepareCommon(vm):
    if not docker.IsInstalled(vm):
      vm.Install('docker')

  def PrepareMaster(vm):
    PrepareCommon(vm)
    vm.RemoteCommand('sudo docker pull cloudsuite/data-analytics')
    vm.RemoteCommand('sudo docker run -d --name master --net host '
                     'cloudsuite/data-analytics master')

  def PrepareSlave(vm):
    PrepareCommon(vm)
    vm.RemoteCommand('sudo docker pull cloudsuite/hadoop')
    vm.RemoteCommand('sudo docker run -d --name slave --net host '
                     'cloudsuite/hadoop slave %s' % master.internal_ip)

  target_arg_tuples = ([(PrepareSlave, [vm], {}) for vm in slaves] +
                       [(PrepareMaster, [master], {})])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))


def Run(benchmark_spec):
  """Run the data analytics benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
    required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  master = benchmark_spec.vm_groups['master'][0]
  results = []

  stdout, _ = master.RemoteCommand('sudo docker exec master benchmark 2>&1',
                                   should_log=True)

  matches = re.findall(r'^Benchmark time: (\d+)ms$', stdout, re.MULTILINE)
  if len(matches) != 1:
    raise errors.Benchmark.RunError('Expected to find benchmark runtime')

  results.append(sample.Sample('Benchmark runtime', float(matches[0]) / 1000,
                               'seconds'))
  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
    required to run the benchmark.
  """
  master = benchmark_spec.vm_groups['master'][0]
  slaves = benchmark_spec.vm_groups['slaves']

  def CleanupMaster(vm):
    vm.RemoteCommand('sudo docker stop master')
    vm.RemoteCommand('sudo docker rm master')
    vm.RemoteCommand('sudo docker rmi cloudsuite/data-analytics')

  def CleanupSlave(vm):
    vm.RemoteCommand('sudo docker stop slave')
    vm.RemoteCommand('sudo docker rm slave')
    vm.RemoteCommand('sudo docker rmi cloudsuite/hadoop')

  target_arg_tuples = ([(CleanupSlave, [vm], {}) for vm in slaves] +
                       [(CleanupMaster, [master], {})])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))
