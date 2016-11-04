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

"""Runs the graph analytics benchmark of Cloudsuite.

More info: http://cloudsuite.ch/graphanalytics/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import docker

flags.DEFINE_integer('cloudsuite_graph_analytics_worker_mem',
                     2,
                     'Amount of memory for the worker, in gigabytes')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_graph_analytics'
BENCHMARK_CONFIG = """
cloudsuite_graph_analytics:
  description: >
    Run Cloudsuite graph analytics benchmark. Specify the number of worker
    VMs with --num_vms.
  vm_groups:
    master:
      vm_spec: *default_single_core
      vm_count: 1
    workers:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  """Reads the config file and overwrites vm_count with num_vms."""

  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['num_vms'].present:
    config['vm_groups']['workers']['vm_count'] = FLAGS.num_vms
  return config


def Prepare(benchmark_spec):
  """Install docker.

  Pull the required images from DockerHub, create datasets, and
  start Spark master and workers.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  master = benchmark_spec.vm_groups['master'][0]
  workers = benchmark_spec.vm_groups['workers']

  def PrepareCommon(vm):
    if not docker.IsInstalled(vm):
      vm.Install('docker')
    vm.Install('cloudsuite/spark')
    vm.Install('cloudsuite/twitter-dataset-graph')
    vm.RemoteCommand('sudo docker create --name data '
                     'cloudsuite/twitter-dataset-graph')

  def PrepareMaster(vm):
    PrepareCommon(vm)
    vm.Install('cloudsuite/graph-analytics')
    master_cmd = ('sudo docker run -d --net host -e SPARK_MASTER_IP=%s '
                  '--name spark-master cloudsuite/spark master' %
                  vm.internal_ip)
    vm.RemoteCommand(master_cmd)

  def PrepareWorker(vm):
    PrepareCommon(vm)
    worker_cmd = ('sudo docker run -d --net host --volumes-from data '
                  '--name spark-worker cloudsuite/spark worker '
                  'spark://%s:7077' % master.internal_ip)
    vm.RemoteCommand(worker_cmd)

  target_arg_tuples = ([(PrepareWorker, [vm], {}) for vm in workers] +
                       [(PrepareMaster, [master], {})])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))


def Run(benchmark_spec):
  """Run the graph analytics benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  master = benchmark_spec.vm_groups['master'][0]
  results = []
  memory_option = ('--executor-memory %dg' %
                   (FLAGS.cloudsuite_graph_analytics_worker_mem))

  benchmark_cmd = ('sudo docker run --rm --net host --volumes-from data '
                   'cloudsuite/graph-analytics %s --master spark://%s:7077' %
                   (memory_option, master.internal_ip))
  stdout, _ = master.RemoteCommand(benchmark_cmd, should_log=True)

  matches = re.findall(r'Running time = (\d+)', stdout)
  if len(matches) != 1:
    errors.Benchmarks.RunError('Expected to find benchmark execution '
                               'time')
  execution_time = matches[0]
  results.append(sample.Sample('Benchmark execution time',
                               float(execution_time) / 1000, 'seconds'))

  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  master = benchmark_spec.vm_groups['master'][0]
  workers = benchmark_spec.vm_groups['workers']

  def CleanupCommon(vm):
    vm.RemoteCommand('sudo docker rm -v data')

  def CleanupMaster(vm):
    vm.RemoteCommand('sudo docker stop spark-master')
    vm.RemoteCommand('sudo docker rm spark-master')
    CleanupCommon(vm)

  def CleanupWorker(vm):
    vm.RemoteCommand('sudo docker stop spark-worker')
    vm.RemoteCommand('sudo docker rm spark-worker')
    CleanupCommon(vm)

  target_arg_tuples = ([(CleanupWorker, [vm], {}) for vm in workers] +
                       [(CleanupMaster, [master], {})])
  vm_util.RunParallelThreads(target_arg_tuples, len(target_arg_tuples))
