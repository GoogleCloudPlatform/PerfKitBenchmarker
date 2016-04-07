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

"""Runs the in-memory analytics benchmark of Cloudsuite.

More info: http://cloudsuite.ch/inmemoryanalytics/
"""

import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

flags.DEFINE_string('cloudsuite_in_memory_analytics_dataset',
                    '/data/ml-latest-small',
                    'Dataset to use for training.')
flags.DEFINE_string('cloudsuite_in_memory_analytics_ratings_file',
                    '/data/myratings.csv',
                    'Ratings file to give the recommendation for.')
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_in_memory_analytics'
BENCHMARK_CONFIG = """
cloudsuite_in_memory_analytics:
  description: >
    Run Cloudsuite in-memory analytics benchmark. Specify the number of worker
    VMs with --num_vms.
  vm_groups:
    master:
      vm_spec: *default_single_core
      vm_count: 1
    workers:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['num_vms'].present:
    config['vm_groups']['workers']['vm_count'] = FLAGS.num_vms
  return config


def _HasDocker(vm):
  resp, _ = vm.RemoteCommand('command -v docker',
                             ignore_failure=True,
                             suppress_warning=True)
  return bool(resp.rstrip())


def Prepare(benchmark_spec):
  """Install docker. Pull the required images from DockerHub. Create datasets.
  Start Spark master and workers.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master = benchmark_spec.vm_groups['master'][0]
  workers = benchmark_spec.vm_groups['workers']

  for vm in vms:
    if not _HasDocker(vm):
      vm.Install('docker')
    vm.RemoteCommand('sudo docker pull cloudsuite/spark')
    vm.RemoteCommand('sudo docker pull cloudsuite/movielens-dataset')

  master.RemoteCommand('sudo docker pull cloudsuite/in-memory-analytics')

  for vm in vms:
    vm.RemoteCommand('sudo docker create --name data '
                     'cloudsuite/movielens-dataset')

  master_cmd = ('sudo docker run -d --net host -e SPARK_MASTER_IP=%s '
                '--name spark-master cloudsuite/spark master' %
                master.internal_ip)
  master.RemoteCommand(master_cmd)

  worker_cmd = ('sudo docker run -d --net host --volumes-from data '
                '--name spark-worker cloudsuite/spark worker '
                'spark://%s:7077' % master.internal_ip)
  for vm in workers:
    vm.RemoteCommand(worker_cmd)


def Run(benchmark_spec):
  """Run the in-memory analytics benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  master = benchmark_spec.vm_groups['master'][0]
  results = []

  benchmark_cmd = ('sudo docker run --rm --net host --volumes-from data '
                   'cloudsuite/in-memory-analytics %s %s '
                   '--master spark://%s:7077' %
                   (FLAGS.cloudsuite_in_memory_analytics_dataset,
                    FLAGS.cloudsuite_in_memory_analytics_ratings_file,
                    master.internal_ip))
  stdout, _ = master.RemoteCommand(benchmark_cmd, should_log=True)

  execution_time = re.findall(r'Benchmark execution time: (\d+)ms', stdout)[0]
  results.append(sample.Sample('Benchmark execution time',
                               float(execution_time) / 1000, 'seconds'))

  return results


def Cleanup(benchmark_spec):
  """Stop and remove docker containers. Remove images.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master = benchmark_spec.vm_groups['master'][0]
  workers = benchmark_spec.vm_groups['workers']

  for vm in workers:
    vm.RemoteCommand('sudo docker stop spark-worker')
    vm.RemoteCommand('sudo docker rm spark-worker')

  master.RemoteCommand('sudo docker stop spark-master')
  master.RemoteCommand('sudo docker rm spark-master')

  for vm in vms:
    vm.RemoteCommand('sudo docker rm -v data')
    vm.RemoteCommand('sudo docker rmi cloudsuite/movielens-dataset')
    vm.RemoteCommand('sudo docker rmi cloudsuite/spark')

  master.RemoteCommand('sudo docker rmi cloudsuite/in-memory-analytics')
