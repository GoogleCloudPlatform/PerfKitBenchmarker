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
"""Run YCSB against MongoDB.

YCSB is a load generator for many 'cloud' databases. MongoDB is a NoSQL
database.

MongoDB homepage: http://www.mongodb.org/
YCSB homepage: https://github.com/brianfrankcooper/YCSB/wiki
"""

import functools
import random
import string

import time
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import mongodb_ycsb_benchmark
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS

flags.DEFINE_string('kubernetes_mongodb_cpu_request', '7.1',
                    'CPU request of mongodb.')
flags.DEFINE_string('kubernetes_mongodb_memory_request', '16Gi',
                    'Memory request of mongodb.')
flags.DEFINE_string('kubernetes_mongodb_cpu_limit', '7.6',
                    'CPU limit of mongodb, should be bigger than CPU request')
flags.DEFINE_string(
    'kubernetes_mongodb_memory_limit', '32Gi',
    'Memory limit of mongodb, should be bigger than memory request')
flags.DEFINE_string('kubernetes_mongodb_disk_size', '200Gi',
                    'Disk size used by mongodb')
# TODO(user): Use GetStorageClass function, once available.
flags.DEFINE_string('kubernetes_mongodb_storage_class', 'premium-rwo',
                    'storageClassType of data disk')

BENCHMARK_NAME = 'kubernetes_mongodb'
BENCHMARK_CONFIG = """
kubernetes_mongodb:
  description: Benchmarks MongoDB server performance.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_single_core
    nodepools:
      mongodb:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-8
            zone: us-central1-a
          Azure:
            zone: westus
            machine_type: Standard_D3_v2
          AWS:
            zone: us-east-1a
            machine_type: c5.xlarge
      clients:
        vm_count: 1
        vm_spec: *default_single_core
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: null
"""


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['container_cluster']['nodepools']['mongodb']['vm_count'] = (
        FLAGS.ycsb_client_vms)
  return config


def _PrepareClient(vm):
  """Install YCSB on the client VM."""
  vm.Install('ycsb')
  # Disable logging for MongoDB driver, which is otherwise quite verbose.
  log_config = """<configuration><root level="WARN"/></configuration>"""

  vm.RemoteCommand("echo '{0}' > {1}/logback.xml".format(
      log_config, ycsb.YCSB_DIR))


def _PrepareDeployment(benchmark_spec):
  """Deploys MongoDB Operator and instance on the cluster."""
  admin_password = ''.join(
      random.choice(string.ascii_letters + string.digits) for _ in range(20))
  benchmark_spec.container_cluster.ApplyManifest(
      'container/kubernetes_mongodb/kubernetes_mongodb_crd.yaml')
  benchmark_spec.container_cluster.ApplyManifest(
      'container/kubernetes_mongodb/kubernetes_mongodb_operator.yaml.j2',
      cpu_request=FLAGS.kubernetes_mongodb_cpu_request,
      cpu_limit=FLAGS.kubernetes_mongodb_cpu_limit,
      memory_request=FLAGS.kubernetes_mongodb_memory_request,
      memory_limit=FLAGS.kubernetes_mongodb_memory_limit,
      disk_size=FLAGS.kubernetes_mongodb_disk_size,
      storage_class=FLAGS.kubernetes_mongodb_storage_class,
      admin_password=admin_password)
  time.sleep(60)

  benchmark_spec.container_cluster.WaitForResource('pod/mongodb-0', 'Ready')
  mongodb_cluster_ip = benchmark_spec.container_cluster.GetClusterIP(
      'mongodb-service')
  benchmark_spec.mongodb_url = 'mongodb://ycsb:{password}@{ip_address}:27017/ycsb?authSource=ycsb'.format(
      password=admin_password, ip_address=mongodb_cluster_ip)


def Prepare(benchmark_spec):
  """Install MongoDB on one VM and YCSB on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  server_partials = [functools.partial(_PrepareDeployment, benchmark_spec)]
  client_partials = [
      functools.partial(_PrepareClient, client)
      for client in benchmark_spec.vm_groups['clients']
  ]

  vm_util.RunThreaded((lambda f: f()), server_partials + client_partials)
  benchmark_spec.executor = ycsb.YCSBExecutor('mongodb', cp=ycsb.YCSB_DIR)


def Run(benchmark_spec):
  return mongodb_ycsb_benchmark.Run(benchmark_spec)


def Cleanup(benchmark_spec):
  """Remove MongoDB and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec
