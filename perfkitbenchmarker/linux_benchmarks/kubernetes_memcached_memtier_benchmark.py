# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run memtier_benchmark against a K8s cluster.

memtier_benchmark is a load generator created by RedisLabs to benchmark
memcached.

Memcached homepage: https://memcached.org/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark

For proper work requires helm installed and added to env variables in CLI
environment running PKB benchmark
Helm needs to be install on machine with permissions to talk to apiserver,
because of that helm is not insstal on vm and this benchmark is
not working out-of-the-box.
wget https://get.helm.sh/helm-v%s-linux-amd64.tar.gz %HELM_VERSION
mkdir helm-v%HELM_VERSION
tar -zxvf helm-v%HELM_VERSION-linux-amd64.tar.gz -c helm-v%HELMVERSON
export PATH="$(echo ~)/helm-v%HELM_VERSION/linux-amd64:$PATH”
"""


import functools
from typing import Any, Dict
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memtier

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_memcached_memtier'
BENCHMARK_CONFIG = """
kubernetes_memcached_memtier:
  description: >
      Run memtier_benchmark against a K8s cluster.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
    nodepools:
      memcached:
        vm_spec: *default_dual_core
        vm_count: 1
      clients:
        vm_spec:
          GCP:
            machine_type: n2-standard-4
          AWS:
            machine_type: m6i.xlarge
          Azure:
            machine_type: Standard_D4s_v5
        vm_count: 1
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
"""
WAIT_FOR_REPLICA_TIMEOUT = '300s'
memcached_port = 11211
_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""

  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.memcached_memtier_client_machine_type:
    vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud][
          'machine_type'
      ] = FLAGS.memcached_memtier_client_machine_type
  if FLAGS.memcached_memtier_server_machine_type:
    vm_spec = config['container_cluster']['nodepools']['memcached']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud][
          'machine_type'
      ] = FLAGS.memcached_memtier_server_machine_type
  return config


def CreateHelmCommand() -> list[str]:
  """Create command for helm to ccreate memcached instances."""

  command = ['helm', 'install', 'benchmarkcache', 'bitnami/memcached']

  for c in [
      (
          'extraEnvVars=- name: MEMCACHED_MAX_CONNECTIONS\n  value: "32768"\n-'
          f' name: MEMCACHED_THREADS\n  value: "{FLAGS.memcached_num_threads}"'
      ),
      'architecture=high-availability',
      'autoscaling.enabled=true',
      'autoscaling.maxReplicas=1',
      'autoscaling.minReplicas=1',
      'service.clusterIP=None',
      'nodeSelector.cloud\\.google\\.com\\/gke-nodepool=memcached',
      'podLabels.app=memcached,commonLabels.app=memcached',
  ]:
    command.append('--set')
    command.append(c)
  command.append('--wait')
  return command


def _PrepareCluster() -> None:
  """Prepares a cluster to run the Memcached benchmark."""

  vm_util.IssueCommand(
      ['helm', 'repo', 'add', 'bitnami', 'https://charts.bitnami.com/bitnami'],
      cwd=vm_util.GetTempDir(),
  )

  vm_util.IssueCommand(
      CreateHelmCommand(),
      cwd=vm_util.GetTempDir(),
  )

  # Waiting for replica to be created
  vm_util.IssueCommand(
      [
          'kubectl',
          'wait',
          '--for=jsonpath={.status.readyReplicas}=1',
          'sts',
          '-l',
          'app=memcached',
          f'--timeout={WAIT_FOR_REPLICA_TIMEOUT}',
      ],
      cwd=vm_util.GetTempDir(),
  )


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Install Memcached on K8s cluster and memtier_benchmark on client VMs."""
  client_vms = bm_spec.vm_groups['clients']
  # Install Memtier and Memcached on the cluster
  prepare_fns = [functools.partial(_PrepareCluster)] + [
      functools.partial(vm.Install, 'memtier') for vm in client_vms
  ]

  background_tasks.RunThreaded(lambda f: f(), prepare_fns)


def Run(bm_spec: _BenchmarkSpec):
  """Runs memtier against memcached and gathers the results.

  Args:
    bm_spec: The benchmark specification. Contains all data that is required to
      run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  client = bm_spec.vm_groups['clients'][0]
  assert bm_spec.container_cluster
  server_ip = bm_spec.container_cluster.GetPodIpsByLabel('app', 'memcached')[0]
  metadata = {
      'memcached_server_size': FLAGS.memcached_size_mb,
      'memcached_server_threads': FLAGS.memcached_num_threads,
  }

  samples = memtier.RunOverAllThreadsPipelinesAndClients(
      [client], server_ip, [memcached_port]
  )
  for sample in samples:
    sample.metadata.update(metadata)
  return samples


def Cleanup(unused_bm_spec: _BenchmarkSpec):
  pass
