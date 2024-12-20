# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
Redis.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
"""


import functools
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import redis_memtier_benchmark
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'kubernetes_redis_memtier_runtime_class_name',
    None,
    'A custom runtimeClassName to apply to the redis pods.',
)

BENCHMARK_NAME = 'kubernetes_redis_memtier'
BENCHMARK_CONFIG = """
kubernetes_redis_memtier:
  description: >
      Run memtier_benchmark against a K8s cluster.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
    nodepools:
      redis:
        vm_spec: *default_dual_core
        vm_count: 3
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
      vm_spec: *default_single_core
      vm_count: 1
"""

_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.redis_memtier_client_machine_type:
    vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_client_machine_type
  if FLAGS.redis_memtier_server_machine_type:
    vm_spec = config['container_cluster']['nodepools']['redis']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_server_machine_type
  return config


def _PrepareCluster(bm_spec: _BenchmarkSpec):
  """Prepares a cluster to run the Redis benchmark."""
  redis_port = redis_server.GetRedisPorts()[0]
  replicas = bm_spec.container_cluster.nodepools['redis'].num_nodes * 2
  with kubernetes_helper.CreateRenderedManifestFile(
      'container/kubernetes_redis_memtier/kubernetes_redis_memtier.yaml.j2',
      {
          'redis_replicas': replicas,
          'redis_port': redis_port,
          # Redis expects cluster bus port as 'the client port + 10000'
          'redis_cluster_port': redis_port + 10000,
          'runtime_class_name': (
              FLAGS.kubernetes_redis_memtier_runtime_class_name
          ),
      },
  ) as rendered_manifest:
    bm_spec.container_cluster.ApplyManifest(rendered_manifest.name)

  bm_spec.container_cluster.WaitForRollout('statefulset/redis')

  pod_ips = bm_spec.container_cluster.GetPodIps('statefulset/redis')
  ip_and_port_list = list(map(lambda ip: '%s:%s' % (ip, redis_port), pod_ips))
  cmd = [
      'redis-cli',
      '--cluster',
      'create',
      '--cluster-replicas',
      '1',
      '--cluster-yes',
  ] + ip_and_port_list
  bm_spec.container_cluster.RunKubectlExec('redis-0', cmd)

  bm_spec.redis_endpoint_ip = pod_ips[0]


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Install Redis on K8s cluster and memtier_benchmark on client VMs."""
  client_vms = bm_spec.vm_groups['clients']
  # Install Memtier and Redis on the cluster
  prepare_fns = [functools.partial(_PrepareCluster, bm_spec)] + [
      functools.partial(vm.Install, 'memtier') for vm in client_vms
  ]

  background_tasks.RunThreaded(lambda f: f(), prepare_fns)

  # Load Redis database
  memtier.Load(
      client_vms, bm_spec.redis_endpoint_ip, redis_server.GetRedisPorts()[0]
  )


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run memtier_benchmark against Redis."""
  return redis_memtier_benchmark.Run(bm_spec)


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  redis_memtier_benchmark.Cleanup(bm_spec)
