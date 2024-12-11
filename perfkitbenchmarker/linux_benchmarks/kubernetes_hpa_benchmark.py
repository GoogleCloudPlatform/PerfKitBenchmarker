# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs a locust based hpa benchmark on a k8s cluster."""

import functools
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker.linux_packages import locust
from perfkitbenchmarker.sample import Sample

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'kubernetes_hpa_runtime_class_name',
    None,
    'A custom runtimeClassName to apply to the pods.',
)

BENCHMARK_NAME = 'kubernetes_hpa'
BENCHMARK_CONFIG = """
kubernetes_hpa:
  description: Benchmarks how quickly hpa reacts to load
  vm_groups:
    default:
      vm_spec: *default_dual_core
      vm_count: 1
  container_specs:
    kubernetes_fib:
      image: fibonacci
  container_registry: {}
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
    nodepools:
      fibpool:
        vm_count: 3
        vm_spec:
          GCP:
            machine_type: n2-standard-4
          AWS:
            machine_type: m6i.xlarge
          Azure:
            machine_type: Standard_D4s_v5
"""


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  return config


def _PrepareCluster(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares a cluster to run the hpa benchmark."""
  cluster: container_service.KubernetesCluster = (
      benchmark_spec.container_cluster
  )
  fib_image = benchmark_spec.container_specs['kubernetes_fib'].image

  cluster.ApplyManifest(
      'container/kubernetes_hpa/fib.yaml.j2',
      fib_image=fib_image,
      runtime_class_name=FLAGS.kubernetes_hpa_runtime_class_name,
  )

  cluster.WaitForResource('deploy/fib', 'available', namespace='fib')


def _PrepareLocust(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares a vm to run locust."""
  vm = benchmark_spec.vms[0]
  locust.Install(vm)
  locust.Prep(vm, locust.Locustfile.RAMPUP)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Install fib workload (and associated hpa) on the K8s Cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """

  prepare_fns = [
      functools.partial(_PrepareCluster, benchmark_spec),
      functools.partial(_PrepareLocust, benchmark_spec),
  ]

  background_tasks.RunThreaded(lambda f: f(), prepare_fns)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[Sample]:
  """Run a benchmark against the Nginx server."""

  # Get the SUT address
  stdout, _, _ = container_service.RunKubectlCommand([
      'get',
      '-n',
      'fib',
      'svc/fib',
      '-o',
      "jsonpath='{.status.loadBalancer.ingress[0].ip}'",
  ])
  addr = 'http://' + stdout.strip() + ':5000'

  # Run locust against the SUT
  vm = benchmark_spec.vms[0]
  samples = locust.Run(vm, addr)

  return list(samples)


def Cleanup(benchmark_spec):
  """Cleanup."""
  del benchmark_spec
