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

from collections.abc import Callable
import functools
import threading
import typing
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
  flags:
    locust_path: locust/rampup.py
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
  cluster.WaitForResource(
      'service/fib',
      '{.status.loadBalancer.ingress[0].ip}',
      namespace='fib',
      condition_type='jsonpath=',
  )


def _PrepareLocust(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares a vm to run locust."""
  vm = benchmark_spec.vms[0]
  locust.Install(vm)
  locust.Prep(vm)


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

  vm = benchmark_spec.vms[0]
  cluster: container_service.KubernetesCluster = typing.cast(
      container_service.KubernetesCluster, benchmark_spec.container_cluster
  )

  samples = []
  stop = threading.Event()

  kmc = KubernetesMetricsCollector(samples, stop)

  def RunLocust():
    for s in locust.Run(vm, addr):
      samples.append(s)
    stop.set()

  background_tasks.RunThreaded(
      lambda f: f(),
      [
          lambda: kmc.ObserveNumReplicas(cluster, 'deploy/fib', 'fib'),
          lambda: kmc.ObserveNumNodes(cluster),
          RunLocust,
      ],
      max_concurrent_threads=3,
  )

  return samples


class KubernetesMetricsCollector:
  """Collects k8s cluster metrics."""

  def __init__(self, samples: List[Sample], stop: threading.Event):
    """Creates a KubernetesMetricsCollector.

    Args:
      samples: List of samples. Observed metrics will be added to this list.
      stop: Event indicating when the test is over. Once set, the Observe*()
        methods will stop.
    """
    self._samples = samples
    self._stop = stop

  def ObserveNumReplicas(
      self,
      cluster: container_service.KubernetesCluster,
      resource_name: str,
      namespace: str = '',
  ) -> None:
    """Periodically samples the number of replicas.

    Adds the result to self._samples.

    Expected to be run in a background thread. Never completes until self._stop
    is signaled.

    Args:
      cluster: The cluster in question.
      resource_name: The deployment/statefulset/etc's name, e.g.
        'deployment/my_deployment'.
      namespace: The namespace of the resource. If omitted, the 'default'
        namespace will be used.
    """
    self._Observe(
        lambda: cluster.GetNumReplicasSamples(resource_name, namespace)
    )

  def ObserveNumNodes(
      self,
      cluster: container_service.KubernetesCluster,
  ) -> None:
    """Periodically samples the number of nodes.

    Adds result to self._samples.

    Expected to be run in a background thread. Never completes until self._stop
    is signaled.

    Args:
      cluster: The cluster in question.
    """
    self._Observe(cluster.GetNumNodesSamples)

  def _Observe(
      self,
      observe_fn: Callable[[], List[Sample]],
  ) -> None:
    while True:
      self._samples.extend(observe_fn())
      if self._stop.wait(timeout=1.0):
        return


def Cleanup(benchmark_spec):
  """Cleanup."""
  del benchmark_spec
