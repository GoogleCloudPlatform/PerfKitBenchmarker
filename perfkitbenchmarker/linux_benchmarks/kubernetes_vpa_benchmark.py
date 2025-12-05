# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs a locust based vpa benchmark on a k8s cluster."""

import collections
from typing import Any, Dict, List

from absl import flags
import numpy as np
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker.linux_benchmarks import kubernetes_hpa_benchmark as hpa
from perfkitbenchmarker.sample import Sample
from scipy import integrate

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_vpa'
BENCHMARK_CONFIG = """
kubernetes_vpa:
  description: Benchmarks how quickly vpa reacts to load
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
  container_specs:
    kubernetes_fib:
      image: fibonacci
  container_registry:
    cloud: GCP
    spec:
      GCP:
        zone: us-central1
  container_cluster:
    cloud: GCP
    type: Kubernetes
    min_vm_count: 10
    max_vm_count: 10
    enable_vpa: True
    vm_spec:
      <<: *default_dual_core
      GCP:
        machine_type: n4-standard-2
        zone: us-central1-a,us-central1-b,us-central1-c
        image: null
  flags:
    locust_path: locust/rampup_vpa.py
"""

_PORT = 5000
_HEALTH_PATH = '/calculate'


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  return hpa.Prepare(benchmark_spec, 'container/kubernetes_vpa/fib.yaml.j2')


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[Sample]:
  """Run a benchmark against the fibonacci server."""

  samples = hpa.Run(benchmark_spec)
  samples.append(_ComputeOverUnderProvisioning(samples))

  return samples


def _ComputeOverUnderProvisioning(samples: List[Sample]) -> Sample:
  """Post process the samples, and compute the over/under provisioning.

  This looks for CPU requests/usage samples and computes `f(x) =
  |(requests-usage)/requests|` at each time period. Then,
  `scipy.integrate.trapezoid` is used to compute the area under the curve. This
  is the total over/under provisioning of each pod. The total over/under
  provisioning is then calculated by summing up across all pods.

  Args:
    samples: List of samples at the end of the benchmark.

  Returns:
    A sample indicating over/under provisioning in % seconds.
  """
  requests = collections.defaultdict(list)
  usages = collections.defaultdict(list)

  for s in samples:
    if s.metric == 'kubernetes_cpu_request':
      requests[s.metadata['pod']].append((s.timestamp, s.value))
    elif s.metric == 'kubernetes_cpu_usage':
      usages[s.metadata['pod']].append((s.timestamp, s.value))

  total_over_under_provisioning = 0
  pods = set(requests.keys()) & set(usages.keys())

  if not pods:
    return Sample('total_over_under_provisioning', 0, '%-seconds', {})

  for pod in pods:
    req_series = sorted(requests[pod])
    usage_series = sorted(usages[pod])

    all_timestamps = sorted(
        list(set([t for t, v in req_series] + [t for t, v in usage_series]))
    )

    req_values = np.interp(
        all_timestamps,
        [t for t, v in req_series],
        [v for t, v in req_series],
    )
    usage_values = np.interp(
        all_timestamps,
        [t for t, v in usage_series],
        [v for t, v in usage_series],
    )

    diff = np.abs((req_values - usage_values) / req_values)

    integral = integrate.trapezoid(diff, all_timestamps)
    total_over_under_provisioning += integral

  return Sample(
      'total_over_under_provisioning',
      total_over_under_provisioning,
      '%-seconds',
      {},
  )


class KubernetesMetricsCollector(hpa.KubernetesMetricsCollector):
  """Collects k8s cluster metrics."""

  def ObserveCPURequests(
      self,
      cluster: container_service.KubernetesCluster,
      resource_name: str,
      namespace: str = '',
  ) -> None:
    """Periodically sample the CPU requests of pods within the given resource.

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
        lambda: cluster.GetCPURequestSamples(resource_name, namespace)
    )

  def ObserveCPUUsage(
      self,
      cluster: container_service.KubernetesCluster,
      resource_name: str,
      namespace: str = '',
  ) -> None:
    """Periodically sample the CPU usage of all pods within the given resource.

    NB: Usage, not utilization.

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
    self._Observe(lambda: cluster.GetCPUUsageSamples(resource_name, namespace))


def Cleanup(benchmark_spec):
  """Cleanup."""
  del benchmark_spec
