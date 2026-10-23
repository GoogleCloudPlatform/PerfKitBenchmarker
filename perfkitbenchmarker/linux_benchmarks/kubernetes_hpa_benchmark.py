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
import datetime
import functools
import json
import logging
import threading
import typing
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import http_poller
from perfkitbenchmarker.linux_packages import locust
from perfkitbenchmarker.resources.container_service import kubernetes_cluster
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.sample import Sample

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_hpa'
BENCHMARK_CONFIG = """
kubernetes_hpa:
  description: Benchmarks how quickly hpa reacts to load
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
  container_specs:
    kubernetes_fib:
      image: fibonacci
  container_registry: {}
  container_cluster:
    cloud: GCP
    type: Kubernetes
    min_vm_count: 3
    max_vm_count: 50
    vm_spec:
      <<: *default_dual_core
      GCP:
        machine_type: n4-standard-2
        zone: us-central1-a,us-central1-b,us-central1-c
        image: null
  flags:
    locust_path: locust/rampup.py
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


def PrepareCluster(benchmark_spec: bm_spec.BenchmarkSpec, manifest_path: str):
  """Prepares a cluster to run the hpa benchmark."""
  cluster: kubernetes_cluster.KubernetesCluster = (
      benchmark_spec.container_cluster  # pyrefly: ignore[bad-assignment]
  )
  fib_image = benchmark_spec.container_specs['kubernetes_fib'].image

  yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
      manifest_path,
      fib_image=fib_image,
      port=_PORT,
  )
  cluster.ModifyPodSpecPlacementYaml(
      yaml_docs,
      'fib',
  )
  kubernetes_commands.ApplyYaml(yaml_docs)

  kubernetes_commands.WaitForResource(
      'deploy/fib', 'available', namespace='fib'
  )


def PrepareLocust(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares a vm to run locust."""
  vm = benchmark_spec.vms[0]
  vm.Install('http_poller')
  locust.Install(vm)  # pyrefly: ignore[bad-argument-type]
  locust.Prep(vm)  # pyrefly: ignore[bad-argument-type]


def Prepare(
    benchmark_spec: bm_spec.BenchmarkSpec,
    manifest_path: str = 'container/kubernetes_hpa/fib.yaml.j2',
):
  """Install fib workload (and associated hpa) on the K8s Cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
    manifest_path: The manifest to apply.
  """

  prepare_fns = [
      functools.partial(PrepareCluster, benchmark_spec, manifest_path),
      functools.partial(PrepareLocust, benchmark_spec),
  ]

  background_tasks.RunThreaded(lambda f: f(), prepare_fns)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[Sample]:
  """Run a benchmark against the fibonacci server."""
  vm = benchmark_spec.vms[0]
  cluster: kubernetes_cluster.KubernetesCluster = typing.cast(
      kubernetes_cluster.KubernetesCluster, benchmark_spec.container_cluster
  )
  addr = cluster.DeployIngress('fib', 'fib', _PORT, _HEALTH_PATH)

  # Confirm the server can be pinged.
  PollServer(vm, addr)

  samples = []
  stop = threading.Event()

  kmc = KubernetesMetricsCollector(samples, stop)

  def RunLocust():
    for s in locust.Run(vm, addr):  # pyrefly: ignore[bad-argument-type]
      samples.append(s)
    stop.set()

  background_tasks.RunThreaded(
      lambda f: f(),
      [
          lambda: kmc.ObserveNumReplicas('deploy/fib', 'fib'),
          kmc.ObserveNumNodes,
          RunLocust,
      ],
      max_concurrent_threads=3,
  )
  kmc.EmitMetrics('fib', 'app=fib', cluster.GetResourceMetadata())

  return samples


@vm_util.Retry(
    retryable_exceptions=(errors.Resource.RetryableGetError,),
)
def PollServer(vm: virtual_machine.BaseVirtualMachine, addr: str) -> None:
  """Polls the server to confirm it is responding."""
  poller = http_poller.HttpPoller()
  response = poller.Run(vm, addr + '/calculate')
  if not response.success:
    raise errors.Resource.RetryableGetError(
        'Failed to contact server; got failing response: %s' % response
    )


def _ParsePodReadyLatencies(stdout: str) -> List[float]:
  """Parses kubectl get pods JSON output to compute scale-out latencies."""
  data = json.loads(stdout)
  if not data.get('items'):
    raise errors.Resource.RetryableGetError('No pods found yet.')

  latencies = []
  all_ready = True

  for pod in data.get('items', []):
    creation_ts = pod.get('metadata', {}).get('creationTimestamp')
    if not creation_ts:
      continue

    ready_condition = next(
        (c for c in pod.get('status', {}).get('conditions', [])
         if c.get('type') == 'Ready'), None)
    if not ready_condition or ready_condition.get('status') != 'True':
      all_ready = False
      break

    ready_ts = ready_condition.get('lastTransitionTime')
    if not ready_ts:
      continue

    create_dt = datetime.datetime.fromisoformat(
        creation_ts.replace('Z', '+00:00'))
    ready_dt = datetime.datetime.fromisoformat(
        ready_ts.replace('Z', '+00:00'))

    latency = (ready_dt - create_dt).total_seconds()
    latencies.append(latency)

  if not all_ready:
    raise errors.Resource.RetryableGetError(
        'Not all bursting pods are ready yet.')

  return latencies


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
      resource_name: str,
      namespace: str = '',
  ) -> None:
    """Periodically samples the number of replicas.

    Adds the result to self._samples.

    Expected to be run in a background thread. Never completes until self._stop
    is signaled.

    Args:
      resource_name: The deployment/statefulset/etc's name, e.g.
        'deployment/my_deployment'.
      namespace: The namespace of the resource. If omitted, the 'default'
        namespace will be used.
    """
    self._Observe(
        lambda: kubernetes_commands.GetNumReplicasSamples(
            resource_name, namespace
        )
    )

  def ObserveNumNodes(
      self,
  ) -> None:
    """Periodically samples the number of nodes.

    Adds result to self._samples.

    Expected to be run in a background thread. Never completes until self._stop
    is signaled.
    """
    self._Observe(kubernetes_commands.GetNumNodesSamples)

  def _Observe(
      self,
      observe_fn: Callable[[], List[Sample]],
  ) -> None:
    """Call the specified function until self._stop is signalled.

    Results are appended to self._samples. Timeouts are ignored.

    Args:
      observe_fn: The function to call.
    """
    success_count = 0
    failure_count = 0
    while True:
      try:
        self._samples.extend(observe_fn())
        success_count += 1
      except (
          errors.VmUtil.IssueCommandError,
          errors.VmUtil.IssueCommandTimeoutError,
      ) as e:
        # Ignore errors, timeouts - there'll be a gap in the data, but that's
        # ok.
        logging.warning(
            'Ignoring exception that occurred while observing cluster: %s', e
        )
        failure_count += 1

      if self._stop.wait(timeout=1.0):
        if success_count + failure_count == 0:
          raise AssertionError(
              'Unexpected: no successful OR unsuccessful attempts occurred?'
          )
        assert success_count / (success_count + failure_count) >= 0.90
        return

  @vm_util.Retry(
      poll_interval=10,
      max_retries=30,
      retryable_exceptions=(errors.Resource.RetryableGetError,),
  )
  def _ParsePodScaleOutLatency(
      self, namespace: str, label_selector: str) -> List[float]:
    """Extracts burst pod startup latencies from Kubernetes event streams.

    To accurately capture the real-world latency impact of bypassing cold VM
    provisioning delays during traffic spikes, we extract the precise delta
    between PodCreated and PodReady events for the newly scaled replicas.

    Args:
      namespace: The strict namespace to query.
      label_selector: The label selector to filter events for bursting pods.

    Returns:
      A list of all latencies observed for pod readiness.
    """
    cmd = [
        'get', 'pods', '-n', namespace, '-l', label_selector,
        '-o', 'json'
    ]
    try:
      stdout, _, _ = kubernetes_commands.kubectl.RunKubectlCommand(
          cmd, raise_on_failure=True)
    except errors.VmUtil.IssueCommandError as e:
      raise errors.Resource.RetryableGetError(f'kubectl failed: {e}')

    return _ParsePodReadyLatencies(stdout)

  def EmitMetrics(
      self, namespace: str, label_selector: str,
      base_metadata: dict[str, typing.Any]) -> None:
    """Emits performance metrics."""
    latencies = self._ParsePodScaleOutLatency(namespace, label_selector)
    if not latencies:
      return

    logging.info('Collected ready latency metrics for %d pods', len(latencies))

    latencies_sorted = sorted(latencies)
    count = len(latencies_sorted)

    metrics = {
        'p50': latencies_sorted[int(count * 0.50)],
        'p90': latencies_sorted[int(count * 0.90)],
        'p99': latencies_sorted[int(count * 0.99)],
        'max': latencies_sorted[-1],
        'average': sum(latencies_sorted) / count,
    }

    for suffix, val in metrics.items():
      self._samples.append(
          Sample(
              f'time_to_ready_pod_scale_out_seconds_{suffix}',
              val,
              'seconds',
              base_metadata,
          )
      )


def Cleanup(benchmark_spec):
  """Cleanup."""
  del benchmark_spec
