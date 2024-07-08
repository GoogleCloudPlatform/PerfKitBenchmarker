# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Benchmark for timing provisioning Kubernetes clusters and pods.

Timing for creation of the cluster and pods are handled by resource.py
(see resource.GetSamples). There is one end-to-end sample created measured from
the beginning of cluster creation through the pod running.
"""

import logging
import time
from typing import Callable, List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.configs import benchmark_config_spec

_TIME_RESIZE = flags.DEFINE_bool(
    'provision_container_cluster_time_resize',
    False,
    'Whether to time resizing the cluster.',
)

BENCHMARK_NAME = 'provision_container_cluster'

BENCHMARK_CONFIG = """
provision_container_cluster:
  description: >
      Time spinning up and deleting a Kubernetes Cluster
  container_specs:
    main_nodepool:
      image: hello-world
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
"""

# kubectl pods don't allow _'s.
SPEC_NAME = 'main_nodepool'
CONTAINER_NAME = 'main-nodepool'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(
    benchmark_config: benchmark_config_spec.BenchmarkConfigSpec,
):
  if benchmark_config.container_cluster.type != container_service.KUBERNETES:
    raise errors.Config.InvalidValue(
        'provision_container_cluster only supports Kubernetes clusters.'
    )


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec):
  """Provision container, because it's not handled by provision stage."""
  cluster = bm_spec.container_cluster
  assert cluster
  container = bm_spec.container_specs[SPEC_NAME]
  logging.info('Deploying container with image %s', container.image)
  cluster.DeployContainer(CONTAINER_NAME, container)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Get samples from resource provisioning."""
  cluster = bm_spec.container_cluster
  assert cluster
  assert isinstance(cluster, container_service.KubernetesCluster)
  container = cluster.containers[CONTAINER_NAME][0]
  samples = container.GetSamples()
  if not cluster.user_managed:
    samples.append(
        sample.Sample(
            'Time to Create Cluster and Pod',
            container.resource_ready_time - cluster.create_start_time,
            'seconds',
        )
    )
    if _TIME_RESIZE.value:
      samples.extend(_BenchmarkClusterResize(cluster))
  return samples


def _BenchmarkClusterResize(
    cluster: container_service.KubernetesCluster,
) -> List[sample.Sample]:
  """Time resizing the cluster.

  Rather than manually creating a pod like when provisioned, deploy a daemonset
  before the resize and read from logs how how long it takes the daemonset to
  add the pod.

  Args:
    cluster: The cluster to test resizing.

  Returns:
    The samples timing the cluster resize.
  """
  samples = []

  cluster.ApplyManifest('container/provision_container_cluster/daemonset.yaml')
  cluster.WaitForRollout('daemonset/daemon-set')

  initial_nodes = set(cluster.GetNodeNames())
  new_node_count = len(initial_nodes) + 1

  resize_start_time = time.time()
  cluster.ResizeNodePool(new_node_count)
  resize_finished_time = time.time()
  samples.append(
      sample.Sample(
          'resize_duration', resize_finished_time - resize_start_time, 'seconds'
      )
  )

  # Even with the WaitForRollout, sometimes nodes/pods are not yet in the API.
  # Manually poll for them to be ready. This is also a reason to prefer the
  # other metrics over simply 'resize_duration'. Polling does not affect the
  # metrics, because they are all from logs.
  def PollForData(
      poll: Callable[[], str], poll_message: str, failure_message: str
  ) -> str:
    for _ in range(20):
      result = poll()
      if result:
        return result
      logging.info(poll_message)
      time.sleep(5)
    raise errors.Benchmarks.RunError(failure_message)

  def GetNode() -> str:
    new_nodes = set(cluster.GetNodeNames()) - initial_nodes
    if new_nodes:
      return new_nodes.pop()
    return ''

  new_node = PollForData(
      GetNode,
      poll_message='Node not found waiting...',
      failure_message='Cluster failed to add new node.',
  )

  cluster.WaitForRollout('daemonset/daemon-set')

  def GetPodOnNode() -> str:
    pod, _, _ = container_service.RunKubectlCommand([
        'get',
        'pod',
        '-o',
        f'jsonpath={{.items[?(@.spec.nodeName=="{new_node}")].metadata.name}}',
    ])
    return pod

  new_pod = PollForData(
      GetPodOnNode,
      poll_message='Pod not found waiting...',
      failure_message='Daemonset failed to schedule new pod.',
  )

  cluster.WaitForResource(f'pod/{new_pod}', 'ready')

  key_event_timestamps = _GetKeyScalingEventTimes(cluster, new_node, new_pod)
  for metric, timestamp in key_event_timestamps.items():
    samples.append(
        sample.Sample(
            'resize_' + metric, timestamp - resize_start_time, 'seconds'
        )
    )
  return samples


def _GetKeyScalingEventTimes(
    cluster: container_service.KubernetesCluster, new_node: str, new_pod: str
) -> dict[str, float]:
  """Extract the time of each known milestone."""
  events = cluster.GetEvents()
  node_events = list(
      event
      for event in events
      if event.resource.kind == 'Node' and event.resource.name == new_node
  )
  pod_events = list(
      event
      for event in events
      if event.resource.kind == 'Pod' and event.resource.name == new_pod
  )

  def First(items, predicate):
    return next(item for item in items if predicate(item))

  key_events = {
      'starting_kubelet': First(
          node_events,
          lambda event: event.message.startswith('Starting kubelet'),
      ),
      'node_registered': First(
          node_events, lambda event: event.reason == 'RegisteredNode'
      ),
      'node_ready': First(
          node_events, lambda event: event.reason == 'NodeReady'
      ),
      'image_pulling': First(
          pod_events, lambda event: event.reason == 'Pulling'
      ),
      'image_pulled': First(pod_events, lambda event: event.reason == 'Pulled'),
      'container_created': First(
          pod_events,
          lambda event: event.message.startswith('Created container'),
      ),
      'container_started': First(
          pod_events,
          lambda event: event.message.startswith('Started container'),
      ),
  }
  return {metric: event.timestamp for metric, event in key_events.items()}


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  assert bm_spec.container_cluster
  bm_spec.container_cluster.containers[SPEC_NAME][0].Delete()
