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
"""Contains classes related to managed kubernetes container services."""

import functools
import json
from typing import Any

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.resources import kubernetes_inference_server
import requests
import yaml

# pylint: disable=unused-import
# Temporarily hoist kubectl related methods into this namespace
from .kubectl import RETRYABLE_KUBECTL_ERRORS
from .kubectl import RunKubectlCommand
from .kubectl import RunRetryableKubectlCommand
# Temporarily hoist k8s cluster commands into this namespace
from .kubernetes_commands import KubernetesClusterCommands
# Temporarily hoist event related classes into this namespace
from .kubernetes_events import KubernetesEvent
from .kubernetes_events import KubernetesEventPoller
from .kubernetes_events import KubernetesEventResource
# pylint: enable=unused-import


BenchmarkSpec = Any  # benchmark_spec lib imports this module.


flags.DEFINE_string(
    'kubeconfig',
    None,
    'Path to kubeconfig to be used by kubectl. '
    "If unspecified, it will be set to a file in this run's "
    'temporary directory.',
)

flags.DEFINE_string('kubectl', 'kubectl', 'Path to kubectl tool')

_K8S_INGRESS = """
apiVersion: extensions/v1beta1
kind: Ingress
metadata:
  name: {service_name}-ingress
spec:
  backend:
    serviceName: {service_name}
    servicePort: 8080
"""
INGRESS_JSONPATH = '{.status.loadBalancer.ingress[0]}'


class KubernetesPod:
  """Representation of a Kubernetes pod.

  It can be created as a PKB managed resource using KubernetesContainer,
  or created with ApplyManifest and directly constructed.
  """

  def __init__(self, name=None, **kwargs):
    super().__init__(**kwargs)
    assert name
    self.name = name

  def _GetPod(self) -> dict[str, Any]:
    """Gets a representation of the POD and returns it."""
    stdout, _, _ = RunKubectlCommand(['get', 'pod', self.name, '-o', 'yaml'])
    pod = yaml.safe_load(stdout)
    self.ip_address = pod.get('status', {}).get('podIP')
    return pod

  def _ValidatePodHasNotFailed(self, status: dict[str, Any]):
    """Raises an exception if the pod has failed."""
    # Inspect the pod's status to determine if it succeeded, has failed, or is
    # doomed to fail.
    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
    phase = status['phase']
    if phase == 'Succeeded':
      return
    elif phase == 'Failed':
      raise container_service.FatalContainerException(
          f'Pod {self.name} failed:\n{yaml.dump(status)}'
      )
    else:
      for condition in status.get('conditions', []):
        if (
            condition['type'] == 'PodScheduled'
            and condition['status'] == 'False'
            and condition['reason'] == 'Unschedulable'
        ):
          # TODO(pclay): Revisit this when we scale clusters.
          raise container_service.FatalContainerException(
              f"Pod {self.name} failed to schedule:\n{condition['message']}"
          )
      for container_status in status.get('containerStatuses', []):
        waiting_status = container_status['state'].get('waiting', {})
        if waiting_status.get('reason') in [
            'ErrImagePull',
            'ImagePullBackOff',
        ]:
          raise container_service.FatalContainerException(
              f'Failed to find container image for {self.name}:\n'
              + yaml.dump(waiting_status.get('message'))
          )

  def WaitForExit(self, timeout: int | None = None) -> dict[str, Any]:
    """Gets the finished running container."""

    @vm_util.Retry(
        timeout=timeout,
        retryable_exceptions=(container_service.RetriableContainerException,),
    )
    def _WaitForExit():
      pod = self._GetPod()
      status = pod['status']
      self._ValidatePodHasNotFailed(status)
      phase = status['phase']
      if phase == 'Succeeded':
        return pod
      else:
        raise container_service.RetriableContainerException(
            f'Pod phase ({phase}) not in finished phases.'
        )

    return _WaitForExit()

  def GetLogs(self):
    """Returns the logs from the container."""
    stdout, _, _ = RunKubectlCommand(['logs', self.name])
    return stdout


# Order KubernetesPod first so that it's constructor is called first.
class KubernetesContainer(KubernetesPod, container_service.BaseContainer):
  """A KubernetesPod based flavor of Container."""

  def _Create(self):
    """Creates the container."""
    run_cmd = [
        'run',
        self.name,
        '--image=%s' % self.image,
        '--restart=Never',
        # Allow scheduling on ARM nodes.
        '--overrides',
        json.dumps({
            'spec': {
                'tolerations': [{
                    'operator': 'Exists',
                    'key': 'kubernetes.io/arch',
                    'effect': 'NoSchedule',
                }]
            }
        }),
    ]

    limits = []
    if self.cpus:
      limits.append(f'cpu={int(1000 * self.cpus)}m')
    if self.memory:
      limits.append(f'memory={self.memory}Mi')
    if limits:
      run_cmd.append('--limits=' + ','.join(limits))

    if self.command:
      run_cmd.extend(['--command', '--'])
      run_cmd.extend(self.command)
    RunKubectlCommand(run_cmd)

  def _Delete(self):
    """Deletes the container."""
    pass

  def _IsReady(self):
    """Returns true if the container has stopped pending."""
    status = self._GetPod()['status']
    super()._ValidatePodHasNotFailed(status)
    return status['phase'] != 'Pending'


class KubernetesContainerService(container_service.BaseContainerService):
  """A Kubernetes flavor of Container Service."""

  def __init__(self, container_spec, name):
    super().__init__(container_spec)
    self.name = name
    self.port = 8080

  def _Create(self):
    run_cmd = [
        'run',
        self.name,
        '--image=%s' % self.image,
        '--port',
        str(self.port),
    ]

    limits = []
    if self.cpus:
      limits.append(f'cpu={int(1000 * self.cpus)}m')
    if self.memory:
      limits.append(f'memory={self.memory}Mi')
    if limits:
      run_cmd.append('--limits=' + ','.join(limits))

    if self.command:
      run_cmd.extend(['--command', '--'])
      run_cmd.extend(self.command)
    RunKubectlCommand(run_cmd)

    expose_cmd = [
        'expose',
        'deployment',
        self.name,
        '--type',
        'NodePort',
        '--target-port',
        str(self.port),
    ]
    RunKubectlCommand(expose_cmd)
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(_K8S_INGRESS.format(service_name=self.name))
      tf.close()
      kubernetes_helper.CreateFromFile(tf.name)

  def _GetIpAddress(self):
    """Attempts to set the Service's ip address."""
    ingress_name = '%s-ingress' % self.name
    get_cmd = [
        'get',
        'ing',
        ingress_name,
        '-o',
        'jsonpath={.status.loadBalancer.ingress[*].ip}',
    ]
    stdout, _, _ = RunKubectlCommand(get_cmd)
    ip_address = stdout
    if ip_address:
      self.ip_address = ip_address

  def _IsReady(self):
    """Returns True if the Service is ready."""
    if self.ip_address is None:
      self._GetIpAddress()
    if self.ip_address is not None:
      url = 'http://%s' % self.ip_address
      r = requests.get(url)
      if r.status_code == 200:
        return True
    return False

  def _Delete(self):
    """Deletes the service."""
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(_K8S_INGRESS.format(service_name=self.name))
      tf.close()
      kubernetes_helper.DeleteFromFile(tf.name)

    delete_cmd = ['delete', 'deployment', self.name]
    RunKubectlCommand(delete_cmd, raise_on_failure=False)


class KubernetesCluster(
    container_service.BaseContainerCluster, KubernetesClusterCommands
):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = container_service.KUBERNETES

  def __init__(self, cluster_spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(cluster_spec)
    self.event_poller: KubernetesEventPoller | None = None
    if cluster_spec.poll_for_events:

      def _GetEventsNoLogging():
        return self._GetEvents(suppress_logging=True)

      self.event_poller = KubernetesEventPoller(_GetEventsNoLogging)

    self.inference_server = (
        kubernetes_inference_server.GetKubernetesInferenceServer(
            cluster_spec.inference_server, self
        )
    )

  def Create(self, restore: bool = False) -> None:
    super().Create(restore)
    if self.inference_server:
      self.inference_server.Create()

  def _PostCreate(self):
    super()._PostCreate()
    if self.event_poller:
      self.event_poller.StartPolling()

  def Delete(self, freeze: bool = False) -> None:
    if self.inference_server:
      self.inference_server.Delete()
    super().Delete(freeze)

  def _PreDelete(self):
    self._DeleteAllFromDefaultNamespace()

  def _Delete(self):
    if self.event_poller:
      self.event_poller.StopPolling()
    self._DeleteAllFromDefaultNamespace()

  def GetEvents(self) -> set['KubernetesEvent']:
    """Gets the events for the cluster, including previously polled events."""
    if self.event_poller:
      return self.event_poller.GetEvents()
    return self._GetEvents()

  def __getstate__(self):
    state = self.__dict__.copy()
    if 'event_poller' in state:
      del state['event_poller']
    return state

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster."""
    result = super().GetResourceMetadata()
    if self.created:
      result['version'] = self.k8s_version
    return result

  def DeployContainer(
      self, name: str, container_spec: container_spec_lib.ContainerSpec
  ):
    """Deploys Containers according to the ContainerSpec."""
    base_name = name
    name = base_name + str(len(self.containers[base_name]))
    container = KubernetesContainer(container_spec=container_spec, name=name)
    self.containers[base_name].append(container)
    container.Create()

  def DeployContainerService(
      self, name: str, container_spec: container_spec_lib.ContainerSpec
  ):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    service = KubernetesContainerService(container_spec, name)
    self.services[name] = service
    service.Create()

  # TODO(pclay): Move to cached property in Python 3.9
  @property
  @functools.lru_cache(maxsize=1)
  def node_memory_allocatable(self) -> units.Quantity:
    """Usable memory of each node in cluster in KiB."""
    stdout, _, _ = RunKubectlCommand(
        # TODO(pclay): Take a minimum of all nodes?
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.allocatable.memory}']
    )
    return units.ParseExpression(stdout)

  @property
  @functools.lru_cache(maxsize=1)
  def node_num_cpu(self) -> int:
    """vCPU of each node in cluster."""
    stdout, _, _ = RunKubectlCommand(
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.capacity.cpu}']
    )
    return int(stdout)

  def LabelDisks(self):
    """Propagate cluster labels to disks if not done by cloud provider."""
    pass

  # TODO(pclay): integrate with kubernetes_disk.
  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    raise NotImplementedError

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Get the node selectors section of a yaml for the provider."""
    return {}

  def ModifyPodSpecPlacementYaml(
      self,
      yaml_dicts: list[dict[str, Any]],
      name: str,
      machine_type: str | None = None,
  ) -> None:
    """Modifies the pod spec yaml in-place with additional needed attributes.

    The placement of pods (with eg node selectors or topology constraints) is
    the most likely to change from cloud to cloud.

    Args:
      yaml_dicts: The list of yaml dicts to search through & modify. See
        https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podspec-v1-core
          for documentation on the pod spec fields. This is modified in place.
      name: The name of the app.
      machine_type: A specified machine type to request.
    """
    modified = False
    for yaml_dict in yaml_dicts:
      if yaml_dict['spec']['template']['spec']:
        self._ModifyPodSpecPlacementYaml(
            yaml_dict['spec']['template']['spec'], name, machine_type
        )
        modified = True
    if not modified:
      raise ValueError(
          'No pod spec yaml found to modify. Was the wrong jinja passed in?'
      )

  def _ModifyPodSpecPlacementYaml(
      self,
      pod_spec_yaml: dict[str, Any],
      name: str,
      machine_type: str | None = None,
  ) -> None:
    """Modifies the pod spec yaml in-place with additional needed attributes.

    The placement of pods (with eg node selectors or topology constraints) is
    the most likely to change from cloud to cloud.

    Args:
      pod_spec_yaml: The pod spec yaml to modify. See
        https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#podspec-v1-core
          for documentation on the pod spec fields. This is modified in place.
      name: The name of the app.
      machine_type: A specified machine type to request.
    """
    del name
    node_selectors = self.GetNodeSelectors(machine_type)
    if node_selectors:
      pod_spec_yaml['nodeSelector'].update(node_selectors)

  @property
  def _ingress_manifest_path(self) -> str:
    """The path to the ingress manifest template file."""
    return 'container/loadbalancer.yaml.j2'

  def DeployIngress(
      self,
      name: str,
      namespace: str,
      port: int,
      health_path: str = '',
      node_selectors: dict[str, str] | None = None,
  ) -> str:
    """Deploys an Ingress/load balancer resource to the cluster.

    Args:
      name: The name of the Ingress resource.
      namespace: The namespace of the resource.
      port: The port to expose to the internet.
      health_path: The path to use for health checks.
      node_selectors: The node selectors to use for the Service.

    Returns:
      The address of the Ingress, with or without the port.
    """
    yaml_docs = self.ConvertManifestToYamlDicts(
        self._ingress_manifest_path,
        name=name,
        namespace=namespace,
        port=port,
        health_path=health_path,
    )
    if node_selectors:
      for yaml_doc in yaml_docs:
        if yaml_doc['kind'] == 'Service':
          yaml_doc['spec']['selector'] = node_selectors
    self.ApplyYaml(yaml_docs)
    return self._WaitForIngress(name, namespace, port)

  def _WaitForIngress(self, name: str, namespace: str, port: int) -> str:
    """Waits for a deployed Ingress/load balancer resource."""
    name = f'service/{name}'
    self.WaitForResource(
        name,
        INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
    )
    stdout, _, _ = RunKubectlCommand([
        'get',
        name,
        '-n',
        namespace,
        '-o',
        f'jsonpath={INGRESS_JSONPATH}',
    ])
    return f'{self._GetAddressFromIngress(stdout)}:{port}'

  def _GetAddressFromIngress(self, ingress_out: str):
    """Gets the endpoint address from the Ingress resource."""
    ingress = json.loads(ingress_out.strip("'"))
    if 'ip' in ingress:
      ip = ingress['ip']
    elif 'hostname' in ingress:
      ip = ingress['hostname']
    else:
      raise errors.Benchmarks.RunError(
          'No IP or hostname found in ingress from stdout ' + ingress_out
      )
    return 'http://' + ip.strip()

  def AddNodepool(self, batch_name: str, pool_id: str):
    """Adds an additional nodepool with the given name to the cluster."""
    pass


@events.benchmark_start.connect
def _SetKubeConfig(unused_sender, benchmark_spec: BenchmarkSpec):
  """Sets the value for the kubeconfig flag if it's unspecified."""
  if not flags.FLAGS.kubeconfig:
    flags.FLAGS.kubeconfig = vm_util.PrependTempDir(
        'kubeconfig' + str(benchmark_spec.sequence_number)
    )
    # Store the value for subsequent run stages.
    benchmark_spec.config.flags['kubeconfig'] = flags.FLAGS.kubeconfig
