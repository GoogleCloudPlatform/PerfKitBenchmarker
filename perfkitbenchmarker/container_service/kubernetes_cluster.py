"""Classes related to KubernetesCluster."""

import functools
import json
import logging
import time
from typing import Any
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.container_service import kubectl
from perfkitbenchmarker.container_service import kubernetes
from perfkitbenchmarker.container_service import kubernetes_commands
from perfkitbenchmarker.container_service import kubernetes_events
from perfkitbenchmarker.resources import kubernetes_inference_server

INGRESS_JSONPATH = '{.status.loadBalancer.ingress[0]}'
RESOURCE_DELETE_SLEEP_SECONDS = 5


class KubernetesCluster(container_service.BaseContainerCluster):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = container_service.KUBERNETES

  def __init__(self, cluster_spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(cluster_spec)
    self.event_poller: kubernetes_events.KubernetesEventPoller | None = None
    if cluster_spec.poll_for_events:

      def _GetEventsNoLogging():
        return kubernetes_commands.GetEvents(suppress_logging=True)

      self.event_poller = kubernetes_events.KubernetesEventPoller(
          _GetEventsNoLogging
      )

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
    _DeleteAllFromDefaultNamespace()

  def _Delete(self):
    if self.event_poller:
      self.event_poller.StopPolling()
    _DeleteAllFromDefaultNamespace()

  def GetEvents(self) -> set['kubernetes_events.KubernetesEvent']:
    """Gets the events for the cluster, including previously polled events."""
    if self.event_poller:
      return self.event_poller.GetEvents()
    return kubernetes_commands.GetEvents()

  def __getstate__(self):
    state = self.__dict__.copy()
    if 'event_poller' in state:
      del state['event_poller']
    return state

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster."""
    result = super().GetResourceMetadata()
    if self.created:
      result['version'] = kubernetes_commands.GetK8sVersion()
    return result

  def DeployContainer(
      self, name: str, container_spec: container_spec_lib.ContainerSpec
  ):
    """Deploys Containers according to the ContainerSpec."""
    base_name = name
    name = base_name + str(len(self.containers[base_name]))
    container = kubernetes.KubernetesContainer(
        container_spec=container_spec, name=name
    )
    self.containers[base_name].append(container)
    container.Create()

  def DeployContainerService(
      self, name: str, container_spec: container_spec_lib.ContainerSpec
  ):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    service = kubernetes.KubernetesContainerService(container_spec, name)
    self.services[name] = service
    service.Create()

  # TODO(pclay): Move to cached property in Python 3.9
  @property
  @functools.lru_cache(maxsize=1)
  def node_memory_allocatable(self) -> units.Quantity:
    """Usable memory of each node in cluster in KiB."""
    stdout, _, _ = kubectl.RunKubectlCommand(
        # TODO(pclay): Take a minimum of all nodes?
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.allocatable.memory}']
    )
    return units.ParseExpression(stdout)

  @property
  @functools.lru_cache(maxsize=1)
  def node_num_cpu(self) -> int:
    """vCPU of each node in cluster."""
    stdout, _, _ = kubectl.RunKubectlCommand(
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
      pod_spec_yaml.setdefault('nodeSelector', {}).update(node_selectors)

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
    yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
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
    kubernetes_commands.ApplyYaml(yaml_docs)
    return self._WaitForIngress(name, namespace, port)

  def _WaitForIngress(self, name: str, namespace: str, port: int) -> str:
    """Waits for a deployed Ingress/load balancer resource."""
    name = f'service/{name}'
    kubernetes_commands.WaitForResource(
        name,
        INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
    )
    stdout, _, _ = kubectl.RunKubectlCommand([
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


def _DeleteAllFromDefaultNamespace():
  """Deletes all resources from a namespace.

  Since StatefulSets do not reclaim PVCs upon deletion, they are explicitly
  deleted here to prevent dynamically provisioned PDs from leaking once the
  cluster has been deleted.
  """
  try:
    # Delete deployments and jobs first as otherwise autorepair will redeploy
    # deleted pods.
    run_cmd = ['delete', 'deployment', '--all', '-n', 'default']
    kubectl.RunRetryableKubectlCommand(run_cmd)

    run_cmd = ['delete', 'job', '--all', '-n', 'default']
    kubectl.RunRetryableKubectlCommand(run_cmd)

    timeout = 60 * 20
    run_cmd = [
        'delete',
        'all',
        '--all',
        '-n',
        'default',
        f'--timeout={timeout}s',
    ]
    kubectl.RunRetryableKubectlCommand(run_cmd, timeout=timeout)

    run_cmd = ['delete', 'pvc', '--all', '-n', 'default']
    kubectl.RunKubectlCommand(run_cmd)
    # There maybe a slight race if resources are cleaned up in the background
    # where deleting the cluster immediately prevents the PVCs from being
    # deleted.
    logging.info(
        'Sleeping for %s seconds to give resources time to delete.',
        RESOURCE_DELETE_SLEEP_SECONDS,
    )
    time.sleep(RESOURCE_DELETE_SLEEP_SECONDS)
  except (
      errors.VmUtil.IssueCommandTimeoutError,
      vm_util.TimeoutExceededRetryError,
  ) as e:
    raise errors.Resource.RetryableDeletionError(
        'Timed out while deleting all resources from default namespace. We'
        ' should still continue trying to delete everything.'
    ) from e
