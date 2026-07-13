"""Classes related to KubernetesCluster."""

import abc
import functools
import json
import logging
import time
from typing import Any
from perfkitbenchmarker import errors
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.resources import kubernetes_inference_server
from perfkitbenchmarker.resources.container_service import (
    container as container_lib,
)
from perfkitbenchmarker.resources.container_service import container_cluster
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.container_service import kubernetes_events

INGRESS_JSONPATH = '{.status.loadBalancer.ingress[0]}'
RESOURCE_DELETE_SLEEP_SECONDS = 5


class KubernetesCluster(container_cluster.BaseContainerCluster):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = container_lib.KUBERNETES

  def __init__(self, cluster_spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(cluster_spec)
    self.cluster_spec = cluster_spec
    self.event_poller: kubernetes_events.KubernetesEventPoller = (
        self._InitializeEventPoller()
    )
    self.inference_server = (
        kubernetes_inference_server.GetKubernetesInferenceServer(
            cluster_spec.inference_server, self
        )
    )

  def _InitializeEventPoller(self) -> kubernetes_events.KubernetesEventPoller:
    return kubernetes_events.KubernetesEventPoller(
        lambda: kubernetes_commands.GetEvents(suppress_logging=True)
    )

  def Create(self, restore: bool = False) -> None:
    super().Create(restore)
    if self.inference_server:
      self.inference_server.Create()

  def _PostCreate(self):
    super()._PostCreate()
    if self.cluster_spec.poll_for_events:
      self.event_poller.StartPolling()

  def Delete(self, freeze: bool = False) -> None:
    if self.inference_server:
      self.inference_server.Delete()
    super().Delete(freeze)

  def _PreDelete(self):
    _DeleteAllFromDefaultNamespace()

  def _Delete(self):
    if self.cluster_spec.poll_for_events:
      self.event_poller.StopPolling()
    _DeleteAllFromDefaultNamespace()

  def GetEvents(self) -> set['kubernetes_events.KubernetesEvent']:
    """Gets the events for the cluster, including previously polled events."""
    return self.event_poller.GetEvents()

  def __getstate__(self):
    state = self.__dict__.copy()
    if 'event_poller' in state:
      del state['event_poller']
    return state

  def __setstate__(self, state):
    self.__dict__ = state
    self.event_poller = self._InitializeEventPoller()

  @functools.cached_property
  def k8s_version(self) -> str:
    """Actual Kubernetes version reported by server."""
    return kubernetes_commands.GetK8sVersion()

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
    k8s_container = kubernetes.KubernetesContainer(
        container_spec=container_spec, name=name
    )
    self.containers[base_name].append(k8s_container)
    k8s_container.Create()

  def DeployContainerService(
      self, name: str, container_spec: container_spec_lib.ContainerSpec
  ):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    service = kubernetes.KubernetesContainerService(container_spec, name)
    self.services[name] = service
    service.Create()

  @functools.cached_property
  def node_memory_allocatable(self) -> units.Quantity:
    """Usable memory of each node in cluster in KiB."""
    stdout, _, _ = kubectl.RunKubectlCommand(
        # TODO(pclay): Take a minimum of all nodes?
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.allocatable.memory}']
    )
    return units.ParseExpression(stdout)

  @functools.cached_property
  def node_num_cpu(self) -> int:
    """vCPU of each node in cluster."""
    stdout, _, _ = kubectl.RunKubectlCommand(
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.capacity.cpu}']
    )
    return int(stdout)

  def LabelDisks(self):
    """Propagate cluster labels to disks if not done by cloud provider."""
    pass

  def HasLocalSsd(self, nodepool_name: str = 'default') -> bool:
    """Returns true if the given nodepool has local SSDs."""
    raise NotImplementedError

  # TODO(pclay): integrate with kubernetes_disk.
  def GetDefaultStorageClass(self) -> str:
    """Gets the default storage class for the provider."""
    raise NotImplementedError

  def GetNodeSelectors(self, machine_type: str | None = None) -> dict[str, str]:
    """Gets the node selectors section of a yaml for the provider."""
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

  def ApplyManifest(self, manifest_file: str, **kwargs) -> Any:
    """Applies a declarative Kubernetes manifest; possibly with jinja."""
    return kubernetes_commands.ApplyManifest(manifest_file, **kwargs)

  def WaitForResource(
      self,
      resource_name: str,
      condition_name: str,
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      wait_for_all: bool = False,
      condition_type: str = 'condition=',
      extra_args: list[str] | None = None,
      **kwargs,
  ) -> None:
    """Waits for a condition on a Kubernetes resource (eg: deployment, pod)."""
    return kubernetes_commands.WaitForResource(
        resource_name,
        condition_name,
        namespace,
        timeout,
        wait_for_all,
        condition_type,
        extra_args,
        **kwargs,
    )

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

  def AddNodepool(self, batch_name: str, pool_id: str) -> None:
    """Adds an additional nodepool with the given name to the cluster.

    Providers that support dynamic nodepool creation should override this
    method. The default implementation is a no-op.

    Args:
      batch_name: The batch identifier for the new node pool.
      pool_id: The pool identifier within the batch.
    """
    pass

  def CreateNodePool(
      self,
      nodepool_config: container_lib.BaseNodePoolConfig,
      node_version: str | None = None,
  ) -> None:
    """Creates a single named node pool on the cluster (blocks until ready).

    Args:
      nodepool_config: Node pool definition (name, machine type, node count).
      node_version: Optional Kubernetes version to pin the node pool to. None
        means use the cluster default.
    """
    raise NotImplementedError

  def DeleteNodePool(self, name: str) -> None:
    """Deletes the named node pool (blocks until removed)."""
    raise NotImplementedError

  def UpgradeNodePool(self, name: str, target_version: str) -> None:
    """Upgrades the named node pool to the given Kubernetes version."""
    raise NotImplementedError

  def UpdateCluster(self) -> None:
    """Performs a lightweight cluster-level update operation (blocks).

    Intended for management-plane benchmarks that need to overlap a real
    cluster-level operation with a node-pool operation. The implementation
    should issue a control-plane mutation (so an actual operation runs) that
    is non-destructive and idempotent across repeated invocations.
    """
    raise NotImplementedError

  def CreateNodePoolAsync(
      self,
      nodepool_config: container_lib.BaseNodePoolConfig,
      node_version: str | None = None,
  ) -> str:
    """Initiates node-pool create; returns opaque op handle. Does NOT wait."""
    raise NotImplementedError

  def UpgradeNodePoolAsync(self, name: str, target_version: str) -> str:
    """Initiates node-pool upgrade; returns opaque op handle. Does NOT wait."""
    raise NotImplementedError

  def DeleteNodePoolAsync(self, name: str) -> str:
    """Initiates node-pool delete; returns opaque op handle. Does NOT wait."""
    raise NotImplementedError

  def UpdateClusterAsync(self) -> str:
    """Initiates cluster-level update. Returns op handle; does NOT wait."""
    raise NotImplementedError

  @abc.abstractmethod
  def GetNodePoolNames(self) -> list[str]:
    """Returns the names of all node pools currently in the cluster.

    Used by the kubernetes_management benchmark to:
      - Sweep stale pkbm* pools before each run (clean-start spec requirement)
      - Re-list live pools after creates before deleting (avoids stale names)
    """

  def WaitForOperation(self, op_handle: str) -> None:
    """Blocks until the operation identified by op_handle completes.

    Args:
      op_handle: provider-specific opaque string from one of the *Async methods
        above.

    Raises:
      errors.Resource.RetryableCreationError or similar on timeout/failure.
    """
    raise NotImplementedError

  def ResolveNodePoolVersions(self) -> tuple[str, str]:
    """Returns (initial, target) K8s versions per benchmark spec.

    Spec contract:
      target  = cluster's current K8s version (the latest available)
      initial = the adjacent minor below target (e.g., target=1.35 -> 1.34)
    Default implementation returns bare-minor strings ("1.34", "1.35") which
    EKS and AKS accept directly. Providers requiring fully-qualified versions
    (notably GKE) must override.
    """
    target = BareMinor(self.k8s_version)
    initial = AdjacentMinorBelow(self.k8s_version)
    return initial, target


def BareMinor(version: str) -> str:
  """Returns the 'major.minor' part of a K8s version string.

  Accepts and normalizes formats like 'v1.35.4', '1.35.4-gke.1234', '1.35'.

  Args:
    version: A Kubernetes version string in any of the accepted formats.

  Returns:
    A 'major.minor' string, e.g. '1.35'.
  """
  if version.startswith('v'):
    version = version[1:]
  bare = version.split('-', 1)[0]
  parts = bare.split('.')
  if len(parts) < 2 or not parts[0].isdigit() or not parts[1].isdigit():
    raise ValueError(f'Cannot parse K8s version: {version!r}')
  return f'{parts[0]}.{parts[1]}'


def AdjacentMinorBelow(version: str) -> str:
  """Returns the bare minor one below the given version: '1.35.4' -> '1.34'."""
  bare = BareMinor(version)
  major_s, minor_s = bare.split('.')
  minor = int(minor_s)
  if minor <= 0:
    raise ValueError(f'No adjacent minor below {version!r}')
  return f'{major_s}.{minor - 1}'


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

    timeout = 60 * 60  # 1 hour for kubectl delete all -n default (teardown)
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
    kubectl.RunRetryableKubectlCommand(run_cmd, timeout=timeout)
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
  except errors.VmUtil.IssueCommandError as e:
    if 'kubeconfig1: no such file or directory' in str(e):
      logging.info('Kubeconfig not found, assuming cluster is already deleted.')
      return
    raise e
