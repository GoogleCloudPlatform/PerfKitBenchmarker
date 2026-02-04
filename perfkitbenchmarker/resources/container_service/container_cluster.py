"""Cluster related methods/classes for container_service."""

import collections
import itertools
from typing import Callable, Iterable

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.resources.container_service import container
from perfkitbenchmarker.resources.container_service import container_registry


DEFAULT_NODEPOOL = container_spec_lib.DEFAULT_NODEPOOL

FLAGS = flags.FLAGS


class BaseContainerCluster(resource.BaseResource):
  """A cluster that can be used to schedule containers."""

  RESOURCE_TYPE = 'BaseContainerCluster'
  REQUIRED_ATTRS = ['CLOUD', 'CLUSTER_TYPE']
  CLOUD: str
  CLUSTER_TYPE: str

  def __init__(self, cluster_spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(user_managed=bool(cluster_spec.static_cluster))
    self.name: str = (
        cluster_spec.static_cluster or 'pkb-' + FLAGS.run_uri
    )
    self.default_nodepool = self._InitializeDefaultNodePool(
        cluster_spec, cluster_spec.vm_spec
    )
    self.nodepools: dict[str, container.BaseNodePoolConfig] = {}
    for name, nodepool_spec in cluster_spec.nodepools.copy().items():
      nodepool = self._InitializeNodePool(
          name, nodepool_spec, nodepool_spec.vm_spec
      )
      self.nodepools[nodepool.name] = nodepool
    self.min_nodes: int = (
        cluster_spec.min_vm_count or self.default_nodepool.num_nodes
    )
    self.max_nodes: int = (
        cluster_spec.max_vm_count or self.default_nodepool.num_nodes
    )
    self.containers: dict[str, list[container.BaseContainer]] = (
        collections.defaultdict(list)
    )
    self.services: dict[str, container.BaseContainerService] = {}
    self._extra_samples: list[sample.Sample] = []
    self.container_registry: container_registry.BaseContainerRegistry | None = (
        None
    )
    self.enable_vpa: bool = cluster_spec.enable_vpa

  @property
  def num_nodes(self) -> int:
    return self.default_nodepool.num_nodes

  @property
  def zone(self) -> str:
    return self.default_nodepool.zone

  def SetContainerRegistry(self, registry):
    """Sets the container registry for the cluster."""
    self.container_registry = registry

  def _InitializeDefaultNodePool(
      self,
      cluster_spec: container_spec_lib.ContainerClusterSpec,
      vm_config: virtual_machine.BaseVmSpec,
  ) -> container.BaseNodePoolConfig:
    nodepool_config = container.BaseNodePoolConfig(
        vm_config,
        DEFAULT_NODEPOOL,
    )
    nodepool_config.num_nodes = cluster_spec.vm_count
    self.InitializeNodePoolForCloud(vm_config, nodepool_config)
    return nodepool_config

  def _InitializeNodePool(
      self,
      name: str,
      nodepool_spec: container_spec_lib.NodepoolSpec,
      vm_config: virtual_machine.BaseVmSpec,
  ) -> container.BaseNodePoolConfig:
    zone = (
        nodepool_spec.vm_spec.zone
        if nodepool_spec.vm_spec
        else self.default_nodepool.zone
    )
    nodepool_config = container.BaseNodePoolConfig(
        vm_config,
        name,
    )
    nodepool_config.sandbox_config = nodepool_spec.sandbox_config
    nodepool_config.zone = zone
    nodepool_config.num_nodes = nodepool_spec.vm_count
    self.InitializeNodePoolForCloud(vm_config, nodepool_config)
    return nodepool_config

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVmSpec,
      nodepool_config: container.BaseNodePoolConfig,
  ):
    """Override to initialize cloud specific configs."""
    pass

  def GetNodePoolFromNodeName(
      self, node_name: str
  ) -> container.BaseNodePoolConfig | None:
    """Get the nodepool from the node name.

    This method assumes that the nodepool name is embedded in the node name.
    Better would be a lookup from the cloud provider.

    Args:
      node_name: The name of the node.

    Returns:
      The associated nodepool, or None if not found.
    """
    nodepool_names = self.nodepools.keys()
    found_pools = []
    if '-default-' in node_name:
      found_pools.append(self.default_nodepool)
    for pool_name in nodepool_names:
      if f'-{pool_name}-' in node_name:
        found_pools.append(self.nodepools[pool_name])
    if len(found_pools) == 1:
      return found_pools[0]
    if len(found_pools) > 1:
      raise ValueError(
          f'Multiple nodepools found for node with name {node_name}:'
          f' {found_pools}. Please change the name of the nodepools used to'
          ' avoid this.'
      )
    return None

  def GetMachineTypeFromNodeName(self, node_name: str) -> str | None:
    """Get the machine type from the node name."""
    nodepool = self.GetNodePoolFromNodeName(node_name)
    if nodepool is None:
      return None
    return nodepool.machine_type

  def DeleteContainers(self):
    """Delete containers belonging to the cluster."""
    for c in itertools.chain(*list(self.containers.values())):
      c.Delete()

  def DeleteServices(self):
    """Delete services belonging to the cluster."""
    for service in self.services.values():
      service.Delete()

  def GetResourceMetadata(self):
    """Returns a dictionary of cluster metadata."""
    nodepools_metadata = {}
    for name, nodepool in self.nodepools.items():
      nodepool_metadata = {
          'size': nodepool.num_nodes,
          'machine_type': nodepool.machine_type,
          'name': name,
      }
      if nodepool.sandbox_config is not None:
        nodepool_metadata['sandbox_config'] = {
            'type': nodepool.sandbox_config.type,
        }
      nodepools_metadata[name] = nodepool_metadata

    metadata = {
        'cloud': self.CLOUD,
        'cluster_type': self.CLUSTER_TYPE,
        'zone': self.default_nodepool.zone,
        'size': self.default_nodepool.num_nodes,
        'machine_type': self.default_nodepool.machine_type,
        'nodepools': nodepools_metadata,
    }

    if (
        self.min_nodes != self.default_nodepool.num_nodes
        or self.max_nodes != self.default_nodepool.num_nodes
    ):
      metadata.update({
          'max_size': self.max_nodes,
          'min_size': self.min_nodes,
      })

    return metadata

  def DeployContainer(self, name, container_spec):
    """Deploys Containers according to the ContainerSpec."""
    raise NotImplementedError()

  def DeployContainerService(self, name, container_spec):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    raise NotImplementedError()

  def AddSamples(self, samples: Iterable[sample.Sample]):
    self._extra_samples += samples

  def GetSamples(self):
    """Return samples with information about deployment times."""
    samples = super().GetSamples()
    for c in itertools.chain(*list(self.containers.values())):
      metadata = {'image': c.image.split('/')[-1]}
      if c.resource_ready_time and c.create_start_time:
        samples.append(
            sample.Sample(
                'Container Deployment Time',
                c.resource_ready_time - c.create_start_time,
                'seconds',
                metadata,
            )
        )
      if c.delete_end_time and c.delete_start_time:
        samples.append(
            sample.Sample(
                'Container Delete Time',
                c.delete_end_time - c.delete_start_time,
                'seconds',
                metadata,
            )
        )
    for service in self.services.values():
      metadata = {'image': service.image.split('/')[-1]}
      if service.resource_ready_time and service.create_start_time:
        samples.append(
            sample.Sample(
                'Service Deployment Time',
                service.resource_ready_time - service.create_start_time,
                'seconds',
                metadata,
            )
        )
      if service.delete_end_time and service.delete_start_time:
        samples.append(
            sample.Sample(
                'Service Delete Time',
                service.delete_end_time - service.delete_start_time,
                'seconds',
                metadata,
            )
        )

    samples += self._extra_samples

    return samples

  def ResizeNodePool(self, new_size: int, node_pool: str = DEFAULT_NODEPOOL):
    """Change the number of nodes in the node pool."""
    raise NotImplementedError

  def GetNodePoolNames(self) -> list[str]:
    """Get node pool names for the cluster."""
    raise NotImplementedError


def GetContainerClusterClass(
    cloud: str, cluster_type: str
) -> Callable[[container_spec_lib.ContainerClusterSpec], BaseContainerCluster]:
  return resource.GetResourceClass(
      BaseContainerCluster, CLOUD=cloud, CLUSTER_TYPE=cluster_type
  )
