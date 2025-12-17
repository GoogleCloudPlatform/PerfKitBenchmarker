# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Contains classes related to managed container services.

For now this just consists of a base cluster class that other container
services will be derived from and a Kubernetes specific variant. This enables
users to run PKB VM based benchmarks on container providers (e.g. Kubernetes)
without pre-provisioning container clusters. In the future, this may be
expanded to support first-class container benchmarks.
"""

import calendar
import collections
import dataclasses
import datetime
import functools
import ipaddress
import itertools
import json
import logging
import multiprocessing
from multiprocessing import synchronize
import os
import queue as py_queue
import re
import time
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Sequence

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import os_types
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import units
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec as container_spec_lib
from perfkitbenchmarker.resources import kubernetes_inference_server
from perfkitbenchmarker.sample import Sample
import requests
import yaml

BenchmarkSpec = Any  # benchmark_spec lib imports this module.

KUBERNETES = container_spec_lib.KUBERNETES
DEFAULT_NODEPOOL = container_spec_lib.DEFAULT_NODEPOOL
FLAGS = flags.FLAGS

flags.DEFINE_boolean(
    'local_container_build',
    False,
    'Force container images to be built locally rather than '
    'just as a fallback if there is no remote image builder '
    'associated with the registry.',
)

flags.DEFINE_boolean(
    'static_container_image',
    True,
    'Whether container images are static (i.e. are not '
    'managed by PKB). If this is set, PKB will accept the '
    'image as fully qualified (including repository) and will '
    'not attempt to build it.',
)

flags.DEFINE_integer(
    'container_cluster_num_vms',
    None,
    'Number of nodes in the cluster. Defaults to container_cluster.vm_count',
)

flags.DEFINE_string(
    'container_cluster_type', KUBERNETES, 'The type of container cluster.'
)

flags.DEFINE_string(
    'container_cluster_version',
    None,
    'Optional version flag to pass to the cluster create '
    'command. If not specified, the cloud-specific container '
    'implementation will chose an appropriate default.',
)

_CONTAINER_CLUSTER_ARCHITECTURE = flags.DEFINE_list(
    'container_cluster_architecture',
    ['linux/amd64'],
    'The architecture(s) that the container cluster uses. '
    'Defaults to linux/amd64',
)


class ContainerException(errors.Error):
  """Exception during the creation or execution of a container."""


class FatalContainerException(
    errors.Resource.CreationError, ContainerException
):
  """Fatal Exception during the creation or execution of a container."""

  pass


class RetriableContainerException(
    errors.Resource.RetryableCreationError, ContainerException
):
  """Retriable Exception during the creation or execution of a container."""

  pass


class BaseContainer(resource.BaseResource):
  """Class representing a single container."""

  def __init__(
      self, container_spec: container_spec_lib.ContainerSpec | None = None
  ):
    # Hack to make container_spec a kwarg
    assert container_spec
    super().__init__()
    self.cpus: float = container_spec.cpus
    self.memory: int = container_spec.memory
    self.command: list[str] = container_spec.command
    self.image: str = container_spec.image
    self.ip_address: str | None = None

  def WaitForExit(self, timeout: int = 1200) -> dict[str, Any]:
    """Gets the successfully finished container.

    Args:
      timeout: The timeout to wait in seconds

    Raises:
      FatalContainerException: If the container fails
      RetriableContainerException: If the container times out wihout succeeding.
    """
    raise NotImplementedError()

  def GetLogs(self):
    """Returns the logs from the container."""
    raise NotImplementedError()


class BaseContainerService(resource.BaseResource):
  """Class representing a service backed by containers."""

  def __init__(self, container_spec: container_spec_lib.ContainerSpec):
    super().__init__()
    self.cpus: float = container_spec.cpus
    self.memory: int = container_spec.memory
    self.command: list[str] = container_spec.command
    self.image: str = container_spec.image
    self.container_port: int = container_spec.container_port
    self.ip_address: str | None = None
    self.port: int | None = None
    self.host_header: str | None = None


class ContainerImage:
  """Simple class for tracking container image names and source locations."""

  def __init__(self, name: str):
    self.name: str = name
    self.directory: str = os.path.dirname(
        data.ResourcePath(os.path.join('docker', self.name, 'Dockerfile'))
    )


class BaseContainerRegistry(resource.BaseResource):
  """Base class for container image registries."""

  RESOURCE_TYPE = 'BaseContainerRegistry'
  CLOUD: str

  def __init__(self, registry_spec: container_spec_lib.ContainerRegistrySpec):
    super().__init__()
    benchmark_spec: BenchmarkSpec = context.GetThreadBenchmarkSpec()
    container_cluster = getattr(benchmark_spec, 'container_cluster', None)
    zone = getattr(container_cluster, 'zone', None)
    project = getattr(container_cluster, 'project', None)
    self.zone: str = registry_spec.zone or zone
    self.project: str = registry_spec.project or project
    self.name: str = registry_spec.name or 'pkb%s' % FLAGS.run_uri
    self.local_build_times: dict[str, float] = {}
    self.remote_build_times: dict[str, float] = {}
    self.metadata.update({'cloud': self.CLOUD})

  def _Create(self):
    """Creates the image registry."""
    pass

  def _Delete(self):
    """Deletes the image registry."""
    pass

  def GetSamples(self):
    """Returns image build related samples."""
    samples = []
    metadata = self.GetResourceMetadata()
    for image_name, build_time in self.local_build_times.items():
      metadata.update({
          'build_type': 'local',
          'image': image_name,
      })
      samples.append(
          sample.Sample('Image Build Time', build_time, 'seconds', metadata)
      )
    for image_name, build_time in self.remote_build_times.items():
      metadata.update({
          'build_type': 'remote',
          'image': image_name,
      })
      samples.append(
          sample.Sample('Image Build Time', build_time, 'seconds', metadata)
      )
    return samples

  def GetFullRegistryTag(self, image: str):
    """Returns the full name of the image for the registry.

    Args:
      image: The PKB name of the image (string).
    """
    raise NotImplementedError()

  def PrePush(self, image: ContainerImage):
    """Prepares registry to push a given image."""
    pass

  def RemoteBuild(self, image: ContainerImage):
    """Build the image remotely.

    Args:
      image: Instance of _ContainerImage representing the image to build.
    """
    raise NotImplementedError()

  def Login(self):
    """Log in to the registry (in order to push to it)."""
    raise NotImplementedError()

  def LocalBuildAndPush(self, image: ContainerImage):
    """Build the image locally and push to registry.

    Assumes we are already authenticated with the registry from self.Login.
    Building and pushing done in one command to support multiarch images
    https://github.com/docker/buildx/issues/59

    Args:
      image: The image to build.
    """
    full_tag = self.GetFullRegistryTag(image.name)
    # Multiarch images require buildx create
    # https://github.com/docker/build-push-action/issues/302
    vm_util.IssueCommand(['docker', 'buildx', 'create', '--use'])
    cmd = ['docker', 'buildx', 'build']
    if _CONTAINER_CLUSTER_ARCHITECTURE.value:
      cmd += ['--platform', ','.join(_CONTAINER_CLUSTER_ARCHITECTURE.value)]
    cmd += ['--no-cache', '--push', '-t', full_tag, image.directory]
    vm_util.IssueRetryableCommand(cmd, timeout=None)
    vm_util.IssueCommand(['docker', 'buildx', 'stop'])

  def GetOrBuild(self, image: str):
    """Finds the image in the registry or builds it.

    TODO(pclay): Add support for build ARGs.

    Args:
      image: The PKB name for the image (string).

    Returns:
      The full image name (including the registry).
    """
    full_image = self.GetFullRegistryTag(image)
    self.Login()
    self._Build(image)
    return full_image

  def _Build(self, image: str):
    """Builds the image and pushes it to the registry if necessary.

    Args:
      image: The PKB name for the image (string).
    """
    image = ContainerImage(image)
    if not FLAGS.local_container_build:
      try:
        build_start = time.time()
        # Build the image remotely using an image building service.
        self.RemoteBuild(image)
        self.remote_build_times[image.name] = time.time() - build_start
        return
      except NotImplementedError:
        pass

    # TODO(pclay): Refactor ECR and remove.
    self.PrePush(image)
    # Build the image locally using docker.
    build_start = time.time()
    self.LocalBuildAndPush(image)
    self.local_build_times[image.name] = time.time() - build_start


def GetContainerRegistryClass(cloud: str) -> type[BaseContainerRegistry]:
  return resource.GetResourceClass(BaseContainerRegistry, CLOUD=cloud)


def NodePoolName(name: str) -> str:
  """Clean node pool names to be usable by all providers."""
  # GKE (or k8s?) requires nodepools use alphanumerics and hyphens
  # AKS requires full alphanumeric
  # PKB likes to use underscores strip them out.
  return name.replace('_', '')


class BaseNodePoolConfig:
  """A node pool's config, where each node in the node pool has the same config.

  See also: https://cloud.google.com/kubernetes-engine/docs/concepts/node-pools
  """

  def __init__(self, vm_config: virtual_machine.BaseVirtualMachine, name: str):
    # Use Virtual Machine class to resolve VM Spec. Note there is no actual VM;
    # we just use it as a data holder to let VM subclass __init__'s handle
    # parsing specific information like disks out of the spec.
    self.machine_type = vm_config.machine_type
    self.name = NodePoolName(name)
    self.zone: str = vm_config.zone
    self.sandbox_config: container_spec_lib.SandboxSpec | None = None
    self.num_nodes: int
    self.disk_type: str
    self.disk_size: int
    # Defined by gce_virtual_machine. Used by google_kubernetes_engine
    self.max_local_disks: int | None
    self.ssd_interface: str | None
    self.gpu_type: str | None
    self.gpu_count: int | None
    self.threads_per_core: int
    self.gce_tags: list[str]
    self.min_cpu_platform: str
    self.cpus: int
    self.memory_mib: int


class BaseContainerCluster(resource.BaseResource):
  """A cluster that can be used to schedule containers."""

  RESOURCE_TYPE = 'BaseContainerCluster'
  REQUIRED_ATTRS = ['CLOUD', 'CLUSTER_TYPE']
  CLOUD: str
  CLUSTER_TYPE: str

  def __init__(self, cluster_spec: container_spec_lib.ContainerClusterSpec):
    super().__init__(user_managed=bool(cluster_spec.static_cluster))
    self.name: str = cluster_spec.static_cluster or 'pkb-' + FLAGS.run_uri
    # Go get a BaseVM, to use strictly for config values.
    default_vm_class = virtual_machine.GetVmClass(
        self.CLOUD, os_types.DEFAULT, provider_info.DEFAULT_VM_PLATFORM
    )
    default_vm_config: virtual_machine.BaseVirtualMachine = default_vm_class(
        cluster_spec.vm_spec
    )  # pytype: disable=not-instantiable
    self.default_nodepool = self._InitializeDefaultNodePool(
        cluster_spec, default_vm_config
    )
    self.nodepools: dict[str, BaseNodePoolConfig] = {}
    for name, nodepool_spec in cluster_spec.nodepools.copy().items():
      vm_config: virtual_machine.BaseVirtualMachine = default_vm_class(
          nodepool_spec.vm_spec
      )  # pytype: disable=not-instantiable
      nodepool = self._InitializeNodePool(name, nodepool_spec, vm_config)
      self.nodepools[nodepool.name] = nodepool
    self.min_nodes: int = (
        cluster_spec.min_vm_count or self.default_nodepool.num_nodes
    )
    self.max_nodes: int = (
        cluster_spec.max_vm_count or self.default_nodepool.num_nodes
    )
    self.containers: dict[str, list[KubernetesContainer]] = (
        collections.defaultdict(list)
    )
    self.services: dict[str, KubernetesContainerService] = {}
    self._extra_samples: list[sample.Sample] = []
    self.container_registry: BaseContainerRegistry | None = None
    self.enable_vpa: bool = cluster_spec.enable_vpa

  @property
  def num_nodes(self) -> int:
    return self.default_nodepool.num_nodes

  @property
  def zone(self) -> str:
    return self.default_nodepool.zone

  def SetContainerRegistry(self, container_registry):
    """Sets the container registry for the cluster."""
    self.container_registry = container_registry

  def _InitializeDefaultNodePool(
      self,
      cluster_spec: container_spec_lib.ContainerClusterSpec,
      vm_config: virtual_machine.BaseVirtualMachine,
  ) -> BaseNodePoolConfig:
    nodepool_config = BaseNodePoolConfig(
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
      vm_config: virtual_machine.BaseVirtualMachine,
  ) -> BaseNodePoolConfig:
    zone = (
        nodepool_spec.vm_spec.zone
        if nodepool_spec.vm_spec
        else self.default_nodepool.zone
    )
    nodepool_config = BaseNodePoolConfig(
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
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: BaseNodePoolConfig,
  ):
    """Override to initialize cloud specific configs."""
    pass

  def GetNodePoolFromNodeName(
      self, node_name: str
  ) -> BaseNodePoolConfig | None:
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
    for container in itertools.chain(*list(self.containers.values())):
      container.Delete()

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
    for container in itertools.chain(*list(self.containers.values())):
      metadata = {'image': container.image.split('/')[-1]}
      if container.resource_ready_time and container.create_start_time:
        samples.append(
            sample.Sample(
                'Container Deployment Time',
                container.resource_ready_time - container.create_start_time,
                'seconds',
                metadata,
            )
        )
      if container.delete_end_time and container.delete_start_time:
        samples.append(
            sample.Sample(
                'Container Delete Time',
                container.delete_end_time - container.delete_start_time,
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
) -> type[BaseContainerCluster]:
  return resource.GetResourceClass(
      BaseContainerCluster, CLOUD=cloud, CLUSTER_TYPE=cluster_type
  )


# Temporarily hoist kubernetes related classes into this namespace
# (Do this at the bottom of the file to avoid circular imports)
# pylint: disable=g-import-not-at-top
from .kubernetes import INGRESS_JSONPATH
from .kubernetes import KubernetesCluster
from .kubernetes import KubernetesClusterCommands
from .kubernetes import KubernetesContainer
from .kubernetes import KubernetesContainerService
from .kubernetes import KubernetesEvent
from .kubernetes import KubernetesEventPoller
from .kubernetes import KubernetesEventResource
from .kubernetes import KubernetesPod
from .kubernetes import RETRYABLE_KUBECTL_ERRORS
from .kubernetes import RunKubectlCommand
from .kubernetes import RunRetryableKubectlCommand
