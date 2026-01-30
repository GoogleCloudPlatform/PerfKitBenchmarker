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
"""Contains classes related to managed container services."""

import os
from typing import Any

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import container_spec as container_spec_lib


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
