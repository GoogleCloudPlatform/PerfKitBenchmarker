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
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import data
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
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

_CONTAINER_CLUSTER_ARCHITECTURE = flags.DEFINE_list(
    'container_cluster_architecture',
    ['linux/amd64'],
    'The architecture(s) that the container cluster uses. '
    'Defaults to linux/amd64',
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
