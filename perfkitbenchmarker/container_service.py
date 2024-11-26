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
import os
import re
import time
from typing import Any, Sequence

from absl import flags
import jinja2
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
import requests
import yaml

BenchmarkSpec = Any  # benchmark_spec lib imports this module.

KUBERNETES = container_spec_lib.KUBERNETES
DEFAULT_NODEPOOL = container_spec_lib.DEFAULT_NODEPOOL

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'kubeconfig',
    None,
    'Path to kubeconfig to be used by kubectl. '
    "If unspecified, it will be set to a file in this run's "
    'temporary directory.',
)

flags.DEFINE_string('kubectl', 'kubectl', 'Path to kubectl tool')

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
RESOURCE_DELETE_SLEEP_SECONDS = 5
_RETRYABLE_KUBECTL_ERRORS = [
    (
        '"unable to decode an event from the watch stream: http2: client'
        ' connection lost"'
    ),
    'Unable to connect to the server: dial tcp',
]


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


def RunKubectlCommand(command: list[str], **kwargs):
  """Run a kubectl command."""
  if 'stack_level' in kwargs:
    kwargs['stack_level'] += 1
  else:
    # IssueCommand defaults stack_level to 1, so 2 skips this function.
    kwargs['stack_level'] = 2
  cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig] + command
  return vm_util.IssueCommand(cmd, **kwargs)


class RetryableKubectlError(Exception):
  """Exception for retryable kubectl errors."""


@vm_util.Retry(retryable_exceptions=(RetryableKubectlError))
def RunRetryableKubectlCommand(
    run_cmd: list[str], **kwargs
) -> tuple[str, str, int]:
  """Runs a kubectl command, retrying somewhat exepected errors."""
  out, err, code = RunKubectlCommand(run_cmd, raise_on_failure=False, **kwargs)
  if err:
    for error_substring in _RETRYABLE_KUBECTL_ERRORS:
      if error_substring in err:
        raise RetryableKubectlError(
            f'Tried running {run_cmd} but it failed with the substring'
            f' {error_substring}. Retrying. Full error is: {err}'
        )
    raise errors.VmUtil.IssueCommandError(err)
  return out, err, code


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
    vm_util.IssueCommand(cmd, timeout=None)
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


@events.benchmark_start.connect
def _SetKubeConfig(unused_sender, benchmark_spec: BenchmarkSpec):
  """Sets the value for the kubeconfig flag if it's unspecified."""
  if not FLAGS.kubeconfig:
    FLAGS.kubeconfig = vm_util.PrependTempDir(
        'kubeconfig' + str(benchmark_spec.sequence_number)
    )
    # Store the value for subsequent run stages.
    benchmark_spec.config.flags['kubeconfig'] = FLAGS.kubeconfig


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
    # Defined by google_kubernetes_engine
    self.max_local_disks: int | None
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
    )
    self.default_nodepool = self._InitializeDefaultNodePool(
        cluster_spec, default_vm_config
    )
    self.nodepools: dict[str, BaseNodePoolConfig] = {}
    for name, nodepool_spec in cluster_spec.nodepools.copy().items():
      vm_config: virtual_machine.BaseVirtualMachine = default_vm_class(
          nodepool_spec.vm_spec
      )
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

  @property
  def num_nodes(self) -> int:
    return self.default_nodepool.num_nodes

  @property
  def zone(self) -> str:
    return self.default_nodepool.zone

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

    return samples

  def ResizeNodePool(self, new_size: int, node_pool: str = DEFAULT_NODEPOOL):
    """Change the number of nodes in the node pool."""
    raise NotImplementedError


def GetContainerClusterClass(
    cloud: str, cluster_type: str
) -> type[BaseContainerCluster]:
  return resource.GetResourceClass(
      BaseContainerCluster, CLOUD=cloud, CLUSTER_TYPE=cluster_type
  )


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
      raise FatalContainerException(
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
          raise FatalContainerException(
              f"Pod {self.name} failed to schedule:\n{condition['message']}"
          )
      for container_status in status.get('containerStatuses', []):
        waiting_status = container_status['state'].get('waiting', {})
        if waiting_status.get('reason') in [
            'ErrImagePull',
            'ImagePullBackOff',
        ]:
          raise FatalContainerException(
              f'Failed to find container image for {self.name}:\n'
              + yaml.dump(waiting_status.get('message'))
          )

  def WaitForExit(self, timeout: int | None = None) -> dict[str, Any]:
    """Gets the finished running container."""

    @vm_util.Retry(
        timeout=timeout, retryable_exceptions=(RetriableContainerException,)
    )
    def _WaitForExit():
      pod = self._GetPod()
      status = pod['status']
      self._ValidatePodHasNotFailed(status)
      phase = status['phase']
      if phase == 'Succeeded':
        return pod
      else:
        raise RetriableContainerException(
            f'Pod phase ({phase}) not in finished phases.'
        )

    return _WaitForExit()

  def GetLogs(self):
    """Returns the logs from the container."""
    stdout, _, _ = RunKubectlCommand(['logs', self.name])
    return stdout


# Order KubernetesPod first so that it's constructor is called first.
class KubernetesContainer(KubernetesPod, BaseContainer):
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


class KubernetesContainerService(BaseContainerService):
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
      url = 'http://%s' % (self.ip_address)
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


class KubernetesClusterCommands:
  """Implementation of many Kubernetes commands.

  All methods just call generic kubectl commands without needing instance
  information.
  """

  @staticmethod
  def _DeleteAllFromDefaultNamespace():
    """Deletes all resources from a namespace.

    Since StatefulSets do not reclaim PVCs upon deletion, they are explicitly
    deleted here to prevent dynamically provisioned PDs from leaking once the
    cluster has been deleted.
    """
    run_cmd = ['delete', 'all', '--all', '-n', 'default']
    RunRetryableKubectlCommand(run_cmd, timeout=60 * 10)
    run_cmd = ['delete', 'pvc', '--all', '-n', 'default']
    RunKubectlCommand(run_cmd)
    # There maybe a slight race if resources are cleaned up in the background
    # where deleting the cluster immediately prevents the PVCs from being
    # deleted.
    logging.info(
        'Sleeping for %s seconds to give resources time to delete.',
        RESOURCE_DELETE_SLEEP_SECONDS,
    )
    time.sleep(RESOURCE_DELETE_SLEEP_SECONDS)

  @staticmethod
  def ApplyManifest(manifest_file: str, **kwargs) -> str:
    """Applies a declarative Kubernetes manifest; possibly with jinja.

    Args:
      manifest_file: The name of the YAML file or YAML template.
      **kwargs: Arguments to the jinja template.

    Returns:
      The name of the deployment, e.g. deployment.apps/mydeploy.
    """

    def _ParseApplyOutput(stdout: str) -> str:
      """Parses the output of kubectl apply to get the name of the resource."""
      # Example input: deployment.apps/pkb123 created
      match = re.search(r'deployment[^\s]+', stdout)
      if not match:
        match = re.search(r'daemonset[^\s]+', stdout)
        if not match:
          raise ValueError(
              'Failed to parse the output of kubectl apply to get the name of'
              ' the deployment.'
          )
      return match.group()

    filename = data.ResourcePath(manifest_file)
    if not filename.endswith('.j2'):
      assert not kwargs
      out, _, _ = RunKubectlCommand(['apply', '-f', filename])
      return _ParseApplyOutput(out)

    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    with open(filename) as template_file, vm_util.NamedTemporaryFile(
        mode='w', suffix='.yaml'
    ) as rendered_template:
      manifest = environment.from_string(template_file.read()).render(kwargs)
      rendered_template.write(manifest)
      rendered_template.close()
      out, _, _ = RunKubectlCommand(['apply', '-f', rendered_template.name])
      return _ParseApplyOutput(out)

  @staticmethod
  def WaitForResource(
      resource_name: str,
      condition_name: str,
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      wait_for_all: bool = False,
  ):
    """Waits for a condition on a Kubernetes resource (eg: deployment, pod)."""
    run_cmd = [
        'wait',
        f'--for=condition={condition_name}',
        f'--timeout={timeout}s',
        resource_name,
    ]
    if namespace:
      run_cmd.append(f'--namespace={namespace}')
    if wait_for_all:
      run_cmd.append('--all')
    RunKubectlCommand(run_cmd, timeout=timeout + 10)

  @staticmethod
  def WaitForRollout(
      resource_name: str, timeout: int = vm_util.DEFAULT_TIMEOUT
  ):
    """Blocks until a Kubernetes rollout is completed."""
    run_cmd = [
        'rollout',
        'status',
        '--timeout=%ds' % timeout,
        resource_name,
    ]

    RunRetryableKubectlCommand(
        run_cmd,
        timeout=timeout,
    )

  @staticmethod
  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def GetLoadBalancerIP(service_name: str):
    """Returns the IP address of a LoadBalancer service when ready."""
    get_cmd = [
        'get',
        'service',
        service_name,
        '-o',
        'jsonpath={.status.loadBalancer.ingress[0].ip}',
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)

    try:
      # Ensure the load balancer is ready by parsing the output IP
      ip_address = ipaddress.ip_address(stdout)
    except ValueError:
      raise errors.Resource.RetryableCreationError(
          "Load Balancer IP for service '%s' is not ready." % service_name
      )

    return format(ip_address)

  @staticmethod
  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def GetClusterIP(service_name: str) -> str:
    """Returns the IP address of a ClusterIP service when ready."""
    get_cmd = [
        'get',
        'service',
        service_name,
        '-o',
        'jsonpath={.spec.clusterIP}',
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)

    if not stdout:
      raise errors.Resource.RetryableCreationError(
          "ClusterIP for service '%s' is not ready." % service_name
      )

    return stdout

  @staticmethod
  def CreateConfigMap(name: str, from_file_dir: str):
    """Creates a Kubernetes ConfigMap.

    Args:
      name: The name of the ConfigMap to create
      from_file_dir: The directory name containing files that will be key/values
        in the ConfigMap
    """
    RunKubectlCommand(
        ['create', 'configmap', name, '--from-file', from_file_dir]
    )

  @staticmethod
  def CreateServiceAccount(
      name: str, clusterrole: str | None = None, namespace='default'
  ):
    """Create a k8s service account and cluster-role-binding."""
    RunKubectlCommand(
        ['create', 'serviceaccount', name, '--namespace', namespace]
    )
    if clusterrole:
      # TODO(pclay): Support customer cluster roles?
      RunKubectlCommand([
          'create',
          'clusterrolebinding',
          f'{name}-role',
          f'--clusterrole={clusterrole}',
          f'--serviceaccount={namespace}:{name}',
          '--namespace',
          namespace,
      ])

  @property
  @functools.lru_cache(maxsize=1)
  def k8s_version(self) -> str:
    """Actual Kubernetes version reported by server."""
    stdout, _, _ = RunKubectlCommand(['version', '-o', 'yaml'])
    return yaml.safe_load(stdout)['serverVersion']['gitVersion']

  @staticmethod
  def GetPodLabel(resource_name):
    run_cmd = [
        'get',
        resource_name,
        '-o',
        'jsonpath="{.spec.selector.matchLabels.app}"',
    ]

    stdout, _, _ = RunKubectlCommand(run_cmd)
    return yaml.safe_load(stdout)

  @staticmethod
  def GetPodIpsByLabel(pod_label_key, pod_label_value) -> list[str]:
    """Returns a list of internal IPs for pod label key-value.

    Args:
      pod_label_key: The pod label name
      pod_label_value: The pod label value
    """
    get_cmd = [
        'get',
        'pods',
        '-l',
        f'{pod_label_key}={pod_label_value}',
        '-o',
        'jsonpath="{.items[*].status.podIP}"',
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)
    return yaml.safe_load(stdout).split()

  @staticmethod
  def GetPodIps(resource_name) -> list[str]:
    """Returns a list of internal IPs for a pod name.

    Args:
      resource_name: The pod resource name
    """
    pod_label = KubernetesClusterCommands.GetPodLabel(resource_name)
    return KubernetesClusterCommands.GetPodIpsByLabel('app', pod_label)

  @staticmethod
  def GetAllPodNames() -> list[str]:
    """Returns all pod names in the cluster."""
    pods, _, _ = RunKubectlCommand([
        'get',
        'pods',
        '--no-headers',
        '-o',
        'name',
    ])
    if not pods:
      return []
    pod_names_with_prefix = pods.split()
    # Remove the 'pod/' prefix from each pod name.
    return [pod_name.replace('pod/', '') for pod_name in pod_names_with_prefix]

  @staticmethod
  def RunKubectlExec(pod_name, cmd):
    run_cmd = ['exec', '-it', pod_name, '--'] + cmd
    RunKubectlCommand(run_cmd)

  @staticmethod
  def _GetPvcs() -> Sequence[Any]:
    stdout, _, _ = RunKubectlCommand(['get', 'pvc', '-o', 'yaml'])
    return yaml.safe_load(stdout)['items']

  @staticmethod
  def GetNodeNames() -> list[str]:
    """Get the node names for the cluster."""
    stdout, _, _ = RunKubectlCommand(
        ['get', 'nodes', '-o', 'jsonpath={.items[*].metadata.name}']
    )
    return stdout.split()

  @staticmethod
  def GetEvents():
    """Get the events for the cluster."""
    stdout, _, _ = RunKubectlCommand(['get', 'events', '-o', 'yaml'])
    k8s_events = []
    for item in yaml.safe_load(stdout)['items']:
      k8s_event = KubernetesEvent.FromDict(item)
      if k8s_event:
        k8s_events.append(k8s_event)
    return k8s_events


class KubernetesCluster(BaseContainerCluster, KubernetesClusterCommands):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = KUBERNETES

  def _Delete(self):
    self._DeleteAllFromDefaultNamespace()

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster."""
    result = super().GetResourceMetadata()
    if self.created:
      result['container_cluster_version'] = self.k8s_version
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


@dataclasses.dataclass
class KubernetesEventResource:
  """Holder for Kubernetes event involved objects."""

  kind: str
  name: str

  @classmethod
  def FromDict(cls, yaml_data: dict[str, Any]) -> 'KubernetesEventResource':
    """Parse Kubernetes Event YAML output."""
    return cls(kind=yaml_data['kind'], name=yaml_data['name'])


@dataclasses.dataclass
class KubernetesEvent:
  """Holder for Kubernetes event data."""

  resource: KubernetesEventResource
  message: str
  # Reason is actually more of a machine readable message.
  reason: str | None
  timestamp: float

  @classmethod
  def FromDict(cls, yaml_data: dict[str, Any]) -> 'KubernetesEvent | None':
    """Parse Kubernetes Event YAML output."""
    if 'message' not in yaml_data:
      return
    try:
      # There are multiple timestamps. They should be equivalent.
      raw_timestamp = yaml_data['lastTimestamp']
      assert raw_timestamp
      # Python 3.10 cannot handle Z as utc in ISO 8601 timestamps
      python_3_10_compatible_timestamp = re.sub('Z$', '+00:00', raw_timestamp)
      timestamp = calendar.timegm(
          datetime.datetime.fromisoformat(
              python_3_10_compatible_timestamp
          ).timetuple()
      )
      return cls(
          message=yaml_data['message'],
          reason=yaml_data.get('reason'),
          resource=KubernetesEventResource.FromDict(
              yaml_data['involvedObject']
          ),
          timestamp=timestamp,
      )
    except KeyError as e:
      logging.exception(
          'Tried parsing event: %s but ran into error: %s', yaml_data, e
      )
      raise e
