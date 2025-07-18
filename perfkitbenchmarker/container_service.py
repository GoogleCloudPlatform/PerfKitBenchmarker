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
import re
import time
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Sequence

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
from perfkitbenchmarker.resources import kubernetes_inference_server
from perfkitbenchmarker.sample import Sample
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
RETRYABLE_KUBECTL_ERRORS = [
    (
        '"unable to decode an event from the watch stream: http2: client'
        ' connection lost"'
    ),
    'read: connection reset by peer',
    'Unable to connect to the server: dial tcp',
    'Unable to connect to the server: net/http: TLS handshake timeout',
    # These come up in kubectl exec
    'error dialing backend:',
    'connect: connection timed out',
    'error sending request:',
    '(abnormal closure): unexpected EOF',
]
INGRESS_JSONPATH = '{.status.loadBalancer.ingress[0]}'


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


def RunKubectlCommand(command: list[str], **kwargs) -> tuple[str, str, int]:
  """Run a kubectl command."""
  kwargs = vm_util.IncrementStackLevel(**kwargs)
  cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig] + command

  orig_suppress_failure = None
  if 'suppress_failure' in kwargs:
    orig_suppress_failure = kwargs['suppress_failure']

  def _DetectTimeoutViaSuppressFailure(stdout, stderr, retcode):
    # Check for kubectl timeout. If found, treat it the same as a regular
    # timeout.
    if retcode != 0:
      for error_substring in RETRYABLE_KUBECTL_ERRORS:
        if error_substring in stderr:
          # Raise timeout error regardless of raise_on_failure - as the intended
          # semantics is to ignore expected errors caused by invoking the
          # command not errors from PKB infrastructure.
          raise_on_timeout = (
              kwargs['raise_on_timeout']
              if 'raise_on_timeout' in kwargs
              else True
          )
          if raise_on_timeout:
            raise errors.VmUtil.IssueCommandTimeoutError(stderr)
    # Else, if the user supplied a suppress_failure function, try that.
    if orig_suppress_failure is not None:
      return orig_suppress_failure(stdout, stderr, retcode)

    # Else, no suppression.
    return False

  kwargs['suppress_failure'] = _DetectTimeoutViaSuppressFailure

  return vm_util.IssueCommand(cmd, **kwargs)


def RunRetryableKubectlCommand(
    run_cmd: list[str], timeout: int | None = None, **kwargs
) -> tuple[str, str, int]:
  """Runs a kubectl command, retrying somewhat exepected errors."""
  if 'raise_on_timeout' in kwargs and kwargs['raise_on_timeout']:
    raise ValueError(
        'RunRetryableKubectlCommand does not allow `raise_on_timeout=True`'
        ' (since timeouts are retryable).'
    )

  kwargs = vm_util.IncrementStackLevel(**kwargs)

  @vm_util.Retry(
      timeout=timeout,
      retryable_exceptions=(errors.VmUtil.IssueCommandTimeoutError,),
  )
  def _RunRetryablePart(run_cmd: list[str], **kwargs) -> tuple[str, str, int]:
    """Inner function retries command so timeout can be passed to decorator."""
    kwargs['stack_level'] += 1
    return RunKubectlCommand(run_cmd, raise_on_timeout=True, **kwargs)

  return _RunRetryablePart(run_cmd, timeout=timeout, **kwargs)


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
    try:
      # Delete deployments and jobs first as otherwise autorepair will redeploy
      # deleted pods.
      run_cmd = ['delete', 'deployment', '--all', '-n', 'default']
      RunRetryableKubectlCommand(run_cmd)

      run_cmd = ['delete', 'job', '--all', '-n', 'default']
      RunRetryableKubectlCommand(run_cmd)

      timeout = 60 * 20
      run_cmd = [
          'delete',
          'all',
          '--all',
          '-n',
          'default',
          f'--timeout={timeout}s',
      ]
      RunRetryableKubectlCommand(run_cmd, timeout=timeout)

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
    except (
        errors.VmUtil.IssueCommandTimeoutError,
        vm_util.TimeoutExceededRetryError,
    ) as e:
      raise errors.Resource.RetryableDeletionError(
          'Timed out while deleting all resources from default namespace. We'
          ' should still continue trying to delete everything.'
      ) from e

  @staticmethod
  def ApplyManifest(
      manifest_file: str, should_log_file: bool = True, **kwargs
  ) -> Iterator[str]:
    """Applies a declarative Kubernetes manifest; possibly with jinja.

    Args:
      manifest_file: The name of the YAML file or YAML template.
      should_log_file: Whether to log the rendered manifest to stdout or not.
      **kwargs: Arguments to the jinja template.

    Returns:
      Names of the resources, e.g. [deployment.apps/mydeploy, pod/foo]
    """

    def _ParseApplyOutput(stdout: str) -> Iterator[str]:
      """Parses the output of kubectl apply to get the name of the resource."""
      # Example input: deployment.apps/pkb123 created
      for line in stdout.splitlines():
        match = re.search(r'([^\s/]+/[^\s/]+) created', line)
        if match:
          yield match.group(1)

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
      if should_log_file:
        logging.info(
            'Rendered manifest file %s with contents:\n%s',
            rendered_template.name,
            manifest,
        )
      out, _, _ = RunKubectlCommand(['apply', '-f', rendered_template.name])
      return _ParseApplyOutput(out)

  @staticmethod
  def WaitForResource(
      resource_name: str,
      condition_name: str,
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      wait_for_all: bool = False,
      condition_type='condition=',
      extra_args: list[str] | None = None,
  ):
    """Waits for a condition on a Kubernetes resource (eg: deployment, pod)."""
    run_cmd = [
        'wait',
        f'--for={condition_type}{condition_name}',
        f'--timeout={timeout}s',
        resource_name,
    ]
    if namespace:
      run_cmd.append(f'--namespace={namespace}')
    if wait_for_all:
      run_cmd.append('--all')
    if extra_args:
      run_cmd.extend(extra_args)
    RunKubectlCommand(run_cmd, timeout=timeout + 10)

  @staticmethod
  def WaitForSucceeded(
      resource_name: str,
      namespace: str | None = None,
      timeout: int = vm_util.DEFAULT_TIMEOUT,
      raise_on_failure: bool = True,
  ) -> tuple[str, str, int]:
    """Waits for a resource to complete (i.e. .status.phase=='Succeeded')."""
    run_cmd = [
        'wait',
        '--for=jsonpath={.status.phase}=Succeeded',
        f'--timeout={timeout}s',
        resource_name,
    ]
    if namespace:
      run_cmd.append(f'--namespace={namespace}')
    return RunKubectlCommand(
        run_cmd, timeout=timeout + 10, raise_on_failure=raise_on_failure
    )

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
  def GetNumReplicasSamples(
      resource_name: str, namespace: Optional[str] = None
  ) -> list[Sample]:
    """Returns a count of the replias (and state) for the specified resource.

    The number of ready and unready replicas should always sum to the total
    replicas.

    Args:
      resource_name: The deployment/statefulset/etc's name, e.g.
        'deployment/my_deployment'.
      namespace: The namespace of the resource. If omitted, the 'default'
        namespace will be used.

    Returns:
      A list of the (total replicas, ready replicas, unready replicas) for this
      resource (as `Sample`s), or an empty list if the resource cannot be found.
    """
    now = int(time.time())
    if namespace is None:
      namespace = 'default'
    stdout, stderr, retcode = RunKubectlCommand(
        [
            'get',
            resource_name,
            '-n',
            namespace,
            "-o=jsonpath='{.status.replicas}, {.status.readyReplicas}'",
        ],
        raise_on_failure=False,
    )
    if retcode != 0:
      if re.match('^Error from server \\(NotFound\\):.*', stderr) is not None:
        # The specified resource wasn't found
        return []
      else:
        # Some other error.
        raise errors.VmUtil.IssueCommandError(
            f'Unable to query list of replicas: {stderr}'
        )

    stdout = stdout.strip("' ")
    replicas = int(stdout.split(',')[0])
    ready_replicas = int(stdout.split(',')[1])
    unready_replicas = replicas - ready_replicas

    def _Sample(count: int, state: str) -> Sample:
      return Sample(
          metric='k8s/num_replicas_' + state,
          value=count,
          unit='',
          metadata={
              'namespace': namespace,
              'resource_name': resource_name,
          },
          timestamp=now,
      )

    return [
        _Sample(replicas, 'any'),
        _Sample(ready_replicas, 'ready'),
        _Sample(unready_replicas, 'unready'),
    ]

  @staticmethod
  def GetNumNodesSamples() -> list[Sample]:
    """Returns a count of nodes in each state for the cluster.

    The number of ready, unready, and unknown nodes should always sum to the
    total nodes.

    Returns:
      A List of the (total nodes, ready nodes, unready nodes, unknown nodes)
      for this cluster as `Sample`s.
    """
    now = int(time.time())

    jsonpath = (
        '{range .items[*]}'
        '{@.status.conditions[?(@.type=="Ready")].status}{"\\n"}'
        '{end}'
    )
    stdout, _, _ = RunKubectlCommand(
        ['get', 'nodes', f"-o=jsonpath='{jsonpath}'"]
    )

    total = ready = unready = unknown = 0
    for line in stdout.splitlines():
      status = line.strip("' ").lower()
      if not status:
        continue
      elif status == 'true':
        ready += 1
      elif status == 'false':
        unready += 1
      else:
        # status should be strictly 'unknown', but we'll also categorize any
        # other unexpected response as 'unknown'
        unknown += 1
      total += 1

    def _Sample(count: int, state: str) -> Sample:
      # TOCONSIDER: maybe include the nodepool name in the metadata?
      return Sample(
          metric='k8s/num_nodes_' + state,
          value=count,
          unit='',
          metadata={},
          timestamp=now,
      )

    return [
        _Sample(total, 'any'),
        _Sample(ready, 'ready'),
        _Sample(unready, 'unready'),
        _Sample(unknown, 'unknown'),
    ]

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
  def GetPodNames() -> list[str]:
    """Returns all pod names in the cluster."""
    return KubernetesClusterCommands.GetAllNamesForResourceType('pods')

  @staticmethod
  def GetNodeNames() -> list[str]:
    """Get the node names for the cluster."""
    return KubernetesClusterCommands.GetAllNamesForResourceType('nodes')

  @staticmethod
  def GetAllNamesForResourceType(resource_type: str) -> list[str]:
    """Get all names for the specified resource. Type should be plural."""
    stdout, _, _ = RunKubectlCommand(
        ['get', resource_type, '-o', 'jsonpath={.items[*].metadata.name}']
    )
    return stdout.split()

  @staticmethod
  def RunKubectlExec(pod_name, cmd):
    run_cmd = ['exec', '-it', pod_name, '--'] + cmd
    RunKubectlCommand(run_cmd)

  @staticmethod
  def _GetPvcs() -> Sequence[Any]:
    stdout, _, _ = RunKubectlCommand(['get', 'pvc', '-o', 'yaml'])
    return yaml.safe_load(stdout)['items']

  @staticmethod
  def _GetEvents(**kwargs) -> set['KubernetesEvent']:
    """Get the events for the cluster."""
    stdout, _, _ = RunRetryableKubectlCommand(
        ['get', 'events', '-o', 'yaml'], **kwargs
    )
    k8s_events = set()
    for item in yaml.safe_load(stdout)['items']:
      k8s_event = KubernetesEvent.FromDict(item)
      if k8s_event:
        k8s_events.add(k8s_event)
    return k8s_events

  @staticmethod
  def GetResourceMetadataByName(
      resource_name: str,
      resource_label: Optional[str] = None,
      output_format: str = 'yaml',
      output_formatter=yaml.safe_load,
      **kwargs,
  ) -> Dict[str, Any]:
    """Gets the resource metadata from a Kubernetes resource."""
    get_cmd = [
        'get',
        resource_name,
    ]
    if resource_label:
      get_cmd.extend(['-l', resource_label])
    get_cmd.extend(['-o', output_format])

    stdout, _, _ = RunRetryableKubectlCommand(get_cmd, **kwargs)

    return output_formatter(stdout)

  @staticmethod
  def RetryableGetPodNameFromJob(
      job_name: str, timeout: int | None = None
  ) -> str:
    """Get the name of the pod from a job, retry until the pod is created.

    Args:
      job_name: The name of the job.
      timeout: The timeout in seconds.

    Returns:
      The name of the pod.
    """

    class EmptyPodNameError(Exception):
      pass

    @vm_util.Retry(timeout=timeout, retryable_exceptions=EmptyPodNameError)
    def _RetryFunction():
      pod_name = KubernetesClusterCommands.GetResourceMetadataByName(
          'pods', f'job-name={job_name}', 'jsonpath={.items[*].metadata.name}'
      )
      if not pod_name:
        raise EmptyPodNameError(
            f'No pod found for job {job_name}, the pod may not be created yet.'
        )
      return pod_name

    return _RetryFunction()

  @staticmethod
  def DeleteResource(
      resource_identifier: str, ignore_not_found: bool = True
  ) -> None:
    """Deletes a kubernetes resource."""
    delete_cmd = [
        'delete',
        resource_identifier,
    ]
    if ignore_not_found:
      delete_cmd.append('--ignore-not-found=true')
    RunKubectlCommand(delete_cmd, raise_on_failure=False)

  @staticmethod
  def GetFileContentFromPod(pod_name: str, file_path: str) -> str:
    """Get the content of a file from a pod.

    Args:
      pod_name: The name of the pod.
      file_path: The path of the file to get.

    Returns:
      The content of the file.
    """
    get_cmd = [
        'exec',
        pod_name,
        '--',
        'cat',
        file_path,
    ]

    stdout, _, _ = RunRetryableKubectlCommand(get_cmd)
    return stdout

  @staticmethod
  def CopyFilesFromPod(
      pod_name: str, src_path: str, target_path: str, **kwargs
  ) -> None:
    """Copy files from a pod source path to the target path on current VM."""
    get_cmd = [
        'cp',
        f'{pod_name}:{src_path}',
        target_path,
    ]

    RunRetryableKubectlCommand(get_cmd, **kwargs)


class KubernetesEventPoller:
  """Wrapper which polls for Kubernetes events."""

  def __init__(self, get_events_fn: Callable[[], set['KubernetesEvent']]):
    self.get_events_fn = get_events_fn
    self.polled_events: set['KubernetesEvent'] = set()
    self.stop_polling = multiprocessing.Event()
    self.event_queue: multiprocessing.Queue = multiprocessing.Queue()
    self.event_poller = multiprocessing.Process(
        target=self._PollForEvents,
        args=((
            self.event_queue,
            self.stop_polling,
        )),
    )
    self.event_poller.daemon = True

  def _PollForEvents(
      self,
      queue: multiprocessing.Queue,
      stop_polling: synchronize.Event,
  ):
    """Polls for events & (ideally asynchronously) waits to poll again.

    Results are appended to the queue. Timeouts are ignored.

    Args:
      queue: The queue to append events to.
      stop_polling: Stop polling when set.
    """
    while True:
      try:
        k8s_events = self.get_events_fn()
        logging.info(
            'From async get events process, got %s events', len(k8s_events)
        )
        for event in k8s_events:
          queue.put(event)
      except errors.VmUtil.IssueCommandTimeoutError:
        logging.info(
            'Async get events command timed out. This may result in missing'
            ' events, but is not a reason to fail the benchmark.'
        )
        pass
      start_sleep_time = time.time()
      while time.time() - start_sleep_time < 60 * 40:
        time.sleep(1)
        if stop_polling.is_set():
          return

  def StartPolling(self):
    """Starts polling for events."""
    self.event_poller.start()

    # Stop polling events even if the resource is not deleted.
    def _StopPollingConnected(unused1, **kwargs):
      del unused1, kwargs
      self.StopPolling()

    events.benchmark_end.connect(_StopPollingConnected, weak=False)

  def StopPolling(self):
    """Stops polling for events, joining the poller process."""
    logging.info('Stopping event poller')
    self.stop_polling.set()
    while not self.event_queue.empty():
      self.polled_events.add(self.event_queue.get())
    if self.event_poller.is_alive():
      self.event_poller.join(timeout=30)
    if self.event_poller.is_alive():
      logging.warning(
          'Event poller process did not join in 30 seconds; killing it.'
      )
      self.event_poller.kill()
      self.event_poller.join(timeout=30)

  def GetEvents(self) -> set['KubernetesEvent']:
    """Gets the events for the cluster, including previously polled events."""
    k8s_events = self.get_events_fn()
    self.polled_events.update(k8s_events)
    while not self.event_queue.empty():
      self.polled_events.add(self.event_queue.get())
    return self.polled_events


class KubernetesCluster(BaseContainerCluster, KubernetesClusterCommands):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = KUBERNETES

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

  def GetNodeSelectors(self) -> list[str]:
    """Get the node selectors section of a yaml for the provider."""
    return []

  def DeployIngress(self, name: str, namespace: str, port: int) -> str:
    """Deploys an Ingress/load balancer resource to the cluster.

    Args:
      name: The name of the Ingress resource.
      namespace: The namespace of the resource.
      port: The port to expose to the internet.

    Returns:
      The address of the Ingress.
    """
    self.ApplyManifest(
        'container/loadbalancer.yaml.j2',
        name=name,
        namespace=namespace,
        port=port,
    )
    self.WaitForResource(
        f'service/{name}',
        INGRESS_JSONPATH,
        namespace=namespace,
        condition_type='jsonpath=',
    )
    stdout, _, _ = RunKubectlCommand([
        'get',
        '-n',
        name,
        f'svc/{name}',
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


@dataclasses.dataclass(eq=True, frozen=True)
class KubernetesEventResource:
  """Holder for Kubernetes event involved objects."""

  kind: str
  name: str | None

  @classmethod
  def FromDict(cls, yaml_data: dict[str, Any]) -> 'KubernetesEventResource':
    """Parse Kubernetes Event YAML output."""
    return cls(kind=yaml_data['kind'], name=yaml_data.get('name'))


@dataclasses.dataclass(eq=True, frozen=True)
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
      return None
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
    except (AssertionError, KeyError) as e:
      logging.warning(
          'Tried parsing event: %s but ran into error: %s', yaml_data, e
      )
      return None
