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

import collections
import functools
import ipaddress
import itertools
import os
import time
from typing import Any, Dict, List, Optional

from absl import flags
import jinja2
from perfkitbenchmarker import context
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import os_types
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import units
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
import requests
import six
import yaml

KUBERNETES = 'Kubernetes'
DEFAULT_NODEPOOL = 'default'

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'kubeconfig', None, 'Path to kubeconfig to be used by kubectl. '
    'If unspecified, it will be set to a file in this run\'s '
    'temporary directory.')

flags.DEFINE_string('kubectl', 'kubectl', 'Path to kubectl tool')

flags.DEFINE_boolean(
    'local_container_build', False,
    'Force container images to be built locally rather than '
    'just as a fallback if there is no remote image builder '
    'associated with the registry.')

flags.DEFINE_boolean(
    'static_container_image', True,
    'Whether container images are static (i.e. are not '
    'managed by PKB). If this is set, PKB will accept the '
    'image as fully qualified (including repository) and will '
    'not attempt to build it.')

flags.DEFINE_boolean(
    'force_container_build', False,
    'Whether to force PKB to build container images even '
    'if they already exist in the registry.')

flags.DEFINE_string(
    'container_cluster_cloud', None,
    'Sets the cloud to use for the container cluster. '
    'This will override both the value set in the config and '
    'the value set using the generic "cloud" flag.')

flags.DEFINE_integer(
    'container_cluster_num_vms', None,
    'Number of nodes in the cluster. Defaults to '
    'container_cluster.vm_count')

flags.DEFINE_string('container_cluster_type', KUBERNETES,
                    'The type of container cluster.')

flags.DEFINE_string(
    'container_cluster_version', None,
    'Optional version flag to pass to the cluster create '
    'command. If not specified, the cloud-specific container '
    'implementation will chose an appropriate default.')

_CONTAINER_CLUSTER_ARCHITECTURE = flags.DEFINE_list(
    'container_cluster_architecture', ['linux/amd64'],
    'The architecture(s) that the container cluster uses. '
    'Defaults to linux/amd64')

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


class ContainerException(errors.Error):
  """Exception during the creation or execution of a container."""


class FatalContainerException(errors.Resource.CreationError,
                              ContainerException):
  """Fatal Exception during the creation or execution of a container."""
  pass


class RetriableContainerException(errors.Resource.RetryableCreationError,
                                  ContainerException):
  """Retriable Exception during the creation or execution of a container."""
  pass


def RunKubectlCommand(command: List[str], **kwargs):
  """Run a kubectl command."""
  cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig] + command
  return vm_util.IssueCommand(cmd, **kwargs)


class ContainerSpec(spec.BaseSpec):
  """Class containing options for creating containers."""

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Apply flag settings to the container spec."""
    super(ContainerSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['image'].present:
      config_values['image'] = flag_values.image
    if flag_values['static_container_image'].present:
      config_values['static_image'] = flag_values.static_container_image

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(ContainerSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'image': (option_decoders.StringDecoder, {
            'default': None
        }),
        'static_image': (option_decoders.BooleanDecoder, {
            'default': False
        }),
        'cpus': (option_decoders.FloatDecoder, {
            'default': None
        }),
        'memory': (custom_virtual_machine_spec.MemoryDecoder, {
            'default': None
        }),
        'command': (_CommandDecoder, {}),
        'container_port': (option_decoders.IntDecoder, {
            'default': 8080
        }),
    })
    return result


class _CommandDecoder(option_decoders.ListDecoder):
  """Decodes the command/arg list for containers."""

  def __init__(self, **kwargs):
    super(_CommandDecoder, self).__init__(
        default=None,
        none_ok=True,
        item_decoder=option_decoders.StringDecoder(),
        **kwargs)


class BaseContainer(resource.BaseResource):
  """Class representing a single container."""

  def __init__(self, container_spec=None, **_):
    # Hack to make container_spec a kwarg
    assert container_spec
    super(BaseContainer, self).__init__()
    self.cpus = container_spec.cpus
    self.memory = container_spec.memory
    self.command = container_spec.command
    self.image = container_spec.image
    self.ip_address = None

  def WaitForExit(self, timeout: int = 1200) -> Dict[str, Any]:
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

  def __init__(self, container_spec):
    super(BaseContainerService, self).__init__()
    self.cpus = container_spec.cpus
    self.memory = container_spec.memory
    self.command = container_spec.command
    self.image = container_spec.image
    self.container_port = container_spec.container_port
    self.ip_address = None
    self.port = None
    self.host_header = None


class _ContainerImage(object):
  """Simple class for tracking container image names and source locations."""

  def __init__(self, name):
    self.name = name
    self.directory = os.path.dirname(
        data.ResourcePath(os.path.join('docker', self.name, 'Dockerfile')))


class ContainerRegistrySpec(spec.BaseSpec):
  """Spec containing options for creating a Container Registry."""

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(ContainerRegistrySpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)
    registry_spec = getattr(self.spec, self.cloud, {})
    self.project = registry_spec.get('project')
    self.zone = registry_spec.get('zone')
    self.name = registry_spec.get('name')

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Apply flag values to the spec."""
    super(ContainerRegistrySpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    if flag_values['container_cluster_cloud'].present:
      config_values['cloud'] = flag_values.container_cluster_cloud
    updated_spec = {}
    if flag_values['project'].present:
      updated_spec['project'] = flag_values.project
    if flag_values['zones'].present:
      updated_spec['zone'] = flag_values.zones[0]
    cloud = config_values['cloud']
    cloud_spec = config_values.get('spec', {}).get(cloud, {})
    cloud_spec.update(updated_spec)
    config_values['spec'] = {cloud: cloud_spec}

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(ContainerRegistrySpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.StringDecoder, {}),
        'spec': (option_decoders.PerCloudConfigDecoder, {
            'default': {}
        })
    })
    return result


def GetContainerRegistryClass(cloud):
  return resource.GetResourceClass(BaseContainerRegistry, CLOUD=cloud)


class BaseContainerRegistry(resource.BaseResource):
  """Base class for container image registries."""

  RESOURCE_TYPE = 'BaseContainerRegistry'

  def __init__(self, registry_spec):
    super(BaseContainerRegistry, self).__init__()
    benchmark_spec = context.GetThreadBenchmarkSpec()
    container_cluster = getattr(benchmark_spec, 'container_cluster', None)
    zone = getattr(container_cluster, 'zone', None)
    project = getattr(container_cluster, 'project', None)
    self.zone = registry_spec.zone or zone
    self.project = registry_spec.project or project
    self.name = registry_spec.name or 'pkb%s' % FLAGS.run_uri
    self.local_build_times = {}
    self.remote_build_times = {}
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
          sample.Sample('Image Build Time', build_time, 'seconds', metadata))
    for image_name, build_time in self.remote_build_times.items():
      metadata.update({
          'build_type': 'remote',
          'image': image_name,
      })
      samples.append(
          sample.Sample('Image Build Time', build_time, 'seconds', metadata))
    return samples

  def GetFullRegistryTag(self, image):
    """Returns the full name of the image for the registry.

    Args:
      image: The PKB name of the image (string).
    """
    raise NotImplementedError()

  def PrePush(self, image):
    """Prepares registry to push a given image."""
    pass

  def RemoteBuild(self, image):
    """Build the image remotely.

    Args:
      image: Instance of _ContainerImage representing the image to build.
    """
    raise NotImplementedError()

  def Login(self):
    """Log in to the registry (in order to push to it)."""
    raise NotImplementedError()

  def LocalBuildAndPush(self, image):
    """Build the image locally and push to registry.

    Assumes we are already authenticated with the registry from self.Login.
    Building and pushing done in one command to support multiarch images
    https://github.com/docker/buildx/issues/59

    Args:
      image: Instance of _ContainerImage representing the image to build.
    """
    full_tag = self.GetFullRegistryTag(image.name)
    # Multiarch images require buildx create
    # https://github.com/docker/build-push-action/issues/302
    vm_util.IssueCommand(['docker', 'buildx', 'create', '--use'])
    cmd = ['docker', 'buildx', 'build']
    if _CONTAINER_CLUSTER_ARCHITECTURE.value:
      cmd += ['--platform', ','.join(_CONTAINER_CLUSTER_ARCHITECTURE.value)]
    cmd += ['--no-cache', '--push', '-t', full_tag, image.directory]
    vm_util.IssueCommand(cmd)
    vm_util.IssueCommand(['docker', 'buildx', 'stop'])

  def GetOrBuild(self, image):
    """Finds the image in the registry or builds it.

    TODO(pclay): Add support for build ARGs.

    Args:
      image: The PKB name for the image (string).

    Returns:
      The full image name (including the registry).
    """
    full_image = self.GetFullRegistryTag(image)
    # Log in to the registry to see if image exists
    self.Login()
    if not FLAGS.force_container_build:
      # manifest inspect inpspects the registry's copy
      inspect_cmd = ['docker', 'manifest', 'inspect', full_image]
      _, _, retcode = vm_util.IssueCommand(
          inspect_cmd, suppress_warning=True, raise_on_failure=False)
      if retcode == 0:
        return full_image
    self._Build(image)
    return full_image

  def _Build(self, image):
    """Builds the image and pushes it to the registry if necessary.

    Args:
      image: The PKB name for the image (string).
    """
    image = _ContainerImage(image)
    build_start = time.time()
    if not FLAGS.local_container_build:
      try:
        # Build the image remotely using an image building service.
        self.RemoteBuild(image)
        self.remote_build_times[image.name] = time.time() - build_start
        return
      except NotImplementedError:
        pass

    self.PrePush(image)
    # Build the image locally using docker.
    build_start = time.time()
    self.LocalBuildAndPush(image)
    self.local_build_times[image.name] = time.time() - build_start


@events.benchmark_start.connect
def _SetKubeConfig(unused_sender, benchmark_spec):
  """Sets the value for the kubeconfig flag if it's unspecified."""
  if not FLAGS.kubeconfig:
    FLAGS.kubeconfig = vm_util.PrependTempDir(
        'kubeconfig' + str(benchmark_spec.sequence_number))
    # Store the value for subsequent run stages.
    benchmark_spec.config.flags['kubeconfig'] = FLAGS.kubeconfig


def NodePoolName(name: str) -> str:
  """Clean node pool names to be usable by all providers."""
  # GKE (or k8s?) requires nodepools use alphanumerics and hyphens
  # AKS requires full alphanumeric
  # PKB likes to use underscores strip them out.
  return name.replace('_', '')


def GetContainerClusterClass(cloud, cluster_type):
  return resource.GetResourceClass(
      BaseContainerCluster, CLOUD=cloud, CLUSTER_TYPE=cluster_type)


class BaseContainerCluster(resource.BaseResource):
  """A cluster that can be used to schedule containers."""

  RESOURCE_TYPE = 'BaseContainerCluster'
  REQUIRED_ATTRS = ['CLOUD', 'CLUSTER_TYPE']

  def __init__(self, cluster_spec):
    super(BaseContainerCluster, self).__init__()
    self.name = 'pkb-%s' % FLAGS.run_uri
    # Use Virtual Machine class to resolve VM Spec. This lets subclasses parse
    # Provider specific information like disks out of the spec.
    for name, nodepool in cluster_spec.nodepools.copy().items():
      nodepool.vm_config = virtual_machine.GetVmClass(
          self.CLOUD, os_types.DEFAULT)(nodepool.vm_spec)
      nodepool.num_nodes = nodepool.vm_count
      # Fix name
      del cluster_spec.nodepools[name]
      cluster_spec.nodepools[NodePoolName(name)] = nodepool
    self.nodepools = cluster_spec.nodepools
    self.vm_config = virtual_machine.GetVmClass(self.CLOUD, os_types.DEFAULT)(
        cluster_spec.vm_spec)
    self.num_nodes = cluster_spec.vm_count
    self.min_nodes = cluster_spec.min_vm_count or self.num_nodes
    self.max_nodes = cluster_spec.max_vm_count or self.num_nodes
    self.containers = collections.defaultdict(list)
    self.services = {}
    self.zone = self.vm_config.zone

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
    nodepools = {}
    for name, nodepool in six.iteritems(self.nodepools):
      nodepool_metadata = {
          'size': nodepool.num_nodes,
          'machine_type': nodepool.vm_config.machine_type,
          'name': name
      }
      nodepools[name] = nodepool_metadata

    metadata = {
        'cloud': self.CLOUD,
        'cluster_type': self.CLUSTER_TYPE,
        'zone': self.zone,
        'size': self.num_nodes,
        'machine_type': self.vm_config.machine_type,
        'nodepools': nodepools
    }

    if self.min_nodes != self.num_nodes or self.max_nodes != self.num_nodes:
      metadata.update({
          'max_size': self.max_nodes,
          'min_size': self.min_nodes,
      })

    return metadata

  def DeployContainer(self, name, container_spec):
    """Deploys Containers according to the ContainerSpec."""
    raise NotImplementedError()

  def DeployContainerService(self, name, container_spec, num_containers):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    raise NotImplementedError()

  def GetSamples(self):
    """Return samples with information about deployment times."""
    samples = []
    samples.append(
        sample.Sample('Cluster Creation Time',
                      self.resource_ready_time - self.create_start_time,
                      'seconds'))
    for container in itertools.chain(*list(self.containers.values())):
      metadata = {'image': container.image.split('/')[-1]}
      if container.resource_ready_time and container.create_start_time:
        samples.append(
            sample.Sample(
                'Container Deployment Time',
                container.resource_ready_time - container.create_start_time,
                'seconds', metadata))
      if container.delete_end_time and container.delete_start_time:
        samples.append(
            sample.Sample(
                'Container Delete Time',
                container.delete_end_time - container.delete_start_time,
                'seconds', metadata))
    for service in self.services.values():
      metadata = {'image': service.image.split('/')[-1]}
      if service.resource_ready_time and service.create_start_time:
        samples.append(
            sample.Sample(
                'Service Deployment Time',
                service.resource_ready_time - service.create_start_time,
                'seconds', metadata))
      if service.delete_end_time and service.delete_start_time:
        samples.append(
            sample.Sample('Service Delete Time',
                          service.delete_end_time - service.delete_start_time,
                          'seconds', metadata))

    return samples


class KubernetesPod:
  """Representation of a Kubernetes pod.

  It can be created as a PKB managed resource using KubernetesContainer,
  or created with ApplyManifest and directly constructed.
  """

  def __init__(self, name=None, **_):
    assert name
    self.name = name

  def _GetPod(self) -> Dict[str, Any]:
    """Gets a representation of the POD and returns it."""
    stdout, _, _ = RunKubectlCommand(['get', 'pod', self.name, '-o', 'yaml'])
    pod = yaml.safe_load(stdout)
    self.ip_address = pod.get('status', {}).get('podIP')
    return pod

  def WaitForExit(self, timeout: int = None) -> Dict[str, Any]:
    """Gets the finished running container."""

    @vm_util.Retry(
        timeout=timeout, retryable_exceptions=(RetriableContainerException,))
    def _WaitForExit():
      # Inspect the pod's status to determine if it succeeded, has failed, or is
      # doomed to fail.
      # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
      pod = self._GetPod()
      status = pod['status']
      phase = status['phase']
      if phase == 'Succeeded':
        return pod
      elif phase == 'Failed':
        raise FatalContainerException(
            f"Pod {self.name} failed:\n{yaml.dump(pod['status'])}")
      else:
        for condition in status.get('conditions', []):
          if (condition['type'] == 'PodScheduled' and
              condition['status'] == 'False' and
              condition['reason'] == 'Unschedulable'):
            # TODO(pclay): Revisit this when we scale clusters.
            raise FatalContainerException(
                f"Pod {self.name} failed to schedule:\n{condition['message']}")
        for container_status in status.get('containerStatuses', []):
          waiting_status = container_status['state'].get('waiting', {})
          if waiting_status.get('reason') in [
              'ErrImagePull', 'ImagePullBackOff'
          ]:
            raise FatalContainerException(
                f'Failed to find container image for {status.name}:\n' +
                yaml.dump(waiting_status.get('message')))
        raise RetriableContainerException(
            f'Pod phase ({phase}) not in finished phases.')

    return _WaitForExit()

  def GetLogs(self):
    """Returns the logs from the container."""
    stdout, _, _ = RunKubectlCommand(['logs', self.name])
    return stdout


class KubernetesContainer(BaseContainer, KubernetesPod):
  """A KubernetesPod based flavor of Container."""

  def _Create(self):
    """Creates the container."""
    run_cmd = [
        'run',
        self.name,
        '--image=%s' % self.image,
        '--restart=Never',
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
    return self._GetPod()['status']['phase'] != 'Pending'


class KubernetesContainerService(BaseContainerService):
  """A Kubernetes flavor of Container Service."""

  def __init__(self, container_spec, name):
    super(KubernetesContainerService, self).__init__(container_spec)
    self.name = name
    self.port = 8080

  def _Create(self):
    run_cmd = [
        'run', self.name,
        '--image=%s' % self.image, '--port',
        str(self.port)
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
        'expose', 'deployment', self.name, '--type', 'NodePort',
        '--target-port',
        str(self.port)
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
        'get', 'ing', ingress_name, '-o',
        'jsonpath={.status.loadBalancer.ingress[*].ip}'
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


class KubernetesCluster(BaseContainerCluster):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = KUBERNETES

  def _DeleteAllFromDefaultNamespace(self):
    """Deletes all resources from a namespace.

    Since StatefulSets do not reclaim PVCs upon deletion, they are explicitly
    deleted here to prevent dynamically provisioned PDs from leaking once the
    cluster has been deleted.
    """
    run_cmd = [
        'delete', 'all', '--all', '-n', 'default'
    ]
    RunKubectlCommand(run_cmd)

    run_cmd = [
        'delete', 'pvc', '--all', '-n', 'default'
    ]
    RunKubectlCommand(run_cmd)

  def _Delete(self):
    self._DeleteAllFromDefaultNamespace()

  def DeployContainer(self, base_name, container_spec):
    """Deploys Containers according to the ContainerSpec."""
    name = base_name + str(len(self.containers[base_name]))
    container = KubernetesContainer(container_spec=container_spec, name=name)
    self.containers[base_name].append(container)
    container.Create()

  def DeployContainerService(self, name, container_spec):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    service = KubernetesContainerService(container_spec, name)
    self.services[name] = service
    service.Create()

  # TODO(pclay): Revisit instance methods that don't rely on instance data.
  def ApplyManifest(self, manifest_file, **kwargs):
    """Applies a declarative Kubernetes manifest; possibly with jinja.

    Args:
      manifest_file: The name of the YAML file or YAML template.
      **kwargs: Arguments to the jinja template.
    """
    filename = data.ResourcePath(manifest_file)
    if not filename.endswith('.j2'):
      assert not kwargs
      RunKubectlCommand(['apply', '-f', filename])
      return

    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    with open(filename) as template_file, vm_util.NamedTemporaryFile(
        mode='w', suffix='.yaml') as rendered_template:
      manifest = environment.from_string(template_file.read()).render(kwargs)
      rendered_template.write(manifest)
      rendered_template.close()
      RunKubectlCommand(['apply', '-f', rendered_template.name])

  def WaitForResource(self, resource_name, condition_name, namespace=None):
    """Waits for a condition on a Kubernetes resource (eg: deployment, pod)."""
    run_cmd = [
        'wait', f'--for=condition={condition_name}',
        f'--timeout={vm_util.DEFAULT_TIMEOUT}s', resource_name
    ]
    if namespace:
      run_cmd.append(f'--namespace={namespace}')
    RunKubectlCommand(run_cmd)

  def WaitForRollout(self, resource_name):
    """Blocks until a Kubernetes rollout is completed."""
    run_cmd = [
        'rollout',
        'status',
        '--timeout=%ds' % vm_util.DEFAULT_TIMEOUT,
        resource_name
    ]

    RunKubectlCommand(run_cmd)

  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def GetLoadBalancerIP(self, service_name):
    """Returns the IP address of a LoadBalancer service when ready."""
    get_cmd = [
        'get', 'service', service_name, '-o',
        'jsonpath={.status.loadBalancer.ingress[0].ip}'
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)

    try:
      # Ensure the load balancer is ready by parsing the output IP
      ip_address = ipaddress.ip_address(stdout)
    except ValueError:
      raise errors.Resource.RetryableCreationError(
          "Load Balancer IP for service '%s' is not ready." % service_name)

    return format(ip_address)

  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def GetClusterIP(self, service_name) -> str:
    """Returns the IP address of a ClusterIP service when ready."""
    get_cmd = [
        'get', 'service', service_name, '-o', 'jsonpath={.spec.clusterIP}'
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)

    if not stdout:
      raise errors.Resource.RetryableCreationError(
          "ClusterIP for service '%s' is not ready." % service_name)

    return stdout

  def CreateConfigMap(self, name, from_file_dir):
    """Creates a Kubernetes ConfigMap.

    Args:
      name: The name of the ConfigMap to create
      from_file_dir: The directory name containing files that will be key/values
        in the ConfigMap
    """
    RunKubectlCommand(
        ['create', 'configmap', name, '--from-file', from_file_dir])

  def CreateServiceAccount(self,
                           name: str,
                           clusterrole: Optional[str] = None,
                           namespace='default'):
    """Create a k8s service account and cluster-role-binding."""
    RunKubectlCommand(
        ['create', 'serviceaccount', name, '--namespace', namespace])
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

  # TODO(pclay): Move to cached property in
  @property
  @functools.lru_cache(maxsize=1)
  def node_memory_allocatable(self) -> units.Quantity:
    """Usable memory of each node in cluster in KiB."""
    stdout, _, _ = RunKubectlCommand(
        # TODO(pclay): Take a minimum of all nodes?
        [
            'get', 'nodes', '-o',
            'jsonpath={.items[0].status.allocatable.memory}'
        ])
    return units.ParseExpression(stdout)

  @property
  @functools.lru_cache(maxsize=1)
  def node_num_cpu(self) -> int:
    """vCPU of each node in cluster."""
    stdout, _, _ = RunKubectlCommand(
        ['get', 'nodes', '-o', 'jsonpath={.items[0].status.capacity.cpu}'])
    return int(stdout)

  def GetPodLabel(self, resource_name):
    run_cmd = [
        'get', resource_name,
        '-o', 'jsonpath="{.spec.selector.matchLabels.app}"'
    ]

    stdout, _, _ = RunKubectlCommand(run_cmd)
    return yaml.safe_load(stdout)

  def GetPodIps(self, resource_name):
    """Returns a list of internal IPs for a pod name.

    Args:
      resource_name: The pod resource name
    """
    pod_label = self.GetPodLabel(resource_name)

    get_cmd = [
        'get', 'pods', '-l', 'app=%s' % pod_label,
        '-o', 'jsonpath="{.items[*].status.podIP}"'
    ]

    stdout, _, _ = RunKubectlCommand(get_cmd)
    return yaml.safe_load(stdout).split()

  def RunKubectlExec(self, pod_name, cmd):
    run_cmd = [
        'exec', '-it', pod_name, '--'
    ] + cmd
    RunKubectlCommand(run_cmd)
