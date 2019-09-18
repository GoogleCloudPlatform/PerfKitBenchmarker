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
import itertools
import os
import time
from perfkitbenchmarker import context
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import data
from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
import requests
import yaml


KUBERNETES = 'Kubernetes'

FLAGS = flags.FLAGS

flags.DEFINE_string('kubeconfig', None,
                    'Path to kubeconfig to be used by kubectl. '
                    'If unspecified, it will be set to a file in this run\'s '
                    'temporary directory.')

flags.DEFINE_string('kubectl', 'kubectl',
                    'Path to kubectl tool')

flags.DEFINE_boolean('local_container_build', False,
                     'Force container images to be built locally rather than '
                     'just as a fallback if there is no remote image builder '
                     'associated with the registry.')

flags.DEFINE_boolean('static_container_image', True,
                     'Whether container images are static (i.e. are not '
                     'managed by PKB). If this is set, PKB will accept the '
                     'image as fully qualified (including repository) and will '
                     'not attempt to build it.')

flags.DEFINE_boolean('force_container_build', False,
                     'Whether to force PKB to build container images even '
                     'if they already exist in the registry.')

flags.DEFINE_string('container_cluster_cloud', None,
                    'Sets the cloud to use for the container cluster. '
                    'This will override both the value set in the config and '
                    'the value set using the generic "cloud" flag.')

flags.DEFINE_integer('container_cluster_num_vms', None,
                     'Number of nodes in the cluster. Defaults to '
                     'container_cluster.vm_count')

flags.DEFINE_string('container_cluster_type', KUBERNETES,
                    'The type of container cluster.')

flags.DEFINE_string('container_cluster_version', None,
                    'Optional version flag to pass to the cluster create '
                    'command. If not specified, the cloud-specific container '
                    'implementation will chose an appropriate default.')

_K8S_FINISHED_PHASES = frozenset(['Succeeded', 'Failed'])
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
        'image': (option_decoders.StringDecoder, {}),
        'static_image': (option_decoders.BooleanDecoder, {'default': False}),
        'cpus': (option_decoders.FloatDecoder, {'default': 1.0}),
        'memory': (custom_virtual_machine_spec.MemoryDecoder, {}),
        'command': (_CommandDecoder, {}),
        'container_port': (option_decoders.IntDecoder, {'default': 8080}),
    })
    return result


class _CommandDecoder(option_decoders.ListDecoder):
  """Decodes the command/arg list for containers."""

  def __init__(self, **kwargs):
    super(_CommandDecoder, self).__init__(
        default=None, none_ok=True,
        item_decoder=option_decoders.StringDecoder(),
        **kwargs)


class BaseContainer(resource.BaseResource):
  """Class representing a single container."""

  def __init__(self, container_spec):
    super(BaseContainer, self).__init__()
    self.cpus = container_spec.cpus
    self.memory = container_spec.memory
    self.command = container_spec.command
    self.image = container_spec.image
    self.ip_address = None

  def WaitForExit(self, timeout=1200):
    """Waits until the container has finished running."""
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
        'spec': (option_decoders.PerCloudConfigDecoder, {'default': {}})
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
    self.metadata.update({
        'cloud': self.CLOUD
    })

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
      samples.append(sample.Sample(
          'Image Build Time', build_time, 'seconds', metadata))
    for image_name, build_time in self.remote_build_times.items():
      metadata.update({
          'build_type': 'remote',
          'image': image_name,
      })
      samples.append(sample.Sample(
          'Image Build Time', build_time, 'seconds', metadata))
    return samples

  def GetFullRegistryTag(self, image):
    """Returns the full name of the image for the registry.

    Args:
      image: The PKB name of the image (string).
    """
    raise NotImplementedError()

  def Push(self, image):
    """Push a locally built image to the repo.

    Args:
      image: Instance of _ContainerImage representing the image to push.
    """
    full_tag = self.GetFullRegistryTag(image.name)
    tag_cmd = ['docker', 'tag', image.name, full_tag]
    vm_util.IssueCommand(tag_cmd)
    push_cmd = ['docker', 'push', full_tag]
    vm_util.IssueCommand(push_cmd)

  def RemoteBuild(self, image):
    """Build the image remotely.

    Args:
      image: Instance of _ContainerImage representing the image to build.
    """
    raise NotImplementedError()

  def Login(self):
    """Log in to the registry (in order to push to it)."""
    raise NotImplementedError()

  def LocalBuild(self, image):
    """Build the image locally.

    Args:
      image: Instance of _ContainerImage representing the image to build.
    """
    build_cmd = [
        'docker', 'build', '--no-cache',
        '-t', image.name, image.directory
    ]
    vm_util.IssueCommand(build_cmd)

  def GetOrBuild(self, image):
    """Finds the image in the registry or builds it.

    Args:
      image: The PKB name for the image (string).

    Returns:
      The full image name (including the registry).
    """
    full_image = self.GetFullRegistryTag(image)
    if not FLAGS.force_container_build:
      inspect_cmd = ['docker', 'image', 'inspect', full_image]
      _, _, retcode = vm_util.IssueCommand(inspect_cmd, suppress_warning=True,
                                           raise_on_failure=False)
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

    # Build the image locally using docker.
    self.LocalBuild(image)
    self.local_build_times[image.name] = time.time() - build_start

    # Log in to the registry so we can push the image.
    self.Login()
    # Push the built image to the registry.
    self.Push(image)


@events.benchmark_start.connect
def _SetKubeConfig(unused_sender, benchmark_spec):
  """Sets the value for the kubeconfig flag if it's unspecified."""
  if not FLAGS.kubeconfig:
    FLAGS.kubeconfig = vm_util.PrependTempDir(
        'kubeconfig' + str(benchmark_spec.sequence_number))
    # Store the value for subsequent run stages.
    benchmark_spec.config.flags['kubeconfig'] = FLAGS.kubeconfig


def GetContainerClusterClass(cloud, cluster_type):
  return resource.GetResourceClass(BaseContainerCluster,
                                   CLOUD=cloud, CLUSTER_TYPE=cluster_type)


class BaseContainerCluster(resource.BaseResource):
  """A cluster that can be used to schedule containers."""

  RESOURCE_TYPE = 'BaseContainerCluster'
  REQUIRED_ATTRS = ['CLOUD', 'CLUSTER_TYPE']

  def __init__(self, cluster_spec):
    super(BaseContainerCluster, self).__init__()
    self.name = 'pkb-%s' % FLAGS.run_uri
    self.machine_type = cluster_spec.vm_spec.machine_type
    if self.machine_type is None:  # custom machine type
      self.cpus = cluster_spec.vm_spec.cpus
      self.memory = cluster_spec.vm_spec.memory
    else:
      self.cpus = None
      self.memory = None
    self.gpu_count = cluster_spec.vm_spec.gpu_count
    self.gpu_type = cluster_spec.vm_spec.gpu_type
    self.zone = cluster_spec.vm_spec.zone
    self.num_nodes = cluster_spec.vm_count
    self.min_nodes = cluster_spec.min_vm_count or self.num_nodes
    self.max_nodes = cluster_spec.max_vm_count or self.num_nodes
    self.containers = collections.defaultdict(list)
    self.services = {}

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
    metadata = {
        'cloud': self.CLOUD,
        'cluster_type': self.CLUSTER_TYPE,
        'machine_type': self.machine_type,
        'zone': self.zone,
        'size': self.num_nodes,
    }
    if self.min_nodes != self.num_nodes or self.max_nodes != self.num_nodes:
      metadata.update({
          'max_size': self.max_nodes,
          'min_size': self.min_nodes,
      })

    if self.gpu_count:
      metadata.update({
          'gpu_type': self.gpu_type,
          'num_gpus': self.gpu_count,
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
    samples.append(sample.Sample(
        'Cluster Creation Time',
        self.resource_ready_time - self.create_start_time,
        'seconds'))
    for container in itertools.chain(*list(self.containers.values())):
      metadata = {'image': container.image.split('/')[-1]}
      if container.resource_ready_time and container.create_start_time:
        samples.append(sample.Sample(
            'Container Deployment Time',
            container.resource_ready_time - container.create_start_time,
            'seconds', metadata))
      if container.delete_end_time and container.delete_start_time:
        samples.append(sample.Sample(
            'Container Delete Time',
            container.delete_end_time - container.delete_start_time,
            'seconds', metadata))
    for service in self.services.values():
      metadata = {'image': service.image.split('/')[-1]}
      if service.resource_ready_time and service.create_start_time:
        samples.append(sample.Sample(
            'Service Deployment Time',
            service.resource_ready_time - service.create_start_time,
            'seconds', metadata))
      if service.delete_end_time and service.delete_start_time:
        samples.append(sample.Sample(
            'Service Delete Time',
            service.delete_end_time - service.delete_start_time,
            'seconds', metadata))

    return samples


class KubernetesContainer(BaseContainer):
  """A Kubernetes flavor of Container."""

  def __init__(self, container_spec, name):
    super(KubernetesContainer, self).__init__(container_spec)
    self.name = name

  def _Create(self):
    """Creates the container."""
    run_cmd = [
        FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
        'run',
        self.name,
        '--image=%s' % self.image,
        '--restart=Never',
        '--limits=cpu=%sm,memory=%sMi' % (int(1000 * self.cpus), self.memory),
    ]
    if self.command:
      run_cmd.extend(['--command', '--'])
      run_cmd.extend(self.command)
    vm_util.IssueCommand(run_cmd)

  def _Delete(self):
    """Deletes the container."""
    pass

  def _IsReady(self):
    """Returns true if the container has stopped pending."""
    return self._GetPod()['status']['phase'] != 'Pending'

  def _GetPod(self):
    """Gets a representation of the POD and returns it."""
    stdout, _, _ = vm_util.IssueCommand([
        FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
        'get', 'pod', self.name, '-o', 'yaml'])
    pod = yaml.load(stdout)
    if pod:
      self.ip_address = pod.get('status', {}).get('podIP', None)
    return pod

  def WaitForExit(self, timeout=None):
    """Waits until the container has finished running."""
    @vm_util.Retry(timeout=timeout)
    def _WaitForExit():
      phase = self._GetPod()['status']['phase']
      if phase not in _K8S_FINISHED_PHASES:
        raise Exception('POD phase (%s) not in finished phases.' % phase)
    _WaitForExit()

  def GetLogs(self):
    """Returns the logs from the container."""
    stdout, _, _ = vm_util.IssueCommand([
        FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'logs', self.name])
    return stdout


class KubernetesContainerService(BaseContainerService):
  """A Kubernetes flavor of Container Service."""

  def __init__(self, container_spec, name):
    super(KubernetesContainerService, self).__init__(container_spec)
    self.name = name
    self.port = 8080

  def _Create(self):
    run_cmd = [
        FLAGS.kubectl,
        'run',
        self.name,
        '--image=%s' % self.image,
        '--limits=cpu=%sm,memory=%sMi' % (int(1000 * self.cpus), self.memory),
        '--port', str(self.port)
    ]
    if self.command:
      run_cmd.extend(['--command', '--'])
      run_cmd.extend(self.command)
    vm_util.IssueCommand(run_cmd)

    expose_cmd = [
        FLAGS.kubectl,
        '--kubeconfig', FLAGS.kubeconfig,
        'expose', 'deployment', self.name,
        '--type', 'NodePort',
        '--target-port', str(self.port)
    ]
    vm_util.IssueCommand(expose_cmd)
    with vm_util.NamedTemporaryFile() as tf:
      tf.write(_K8S_INGRESS.format(service_name=self.name))
      tf.close()
      kubernetes_helper.CreateFromFile(tf.name)

  def _GetIpAddress(self):
    """Attempts to set the Service's ip address."""
    ingress_name = '%s-ingress' % self.name
    get_cmd = [
        FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
        'get', 'ing', ingress_name,
        '-o', 'jsonpath="{.status.loadBalancer.ingress[*][\'ip\']}"'
    ]
    stdout, _, _ = vm_util.IssueCommand(get_cmd)
    ip_address = yaml.load(stdout)
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

    delete_cmd = [
        FLAGS.kubectl,
        '--kubeconfig', FLAGS.kubeconfig,
        'delete', 'deployment',
        self.name
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)


class KubernetesCluster(BaseContainerCluster):
  """A Kubernetes flavor of Container Cluster."""

  CLUSTER_TYPE = KUBERNETES

  def DeployContainer(self, base_name, container_spec):
    """Deploys Containers according to the ContainerSpec."""
    name = base_name + str(len(self.containers[base_name]))
    container = KubernetesContainer(container_spec, name)
    self.containers[base_name].append(container)
    container.Create()

  def DeployContainerService(self, name, container_spec):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    service = KubernetesContainerService(container_spec, name)
    self.services[name] = service
    service.Create()
