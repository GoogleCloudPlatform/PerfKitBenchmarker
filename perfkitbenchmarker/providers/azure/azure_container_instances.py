# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Contains classes/functions related to Azure Container Instances.

For now, these only support benchmarks that don't make use of the container's
private ip since they can't be connected to vnets.
"""

import json
from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS


class AciContainer(container_service.BaseContainer):
  """Class representing an ACI container."""

  def __init__(self, container_spec, name, resource_group):
    super(AciContainer, self).__init__(container_spec)
    self.name = name
    self.resource_group = resource_group
    benchmark_spec = context.GetThreadBenchmarkSpec()
    self.registry = benchmark_spec.container_registry

  def _Create(self):
    """Creates the container."""
    create_cmd = [
        azure.AZURE_PATH, 'container', 'create',
        '--name', self.name,
        '--image', self.image,
        '--restart-policy', 'Never',
        '--cpu', str(int(self.cpus)),
        '--memory', '%0.1f' % (self.memory / 1024.0),
    ] + self.resource_group.args
    if self.registry and self.registry.CLOUD == providers.AZURE:
      create_cmd.extend([
          '--registry-login-server', self.registry.login_server,
          '--registry-username', self.registry.service_principal.app_id,
          '--registry-password', self.registry.service_principal.password,
      ])
    if self.command:
      # Note that this is inconsistent with other containers which use lists
      # of command/args. This creates some differences mostly when
      # the command contains quotes.
      create_cmd.extend(['--command-line', ' '.join(self.command)])
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the container."""
    delete_cmd = [
        azure.AZURE_PATH, 'container', 'delete',
        '--name', self.name, '--yes',
    ] + self.resource_group.args
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  @property
  def ip_address(self):
    """Container instances don't have private ips yet."""
    raise NotImplementedError('ACI containers don\'t have private ips.')

  @ip_address.setter
  def ip_address(self, value):
    """Sets the containers ip_address."""
    self.__ip_address = value

  def _GetContainerInstance(self):
    """Gets a representation of the container and returns it."""
    show_cmd = [
        azure.AZURE_PATH, 'container', 'show', '--name', self.name
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(show_cmd)
    return json.loads(stdout)

  def _IsReady(self):
    """Returns true if the container has stopped pending."""
    state = self._GetContainerInstance()['instanceView']['state']
    return state != 'Pending'

  def WaitForExit(self, timeout=None):
    """Waits until the container has finished running."""
    @vm_util.Retry(timeout=timeout)
    def _WaitForExit():
      container = self._GetContainerInstance()['containers'][0]
      state = container['instanceView']['currentState']['state']
      if state != 'Terminated':
        raise Exception('Container not in terminated state (%s).' % state)
    _WaitForExit()

  def GetLogs(self):
    """Returns the logs from the container."""
    logs_cmd = [
        azure.AZURE_PATH, 'container', 'logs', '--name', self.name
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(logs_cmd)
    return stdout


class AciCluster(container_service.BaseContainerCluster):
  """Class that can deploy ACI containers."""

  CLOUD = providers.AZURE
  CLUSTER_TYPE = 'aci'

  def __init__(self, cluster_spec):
    super(AciCluster, self).__init__(cluster_spec)
    self.location = util.GetLocationFromZone(self.zone)
    self.resource_group = azure_network.GetResourceGroup(self.location)

  def _Create(self):
    """ACI has no cluster."""
    pass

  def _Delete(self):
    """ACI has no cluster."""
    pass

  def _CreateDependencies(self):
    """Creates the resource group."""
    self.resource_group.Create()

  def _DeleteDependencies(self):
    """Deletes the resource group."""
    self.resource_group.Delete()

  def DeployContainer(self, base_name, container_spec):
    """Deploys Containers according to the ContainerSpec."""
    name = base_name + str(len(self.containers[base_name]))
    container = AciContainer(container_spec, name, self.resource_group)
    self.containers[base_name].append(container)
    container.Create()

  def DeployContainerService(self, name, container_spec):
    """Deploys a ContainerSerivice according to the ContainerSpec."""
    raise NotImplementedError()
