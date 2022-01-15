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

"""Contains classes/functions related to Azure Kubernetes Service."""

import json
from typing import List

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import service_principal
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS


class AzureContainerRegistry(container_service.BaseContainerRegistry):
  """Class for building and storing container images on Azure."""

  CLOUD = providers.AZURE

  def __init__(self, registry_spec):
    super(AzureContainerRegistry, self).__init__(registry_spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.resource_group = azure_network.GetResourceGroup(self.region)
    self.login_server = None
    self.sku = 'Basic'
    self._deleted = False
    self.acr_id = None
    self.service_principal = service_principal.ServicePrincipal.GetInstance()

  def _Exists(self):
    """Returns True if the registry exists."""
    if self._deleted:
      return False
    stdout, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH, 'acr', 'show', '--name', self.name,
    ], suppress_warning=True, raise_on_failure=False)
    try:
      registry = json.loads(stdout)
      self.login_server = registry['loginServer']
      self.acr_id = registry['id']
      return True
    except ValueError:
      return False

  def _Create(self):
    """Creates the registry."""
    if self._Exists():
      return
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'acr', 'create',
        '--name', self.name,
        '--sku', self.sku
    ] + self.resource_group.args)

  def _Delete(self):
    """Deletes the registry."""
    # This will be deleted along with the resource group
    self._deleted = True

  def _PostCreate(self):
    """Allow the service principle to read from the repository."""
    # If we bootstrapped our own credentials into the AKS cluster it already,
    # has read permission, because it created the repo.
    if not FLAGS.bootstrap_azure_service_principal:
      create_role_assignment_cmd = [
          azure.AZURE_PATH, 'role', 'assignment', 'create',
          '--assignee', self.service_principal.app_id,
          '--role', 'Reader',
          '--scope', self.acr_id,
      ]
      vm_util.IssueRetryableCommand(create_role_assignment_cmd)

  def _CreateDependencies(self):
    """Creates the resource group."""
    self.resource_group.Create()
    self.service_principal.Create()

  def _DeleteDependencies(self):
    """Deletes the resource group."""
    self.service_principal.Delete()

  def Login(self):
    """Logs in to the registry."""
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'acr', 'login',
        '--name', self.name,
    ])

  def GetFullRegistryTag(self, image):
    """Gets the full tag of the image."""
    full_tag = '{login_server}/{name}'.format(
        login_server=self.login_server, name=image)
    return full_tag


class AksCluster(container_service.KubernetesCluster):
  """Class representing an Azure Kubernetes Service cluster."""

  CLOUD = providers.AZURE

  def __init__(self, spec):
    """Initializes the cluster."""
    super(AksCluster, self).__init__(spec)
    if util.IsZone(spec.vm_spec.zone):
      raise errors.Config.InvalidValue(
          'Availability zones are currently not supported by Aks Cluster')
    self.region = util.GetRegionFromZone(self.zone)
    self.resource_group = azure_network.GetResourceGroup(self.region)
    self.name = 'pkbcluster%s' % FLAGS.run_uri
    # TODO(pclay): replace with built in service principal once I figure out how
    # to make it work with ACR
    self.service_principal = service_principal.ServicePrincipal.GetInstance()
    self.cluster_version = FLAGS.container_cluster_version
    self._deleted = False

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super(AksCluster, self).GetResourceMetadata()
    result['container_cluster_version'] = self.cluster_version
    result['boot_disk_type'] = self.vm_config.os_disk.disk_type
    result['boot_disk_size'] = self.vm_config.os_disk.disk_size
    return result

  def _Create(self):
    """Creates the AKS cluster."""
    cmd = [
        azure.AZURE_PATH, 'aks', 'create',
        '--name', self.name,
        '--location', self.region,
        '--ssh-key-value', vm_util.GetPublicKeyPath(),
        '--service-principal', self.service_principal.app_id,
        # TODO(pclay): avoid logging client secret
        '--client-secret',
        self.service_principal.password,
        '--nodepool-name',
        container_service.DEFAULT_NODEPOOL,
        '--nodepool-labels',
        f'pkb_nodepool={container_service.DEFAULT_NODEPOOL}',
    ] + self._GetNodeFlags(self.num_nodes, self.vm_config)

    # TODO(pclay): expose quota and capacity errors
    # Creating an AKS cluster with a fresh service principal usually fails due
    # to a race condition. Active Directory knows the service principal exists,
    # but AKS does not. (https://github.com/Azure/azure-cli/issues/9585)
    # Use 5 min timeout on service principle retry. cmd will fail fast.
    vm_util.Retry(timeout=300)(vm_util.IssueCommand)(
        cmd,
        # Half hour timeout on creating the cluster.
        timeout=1800)

    for name, node_pool in self.nodepools.items():
      self._CreateNodePool(name, node_pool)

  def _CreateNodePool(self, name: str, node_pool):
    """Creates a node pool."""
    cmd = [
        azure.AZURE_PATH, 'aks', 'nodepool', 'add',
        '--cluster-name', self.name,
        '--name', name,
        '--labels', f'pkb_nodepool={name}',
    ] + self._GetNodeFlags(node_pool.num_nodes, node_pool.vm_config)
    vm_util.IssueCommand(cmd, timeout=600)

  def _GetNodeFlags(self, num_nodes: int, vm_config) -> List[str]:
    """Common flags for create and nodepools add."""
    args = [
        '--node-vm-size', vm_config.machine_type,
        '--node-count', str(num_nodes),
    ] + self.resource_group.args
    if self.vm_config.os_disk and self.vm_config.os_disk.disk_size:
      args += ['--node-osdisk-size', str(self.vm_config.os_disk.disk_size)]
    if self.cluster_version:
      args += ['--kubernetes-version', self.cluster_version]
    return args

  def _Exists(self):
    """Returns True if the cluster exists."""
    if self._deleted:
      return False
    stdout, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH, 'aks', 'show', '--name', self.name,
    ] + self.resource_group.args, raise_on_failure=False)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def _Delete(self):
    """Deletes the AKS cluster."""
    # Do not call super._Delete() as it will try to delete containers and the
    # cluster may have already been deleted by deleting a corresponding
    # AzureContainerRegistry. The registry deletes the shared resource group.
    #
    # Normally only azure networks manage resource groups because,
    # PKB assumes all benchmarks use VMs and VMs always depend on networks.
    # However container benchmarks do not provision networks and
    # directly manage their own resource groups. However ContainerClusters and
    # ContainerRegistries can be used independently so they must both directly
    # mangage the undlerlying resource group. This is indempotent, but can cause
    # AKS clusters to have been deleted before calling _Delete().
    #
    # If it has not yet been deleted it will be deleted along with the resource
    # group.
    self._deleted = True

  def _PostCreate(self):
    """Tags the cluster resource group."""
    super(AksCluster, self)._PostCreate()
    cluster_resource_group_name = 'MC_%s_%s_%s' % (
        self.resource_group.name, self.name, self.zone)
    set_tags_cmd = [
        azure.AZURE_PATH, 'group', 'update', '-g', cluster_resource_group_name,
        '--set', util.GetTagsJson(self.resource_group.timeout_minutes)
    ]
    vm_util.IssueCommand(set_tags_cmd)

  def _IsReady(self):
    """Returns True if the cluster is ready."""
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'aks', 'get-credentials',
        '--admin',
        '--name', self.name,
        '--file', FLAGS.kubeconfig,
    ] + self.resource_group.args, suppress_warning=True)
    version_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'version']
    _, _, retcode = vm_util.IssueCommand(version_cmd, suppress_warning=True,
                                         raise_on_failure=False)
    if retcode:
      return False
    # POD creation will fail until the default service account is created.
    get_cmd = [
        FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
        'get', 'serviceAccounts'
    ]
    stdout, _, _ = vm_util.IssueCommand(get_cmd)
    return 'default' in stdout

  def _CreateDependencies(self):
    """Creates the resource group."""
    self.resource_group.Create()
    self.service_principal.Create()

  def _DeleteDependencies(self):
    """Deletes the resource group."""
    self.service_principal.Delete()
