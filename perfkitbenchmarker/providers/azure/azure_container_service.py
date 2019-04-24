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

"""Contains classes/functions related to Azure Container Service."""

import json

from perfkitbenchmarker import container_service
from perfkitbenchmarker import flags
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
    self.resource_group = azure_network.GetResourceGroup(self.zone)
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
    ], suppress_warning=True)
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
    self.resource_group.Delete()
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
    self.resource_group = azure_network.GetResourceGroup(self.zone)
    self.name = 'pkbcluster%s' % FLAGS.run_uri
    self.service_principal = service_principal.ServicePrincipal.GetInstance()
    self._deleted = False

  # TODO(ferneyhough): Consider adding
  # --kubernetes-version=FLAGS.container_cluster_version.
  def _Create(self):
    """Creates the AKS cluster."""
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'aks', 'create',
        '--name', self.name,
        '--enable-rbac',
        '--node-vm-size', self.machine_type,
        '--node-count', str(self.num_nodes),
        '--location', self.zone,
        '--dns-name-prefix', 'pkb' + FLAGS.run_uri,
        '--ssh-key-value', vm_util.GetPublicKeyPath(),
        '--service-principal', self.service_principal.app_id,
        '--client-secret', self.service_principal.password,
    ] + self.resource_group.args, timeout=1800)

  def _Exists(self):
    """Returns True if the cluster exists."""
    if self._deleted:
      return False
    stdout, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH, 'aks', 'show', '--name', self.name,
    ] + self.resource_group.args)
    try:
      json.loads(stdout)
      return True
    except ValueError:
      return False

  def _Delete(self):
    """Deletes the AKS cluster."""
    # This will be deleted along with the resource group
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
    _, _, retcode = vm_util.IssueCommand(version_cmd, suppress_warning=True)
    if retcode:
      return False
    # POD creation will fail until the default service account in created.
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
    self.resource_group.Delete()
    self.service_principal.Delete()
