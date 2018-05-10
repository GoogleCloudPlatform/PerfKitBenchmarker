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
      return True
    except Exception:
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

  def _CreateDependencies(self):
    """Creates the resource group."""
    self.resource_group.Create()

  def _DeleteDependencies(self):
    """Deletes the resource group."""
    self.resource_group.Delete()

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


class AcsKubernetesCluster(container_service.KubernetesCluster):

  CLOUD = providers.AZURE

  def __init__(self, spec):
    """Initializes the cluster."""
    super(AcsKubernetesCluster, self).__init__(spec)
    self.resource_group = azure_network.GetResourceGroup(self.zone)
    self.name = 'pkbcluster%s' % FLAGS.run_uri
    self._deleted = False

  # TODO(ferneyhough): Consider adding
  # --api-version=FLAGS.container_cluster_version.
  def _Create(self):
    """Creates the ACS cluster."""
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'acs', 'create',
        '--name', self.name,
        '--agent-vm-size', self.machine_type,
        '--agent-count', str(self.num_nodes),
        '--location', self.zone,
        '--orchestrator-type', 'Kubernetes',
        '--dns-prefix', 'pkb' + FLAGS.run_uri,
        '--ssh-key-value', vm_util.GetPublicKeyPath(),
    ] + self.resource_group.args, timeout=1800)

  def _Exists(self):
    """Returns True if the cluster exists."""
    if self._deleted:
      return False
    stdout, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH, 'acs', 'show', '--name', self.name,
    ] + self.resource_group.args)
    try:
      json.loads(stdout)
      return True
    except:
      return False

  def _Delete(self):
    """Deletes the ACS cluster."""
    # This will be deleted along with the resource group
    self._deleted = True

  def _IsReady(self):
    """Returns True if the cluster is ready."""
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'acs', 'kubernetes', 'get-credentials',
        '--name', self.name, '--file', FLAGS.kubeconfig,
        '--ssh-key-file', vm_util.GetPrivateKeyPath(),
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

  def _DeleteDependencies(self):
    """Deletes the resource group."""
    self.resource_group.Delete()
