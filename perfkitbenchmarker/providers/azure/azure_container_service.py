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


class AcsKubernetesCluster(container_service.KubernetesCluster):

  CLOUD = providers.AZURE

  def __init__(self, spec):
    """Initializes the cluster."""
    super(AcsKubernetesCluster, self).__init__(spec)
    self.resource_group = azure_network.GetResourceGroup(self.zone)
    self.name = 'pkbcluster%s' % FLAGS.run_uri
    self._deleted = False

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
    ] + self.resource_group.args, timeout=600)

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

  def _PostCreate(self):
    """Get cluster info."""
    vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'acs', 'kubernetes', 'get-credentials',
        '--name', self.name, '--file', FLAGS.kubeconfig,
        '--ssh-key-file', vm_util.GetPrivateKeyPath(),
    ] + self.resource_group.args)

  def _CreateDependencies(self):
    """Creates the resource group."""
    self.resource_group.Create()

  def _DeleteDependencies(self):
    """Deletes the resource group."""
    self.resource_group.Delete()
