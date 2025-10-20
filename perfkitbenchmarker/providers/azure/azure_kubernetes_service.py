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
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS


class AzureContainerRegistry(container_service.BaseContainerRegistry):
  """Class for building and storing container images on Azure."""

  CLOUD = provider_info.AZURE

  def __init__(self, registry_spec):
    super().__init__(registry_spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.resource_group = azure_network.GetResourceGroup(self.region)
    self.login_server = None
    self.sku = 'Basic'
    self._deleted = False
    self.acr_id = None

  def _Exists(self):
    """Returns True if the registry exists."""
    if self._deleted:
      return False
    stdout, _, _ = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'acr', 'show', '--name', self.name]
        + self.resource_group.args,
        raise_on_failure=False,
    )
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
    vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'acr',
            'create',
            '--name',
            self.name,
            '--sku',
            self.sku,
        ]
        + self.resource_group.args
    )

  def _Delete(self):
    """Deletes the registry."""
    # This will be deleted along with the resource group
    self._deleted = True

  def _CreateDependencies(self):
    """Creates the resource group."""
    self.resource_group.Create()

  def Login(self):
    """Logs in to the registry."""
    vm_util.IssueCommand([
        azure.AZURE_PATH,
        'acr',
        'login',
        '--name',
        self.name,
    ])

  def GetFullRegistryTag(self, image):
    """Gets the full tag of the image."""
    full_tag = '{login_server}/{name}'.format(
        login_server=self.login_server, name=image
    )
    return full_tag


class AksCluster(container_service.KubernetesCluster):
  """Class representing an Azure Kubernetes Service cluster."""

  CLOUD = provider_info.AZURE

  def __init__(self, spec):
    """Initializes the cluster."""
    super().__init__(spec)
    self.region = util.GetRegionFromZone(self.zone)
    self.resource_group = azure_network.GetResourceGroup(self.region)
    self.node_resource_group = None
    self.name = 'pkbcluster%s' % FLAGS.run_uri
    # TODO(pclay): replace with built in service principal once I figure out how
    # to make it work with ACR
    self.cluster_version = FLAGS.container_cluster_version
    self._deleted = False

  def InitializeNodePoolForCloud(
      self,
      vm_config: virtual_machine.BaseVirtualMachine,
      nodepool_config: container_service.BaseNodePoolConfig,
  ):
    nodepool_config.disk_type = (
        vm_config.create_os_disk_strategy.disk.disk_type  # pytype: disable=attribute-error
    )
    nodepool_config.disk_size = (
        vm_config.create_os_disk_strategy.disk.disk_size  # pytype: disable=attribute-error
    )

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = super().GetResourceMetadata()
    result['boot_disk_type'] = self.default_nodepool.disk_type
    result['boot_disk_size'] = self.default_nodepool.disk_size
    return result

  def _IsAutoscalerEnabled(self):
    """Returns True if the cluster autoscaler is enabled."""
    return (
        self.min_nodes != self.default_nodepool.num_nodes
        or self.max_nodes != self.default_nodepool.num_nodes
    )

  def _Create(self):
    """Creates the AKS cluster."""
    cmd = [
        azure.AZURE_PATH,
        'aks',
        'create',
        '--name',
        self.name,
        '--location',
        self.region,
        '--enable-managed-identity',
        '--ssh-key-value',
        vm_util.GetPublicKeyPath(),
        '--nodepool-name',
        container_service.DEFAULT_NODEPOOL,
        '--nodepool-labels',
        f'pkb_nodepool={container_service.DEFAULT_NODEPOOL}',
    ] + self._GetNodeFlags(self.default_nodepool)
    if FLAGS.azure_aks_auto_node_provisioning:
      # For provision_node_pools benchmark, add auto provisioning mode
      cmd.append('--node-provisioning-mode=auto')
    else:
      if self._IsAutoscalerEnabled():
        cmd += [
            '--enable-cluster-autoscaler',
            f'--min-count={self.min_nodes}',
            f'--max-count={self.max_nodes}',
        ]

    # TODO(pclay): expose quota and capacity errors
    # Creating an AKS cluster with a fresh service principal usually fails due
    # to a race condition. Active Directory knows the service principal exists,
    # but AKS does not. (https://github.com/Azure/azure-cli/issues/9585)
    # Use 5 min timeout on service principle retry. cmd will fail fast.
    vm_util.Retry(timeout=300)(vm_util.IssueCommand)(
        cmd,
        # Half hour timeout on creating the cluster.
        timeout=1800,
    )

    for _, nodepool in self.nodepools.items():
      self._CreateNodePool(nodepool)

  def _CreateNodePool(
      self, nodepool_config: container_service.BaseNodePoolConfig
  ):
    """Creates a node pool."""
    cmd = [
        azure.AZURE_PATH,
        'aks',
        'nodepool',
        'add',
        '--cluster-name',
        self.name,
        '--name',
        _AzureNodePoolName(nodepool_config.name),
        '--labels',
        f'pkb_nodepool={nodepool_config.name}',
    ] + self._GetNodeFlags(nodepool_config)
    vm_util.IssueCommand(cmd, timeout=600)

  def _GetNodeFlags(
      self, nodepool_config: container_service.BaseNodePoolConfig
  ) -> List[str]:
    """Common flags for create and nodepools add."""
    args = [
        '--node-vm-size',
        nodepool_config.machine_type,
    ] + self.resource_group.args
    node_count = nodepool_config.num_nodes
    if self._IsAutoscalerEnabled():
      node_count = max(self.min_nodes, node_count)
      node_count = min(self.max_nodes, node_count)
    args += [f'--node-count={node_count}']
    if self.default_nodepool.zone and self.default_nodepool.zone != self.region:
      zones = ' '.join(
          zone[-1] for zone in self.default_nodepool.zone.split(',')
      )
      args += ['--zones', zones]
    if self.default_nodepool.disk_size:
      args += ['--node-osdisk-size', str(self.default_nodepool.disk_size)]
    if self.cluster_version:
      args += ['--kubernetes-version', self.cluster_version]
    return args

  def _Exists(self):
    """Returns True if the cluster exists."""
    if self._deleted:
      return False
    stdout, _, _ = vm_util.IssueCommand(
        [
            azure.AZURE_PATH,
            'aks',
            'show',
            '--name',
            self.name,
        ]
        + self.resource_group.args,
        raise_on_failure=False,
    )
    try:
      cluster = json.loads(stdout)
      self.node_resource_group = cluster['nodeResourceGroup']
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
    if self.event_poller:
      self.event_poller.StopPolling()
    self._deleted = True

  def _AttachContainerRegistry(self):
    """Attaches the container registry if it exists."""
    if not self.container_registry:
      return
    attach_registry_cmd = [
        azure.AZURE_PATH,
        'aks',
        'update',
        '--name',
        self.name,
        '--resource-group',
        self.resource_group.name,
        '--attach-acr',
        self.container_registry.name,
    ]
    vm_util.IssueCommand(attach_registry_cmd)

  def _PostCreate(self):
    """Tags the cluster resource group."""
    super()._PostCreate()
    self._GetCredentials(use_admin=True)
    self._WaitForDefaultServiceAccount()
    set_tags_cmd = [
        azure.AZURE_PATH,
        'group',
        'update',
        '-g',
        self.node_resource_group,
        '--set',
        util.GetTagsJson(self.resource_group.timeout_minutes),
    ]
    vm_util.IssueCommand(set_tags_cmd)
    self._AttachContainerRegistry()

  def _GetCredentials(self, use_admin: bool) -> None:
    """Helper method to get credentials and check service account readiness.

    Args:
        use_admin (bool): Whether to use the `--admin` flag in the `aks
          get-credentials` command.
    """
    # Fetch Kubernetes credentials
    cmd = [
        azure.AZURE_PATH,
        'aks',
        'get-credentials',
        '--name',
        self.name,
        '--file',
        FLAGS.kubeconfig,
    ]
    if use_admin:
      cmd.append('--admin')
    cmd += self.resource_group.args
    vm_util.IssueCommand(cmd)

  def _IsReady(self) -> bool:
    """Returns True if the cluster is ready."""
    show_cmd = [
        azure.AZURE_PATH,
        'aks',
        'show',
        '--name',
        self.name,
        '--query',
        'provisioningState',
        '--output',
        'tsv',
    ] + self.resource_group.args
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)

    try:
      provisioning_state = stdout.strip()
      if provisioning_state == 'Failed':
        raise errors.Resource.CreationError('Cluster provisioning failed.')
      if provisioning_state != 'Succeeded':
        return False
    except json.JSONDecodeError:
      return False
    return True

  def _WaitForDefaultServiceAccount(self):
    """Waits for the default service account to be created, or throws an error.

    POD creation will fail until the default service account is created.
    """

    @vm_util.Retry(
        poll_interval=self.POLL_INTERVAL,
        fuzz=0,
        timeout=self.READY_TIMEOUT,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _GetServiceAccount():
      """Inner function to retry but passing self.POLL_INTERVAL as default.

      Raises:
        errors.Resource.CreationError: If the user does not have sufficient
          permissions to list service accounts.
        errors.VmUtil.IssueCommandError: If the command to get service accounts
          fails for any other reason.
        errors.Resource.RetryableCreationError: If the default service account
          has not yet been created. This is automatically retried by the
          outer decorator.
      """
      get_service_account_cmd = [
          FLAGS.kubectl,
          '--kubeconfig',
          FLAGS.kubeconfig,
          'get',
          'serviceAccounts',
      ]
      stdout, err, code = vm_util.IssueCommand(
          get_service_account_cmd, raise_on_failure=False
      )
      if 'default' not in stdout:
        raise errors.Resource.RetryableCreationError(
            'Service account not yet ready.'
        )
      if code != 0:
        if 'User does not have access to the resource in Azure' in err:
          raise errors.Resource.RetryableCreationError(
              'First kubectl command failed with permission denied, but'
              ' retrying as this can just be a race condition.'
          )
        raise errors.Resource.CreationError(
            'First kubectl command failed with error: %s' % err
        )

    _GetServiceAccount()

  def _CreateDependencies(self):
    """Creates the resource group."""
    self.resource_group.Create()

  def GetDefaultStorageClass(self) -> str:
    """Get the default storage class for the provider."""
    # https://docs.microsoft.com/en-us/azure/aks/csi-storage-drivers
    # Premium_LRS
    return 'managed-csi-premium'

  def ResizeNodePool(
      self, new_size: int, node_pool: str = container_service.DEFAULT_NODEPOOL
  ):
    """Change the number of nodes in the node pool."""
    cmd = [
        azure.AZURE_PATH,
        'aks',
        'nodepool',
        'scale',
        '--cluster-name',
        self.name,
        '--name',
        _AzureNodePoolName(node_pool),
        f'--node-count={new_size}',
    ] + self.resource_group.args
    vm_util.IssueCommand(cmd)

  def GetNodePoolNames(self) -> list[str]:
    """Get node pool names for the cluster."""
    if FLAGS.azure_aks_auto_node_provisioning:
      cmd = [
          FLAGS.kubectl,
          '--kubeconfig',
          FLAGS.kubeconfig,
          'get',
          'nodepools',
          '-o',
          'json',
      ]
      stdout, _, _ = vm_util.IssueCommand(cmd)
      nodepools = json.loads(stdout).get('items', [])
      return [nodepool['metadata']['name'] for nodepool in nodepools]
    else:
      cmd = [
          azure.AZURE_PATH,
          'aks',
          'nodepool',
          'list',
          '--cluster-name',
          self.name,
      ] + self.resource_group.args
      stdout, _, _ = vm_util.IssueCommand(cmd)
      nodepools = json.loads(stdout)
      return [nodepool['name'] for nodepool in nodepools]

  def AddNodepool(self, batch_name, pool_id):
    """Add a Karpenter NodePool and AKSNodeClass to the AKS cluster."""
    self.ApplyManifest(
        'provision_node_pools/aks/nodepool.yaml.j2',
        batch=batch_name,
        id=pool_id,
        cluster_name=self.name,
        spot=FLAGS.azure_aks_spot_nodepools,
    )


class AksAutomaticCluster(AksCluster):
  """Class representing an AKS Automatic cluster, which has managed node pools.

  This feature is currently in preview. To provision an AKS Automatic cluster,
  you'll need to install the Azure CLI 'aks-preview' extension.
  For more details, see the official documentation:
  https://learn.microsoft.com/en-us/azure/aks/automatic/quick-automatic-managed-network
  """

  CLOUD = provider_info.AZURE
  CLUSTER_TYPE = 'Auto'

  def _Create(self):
    """Creates the Automatic AKS cluster with tags."""
    tags_dict = util.GetResourceTags(self.resource_group.timeout_minutes)
    tags_list = [f'{k}={v}' for k, v in tags_dict.items()]
    cmd = [
        azure.AZURE_PATH,
        'aks',
        'create',
        '--name',
        self.name,
        '--location',
        self.region,
        '--ssh-key-value',
        vm_util.GetPublicKeyPath(),
        '--resource-group',
        self.resource_group.name,
        '--sku',
        'automatic',
        '--tags',
    ] + tags_list
    vm_util.IssueCommand(
        cmd,
        # Half hour timeout on creating the cluster.
        timeout=1800,
    )

  def _CreateRoleAssignment(self):
    """Creates a role assignment for the current user."""
    full_cluster_id, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH,
        'aks',
        'show',
        '--name',
        self.name,
        '--resource-group',
        self.resource_group.name,
        '--query',
        'id',
        '--output',
        'tsv',
    ])
    full_cluster_id = full_cluster_id.strip()
    current_user, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH,
        'account',
        'show',
        '--query',
        'user.name',
        '--output',
        'tsv',
    ])
    current_user = current_user.strip()
    create_role_assignment_cmd = [
        azure.AZURE_PATH,
        'role',
        'assignment',
        'create',
        '--assignee',
        current_user,
        '--role',
        'Azure Kubernetes Service RBAC Admin',
        '--scope',
        full_cluster_id,
    ]
    vm_util.IssueCommand(create_role_assignment_cmd)

  def _PostCreate(self):
    """Skip the superclass's _PostCreate() method.

    Needed as node_resource_group is pre-configured and fully managed in
    Automatic clusters & role assignment must be created before authenticating.
    """
    super(container_service.KubernetesCluster, self)._PostCreate()
    user_type, _, _ = vm_util.IssueCommand([
        azure.AZURE_PATH,
        'account',
        'show',
        '--query',
        'user.type',
        '--output',
        'tsv',
    ])
    user_type = user_type.strip()
    if user_type == 'servicePrincipal':
      self._CreateRoleAssignment()
    self._GetCredentials(use_admin=False)
    self._WaitForDefaultServiceAccount()
    self._AttachContainerRegistry()


def _AzureNodePoolName(pkb_nodepool_name: str) -> str:
  """Truncate nodepool name for AKS."""
  # https://learn.microsoft.com/en-us/azure/aks/create-node-pools#limitations
  return pkb_nodepool_name[:12]
