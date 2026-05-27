"""Tests for the Azure Kubernetes Service provider."""
# pylint: disable=invalid-name,protected-access

import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.azure import azure_kubernetes_service
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case  # pylint: disable=no-name-in-module


class AzureKubernetesServiceTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the AksCluster provider."""

  def setUp(self):
    """Sets up mocks and creates a default AksCluster for each test."""
    super().setUp()
    self.enter_context(
        mock.patch.object(
            azure_network,
            'GetResourceGroup',
            autospec=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            azure_network.AzureNetwork,
            'GetNetwork',
            autospec=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            azure_network.AzureFirewall,
            'GetFirewall',
            autospec=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            util,
            'GetResourceTags',
            return_value={},
        )
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'GetPublicKeyPath',
            return_value='test_key_path',
        )
    )
    self.enter_context(flagsaver.flagsaver(run_uri='123'))
    self.spec_dict = {
        'cloud': 'Azure',
        'vm_spec': {
            'Azure': {
                'machine_type': 'Standard_D2s_v5',
                'zone': 'westus2-1',
            },
        },
    }
    self.initAksCluster(self.spec_dict)

  def initAksCluster(self, spec_dict):
    self.spec = container_spec.ContainerClusterSpec(
        'NAME',
        **spec_dict,
    )
    self.aks = azure_kubernetes_service.AksCluster(self.spec)
    self.aks.resource_group.args = []

  def testCreate(self):
    """AksCluster.Create() issues the expected az aks create command."""
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
            'az aks show': [
                (
                    (
                        '{"provisioningState": "Succeeded",'
                        ' "nodeResourceGroup": "node-resource-group"}'
                    ),
                    '',
                    0,
                ),
                ('Succeeded', '', 0),
            ],
            'get serviceAccounts': [('default, foo', '', 0)],
        },
    )
    self.aks.Create()
    self.assertEqual(
        mock_cmd.func_to_mock.mock_calls[0],
        mock.call(
            [
                'az',
                'aks',
                'create',
                '--name',
                'pkbcluster123',
                '--location',
                'westus2',
                '--enable-managed-identity',
                '--ssh-key-value',
                'test_key_path',
                '--nodepool-name',
                'default',
                '--nodepool-labels',
                'pkb_nodepool=default',
                '--node-vm-size',
                'Standard_D2s_v5',
                '--node-count=1',
                '--zones',
                '1',
            ],
            timeout=1800,
            raise_on_failure=False,
        ),
    )

  def testCreateError(self):
    """AksCluster.Create() raises CreationError when az aks create fails."""
    self.MockIssueCommand(
        {
            'az aks create': [('out', 'Error could not create', 1)],
            'az aks nodepool': [('', '', 0)],
            'az aks show': [
                (
                    (
                        '{"provisioningState": "Succeeded",'
                        ' "nodeResourceGroup": "node-resource-group"}'
                    ),
                    '',
                    0,
                ),
                ('Succeeded', '', 0),
            ],
            'get serviceAccounts': [('default, foo', '', 0)],
        },
    )
    with self.assertRaises(errors.Resource.CreationError):
      self.aks.Create()

  def testCreateNodepool(self):
    """Additional nodepools appear in az aks nodepool add commands."""
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
        },
    )
    self.spec_dict['nodepools'] = {
        'client': {
            'vm_spec': {
                'Azure': {
                    'machine_type': 'Standard_D4s_v5',
                    'zone': 'eastus2-1',
                }
            }
        },
    }
    self.initAksCluster(self.spec_dict)
    self.aks._Create()
    self.assertLen(mock_cmd.func_to_mock.mock_calls, 2)
    self.assertEqual(
        mock_cmd.func_to_mock.mock_calls[1],
        mock.call(
            [
                'az',
                'aks',
                'nodepool',
                'add',
                '--cluster-name',
                'pkbcluster123',
                '--name',
                'client',
                '--labels',
                'pkb_nodepool=client',
                '--node-vm-size',
                'Standard_D4s_v5',
                '--node-count=1',
                '--zones',
                '1',
            ],
            timeout=600,
            raise_on_failure=False,
        ),
    )

  def testCreateAutoscaler(self):
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
        },
    )
    self.spec_dict['min_vm_count'] = 2
    self.spec_dict['max_vm_count'] = 4
    self.initAksCluster(self.spec_dict)
    self.aks._Create()
    self.assertContainsSubset(
        [
            '--enable-cluster-autoscaler',
            '--min-count=2',
            '--max-count=4',
            '--node-count=2',
        ],
        mock_cmd.func_to_mock.mock_calls[0].args[0],
    )

  def testCreateAutoscaler_NodepoolAndClamps(self):
    """Autoscaler min/max/desired values propagate to nodepool add commands."""
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
        },
    )
    self.spec_dict['nodepools'] = {
        'client': {
            'vm_spec': {
                'Azure': {
                    'machine_type': 'Standard_D4s_v5',
                }
            },
            'min_vm_count': 4,
            'max_vm_count': 6,
            'vm_count': 3,
        },
    }
    self.initAksCluster(self.spec_dict)
    self.aks._Create()
    self.assertIn(
        '--enable-cluster-autoscaler --min-count=4 --max-count=6'
        + ' --node-count=4',
        mock_cmd.all_commands,
    )

  @flagsaver.flagsaver(kubectl='kubectl', kubeconfig='dummy')
  def testFullCreateAksAutomatic(self):
    """AksAutomaticCluster.Create() issues RBAC and policy assignment cmds."""
    aks_auto = azure_kubernetes_service.AksAutomaticCluster(self.spec)
    aks_auto.resource_group.name = 'resource-group'
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
            '--query id': [('cluster-id', '', 0)],
            'az aks show': [
                (
                    (
                        '{"provisioningState": "Succeeded",'
                        ' "nodeResourceGroup": "node-resource-group"}'
                    ),
                    '',
                    0,
                ),
                ('Succeeded', '', 0),
            ],
            'get serviceAccounts': [('default, foo', '', 0)],
            'az account show': [
                ('servicePrincipal', '', 0),
                ('user-name', '', 0),
                (
                    'test-user@example.com\n'
                    + '12345678-1234-1234-1234-123456789abc',
                    '',
                    0,
                ),
                ('cluster-id', '', 0),
            ],
            'az role assignment': [('', '', 0)],
            'az policy assignment': [('', '', 0)],
            'kubectl --kubeconfig dummy get constraints': [('dryrun', '', 0)],
        },
    )
    aks_auto.Create()
    self.assertIn(
        'az role assignment create --assignee user-name --role Azure Kubernetes'
        + ' Service RBAC Admin',
        mock_cmd.all_commands,
    )
    self.assertIn(
        'az role assignment create --role Resource Policy Contributor',
        mock_cmd.all_commands,
    )
    self.assertIn(
        'az policy assignment update --name'
        + ' aks-deployment-safeguards-policy-assignment',
        mock_cmd.all_commands,
    )

  def testGetNodePoolNames(self):
    """GetNodePoolNames returns pool names from az aks nodepool list output."""
    self.MockIssueCommand(
        {
            'az aks nodepool list': [(
                """[
  {
      "count": 4,
      "name": "default"
  },
  {
      "count": 1,
      "name": "nodepool1"
  }
]
""",
                '',
                0,
            )],
        },
    )
    self.assertEqual(self.aks.GetNodePoolNames(), ['default', 'nodepool1'])


class AksManagementPlaneTest(AzureKubernetesServiceTest):
  """Tests for AKS management-plane methods (k8s_management_benchmark)."""

  # These tests are inherited from AzureKubernetesServiceTest but are not
  # relevant to the management-plane test suite. Override to skip them so
  # they don't pollute the AksManagementPlaneTest results.
  def testCreate(self):
    pass

  def testCreateError(self):
    pass

  def _make_nodepool_config(self, name='pkbpool0',
                             machine_type='Standard_D2s_v5',
                             num_nodes=2):
    cfg = mock.MagicMock()
    cfg.name = name
    cfg.num_nodes = num_nodes
    cfg.machine_type = machine_type
    cfg.min_nodes = num_nodes
    cfg.max_nodes = num_nodes
    cfg.disk_size = 100
    return cfg

  # ---- CreateNodePool -------------------------------------------------------

  def testCreateNodePool(self):
    """CreateNodePool issues 'az aks nodepool add' with cluster-name."""
    mock_cmd = self.MockIssueCommand({'az aks nodepool add': [('', '', 0)]})
    self.aks.CreateNodePool(self._make_nodepool_config('testpool'))

    self.assertIn('az aks nodepool add', mock_cmd.all_commands)
    self.assertIn('--cluster-name', mock_cmd.all_commands)
    self.assertIn('--labels', mock_cmd.all_commands)

  def testCreateNodePoolWithVersion(self):
    """CreateNodePool passes --kubernetes-version when node_version is set."""
    self.aks.cluster_version = '1.33'
    mock_cmd = self.MockIssueCommand({'az aks nodepool add': [('', '', 0)]})
    self.aks.CreateNodePool(
        self._make_nodepool_config('verpool'), node_version='1.32'
    )
    self.assertIn('--kubernetes-version 1.32', mock_cmd.all_commands)

  def testCreateNodePoolRaisesOnFailure(self):
    """CreateNodePool raises CreationError when CLI fails."""
    self.MockIssueCommand({'az aks nodepool add': [('', 'error', 1)]})
    with self.assertRaises(errors.Resource.CreationError):
      self.aks.CreateNodePool(self._make_nodepool_config('failpool'))

  # ---- DeleteNodePool -------------------------------------------------------

  def testDeleteNodePool(self):
    """DeleteNodePool issues 'az aks nodepool delete' with cluster-name."""
    mock_cmd = self.MockIssueCommand(
        {'az aks nodepool delete': [('', '', 0)]}
    )
    self.aks.DeleteNodePool('old-pool')

    self.assertIn('az aks nodepool delete', mock_cmd.all_commands)
    self.assertIn('--cluster-name', mock_cmd.all_commands)

  # ---- UpgradeNodePool ------------------------------------------------------

  def testUpgradeNodePool(self):
    """UpgradeNodePool issues 'az aks nodepool upgrade' with version."""
    mock_cmd = self.MockIssueCommand(
        {'az aks nodepool upgrade': [('', '', 0)]}
    )
    self.aks.UpgradeNodePool('my-pool', '1.34')

    self.assertIn('az aks nodepool upgrade', mock_cmd.all_commands)
    self.assertIn('--kubernetes-version 1.34', mock_cmd.all_commands)

  # ---- UpdateCluster --------------------------------------------------------

  def testUpdateCluster(self):
    """UpdateCluster issues 'az aks update' with a timestamp tag."""
    mock_cmd = self.MockIssueCommand({'az aks update': [('', '', 0)]})
    self.aks.UpdateCluster()

    self.assertIn('az aks update', mock_cmd.all_commands)
    self.assertIn('--tags', mock_cmd.all_commands)
    self.assertIn('k8s-mgmt-ts=', mock_cmd.all_commands)

  # ---- CreateNodePoolAsync --------------------------------------------------

  def testCreateNodePoolAsyncReturnsNpSucceededHandle(self):
    """CreateNodePoolAsync issues nodepool add with --no-wait."""
    mock_cmd = self.MockIssueCommand(
        {'az aks nodepool add': [('', '', 0)]}
    )
    handle = self.aks.CreateNodePoolAsync(self._make_nodepool_config('apool'))

    self.assertIn('--no-wait', mock_cmd.all_commands)
    self.assertTrue(handle.startswith('np_succeeded:'))

  def testCreateNodePoolAsyncRaisesOnFailure(self):
    """CreateNodePoolAsync raises CreationError on CLI failure."""
    self.MockIssueCommand({'az aks nodepool add': [('', 'err', 1)]})
    with self.assertRaises(errors.Resource.CreationError):
      self.aks.CreateNodePoolAsync(self._make_nodepool_config('failpool'))

  # ---- UpgradeNodePoolAsync -------------------------------------------------

  def testUpgradeNodePoolAsyncReturnsNpSucceededHandle(self):
    """UpgradeNodePoolAsync issues upgrade with --no-wait."""
    mock_cmd = self.MockIssueCommand(
        {'az aks nodepool upgrade': [('', '', 0)]}
    )
    handle = self.aks.UpgradeNodePoolAsync('my-pool', '1.34')

    self.assertIn('--no-wait', mock_cmd.all_commands)
    self.assertTrue(handle.startswith('np_succeeded:'))
    self.assertIn('--kubernetes-version 1.34', mock_cmd.all_commands)

  # ---- DeleteNodePoolAsync --------------------------------------------------

  def testDeleteNodePoolAsyncReturnsNpGoneHandle(self):
    """DeleteNodePoolAsync issues delete with --no-wait."""
    mock_cmd = self.MockIssueCommand(
        {'az aks nodepool delete': [('', '', 0)]}
    )
    handle = self.aks.DeleteNodePoolAsync('rm-pool')

    self.assertIn('--no-wait', mock_cmd.all_commands)
    self.assertTrue(handle.startswith('np_gone:'))

  # ---- UpdateClusterAsync ---------------------------------------------------

  def testUpdateClusterAsyncScalesSystemPool(self):
    """UpdateClusterAsync scales the system pool; returns cluster_succeeded."""
    pools_json = '[{"name": "nodepool1", "count": 2}]'
    self.MockIssueCommand({
        'az aks nodepool list': [(pools_json, '', 0)],
        'az aks nodepool scale': [('', '', 0)],
    })
    handle = self.aks.UpdateClusterAsync()
    self.assertEqual('cluster_succeeded', handle)

  def testUpdateClusterAsyncFallbackTagUpdate(self):
    """UpdateClusterAsync falls back to tag update when nodepool list fails."""
    self.MockIssueCommand({
        'az aks nodepool list': [('', 'err', 1)],
        'az aks update': [('', '', 0)],
    })
    handle = self.aks.UpdateClusterAsync()
    self.assertEqual('cluster_succeeded', handle)

  # ---- WaitForOperation -----------------------------------------------------

  def testWaitForOperationNpSucceeded(self):
    """WaitForOperation(np_succeeded:name) returns on Succeeded state."""
    self.MockIssueCommand(
        {'az aks nodepool show': [('Succeeded\n', '', 0)]}
    )
    # Should not raise
    self.aks.WaitForOperation('np_succeeded:mypool')

  def testWaitForOperationNpSucceededFailedRaises(self):
    """WaitForOperation raises CreationError on Failed provisioningState."""
    self.MockIssueCommand(
        {'az aks nodepool show': [('Failed\n', '', 0)]}
    )
    with self.assertRaises(errors.Resource.CreationError):
      self.aks.WaitForOperation('np_succeeded:failpool')

  def testWaitForOperationNpGone(self):
    """WaitForOperation(np_gone:name) returns when nodepool is not found."""
    self.MockIssueCommand({
        'az aks nodepool show': [('', 'NotFound', 1)]
    })
    # Should not raise
    self.aks.WaitForOperation('np_gone:deleted-pool')

  def testWaitForOperationClusterSucceeded(self):
    """WaitForOperation(cluster_succeeded) returns on Succeeded state."""
    self.MockIssueCommand({
        'az aks show': [('Succeeded\n', '', 0)]
    })
    # Should not raise
    self.aks.WaitForOperation('cluster_succeeded')

  def testWaitForOperationClusterSucceededFailedRaises(self):
    """WaitForOperation raises CreationError when cluster update is Failed."""
    self.MockIssueCommand({
        'az aks show': [('Failed\n', '', 0)]
    })
    with self.assertRaises(errors.Resource.CreationError):
      self.aks.WaitForOperation('cluster_succeeded')

  def testWaitForOperationUnknownHandleRaises(self):
    """WaitForOperation raises ValueError for an unknown handle prefix."""
    with self.assertRaises(ValueError):
      self.aks.WaitForOperation('bad_handle:something')

  # ---- ResolveNodePoolVersions ----------------------------------------------

  def testResolveNodePoolVersionsNMinus1Math(self):
    """ResolveNodePoolVersions returns (N-1, N) from cluster_version."""
    self.aks.cluster_version = '1.34'
    initial, target = self.aks.ResolveNodePoolVersions()
    self.assertEqual('1.33', initial)
    self.assertEqual('1.34', target)

  def testResolveNodePoolVersionsStripsMinorPatch(self):
    """ResolveNodePoolVersions strips patch from full version string."""
    self.aks.cluster_version = '1.33.5'
    initial, target = self.aks.ResolveNodePoolVersions()
    self.assertEqual('1.32', initial)
    self.assertEqual('1.33', target)

  # ---- _GetNodeFlags with version_override ----------------------------------

  def testGetNodeFlagsVersionOverride(self):
    """_GetNodeFlags uses version_override instead of cluster_version."""
    self.aks.cluster_version = '1.34'
    cfg = self._make_nodepool_config()
    flags = self.aks._GetNodeFlags(cfg, version_override='1.33')
    self.assertIn('--kubernetes-version', flags)
    idx = flags.index('--kubernetes-version')
    self.assertEqual('1.33', flags[idx + 1])

  def testGetNodeFlagsUsesClusterVersionWhenNoOverride(self):
    """_GetNodeFlags uses cluster_version when version_override is None."""
    self.aks.cluster_version = '1.34'
    cfg = self._make_nodepool_config()
    flags = self.aks._GetNodeFlags(cfg, version_override=None)
    self.assertIn('--kubernetes-version', flags)
    idx = flags.index('--kubernetes-version')
    self.assertEqual('1.34', flags[idx + 1])


if __name__ == '__main__':
  unittest.main()
