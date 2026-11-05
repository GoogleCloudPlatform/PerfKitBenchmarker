import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.azure import azure_kubernetes_service
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case


class AzureKubernetesServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
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
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
        },
    )
    self.spec_dict['nodepools'] = {
        'client': {
            'vm_spec': {  # pyrefly: ignore[bad-assignment]
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
    self.spec_dict['min_vm_count'] = 2  # pyrefly: ignore[bad-assignment]
    self.spec_dict['max_vm_count'] = 4  # pyrefly: ignore[bad-assignment]
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
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
        },
    )
    self.spec_dict['nodepools'] = {
        'client': {
            'vm_spec': {  # pyrefly: ignore[bad-assignment]
                'Azure': {
                    'machine_type': 'Standard_D4s_v5',
                }
            },
            'min_vm_count': 4,  # pyrefly: ignore[bad-assignment]
            'max_vm_count': 6,  # pyrefly: ignore[bad-assignment]
            'vm_count': 3,  # pyrefly: ignore[bad-assignment]
        },
    }
    self.initAksCluster(self.spec_dict)
    self.aks._Create()
    self.assertIn(
        '--enable-cluster-autoscaler --min-count=4 --max-count=6'
        ' --node-count=4',
        mock_cmd.all_commands,
    )

  def _CreateCmds(self, vm_count=None, **spec_overrides):
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
        },
    )
    spec_dict = {
        'cloud': 'Azure',
        'vm_spec': {
            'Azure': {
                'machine_type': 'Standard_D2s_v5',
                'zone': 'westus2-1',
            },
        },
        'nodepools': {
            'client': {
                'vm_spec': {
                    'Azure': {'machine_type': 'Standard_D4s_v5'},
                },
            },
        },
        **spec_overrides,
    }
    if vm_count:
      spec_dict['vm_count'] = vm_count
    self.initAksCluster(spec_dict)
    self.aks._Create()
    return [c.args[0] for c in mock_cmd.func_to_mock.mock_calls]

  def testCreateNoNewFlagsByDefault(self):
    create_cmd, nodepool_cmd = self._CreateCmds()
    for flag in (
        '--tier',
        '--max-pods',
        '--node-osdisk-type',
        '--network-plugin',
        '--network-dataplane',
    ):
      self.assertNotIn(flag, create_cmd)
      self.assertNotIn(flag, nodepool_cmd)
    self.assertEmpty(
        self.aks.GetResourceMetadata().keys()
        & {
            'azure_aks_tier',
            'max_pods',
            'azure_aks_node_osdisk_type',
            'azure_aks_overlay_cilium',
            'enable_network_policy',
        }
    )

  @flagsaver.flagsaver(azure_aks_standard_tier=True)
  def testCreateStandardTierFlag(self):
    create_cmd, _ = self._CreateCmds()
    self.assertEqual(create_cmd.count('--tier'), 1)
    self.assertEqual(create_cmd[create_cmd.index('--tier') + 1], 'standard')

  def testCreateLargeClusterDefaultsToStandardTier(self):
    create_cmd, _ = self._CreateCmds(vm_count=300)
    self.assertEqual(create_cmd.count('--tier'), 1)
    self.assertEqual(create_cmd[create_cmd.index('--tier') + 1], 'standard')
    self.assertEqual(
        self.aks.GetResourceMetadata()['azure_aks_tier'], 'standard'
    )

  def testCreateMaxPodsAndOsDiskType(self):
    create_cmd, nodepool_cmd = self._CreateCmds(
        max_pods=110,
        vm_spec={
            'Azure': {
                'machine_type': 'Standard_D2s_v5',
                'zone': 'westus2-1',
                'boot_disk_type': 'Ephemeral',
            },
        },
    )
    self.assertEqual(create_cmd[create_cmd.index('--max-pods') + 1], '110')
    self.assertEqual(
        create_cmd[create_cmd.index('--node-osdisk-type') + 1], 'Ephemeral'
    )
    self.assertEqual(nodepool_cmd[nodepool_cmd.index('--max-pods') + 1], '110')
    self.assertNotIn('--node-osdisk-type', nodepool_cmd)

  def testStandardSkuBootDiskTypeDoesNotPassOsDiskTypeFlag(self):
    create_cmd, _ = self._CreateCmds(
        vm_spec={
            'Azure': {
                'machine_type': 'Standard_D2s_v5',
                'zone': 'westus2-1',
                'boot_disk_type': 'Premium_LRS',
            },
        },
    )
    self.assertNotIn('--node-osdisk-type', create_cmd)

  def testNodepoolOverridesZoneAndDiskSize(self):
    mock_cmd = self.MockIssueCommand(
        {
            'az aks create': [('', '', 0)],
            'az aks nodepool': [('', '', 0)],
        },
    )
    spec_dict = {
        'cloud': 'Azure',
        'vm_spec': {
            'Azure': {'machine_type': 'Standard_D2s_v5', 'zone': 'eastus2'},
        },
        'nodepools': {
            'sandbox': {
                'max_pods': 250,
                'vm_spec': {
                    'Azure': {
                        'machine_type': 'Standard_D8ads_v6',
                        'zone': 'eastus2-3',
                        'boot_disk_size': 100,
                        'boot_disk_type': 'Ephemeral',
                    }
                },
            }
        },
    }
    self.initAksCluster(spec_dict)
    self.aks._Create()
    create_cmd, nodepool_cmd = [
        c.args[0] for c in mock_cmd.func_to_mock.mock_calls
    ]
    self.assertNotIn('--zones', create_cmd)
    self.assertNotIn('--max-pods', create_cmd)
    self.assertEqual(nodepool_cmd[nodepool_cmd.index('--zones') + 1], '3')
    self.assertEqual(nodepool_cmd[nodepool_cmd.index('--max-pods') + 1], '250')
    self.assertEqual(
        nodepool_cmd[nodepool_cmd.index('--node-osdisk-size') + 1], '100'
    )
    self.assertEqual(
        nodepool_cmd[nodepool_cmd.index('--node-osdisk-type') + 1], 'Ephemeral'
    )

  def testCreateOverlayCilium(self):
    create_cmd, _ = self._CreateCmds(enable_network_policy=True)
    self.assertEqual(create_cmd.count('--network-plugin'), 1)
    self.assertEqual(create_cmd.count('--network-plugin-mode'), 1)
    self.assertEqual(create_cmd.count('--network-dataplane'), 1)
    self.assertEqual(
        create_cmd[create_cmd.index('--network-plugin') + 1], 'azure'
    )
    self.assertEqual(
        create_cmd[create_cmd.index('--network-dataplane') + 1], 'cilium'
    )

  def testCreateOverlayCiliumNoDuplicatesInAnyCombination(self):
    for cilium in (False, True):
      for nap in (False, True):
        for vm_count in (None, 300):
          with flagsaver.flagsaver(azure_aks_auto_node_provisioning=nap):
            create_cmd, _ = self._CreateCmds(
                vm_count=vm_count, enable_network_policy=cilium
            )
          expect_overlay = cilium or nap or vm_count
          self.assertEqual(
              create_cmd.count('--network-plugin'), 1 if expect_overlay else 0
          )
          self.assertEqual(
              create_cmd.count('--network-plugin-mode'),
              1 if expect_overlay else 0,
          )
          self.assertEqual(
              create_cmd.count('--network-dataplane'),
              1 if (cilium or nap) else 0,
          )
          self.assertEqual(create_cmd.count('--tier'), 1 if vm_count else 0)

  @flagsaver.flagsaver(azure_aks_standard_tier=True)
  def testMetadataWhenSet(self):
    self.initAksCluster({
        'cloud': 'Azure',
        'max_pods': 50,
        'enable_network_policy': True,
        'vm_spec': {
            'Azure': {
                'machine_type': 'Standard_D2s_v5',
                'zone': 'westus2-1',
                'boot_disk_type': 'Managed',
            },
        },
    })
    metadata = self.aks.GetResourceMetadata()
    self.assertEqual(metadata['azure_aks_tier'], 'standard')
    self.assertEqual(metadata['max_pods'], 50)
    self.assertEqual(metadata['azure_aks_node_osdisk_type'], 'Managed')
    self.assertTrue(metadata['enable_network_policy'])
    self.assertTrue(metadata['azure_aks_overlay_cilium'])

  @flagsaver.flagsaver(kubectl='kubectl', kubeconfig='dummy')
  def testFullCreateAksAutomatic(self):
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
                    'test-user@example.com\n12345678-1234-1234-1234-123456789abc',
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
        ' Service RBAC Admin',
        mock_cmd.all_commands,
    )
    self.assertIn(
        'az role assignment create --role Resource Policy Contributor',
        mock_cmd.all_commands,
    )
    self.assertIn(
        'az policy assignment update --name'
        ' aks-deployment-safeguards-policy-assignment',
        mock_cmd.all_commands,
    )

  def testGetNodeSelectorsAutoNodeProvisioning(self):
    self.assertEqual(self.aks.GetNodeSelectors(), {})
    with flagsaver.flagsaver(azure_aks_auto_node_provisioning=True):
      self.assertEqual(
          self.aks.GetNodeSelectors(),
          {'kubernetes.azure.com/mode': 'user'},
      )

  def testGetNodePoolNames(self):
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


if __name__ == '__main__':
  unittest.main()
