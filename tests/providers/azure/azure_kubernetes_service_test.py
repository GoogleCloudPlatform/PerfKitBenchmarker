import unittest
from unittest import mock
from absl.testing import flagsaver
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
        ),
    )

  def testCreateNodepool(self):
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

  def testFullCreateAksAutomatic(self):
    aks_auto = azure_kubernetes_service.AksAutomaticCluster(self.spec)
    aks_auto.resource_group.name = 'resource-group'
    self.enter_context(
        flagsaver.flagsaver(kubectl='kubectl', kubeconfig='dummy')
    )
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
    mock_cmd.func_to_mock.assert_has_calls(
        [
            mock.call(
                [
                    'az',
                    'role',
                    'assignment',
                    'create',
                    '--assignee',
                    'user-name',
                    '--role',
                    'Azure Kubernetes Service RBAC Admin',
                    '--scope',
                    'cluster-id',
                ],
            ),
            # Resource Policy Contributor role for Safeguards management
            mock.call(
                [
                    'az',
                    'role',
                    'assignment',
                    'create',
                    '--role',
                    'Resource Policy Contributor',
                    '--assignee',
                    'test-user@example.com',
                    '--scope',
                    '/subscriptions/12345678-1234-1234-1234-123456789abc/resourceGroups/resource-group',
                ],
            ),
            # Safeguard policy relaxation
            mock.call(
                [
                    'az',
                    'policy',
                    'assignment',
                    'update',
                    '--name',
                    'aks-deployment-safeguards-policy-assignment',
                    '--scope',
                    '/subscriptions/cluster-id/resourceGroups/resource-group/providers/Microsoft.ContainerService/managedClusters/pkbcluster123',
                    '--set',
                    'enforcement_mode="DoNotEnforce"',
                ],
            ),
        ],
        any_order=True,
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
