import tempfile
import unittest
from unittest import mock
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import container_service
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import elastic_kubernetes_service
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case


EKS_SPEC_DICT = {
    'cloud': 'AWS',
    'vm_spec': {
        'AWS': {
            'machine_type': 'm5.large',
            'zone': 'us-west-1a',
        },
    },
}
EKS_SPEC = container_spec.ContainerClusterSpec(
    'NAME',
    **EKS_SPEC_DICT,
)


class BaseEksTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            vm_util,
            'GetTempDir',
            return_value=tempfile.gettempdir(),
        )
    )

    mock_network = mock.create_autospec(aws_network.AwsNetwork)
    mock_network.subnet = {'id': 'subnet1'}
    mock_network.vpc = {'default_security_group_id': 'group1'}
    mock_network.placement_group = None
    self.enter_context(
        mock.patch.object(util, 'GetAccount', return_value='1234')
    )
    self.enter_context(
        mock.patch.object(
            network.BaseNetwork,
            'GetNetwork',
            return_value=mock_network,
        )
    )
    self.enter_context(
        mock.patch.object(
            network.BaseFirewall,
            'GetFirewall',
            return_value=mock.create_autospec(aws_network.AwsFirewall),
        )
    )
    self.enter_context(flagsaver.flagsaver(run_uri='123p'))
    self.patched_read_json = None

  def MockJsonRead(self, cluster: elastic_kubernetes_service.BaseEksCluster):
    self.patched_read_json = self.enter_context(
        mock.patch.object(
            cluster,
            '_WriteJsonToFile',
            return_value='/tmp/test.json',
        )
    )


class ElasticKubernetesServiceTest(BaseEksTest):

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksCluster(EKS_SPEC)

  def testEksClusterCreateRegion(self):
    self.MockIssueCommand({'create cluster': [('Cluster created', '', 0)]})
    spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'AWS',
            'vm_spec': {
                'AWS': {'machine_type': 'm5.large', 'zone': 'us-east-1'}
            },
        },
    )
    cluster = elastic_kubernetes_service.EksCluster(spec)
    self.MockJsonRead(cluster)
    cluster._Create()
    assert self.patched_read_json is not None
    called_json = self.patched_read_json.call_args_list[0][0][0]
    self.assertNotIn('availabilityZones', called_json)
    self.assertEqual({'nat': {'gateway': 'Disable'}}, called_json['vpc'])
    self.assertEqual({'withOidc': True}, called_json['iam'])
    self.assertEqual(
        [
            {
                'name': 'default',
                'instanceType': 'm5.large',
                'desiredCapacity': 1,
                'amiFamily': 'AmazonLinux2023',
                'labels': {
                    'pkb_nodepool': 'default',
                },
                'tags': {},
                'ssh': {'allow': True, 'publicKeyPath': 'perfkit-key-123p'},
            },
        ],
        called_json['managedNodeGroups'],
    )

  def testEksClusterCreateZone(self):
    issue_command = self.MockIssueCommand(
        {'create cluster': [('Cluster created', '', 0)]}
    )
    cluster = elastic_kubernetes_service.EksCluster(EKS_SPEC)
    self.MockJsonRead(cluster)
    cluster._Create()
    issue_command.func_to_mock.assert_has_calls([
        mock.call([
            'eksctl',
            'create',
            'iamserviceaccount',
            '--name=ebs-csi-controller-sa',
            '--namespace=kube-system',
            '--region=us-west-1',
            '--cluster=pkb-123p',
            '--attach-policy-arn=arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy',
            '--approve',
            '--role-only',
            '--role-name=AmazonEKS_EBS_CSI_DriverRole_pkb-123p',
        ]),
        mock.call([
            'eksctl',
            'create',
            'addon',
            '--name=aws-ebs-csi-driver',
            '--region=us-west-1',
            '--cluster=pkb-123p',
            '--service-account-role-arn=arn:aws:iam::1234:role/AmazonEKS_EBS_CSI_DriverRole_pkb-123p',
        ]),
    ])
    assert self.patched_read_json is not None
    called_json = self.patched_read_json.call_args_list[0][0][0]
    self.assertIn(
        ('availabilityZones', ['us-west-1a', 'us-west-1b']), called_json.items()
    )

  def testEksClusterNodepools(self):
    self.MockIssueCommand({'create cluster': [('Cluster created', '', 0)]})
    spec2 = EKS_SPEC_DICT.copy()
    spec2['nodepools'] = {
        'nginx': {
            'vm_count': 3,
            'vm_spec': {
                'AWS': {
                    'machine_type': 'm6i.xlarge',
                    'zone': 'us-west-1b',
                }
            },
        }
    }
    cluster = elastic_kubernetes_service.EksCluster(
        container_spec.ContainerClusterSpec('NAME', **spec2)
    )
    self.MockJsonRead(cluster)
    cluster._Create()
    assert self.patched_read_json is not None
    called_json = self.patched_read_json.call_args_list[0][0][0]
    self.assertEqual(
        called_json['availabilityZones'], ['us-west-1a', 'us-west-1b']
    )
    node_groups = called_json['managedNodeGroups']
    self.assertLen(node_groups, 2)
    self.assertEqual(
        {
            'name': 'nginx',
            'instanceType': 'm6i.xlarge',
            'desiredCapacity': 3,
            'amiFamily': 'AmazonLinux2023',
            'labels': {
                'pkb_nodepool': 'nginx',
            },
            'availabilityZones': ['us-west-1b'],
            'tags': {},
            'ssh': {'allow': True, 'publicKeyPath': 'perfkit-key-123p'},
        },
        node_groups[1],
    )

  @parameterized.named_parameters(
      ('default nodepool', 'default', 'default'),
      ('standard nodepool', 'nginx', 'nginx'),
  )
  def testEksClusterGetNodepoolFromName(self, nodepool_name, expected_name):
    self.MockIssueCommand({'get node': [(nodepool_name, '', 0)]})
    spec2 = EKS_SPEC_DICT.copy()
    spec2['nodepools'] = {
        'nginx': {
            'vm_count': 3,
            'vm_spec': {
                'AWS': {
                    'machine_type': 'm6i.xlarge',
                    'zone': 'us-west-1b',
                }
            },
        }
    }
    cluster = elastic_kubernetes_service.EksCluster(
        container_spec.ContainerClusterSpec('NAME', **spec2)
    )
    nodepool = cluster.GetNodePoolFromNodeName('sample-node')
    self.assertIsNotNone(nodepool)
    self.assertEqual(nodepool.name, expected_name)

  def testEksClusterNotFound(self):
    self.MockIssueCommand({'get node': [('', '', 0)]})
    spec2 = EKS_SPEC_DICT.copy()
    spec2['nodepools'] = {
        'nginx': {
            'vm_count': 3,
            'vm_spec': {
                'AWS': {
                    'machine_type': 'm6i.xlarge',
                    'zone': 'us-west-1b',
                }
            },
        }
    }
    cluster = elastic_kubernetes_service.EksCluster(
        container_spec.ContainerClusterSpec('NAME', **spec2)
    )
    nodepool = cluster.GetNodePoolFromNodeName('sample-node')
    self.assertIsNone(nodepool)

  def testEksClusterGetMachineTypeFromNodeName(self):
    self.MockIssueCommand({'get node': [("'node1,m6i.xlarge'\n", '', 0)]})
    cluster = elastic_kubernetes_service.EksCluster(
        container_spec.ContainerClusterSpec('NAME', **EKS_SPEC_DICT)
    )
    machine_type = cluster.GetMachineTypeFromNodeName('node1')
    self.assertIsNotNone(machine_type)
    self.assertEqual(machine_type, 'm6i.xlarge')


class EksAutoClusterTest(BaseEksTest):

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksAutoCluster(EKS_SPEC)

  def testEksClusterCreate(self):
    self.MockIssueCommand({'create cluster': [('Cluster created', '', 0)]})
    cluster = elastic_kubernetes_service.EksAutoCluster(EKS_SPEC)
    self.MockJsonRead(cluster)
    cluster._Create()
    assert self.patched_read_json is not None
    called_json = self.patched_read_json.call_args_list[0][0][0]
    self.assertEqual(called_json['autoModeConfig'], {'enabled': True})

  def testEksClusterIsReady(self):
    self.enter_context(
        mock.patch.object(
            container_service,
            'RunKubectlCommand',
            return_value=(
                (
                    r'^[[0;32mKubernetes control plane^[[0m is running at'
                    r' ^[[0;33mhttps://RAND1234.gr7.us-west-1.eks.amazonaws.com^[[0mTo'
                    " further debug and diagnose cluster problems, use 'kubectl"
                    " cluster-info dump'."
                ),
                '',
                0,
            ),
        )
    )
    cluster = elastic_kubernetes_service.EksAutoCluster(EKS_SPEC)
    self.assertTrue(cluster._IsReady())


class EksKarpenterTest(BaseEksTest):

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksKarpenterCluster(EKS_SPEC)

  @flagsaver.flagsaver(kubeconfig='/tmp/kubeconfig')
  def testEksYamlCreateFull(self):
    self.enter_context(
        mock.patch.object(
            util,
            'MakeDefaultTags',
            return_value={
                'benchmark': 'kubernetes_scale',
                'cloud': 'aws',
            },
        )
    )
    cluster = elastic_kubernetes_service.EksKarpenterCluster(EKS_SPEC)
    self.MockJsonRead(cluster)
    self.MockIssueCommand(
        {'create cluster': [('Cluster created', '', 0)], 'curl': [('', '', 0)]}
    )
    cluster._Create()
    assert self.patched_read_json is not None
    called_json = self.patched_read_json.call_args_list[0][0][0]
    self.assertEqual(
        called_json['metadata']['tags'],
        {
            'benchmark': 'kubernetes_scale',
            'cloud': 'aws',
            'karpenter.sh/discovery': 'pkb-123p',
        },
    )
    self.assertEqual(
        called_json['iam']['podIdentityAssociations'],
        [{
            'namespace': 'kube-system',
            'serviceAccountName': 'karpenter',
            'roleName': 'pkb-123p-karpenter',
            'permissionPolicyARNs': [
                'arn:aws:iam::1234:policy/KarpenterControllerPolicy-pkb-123p'
            ],
        }],
    )
    self.assertEqual(
        called_json['iamIdentityMappings'],
        [{
            'arn': 'arn:aws:iam::1234:role/KarpenterNodeRole-pkb-123p',
            'username': 'system:node:{{EC2PrivateDNSName}}',
            'groups': ['system:bootstrappers', 'system:nodes'],
        }],
    )
    self.assertEqual(
        called_json['addons'], [{'name': 'eks-pod-identity-agent'}]
    )

  def testRecursiveDictionaryUpdate(self):
    base = {'a': 1, 'deep': {'c': 2}}
    update = {'a': 3, 'deep': {'d': 4}, 'f': 12}
    expected = {'a': 3, 'deep': {'c': 2, 'd': 4}, 'f': 12}
    self.assertEqual(
        expected,
        elastic_kubernetes_service.RecursivelyUpdateDictionary(base, update),
    )


if __name__ == '__main__':
  unittest.main()
