import json
import os
import tempfile
import unittest
from unittest import mock
from urllib import parse
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import elastic_kubernetes_service
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from tests import matchers
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

  def testEksClusterNodepoolsAutoscaling(self):
    self.MockIssueCommand({'create cluster': [('Cluster created', '', 0)]})
    spec2 = EKS_SPEC_DICT.copy()
    spec2['min_vm_count'] = 1
    spec2['max_vm_count'] = 5
    spec2['vm_count'] = 2
    spec2['nodepools'] = {
        'nginx': {
            'vm_count': 3,
            'min_vm_count': 2,
            'max_vm_count': 10,
            'vm_spec': {
                'AWS': {
                    'machine_type': 'm6i.xlarge',
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
    node_groups = called_json['managedNodeGroups']
    self.assertLen(node_groups, 2)
    # default nodepool
    self.assertEqual(node_groups[0]['minSize'], 1)
    self.assertEqual(node_groups[0]['maxSize'], 5)
    self.assertEqual(node_groups[0]['desiredCapacity'], 2)
    # nginx nodepool
    self.assertEqual(node_groups[1]['minSize'], 2)
    self.assertEqual(node_groups[1]['maxSize'], 10)
    self.assertEqual(node_groups[1]['desiredCapacity'], 3)

  def testEksClusterNodepoolLabels(self):
    cluster = elastic_kubernetes_service.EksCluster(EKS_SPEC)
    nodepool = cluster.default_nodepool
    nodepool.node_labels = {'env': 'prod', 'team': 'ml'}
    actual = cluster._RenderNodeGroupJson(nodepool)
    self.assertEqual(
        actual['labels'],
        {'pkb_nodepool': nodepool.name, 'env': 'prod', 'team': 'ml'},
    )

  def testEksClusterNodepoolTaints(self):
    cluster = elastic_kubernetes_service.EksCluster(EKS_SPEC)
    nodepool = cluster.default_nodepool
    nodepool.node_taints = [
        'sandbox.gke.io/runtime=runsc:NoSchedule',
        'dedicated:NoExecute',
    ]
    actual = cluster._RenderNodeGroupJson(nodepool)
    self.assertEqual(
        actual['taints'],
        [
            {
                'key': 'sandbox.gke.io/runtime',
                'value': 'runsc',
                'effect': 'NoSchedule',
            },
            {'key': 'dedicated', 'effect': 'NoExecute'},
        ],
    )


  def testParseTaint(self):
    self.assertEqual(
        elastic_kubernetes_service._ParseTaint('key=value:NoSchedule'),
        {'key': 'key', 'value': 'value', 'effect': 'NoSchedule'},
    )
    self.assertEqual(
        elastic_kubernetes_service._ParseTaint('dedicated:NoExecute'),
        {'key': 'dedicated', 'effect': 'NoExecute'},
    )

  def testGetNodePoolNames(self):
    # Mock the output of the aws cli command
    cluster = elastic_kubernetes_service.EksCluster(EKS_SPEC)

    self.MockIssueCommand({
        'eksctl get nodegroup': [(
            json.dumps([
                {'Name': 'default'},
                {'Name': 'nodegroup1'},
                {'Name': 'nodegroup2'},
            ]),
            '',
            0,
        )],
    })
    self.assertEqual(
        cluster.GetNodePoolNames(), ['default', 'nodegroup1', 'nodegroup2']
    )

  def testGetNodePoolNamesKarpenter(self):
    cluster = elastic_kubernetes_service.EksKarpenterCluster(EKS_SPEC)
    self.MockIssueCommand({
        'kubectl --kubeconfig  get nodepool -o json': [(
            json.dumps({
                'items': [
                    {'metadata': {'name': 'karpenter-ng'}},
                    {'metadata': {'name': 'default'}},
                ]
            }),
            '',
            0,
        )]
    })
    self.assertEqual(cluster.GetNodePoolNames(), ['karpenter-ng', 'default'])

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
    self.assertEqual(
        called_json['autoModeConfig'],
        {'enabled': True, 'nodePools': ['general-purpose', 'system']},
    )

  def testEksClusterIsReady(self):
    self.enter_context(
        mock.patch.object(
            kubectl,
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


class InferenceS3StorageTest(BaseEksTest):
  """Tests S3 PV/PVC apply for kubernetes_ai_inference on AWS."""

  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testApplyInferenceS3PvAndPvcRaisesWhenFlagsMissing(
      self, apply_manifest_mock
  ):
    with flagsaver.flagsaver(
        k8s_inference_server_s3_bucket=None,
        k8s_inference_server_s3_region=None,
    ):
      with self.assertRaises(errors.Resource.CreationError):
        elastic_kubernetes_service.ApplyInferenceS3PvAndPvc()
    apply_manifest_mock.assert_not_called()

  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  def testApplyInferenceS3PvAndPvcRendersTemplate(self, apply_manifest_mock):
    with flagsaver.flagsaver(
        k8s_inference_server_s3_bucket='my-bucket',
        k8s_inference_server_s3_region='us-east-1',
    ):
      elastic_kubernetes_service.ApplyInferenceS3PvAndPvc()
    apply_manifest_mock.assert_called_once_with(
        'container/kubernetes_ai_inference/s3_pv_pvc.yaml.j2',
        s3_bucket='my-bucket',
        s3_region='us-east-1',
    )


class EksAutoClusterS3CsiTest(BaseEksTest):
  """Tests S3 CSI addon installation with IAM glue (Role/Policy + PIA)."""

  def setUp(self):
    super().setUp()
    self.mock_retryable = self.enter_context(
        mock.patch.object(
            vm_util, 'IssueRetryableCommand', return_value=('', '', 0)
        )
    )

  def testInstallS3CsiAddonRequiresBucket(self):
    cluster = elastic_kubernetes_service.EksAutoCluster(EKS_SPEC)
    with self.assertRaises(errors.Config.InvalidValue):
      cluster._InstallS3CsiAddon()

  @flagsaver.flagsaver(k8s_inference_server_s3_bucket='my-bucket')
  def testInstallS3CsiAddonIssuesAllCommands(self):
    mock_cmd = self.MockIssueCommand({})
    cluster = elastic_kubernetes_service.EksAutoCluster(EKS_SPEC)
    cluster._InstallS3CsiAddon()

    # create-addon + wait addon-active = 2 retryable calls.
    self.assertEqual(self.mock_retryable.call_count, 2)
    # IAM glue + Pod Identity Association are issued via IssueCommand.
    self.assertIn('iam create-policy', mock_cmd.all_commands)
    self.assertIn('iam create-role', mock_cmd.all_commands)
    self.assertIn('iam attach-role-policy', mock_cmd.all_commands)
    self.assertIn('create-pod-identity-association', mock_cmd.all_commands)
    # Policy is scoped to the requested bucket.
    self.assertIn('PkbS3CsiDriverReadOnly-my-bucket', mock_cmd.all_commands)
    self.assertIn('arn:aws:s3:::my-bucket', mock_cmd.all_commands)
    # Role ARN is constructed from account + role name (no get-role call).
    self.assertIn(
        'arn:aws:iam::1234:role/PkbS3CsiDriverRole', mock_cmd.all_commands
    )


class EksKarpenterTest(BaseEksTest):

  def setUp(self):
    super().setUp()
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

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksKarpenterCluster(EKS_SPEC)

  @flagsaver.flagsaver(kubeconfig='/tmp/kubeconfig')
  def testEksYamlCreateFull(self):
    cluster = elastic_kubernetes_service.EksKarpenterCluster(EKS_SPEC)
    self.MockJsonRead(cluster)
    mock_cmd = self.MockIssueCommand({
        'cloudformation deploy': [
            ('Deployed cloud-formation-template.yaml', '', 0)
        ],
        'create cluster': [('Cluster created', '', 0)],
        'curl': [('', '', 0)],
    })
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
    mock_cmd.func_to_mock.assert_has_calls([
        mock.call(
            matchers.HASALLOF(
                'cloudformation',
                'deploy',
                'benchmark=kubernetes_scale',
                'cloud=aws',
            )
        ),
    ])

  @parameterized.named_parameters(
      (
          'autoscaling',
          {
              'max_vm_count': 10,
              'min_vm_count': 2,
          },
          'limits:\n    cpu: 1000',
      ),
      (
          'static',
          {
              'vm_count': 5,
          },
          'replicas: 5',
      ),
  )
  @flagsaver.flagsaver(kubeconfig='/tmp/kubeconfig')
  def testEksYamlCreateFullNodepools(self, nodepool_config, expected_content):
    # Mock resources for _PostCreate
    self.MockIssueCommand({
        'helm upgrade --install karpenter': [('', '', 0)],
        'ssm get-parameter': [('image-123', '', 0)],
        'ec2 describe-images': [('amazon-linux-2023-1.34-v20240416', '', 0)],
        'kubectl apply': [('', '', 0)],
        'get events': [(json.dumps({'items': []}), '', 0)],
    })

    # Mock data.ResourcePath to point to actual data files
    def MockResourcePath(resource):
      return os.path.join(
          os.path.dirname(elastic_kubernetes_service.__file__),
          '../../data',
          resource,
      )

    self.enter_context(
        mock.patch.object(data, 'ResourcePath', side_effect=MockResourcePath)
    )

    spec_dict = EKS_SPEC_DICT.copy()
    spec_dict['nodepools'] = {
        'pool1': {
            'vm_spec': {
                'AWS': {
                    'zone': 'us-west-1b',
                }
            },
            'machine_families': ['m6i', 'm7i'],
            **nodepool_config,
        }
    }
    cluster = elastic_kubernetes_service.EksKarpenterCluster(
        container_spec.ContainerClusterSpec('NAME', **spec_dict)
    )

    with self.assertLogs(level='INFO') as logs:
      cluster._PostCreate()

    full_logs = '\n'.join(logs.output)
    self.assertContainsInOrder(
        [
            # Verify default nodepool
            'name: default',
            'node.kubernetes.io/instance-type',
            'm5.large',
            # Verify additional nodepool
            'name: pool1',
            'karpenter.k8s.aws/instance-family',
            'm6i',
            'm7i',
        ],
        full_logs,
    )
    self.assertIn(expected_content, full_logs)

  def testRecursiveDictionaryUpdate(self):
    base = {'a': 1, 'deep': {'c': 2}}
    update = {'a': 3, 'deep': {'d': 4}, 'f': 12}
    expected = {'a': 3, 'deep': {'c': 2, 'd': 4}, 'f': 12}
    self.assertEqual(
        expected,
        elastic_kubernetes_service.RecursivelyUpdateDictionary(base, update),
    )

  def testIngressAddressParsing(self):
    """Test parsing AWS ALB address with dualstack prefix removal."""
    test_cases = [
        (
            'http://dualstack.k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
            'k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
        ),
        (
            'https://dualstack.k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
            'k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
        ),
        (
            'dualstack.k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
            'k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
        ),
        (
            'k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
            'k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com',
        ),
    ]
    for address, expected in test_cases:
      with self.subTest(address=address):
        host = (
            parse.urlparse(address).hostname
            if address.startswith('http')
            else address
        )
        normalized = (host or '').replace('dualstack.', '')
        self.assertEqual(normalized, expected)


if __name__ == '__main__':
  unittest.main()
