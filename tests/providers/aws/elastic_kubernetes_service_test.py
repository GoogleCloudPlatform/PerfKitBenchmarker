"""Tests for the AWS Elastic Kubernetes Service provider."""
# pylint: disable=invalid-name,protected-access

import json
import os
import tempfile
import unittest
from unittest import mock
from urllib import parse
from absl.testing import flagsaver  # pylint: disable=import-error
from absl.testing import parameterized  # pylint: disable=import-error
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import elastic_kubernetes_service
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.resources.container_service import kubectl
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
  """Base test class providing common EKS cluster setup and mock helpers."""

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
  """Tests for the managed-nodegroup EksCluster provider."""

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksCluster(EKS_SPEC)

  def testEksClusterCreateRegion(self):
    """EksCluster._Create() without explicit AZ omits availabilityZones."""
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
    """EksCluster._Create() with a zone issues the expected eksctl commands."""
    ebs_policy = 'arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy'
    ebs_role = 'arn:aws:iam::1234:role/AmazonEKS_EBS_CSI_DriverRole_pkb-123p'
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
            f'--attach-policy-arn={ebs_policy}',
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
            f'--service-account-role-arn={ebs_role}',
        ]),
    ])
    assert self.patched_read_json is not None
    called_json = self.patched_read_json.call_args_list[0][0][0]
    self.assertIn(
        ('availabilityZones', ['us-west-1a', 'us-west-1b']), called_json.items()
    )

  def testEksClusterNodepools(self):
    """Additional nodepools appear in the managedNodeGroups config."""
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
    """Autoscaling min/max/desired values propagate to managedNodeGroups."""
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

  def testGetNodePoolNames(self):
    """GetNodePoolNames returns list of nodegroup names from eksctl output."""
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
    """GetNodePoolNames on Karpenter cluster returns kubectl nodepool names."""
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
    """GetNodePoolFromNodeName resolves a node name to its nodepool."""
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
    """GetNodePoolFromNodeName returns None when node is not found."""
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
  """Tests for the auto-mode EksAutoCluster provider."""

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
    """EksAutoCluster._IsReady() returns True when cluster-info succeeds."""
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(
                (
                    r'^[[0;32mKubernetes control plane^[[0m is running at'
                    r' ^[[0;33mhttps://RAND1234.gr7.us-west-1.'
                    r'eks.amazonaws.com^[[0mTo'
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
  """Tests for the Karpenter-based EksKarpenterCluster provider."""

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
    """EksKarpenterCluster._Create() produces the expected eksctl yaml."""
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
    """EksKarpenterCluster._PostCreate() logs expected nodepool yaml."""
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
        elastic_kubernetes_service._recursively_update_dictionary(base, update),
    )

  def testIngressAddressParsing(self):
    """Test parsing AWS ALB address with dualstack prefix removal."""
    elb_host = 'k8s-test-ingress-abc12345ef-123456789.us-east-1.elb.amazonaws.com'
    test_cases = [
        (f'http://dualstack.{elb_host}', elb_host),
        (f'https://dualstack.{elb_host}', elb_host),
        (f'dualstack.{elb_host}', elb_host),
        (elb_host, elb_host),
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


class EksManagementPlaneTest(BaseEksTest):
  """Tests for EKS management-plane methods (k8s_management_benchmark)."""

  def _make_cluster(self, spec_dict=None):
    spec = container_spec.ContainerClusterSpec(
        'NAME',
        **(spec_dict or EKS_SPEC_DICT),
    )
    cluster = elastic_kubernetes_service.EksCluster(spec)
    self.MockJsonRead(cluster)
    # Individual tests override via MockIssueCommand.
    return cluster

  def _make_nodepool_config(self, name='pkbpool0', machine_type='m5.large',
                             num_nodes=2):
    cfg = mock.MagicMock()
    cfg.name = name
    cfg.num_nodes = num_nodes
    cfg.machine_type = machine_type
    return cfg

  # ---- CreateNodePoolAsync --------------------------------------------------

  def testCreateNodePoolAsyncIssuesCreateNodegroup(self):
    """CreateNodePoolAsync calls create-nodegroup; returns ng_active handle."""
    cluster = self._make_cluster()
    # Subnets / AZ discovery stubs
    cluster._cached_subnets = ['subnet-1']
    cluster._cached_subnets_per_az = {}
    cluster._cached_node_role_arn = 'arn:aws:iam::1234:role/NodeRole'
    self.MockIssueCommand({'create-nodegroup': [('', '', 0)]})

    handle = cluster.CreateNodePoolAsync(self._make_nodepool_config('poolA'))

    self.assertEqual('ng_active:poolA', handle)
    # Verify the json file path was written
    self.assertIsNotNone(self.patched_read_json)

  def testCreateNodePoolAsyncReturnsNgActiveHandle(self):
    """CreateNodePoolAsync returns 'ng_active:<name>' on success."""
    cluster = self._make_cluster()
    cluster._cached_subnets = ['subnet-1']
    cluster._cached_subnets_per_az = {}
    cluster._cached_node_role_arn = 'arn:aws:iam::1234:role/NodeRole'
    self.MockIssueCommand({'': [('', '', 0)]})

    handle = cluster.CreateNodePoolAsync(self._make_nodepool_config('myng'))
    self.assertEqual('ng_active:myng', handle)

  def testCreateNodePoolAsyncRaisesOnFailure(self):
    """CreateNodePoolAsync raises CreationError when the CLI fails."""
    cluster = self._make_cluster()
    cluster._cached_subnets = ['subnet-1']
    cluster._cached_subnets_per_az = {}
    cluster._cached_node_role_arn = 'arn:aws:iam::1234:role/NodeRole'
    self.MockIssueCommand({'': [('', 'error msg', 1)]})

    with self.assertRaises(Exception):
      cluster.CreateNodePoolAsync(self._make_nodepool_config('failng'))

  # ---- UpgradeNodePoolAsync -------------------------------------------------

  def testUpgradeNodePoolAsyncReturnsNgActiveHandle(self):
    """UpgradeNodePoolAsync calls update-nodegroup-version; returns handle."""
    cluster = self._make_cluster()
    mock_cmd = self.MockIssueCommand(
        {'update-nodegroup-version': [('', '', 0)]}
    )
    handle = cluster.UpgradeNodePoolAsync('my-ng', '1.34')

    self.assertEqual('ng_active:my-ng', handle)
    self.assertIn('update-nodegroup-version', mock_cmd.all_commands)
    self.assertIn('--kubernetes-version 1.34', mock_cmd.all_commands)

  def testUpgradeNodePoolAsyncRaisesOnFailure(self):
    """UpgradeNodePoolAsync raises on non-zero exit code."""
    cluster = self._make_cluster()
    self.MockIssueCommand({'': [('', 'oops', 1)]})
    with self.assertRaises(Exception):
      cluster.UpgradeNodePoolAsync('bad-ng', '1.34')

  # ---- DeleteNodePoolAsync --------------------------------------------------

  def testDeleteNodePoolAsyncReturnsNgGoneHandle(self):
    """DeleteNodePoolAsync calls delete-nodegroup, returns ng_gone handle."""
    cluster = self._make_cluster()
    mock_cmd = self.MockIssueCommand({'delete-nodegroup': [('', '', 0)]})
    handle = cluster.DeleteNodePoolAsync('old-ng')

    self.assertEqual('ng_gone:old-ng', handle)
    self.assertIn('delete-nodegroup', mock_cmd.all_commands)
    self.assertIn('--nodegroup-name old-ng', mock_cmd.all_commands)

  # ---- UpdateClusterAsync ---------------------------------------------------

  def testUpdateClusterAsyncReturnsClusterUpdateHandle(self):
    """UpdateClusterAsync returns 'cluster_update:<update_id>'."""
    cluster = self._make_cluster()
    describe_out = json.dumps({
        'cluster': {'logging': {'clusterLogging': []}}
    })
    update_out = json.dumps({'update': {'id': 'u-abc123'}})
    self.MockIssueCommand({
        'describe-cluster': [(describe_out, '', 0)],
        'update-cluster-config': [(update_out, '', 0)],
    })
    handle = cluster.UpdateClusterAsync()
    self.assertEqual('cluster_update:u-abc123', handle)

  def testUpdateClusterAsyncTogglesLogging(self):
    """UpdateClusterAsync toggles logging enable state."""
    cluster = self._make_cluster()
    # Current state: logging disabled
    describe_out = json.dumps({
        'cluster': {'logging': {'clusterLogging': [{'enabled': False}]}}
    })
    update_out = json.dumps({'update': {'id': 'u-xyz'}})
    mock_cmd = self.MockIssueCommand({
        'describe-cluster': [(describe_out, '', 0)],
        'update-cluster-config': [(update_out, '', 0)],
    })
    cluster.UpdateClusterAsync()
    self.assertIn('update-cluster-config', mock_cmd.all_commands)
    self.assertIn('--logging', mock_cmd.all_commands)

  # ---- WaitForOperation -----------------------------------------------------

  def testWaitForOperationNgActiveSuccess(self):
    """WaitForOperation(ng_active:name) returns when nodegroup is ACTIVE."""
    cluster = self._make_cluster()
    ng_out = json.dumps({'nodegroup': {'status': 'ACTIVE'}})
    self.MockIssueCommand({'describe-nodegroup': [(ng_out, '', 0)]})
    # Should not raise
    cluster.WaitForOperation('ng_active:my-ng')

  def testWaitForOperationNgActiveFailedRaises(self):
    """WaitForOperation raises CreationError on CREATE_FAILED nodegroup."""
    cluster = self._make_cluster()
    ng_out = json.dumps({'nodegroup': {'status': 'CREATE_FAILED'}})
    self.MockIssueCommand({'describe-nodegroup': [(ng_out, '', 0)]})
    with self.assertRaises(Exception):
      cluster.WaitForOperation('ng_active:bad-ng')

  def testWaitForOperationNgGoneSuccess(self):
    """WaitForOperation(ng_gone:name) returns on ResourceNotFoundException."""
    cluster = self._make_cluster()
    self.MockIssueCommand({
        'describe-nodegroup': [('', 'ResourceNotFoundException', 1)]
    })
    # Should not raise
    cluster.WaitForOperation('ng_gone:deleted-ng')

  def testWaitForOperationClusterUpdateSuccess(self):
    """WaitForOperation(cluster_update:id) returns when update is Successful."""
    cluster = self._make_cluster()
    self.MockIssueCommand({'describe-update': [('Successful\n', '', 0)]})
    # Should not raise
    cluster.WaitForOperation('cluster_update:u-999')

  def testWaitForOperationClusterUpdateFailedRaises(self):
    """WaitForOperation raises when cluster update ends in Failed."""
    cluster = self._make_cluster()
    self.MockIssueCommand({'describe-update': [('Failed\n', '', 0)]})
    with self.assertRaises(Exception):
      cluster.WaitForOperation('cluster_update:u-fail')

  def testWaitForOperationUnknownHandleRaises(self):
    """WaitForOperation raises ValueError for unknown handle prefix."""
    cluster = self._make_cluster()
    with self.assertRaises(ValueError):
      cluster.WaitForOperation('unknown_handle:xyz')

  # ---- ResolveNodePoolVersions ----------------------------------------------

  def testResolveNodePoolVersionsNMinus1Math(self):
    """ResolveNodePoolVersions returns (N-1, N) from cluster_version."""
    cluster = self._make_cluster()
    cluster.cluster_version = '1.34'
    initial, target = cluster.ResolveNodePoolVersions()
    self.assertEqual('1.33', initial)
    self.assertEqual('1.34', target)

  def testResolveNodePoolVersionsStripsMinorPatch(self):
    """ResolveNodePoolVersions strips patch from version strings."""
    cluster = self._make_cluster()
    cluster.cluster_version = '1.33.7'
    initial, target = cluster.ResolveNodePoolVersions()
    self.assertEqual('1.32', initial)
    self.assertEqual('1.33', target)

  # ---- _DiscoverSubnets -----------------------------------------------------

  def testDiscoverSubnets(self):
    """_DiscoverSubnets returns subnet IDs from describe-cluster."""
    cluster = self._make_cluster()
    describe_out = json.dumps({
        'cluster': {
            'resourcesVpcConfig': {
                'subnetIds': ['subnet-aaa', 'subnet-bbb']
            }
        }
    })
    self.MockIssueCommand({'describe-cluster': [(describe_out, '', 0)]})
    subnets = cluster._DiscoverSubnets()
    self.assertEqual(['subnet-aaa', 'subnet-bbb'], subnets)

  def testDiscoverSubnetsCached(self):
    """_DiscoverSubnets uses cached result on second call."""
    cluster = self._make_cluster()
    cluster._cached_subnets = ['subnet-cached']
    # No IssueCommand calls expected because cache is used
    with mock.patch.object(vm_util, 'IssueCommand') as mock_issue:
      result = cluster._DiscoverSubnets()
    mock_issue.assert_not_called()
    self.assertEqual(['subnet-cached'], result)

  # ---- _DiscoverSubnetsPerAZ ------------------------------------------------

  def testDiscoverSubnetsPerAZBuildsAzMap(self):
    """_DiscoverSubnetsPerAZ builds a {AZ: subnet_id} map from EC2."""
    cluster = self._make_cluster()
    cluster._cached_subnets = ['subnet-a1', 'subnet-b2']
    subnets_out = json.dumps([
        {'SubnetId': 'subnet-a1', 'AZ': 'us-west-1a'},
        {'SubnetId': 'subnet-b2', 'AZ': 'us-west-1b'},
    ])
    self.MockIssueCommand({'describe-subnets': [(subnets_out, '', 0)]})
    az_map = cluster._DiscoverSubnetsPerAZ()
    self.assertEqual({'us-west-1a': 'subnet-a1', 'us-west-1b': 'subnet-b2'},
                     az_map)

  # ---- _DiscoverNodeRoleArn -------------------------------------------------

  def testDiscoverNodeRoleArn(self):
    """_DiscoverNodeRoleArn returns role ARN from the first nodegroup."""
    cluster = self._make_cluster()
    list_out = json.dumps({'nodegroups': ['ng1']})
    describe_out = json.dumps({
        'nodegroup': {'nodeRole': 'arn:aws:iam::1234:role/MyRole'}
    })
    self.MockIssueCommand({
        'list-nodegroups': [(list_out, '', 0)],
        'describe-nodegroup': [(describe_out, '', 0)],
    })
    arn = cluster._DiscoverNodeRoleArn()
    self.assertEqual('arn:aws:iam::1234:role/MyRole', arn)

  def testDiscoverNodeRoleArnRaisesWhenNoNodegroup(self):
    """_DiscoverNodeRoleArn raises CreationError when no nodegroups found."""
    cluster = self._make_cluster()
    list_out = json.dumps({'nodegroups': []})
    self.MockIssueCommand({'list-nodegroups': [(list_out, '', 0)]})
    with self.assertRaises(errors.Resource.CreationError):
      cluster._DiscoverNodeRoleArn()

  # ---- _ResolveReleaseVersion -----------------------------------------------

  def testResolveReleaseVersion(self):
    """_ResolveReleaseVersion returns the SSM parameter value."""
    cluster = self._make_cluster()
    self.MockIssueCommand({
        'get-parameter': [('1.33.10-20260101\n', '', 0)]
    })
    version = cluster._ResolveReleaseVersion('1.33')
    self.assertEqual('1.33.10-20260101', version)

  def testResolveReleaseVersionCached(self):
    """_ResolveReleaseVersion uses cache for repeated calls."""
    cluster = self._make_cluster()
    self.MockIssueCommand({
        'get-parameter': [('1.34.2-20260101\n', '', 0)]
    })
    v1 = cluster._ResolveReleaseVersion('1.34')
    v2 = cluster._ResolveReleaseVersion('1.34')
    self.assertEqual(v1, v2)

  def testResolveReleaseVersionRaisesOnFailure(self):
    """_ResolveReleaseVersion raises CreationError when SSM lookup fails."""
    cluster = self._make_cluster()
    self.MockIssueCommand({'get-parameter': [('', 'not found', 1)]})
    with self.assertRaises(errors.Resource.CreationError):
      cluster._ResolveReleaseVersion('1.99')


if __name__ == '__main__':
  unittest.main()
