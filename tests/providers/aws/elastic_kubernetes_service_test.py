import tempfile
import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import container_service
from perfkitbenchmarker import network
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import elastic_kubernetes_service
from perfkitbenchmarker.providers.aws import util
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


class ElasticKubernetesServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
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

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksCluster(EKS_SPEC)

  def testEksClusterCreateNoSpec(self):
    issue_command = self.MockIssueCommand(
        {'create cluster': [('Cluster created', '', 0)]}
    )
    spec = container_spec.ContainerClusterSpec(
        'NAME',
        **{
            'cloud': 'AWS',
            'vm_spec': {
                'AWS': {'machine_type': 'm5.large', 'zone': 'us-east-1'}
            },
        },
    )  # {}, flag_values=flags.FLAGS)  #
    cluster = elastic_kubernetes_service.EksCluster(spec)
    cluster._Create()
    issue_command.func_to_mock.assert_has_calls([
        mock.call(
            matchers.NOT(['node-zones']),
            timeout=1800,
            raise_on_failure=False,
        ),
    ])

  def testEksClusterCreate(self):
    issue_command = self.MockIssueCommand(
        {'create cluster': [('Cluster created', '', 0)]}
    )
    cluster = elastic_kubernetes_service.EksCluster(EKS_SPEC)
    cluster._Create()
    issue_command.func_to_mock.assert_has_calls([
        mock.call(
            [
                'eksctl',
                'create',
                'cluster',
                '--managed=True',
                '--name=pkb-123p',
                '--node-labels=pkb_nodepool=default',
                '--node-type=m5.large',
                '--node-zones=us-west-1a',
                '--nodegroup-name=default',
                '--nodes=1',
                '--region=us-west-1',
                '--ssh-public-key=perfkit-key-123p',
                '--vpc-nat-mode=Disable',
                '--with-oidc=True',
                '--zones=us-west-1a,us-west-1b',
            ],
            timeout=1800,
            raise_on_failure=False,
        ),
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

  def testEksClusterNodepoools(self):
    issue_command = self.MockIssueCommand(
        {'create cluster': [('Cluster created', '', 0)]}
    )
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
    cluster._Create()
    issue_command.func_to_mock.assert_any_call(
        [
            'eksctl',
            'create',
            'cluster',
            '--managed=True',
            '--name=pkb-123p',
            '--node-labels=pkb_nodepool=default',
            '--node-type=m5.large',
            '--node-zones=us-west-1a',
            '--nodegroup-name=default',
            '--nodes=1',
            '--region=us-west-1',
            '--ssh-public-key=perfkit-key-123p',
            '--vpc-nat-mode=Disable',
            '--with-oidc=True',
            '--zones=us-west-1a,us-west-1b',
        ],
        timeout=1800,
        raise_on_failure=False,
    )
    issue_command.func_to_mock.assert_any_call(
        [
            'eksctl',
            'create',
            'nodegroup',
            '--cluster=pkb-123p',
            '--name=nginx',
            '--node-labels=pkb_nodepool=nginx',
            '--node-type=m6i.xlarge',
            '--node-zones=us-west-1b',
            '--nodes=3',
            '--region=us-west-1',
            '--skip-outdated-addons-check=True',
            '--ssh-public-key=perfkit-key-123p',
        ],
        timeout=600,
    )


class EksAutoClusterTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
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

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksAutoCluster(EKS_SPEC)

  def testEksClusterCreate(self):
    issue_command = self.MockIssueCommand(
        {'create cluster': [('Cluster created', '', 0)]}
    )
    cluster = elastic_kubernetes_service.EksAutoCluster(EKS_SPEC)
    cluster._Create()
    issue_command.func_to_mock.assert_any_call(
        [
            'eksctl',
            'create',
            'cluster',
            '--enable-auto-mode=True',
            '--name=pkb-123p',
            '--region=us-west-1',
            '--with-oidc=True',
            '--zones=us-west-1a,us-west-1b',
        ],
        timeout=1800,
        raise_on_failure=False,
    )

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


EXPECTED_EKS_CREATE_FILE = """{
  "apiVersion": "eksctl.io/v1alpha5",
  "kind": "ClusterConfig",
  "metadata": {
    "name": "pkb-123p",
    "region": "us-west-1",
    "version": "1.32",
    "tags": {
      "benchmark": "kubernetes_scale",
      "cloud": "aws",
      "owner": "cloud-performance",
      "karpenter.sh/discovery": "pkb-123p"
    }
  },
  "iam": {
    "withOidc": true,
    "podIdentityAssociations": [
      {
        "namespace": "kube-system",
        "serviceAccountName": "karpenter",
        "roleName": "pkb-123p-karpenter",
        "permissionPolicyARNs": [
          "arn:aws:iam::1234:policy/KarpenterControllerPolicy-pkb-123p"
        ]
      }
    ]
  },
  "iamIdentityMappings": [
    {
      "arn": "arn:aws:iam::1234:role/KarpenterNodeRole-pkb-123p",
      "username": "system:node:{{EC2PrivateDNSName}}",
      "groups": [
        "system:bootstrappers",
        "system:nodes"
      ]
    }
  ],
  "addons": [
    {
      "name": "eks-pod-identity-agent"
    }
  ],
  "managedNodeGroups": [
    {
      "name": "default",
      "instanceType": "m5.large",
      "desiredCapacity": 1,
      "amiFamily": "AmazonLinux2023",
      "minSize": 1,
      "maxSize": 1
    }
  ]
}"""


class EksKarpenterTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
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

  def testInitEksClusterWorks(self):
    elastic_kubernetes_service.EksKarpenterCluster(EKS_SPEC)

  def testEksYamlCreate(self):
    self.enter_context(
        mock.patch.object(
            vm_util,
            'GetTempDir',
            return_value=tempfile.gettempdir(),
        )
    )
    self.enter_context(
        mock.patch.object(
            util,
            'MakeDefaultTags',
            return_value={
                'benchmark': 'kubernetes_scale',
                'cloud': 'aws',
                'owner': 'cloud-performance',
            },
        )
    )
    cluster = elastic_kubernetes_service.EksKarpenterCluster(EKS_SPEC)
    file = cluster._RenderEksCreateJsonToFile()
    with open(file, 'r') as f:
      contents = f.read()
    self.assertEqual(EXPECTED_EKS_CREATE_FILE, contents)


if __name__ == '__main__':
  unittest.main()
