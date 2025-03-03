import unittest
from unittest import mock
from absl.testing import flagsaver
from perfkitbenchmarker import network
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


if __name__ == '__main__':
  unittest.main()
