"""Unit tests for aws_cluster.py.
"""

import unittest

from absl import flags
import mock
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_cluster
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class TestAwsClusterSpec(aws_cluster.AWSClusterSpec):
  pass


class TestAwsVM(pkb_common_test_case.TestVirtualMachine):

  def __init__(self, vm_spec):
    super().__init__(vm_spec)
    self.network = TestAwsNetwork()


class TestAwsNetwork(object):

  def __init__(self):
    self.regional_network = mock.MagicMock()
    self.regional_network._reference_count = 0


class AwsClusterTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    vm_config = {
        'cloud': 'AWS',
        'os_type': 'ubuntu2004',
        'vm_spec': {'AWS': {}}
    }
    FLAGS.run_uri = 'run12345'
    self.cluster_spec = TestAwsClusterSpec(
        'test_aws_cluster', headnode=vm_config, workers=vm_config)
    self.cluster_spec.workers.vm_spec.zone = 'us-fake-1a'
    self.mock_issue = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', autospec=True))
    self.mock_issue.return_value = ('fake_key', None, None)
    self.cluster = aws_cluster.AWSCluster(self.cluster_spec)
    self.cluster.name = 'test_aws_cluster'
    self.cluster.num_workers = 2
    self.mock_vm_class = self.enter_context(
        mock.patch.object(virtual_machine, 'GetVmClass', autospec=True)
    )
    self.mock_vm_class.return_value = TestAwsVM

  def testCreateDependencies(self):
    self.mock_issue.side_effect = [
        ('fake_key', None, None),  # Mock 'cat key' from __init__
        ('', None, None),  # Mock import key from ImportKeyfile
        # Mock vpc creation
        ('''Creating CloudFormation stack...
Stack Name: parallelclusternetworking-pubpriv-12345
Status: parallelclusternetworking-pubpriv-12345 - CREATE_COMPLETE''',
         None, None),
        # Mock vpc describe
        ('''
{
    "Vpcs": [
        {
            "VpcId": "vpc-fakevpc"
        }
    ]
}''', None, None),
        # Mock security group
        ('''
{
    "SecurityGroups": [
        {
            "GroupId": "sg-fake"
        }
    ]
}''', None, None),
        # Mock subnet extraction
        ('''
HeadNode:
  InstanceType: t2.micro
  Networking:
    SubnetId: subnet-123
Scheduling:
  SlurmQueues:
  - Name: queue1
    Networking:
      SubnetIds:
      - subnet-456
         ''', None, None),
        ('''{
    "InternetGateways": [
        {
            "InternetGatewayId": "igw-123"
        }
    ]
}
''', None, None),  # Internet Gateway
        ('''{
    "RouteTables": [
        {
            "Associations": [
                {
                    "Main": false
                }
            ],
            "RouteTableId": "rtb-123"
        },
        {
            "Associations": [
                {
                    "Main": true
                }
            ],
            "RouteTableId": "rtb-456"
        },
        {
            "Associations": [
                {
                    "Main": false
                }
            ],
            "RouteTableId": "rtb-789"
        }
    ]
}''', None, None),  # Route Table
        ('''{
    "NatGateways": [
        {
            "NatGatewayAddresses": [
                {
                    "NetworkInterfaceId": "eni-123"
                }
            ],
            "NatGatewayId": "nat-123"
        }
    ]
}
''', None, None),  # Nat Gateway,
        # Mock tagging
        ('', None, None),
        ('', None, None),
        ('', None, None),
        ('', None, None),
        ('', None, None),
        ('', None, None),
        ('', None, None),
        ('', None, None),
    ]
    self.cluster._CreateDependencies()
    self.assertEqual(
        self.cluster._config,
        '''Region: us-fake-1
Image:
  Os: ubuntu2004
Tags:

SharedStorage:
  - MountDir: /opt/apps
    Name: nfs
    StorageType: Ebs
    EbsSettings:
      VolumeType: gp3
      Iops: 5000
      Size: None
      Encrypted: True
HeadNode:
  InstanceType: None
  Networking:
    ElasticIp: true
    SubnetId: subnet-123
  Ssh:
    KeyName: perfkit-key-run12345
  SharedStorageType: Ebs
Scheduling:
  Scheduler: slurm
  SlurmQueues:
  - Name: queue1
    CapacityType: ONDEMAND
    ComputeResources:
    - Name: test_aws_cluster
      Instances:
      - InstanceType: None
      MinCount: 2
      MaxCount: 2
      DisableSimultaneousMultithreading: False
      Efa:
        Enabled: False
    Networking:
      PlacementGroup:
        Enabled: true
      SubnetIds:
      - subnet-456
Monitoring:
  Logs:
    CloudWatch:
      Enabled: false
    Rotation:
      Enabled: false
  Dashboards:
    CloudWatch:
      Enabled: false
  DetailedMonitoring: false
  Alarms:
    Enabled: false''')

  def testPostCreate(self):
    self.mock_issue.return_value = (
        '''{
  "instances": [
    {
      "instanceId": "i-05f03402c2fd5ba3e",
      "publicIpAddress": "54.82.234.236",
      "instanceType": "m6i.4xlarge",
      "nodeType": "HeadNode",
      "privateIpAddress": "10.0.0.4"
    },
    {
      "instanceId": "i-0f54fd02096b22aee",
      "queueName": "queue1",
      "instanceType": "c6i.4xlarge",
      "nodeType": "ComputeNode",
      "privateIpAddress": "10.0.21.77"
    },
    {
      "instanceId": "i-058afb44c0e27a1c0",
      "queueName": "queue1",
      "instanceType": "c6i.4xlarge",
      "nodeType": "ComputeNode",
      "privateIpAddress": "10.0.30.141"
    }
  ]
}''', None, None)
    self.mock_generate_ssh_config = self.enter_context(
        mock.patch.object(vm_util, 'GenerateSSHConfig', autospec=True))
    # Test _PostCreate
    self.cluster._PostCreate()
    self.assertEqual(
        self.cluster.headnode_vm.name, 'pkb-run12345-0')
    self.assertEqual(
        self.cluster.headnode_vm.ip_address, '54.82.234.236')
    self.assertEqual(
        self.cluster.headnode_vm.internal_ip, '10.0.0.4')
    self.assertEqual(
        self.cluster.worker_vms[0].name, 'pkb-run12345-1')
    self.assertEqual(
        self.cluster.worker_vms[0].internal_ip, '10.0.21.77')
    self.assertEqual(
        self.cluster.worker_vms[0].proxy_jump, 'pkb-run12345-0')
    self.assertEqual(
        self.cluster.worker_vms[1].name, 'pkb-run12345-2')
    self.assertEqual(
        self.cluster.worker_vms[1].internal_ip, '10.0.30.141')
    self.assertEqual(
        self.cluster.worker_vms[1].proxy_jump, 'pkb-run12345-0')
    self.assertEqual(
        self.cluster.vms, [self.cluster.headnode_vm] + self.cluster.worker_vms)


if __name__ == '__main__':
  unittest.main()
