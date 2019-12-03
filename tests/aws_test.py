# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.providers.aws."""

import json
import os.path
import unittest

import mock

from parameterized import parameterized

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'uid'
_COMPONENT = 'test_component'


def _AwsCommand(region, topic, *args):
  return [
      'aws', '--output', 'json', 'ec2', topic, '--region={}'.format(region)
  ] + list(args)


class AwsVolumeExistsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsVolumeExistsTestCase, self).setUp()
    p = mock.patch(util.__name__ + '.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)
    self.disk = aws_disk.AwsDisk(
        aws_disk.AwsDiskSpec(_COMPONENT, disk_type='standard'),
        'us-east-1', 'm4.2xlarge')
    self.disk.id = 'vol-foo'

  def testVolumePresent(self):
    response = ('{"Volumes":[{"Size":500,"Attachments":[],"SnapshotId":null,'
                '"CreateTime": "2015-05-04T23:47:31.726Z","VolumeId":'
                '"vol-5859691f","AvailabilityZone":"us-east-1a","VolumeType":'
                '"standard","State":"creating"}]}')
    util.IssueRetryableCommand.side_effect = [(response, None)]
    self.assertTrue(self.disk._Exists())

  def testVolumeDeleted(self):
    response = ('{"Volumes":[{"VolumeId":"vol-e45b6ba3","Size": 500,'
                '"AvailabilityZone": "us-east-1a","CreateTime":'
                '"2015-05-04T23:53:42.952Z","Attachments":[],"State":'
                '"deleting","SnapshotId":null,"VolumeType": "standard"}]}')
    util.IssueRetryableCommand.side_effect = [(response, None)]
    self.assertFalse(self.disk._Exists())


class AwsVpcTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsVpcTestCase, self).setUp()
    p = mock.patch('perfkitbenchmarker.providers.aws.'
                   'util.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)
    self.vpc = aws_network.AwsVpc('region')

  @mock.patch.object(vm_util, 'IssueCommand')
  def testCreateNormalVpc(self, mock_cmd):
    vpc_id = 'vpc-1234'
    security_group_id = 'sg-1234'
    res1 = {'Vpc': {'VpcId': vpc_id}}
    res2 = {'Vpcs': [1]}
    res3 = {'SecurityGroups': [{'GroupId': security_group_id}]}
    mock_cmd.side_effect = [(json.dumps(res1), '', 0),
                            (json.dumps(res3), '', 0)]
    util.IssueRetryableCommand.return_value = json.dumps(res2), ''
    self.assertFalse(self.vpc.user_managed)
    self.vpc.Create()
    self.assertEqual(vpc_id, self.vpc.id)
    self.assertEqual(security_group_id, self.vpc.default_security_group_id)

  def testVpcDeleted(self):
    response = '{"Vpcs": [] }'
    util.IssueRetryableCommand.side_effect = [(response, None)]
    self.assertFalse(self.vpc._Exists())

  def testVpcPresent(self):
    response = ('{"Vpcs":[{"InstanceTenancy":"default","CidrBlock":'
                '"10.0.0.0/16","IsDefault":false,"DhcpOptionsId":'
                '"dopt-59b12f38","State":"available","VpcId":"vpc-2289a647"'
                '}]}')
    util.IssueRetryableCommand.side_effect = [(response, None)]
    self.assertTrue(self.vpc._Exists())

  def testNetworkSpec(self):
    zone = 'us-west-1a'
    spec = aws_network.AwsNetworkSpec(zone)
    self.assertEqual(zone, spec.zone)
    self.assertIsNone(spec.vpc_id)
    self.assertIsNone(spec.subnet_id)
    self.assertIsNone(spec.cidr_block)

  @mock.patch.object(vm_util, 'IssueCommand')
  def testInternetGatewayLifecycle(self, mock_cmd):
    region = 'us-west-1'
    gateway_id = 'igw-123'
    create_response = {'InternetGateway': {'InternetGatewayId': gateway_id}}
    exists_response = {'InternetGateways': [{'InternetGatewayId': gateway_id}]}
    exists_response_no_resources = {'InternetGateways': []}
    delete_response = {}
    create_cmd = _AwsCommand(region, 'create-internet-gateway')
    describe_cmd = _AwsCommand(
        region, 'describe-internet-gateways',
        '--filter=Name=internet-gateway-id,Values={}'.format(gateway_id))
    delete_cmd = _AwsCommand(region, 'delete-internet-gateway',
                             '--internet-gateway-id={}'.format(gateway_id))

    # Test the Create() command
    mock_cmd.side_effect = [(json.dumps(create_response), '', 0)]
    util.IssueRetryableCommand.side_effect = [(json.dumps(exists_response), '')]
    gateway = aws_network.AwsInternetGateway(region)
    gateway.Create()
    mock_cmd.assert_called_with(create_cmd)
    util.IssueRetryableCommand.assert_called_with(describe_cmd)

    # Reset the mocks and call the Delete() command.
    mock_cmd.reset_mock()
    util.IssueRetryableCommand.reset_mock()
    # Confirms that _Delete() is called twice as the first describe response
    # still shows the gateway id as active.
    mock_cmd.side_effect = [(json.dumps(delete_response), '', 0)] * 2
    util.IssueRetryableCommand.side_effect = [
        (json.dumps(exists_response), ''),
        (json.dumps(exists_response_no_resources), '')
    ]
    gateway.Delete()
    mock_cmd.assert_called_with(delete_cmd, raise_on_failure=False)
    util.IssueRetryableCommand.assert_called_with(describe_cmd)

  @mock.patch.object(vm_util, 'IssueCommand')
  def testSubnetLifecycle(self, mock_cmd):
    zone = 'us-west-1a'
    vpc_id = 'vpc-1234'
    region = 'us-west-1'
    subnet_id = 'subnet-1234'
    create_response = {'Subnet': {'SubnetId': subnet_id}}
    exists_response = {'Subnets': [{'SubnetId': subnet_id, 'VpcId': vpc_id}]}
    exists_response_no_resources = {'Subnets': []}
    delete_response = {}
    create_cmd = _AwsCommand(region, 'create-subnet',
                             '--vpc-id={}'.format(vpc_id),
                             '--cidr-block=10.0.0.0/24',
                             '--availability-zone={}'.format(zone))
    describe_cmd = _AwsCommand(
        region, 'describe-subnets',
        '--filter=Name=vpc-id,Values={}'.format(vpc_id),
        '--filter=Name=subnet-id,Values={}'.format(subnet_id))
    delete_cmd = _AwsCommand(region, 'delete-subnet',
                             '--subnet-id={}'.format(subnet_id))

    # Test the Create() command
    mock_cmd.side_effect = [(json.dumps(create_response), '', 0)]
    util.IssueRetryableCommand.side_effect = [(json.dumps(exists_response), '')]
    subnet = aws_network.AwsSubnet(zone, vpc_id)
    subnet.Create()
    mock_cmd.assert_called_with(create_cmd)
    util.IssueRetryableCommand.assert_called_with(describe_cmd)

    # Reset the mocks and call the Delete() command.
    mock_cmd.reset_mock()
    util.IssueRetryableCommand.reset_mock()
    mock_cmd.side_effect = [(json.dumps(delete_response), '', 0)]
    util.IssueRetryableCommand.side_effect = [
        (json.dumps(exists_response_no_resources), '')
    ]
    subnet.Delete()
    mock_cmd.assert_called_with(delete_cmd, raise_on_failure=False)
    util.IssueRetryableCommand.assert_called_with(describe_cmd)

  @mock.patch.object(vm_util, 'IssueCommand')
  def testStaticVpc(self, mock_cmd):
    vpc_id = 'vpc-1234'
    security_group_id = 'sg-1234'
    security_group_res = {'SecurityGroups': [{'GroupId': security_group_id}]}
    does_exist_res = {'Vpcs': [1]}
    does_not_exist_res = {'Vpcs': []}
    mock_cmd.return_value = json.dumps(security_group_res), '', 0
    util.IssueRetryableCommand.side_effect = [(json.dumps(does_exist_res), ''),
                                              (json.dumps(does_not_exist_res),
                                               '')]
    vpc = aws_network.AwsVpc('region', vpc_id)
    self.assertTrue(vpc.user_managed)
    # show Create() works
    vpc.Create()
    self.assertEqual(vpc_id, vpc.id)
    self.assertEqual(security_group_id, vpc.default_security_group_id)
    # show Delete() doesn't delete, but it does call _Exists() again
    vpc.Delete()

  def testNetworkSpecWithVpcId(self):
    zone = 'us-west-1a'
    vpc_id = 'vpc-1234'
    subnet_id = 'subnet-1234'
    cidr = '10.0.0.0/24'
    res = {
        'Subnets': [{
            'VpcId': vpc_id,
            'SubnetId': subnet_id,
            'CidrBlock': cidr
        }]
    }
    util.IssueRetryableCommand.return_value = json.dumps(res), None
    spec = aws_network.AwsNetworkSpec(zone, vpc_id)
    self.assertEqual(zone, spec.zone)
    self.assertEqual(vpc_id, spec.vpc_id)
    self.assertEqual(subnet_id, spec.subnet_id)
    self.assertEqual(cidr, spec.cidr_block)


class AwsVirtualMachineTestCase(pkb_common_test_case.PkbCommonTestCase):

  def open_json_data(self, filename):
    path = os.path.join(os.path.dirname(__file__),
                        'data', filename)
    with open(path) as f:
      return f.read()

  def setUp(self):
    super(AwsVirtualMachineTestCase, self).setUp()
    FLAGS.cloud = providers.AWS
    FLAGS.os_type = os_types.DEBIAN
    FLAGS.run_uri = 'aaaaaa'
    FLAGS.temp_dir = 'tmp'
    p = mock.patch('perfkitbenchmarker.providers.aws.'
                   'util.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)
    p2 = mock.patch('perfkitbenchmarker.'
                    'vm_util.IssueCommand')
    p2.start()
    self.addCleanup(p2.stop)

    # VM Creation depends on there being a BenchmarkSpec.
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=FLAGS, vm_groups={})
    self.spec = benchmark_spec.BenchmarkSpec(mock.MagicMock(), config_spec,
                                             _BENCHMARK_UID)
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

    self.vm = aws_virtual_machine.AwsVirtualMachine(
        aws_virtual_machine.AwsVmSpec('test_vm_spec.AWS', zone='us-east-1a',
                                      machine_type='c3.large',
                                      spot_price=123.45))
    self.vm.id = 'i-foo'
    self.vm.image = 'ami-12345'
    self.vm.client_token = '00000000-1111-2222-3333-444444444444'
    network_mock = mock.MagicMock()
    network_mock.subnet = mock.MagicMock(id='subnet-id')
    placement_group = mock.MagicMock()
    placement_group.name = 'placement_group_name'
    placement_group.strategy = 'cluster'
    network_mock.placement_group = placement_group
    self.vm.network = network_mock
    self.vm.placement_group = placement_group

    self.response = self.open_json_data('aws-describe-instance.json')
    self.sir_response = self.open_json_data(
        'aws-describe-spot-instance-requests.json')
    self.vm.network.is_static = False
    self.vm.network.regional_network.vpc.default_security_group_id = 'sg-1234'

  def testInstancePresent(self):
    util.IssueRetryableCommand.side_effect = [(self.response, None)]
    self.assertTrue(self.vm._Exists())

  def testInstanceQuotaExceededDuringCreate(self):
    stderr = (
        'An error occurred (InstanceLimitExceeded) when calling the '
        'RunInstances operation: You have requested more instances (21) than '
        'your current instance limit of 20 allows for the specified instance '
        'type. Please visit http://aws.amazon.com/contact-us/ec2-request to '
        'request an adjustment to this limit.'
    )
    vm_util.IssueCommand.side_effect = [(None, stderr, None)]
    with self.assertRaises(errors.Benchmarks.QuotaFailure) as e:
      self.vm._Create()
    self.assertEqual(str(e.exception), stderr)

  def testInstanceStockedOutDuringCreate(self):
    stderr = ('An error occurred (InsufficientInstanceCapacity) when calling '
              'the RunInstances operation (reached max retries: 4): '
              'Insufficient capacity.')
    vm_util.IssueCommand.side_effect = [(None, stderr, None)]
    with self.assertRaises(
        errors.Benchmarks.InsufficientCapacityCloudFailure) as e:
      self.vm._Create()
    self.assertEqual(str(e.exception), stderr)

  def testInstanceStockedOutAfterCreate(self):
    """This tests when run-instances succeeds and returns a pending instance.

    The instance then is not fulfilled and transitions to terminated.
    """
    response = self.open_json_data('aws-describe-instance-stockout.json')
    util.IssueRetryableCommand.side_effect = [(response, None)]
    with self.assertRaises(
        errors.Benchmarks.InsufficientCapacityCloudFailure) as e:
      self.vm._Exists()
    self.assertEqual(str(e.exception), 'Server.InsufficientInstanceCapacity:'
                     ' Insufficient capacity to satisfy instance request')

  def testInternalServerErrorAfterCreate(self):
    """This tests when run-instances succeeds and returns a pending instance.

    The instance then is not fulfilled and transitions to shutting-down due to
    an internal server error.
    """
    response = json.loads(self.response)
    state = response['Reservations'][0]['Instances'][0]['State']
    state['Name'] = 'shutting-down'
    state_reason = {'Code': 'Server.InternalError'}
    response['Reservations'][0]['Instances'][0]['StateReason'] = state_reason
    util.IssueRetryableCommand.side_effect = [(json.dumps(response), None)]
    initial_client_token = self.vm.client_token
    self.assertFalse(self.vm._Exists())
    # Token should be refreshed
    self.assertNotEqual(initial_client_token, self.vm.client_token)

  def testInstanceDeleted(self):
    response = json.loads(self.response)
    state = response['Reservations'][0]['Instances'][0]['State']
    state['Name'] = 'shutting-down'
    state_reason = {'Code': 'shutting-down-test'}
    response['Reservations'][0]['Instances'][0]['StateReason'] = state_reason
    util.IssueRetryableCommand.side_effect = [(json.dumps(response), None)]
    self.assertFalse(self.vm._Exists())

  @mock.patch.object(util, 'FormatTagSpecifications')
  def testCreateSpot(self, mock_cmd):
    mock_cmd.return_value = 'foobar'
    vm_util.IssueCommand.side_effect = [(None, '', None)]

    self.vm.use_spot_instance = True
    self.vm._Create()

    vm_util.IssueCommand.assert_called_with([
        'aws',
        '--output',
        'json',
        'ec2',
        'run-instances',
        '--region=us-east-1',
        '--subnet-id=subnet-id',
        '--client-token=00000000-1111-2222-3333-444444444444',
        '--image-id=ami-12345',
        '--instance-type=c3.large',
        '--key-name=perfkit-key-aaaaaa',
        '--tag-specifications=foobar',
        '--associate-public-ip-address',
        '--block-device-mappings=[{"VirtualName": "ephemeral0", '
        '"DeviceName": "/dev/xvdb"}, {"VirtualName": "ephemeral1", '
        '"DeviceName": "/dev/xvdc"}]',
        '--placement=AvailabilityZone=us-east-1a,'
        'GroupName=placement_group_name',
        '--instance-market-options={"MarketType": "spot", '
        '"SpotOptions": {"SpotInstanceType": "one-time", '
        '"InstanceInterruptionBehavior": "terminate", "MaxPrice": "123.45"}}'
    ], raise_on_failure=False)
    self.vm.use_spot_instance = False

  def testCreateSpotFailure(self):
    stderr = ('An error occurred (InsufficientInstanceCapacity) when calling '
              'the RunInstances operation (reached max retries: 4): '
              'Insufficient capacity.')
    vm_util.IssueCommand.side_effect = [(None, stderr, None)]

    with self.assertRaises(
        errors.Benchmarks.InsufficientCapacityCloudFailure) as e:
      self.vm.use_spot_instance = True
      self.vm._Create()
    self.assertEqual(str(e.exception), stderr)
    self.assertEqual(self.vm.spot_status_code,
                     'InsufficientSpotInstanceCapacity')
    self.assertTrue(self.vm.early_termination)
    self.vm.use_spot_instance = False

  def testDeleteCancelsSpotInstanceRequest(self):
    self.vm.spot_instance_request_id = 'sir-abc'
    self.vm.use_spot_instance = True
    self.vm._Delete()

    vm_util.IssueCommand.assert_called_with(
        ['aws',
         '--output',
         'json',
         '--region=us-east-1',
         'ec2',
         'cancel-spot-instance-requests',
         '--spot-instance-request-ids=sir-abc'],
        raise_on_failure=False)
    self.vm.use_spot_instance = False

  def testFirewallAllowPortNonStatic(self):
    check_firewall = {'SecurityGroups': []}
    util.IssueRetryableCommand.return_value = json.dumps(check_firewall), ''
    self.vm.firewall.AllowPort(self.vm, 22)
    # only checking that the 2nd of the tcp/udp calls are done
    cmd = _AwsCommand('us-east-1', 'authorize-security-group-ingress',
                      '--group-id=sg-1234', '--port=22-22', '--cidr=0.0.0.0/0',
                      '--protocol=udp')
    util.IssueRetryableCommand.assert_called_with(cmd)

  def testFirewallAllowPortStaticVm(self):
    self.vm.is_static = True
    self.vm.firewall.AllowPort(self.vm, 22)
    util.IssueRetryableCommand.assert_not_called()
    self.vm.is_static = False

  def testFirewallAllowPortStaticNetwork(self):
    self.vm.network.is_static = True
    self.vm.firewall.AllowPort(self.vm, 22)
    util.IssueRetryableCommand.assert_not_called()
    self.vm.network.is_static = False

  @parameterized.expand([
      (True, 'network-interfaces', 'associate-public-ip-address'),
      (False, 'associate-public-ip-address', 'network-interfaces'),
  ])
  def testElasticNetwork(self, use_efa, should_find, should_not_find):
    # with EFA the "--associate-public-ip-address" flag is not used, instead
    # putting that attribute into --network-interfaces
    FLAGS.aws_efa = use_efa
    aws_cmd = CreateVm()
    self.assertRegex(aws_cmd, 'run-instances .*--' + should_find)
    self.assertNotRegex(aws_cmd, 'run-instances .*--' + should_not_find)


def CreateVm():
  """Returns the AWS run-instances command line."""
  vm_util.IssueCommand.side_effect = [('', '', 0)]
  util.IssueRetryableCommand.side_effect = [('', '', 0)]
  vm_spec = aws_virtual_machine.AwsVmSpec(
      'test_vm_spec.AWS', zone='us-west-1a', machine_type='c5n.18xlarge')
  vm = aws_virtual_machine.AwsVirtualMachine(vm_spec)
  vm.network.regional_network = mock.Mock()
  vm.network.subnet = mock.Mock(id='subnet-1234')
  vm.network.placement_group = mock.Mock()
  vm.network.Create()
  vm._Create()
  return ' '.join(vm_util.IssueCommand.call_args[0][0])


class AwsIsRegionTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testBadFormat(self):
    with self.assertRaises(ValueError):
      util.IsRegion('us-east')

  def testZone(self):
    self.assertFalse(util.IsRegion('us-east-1a'))

  def testRegion(self):
    self.assertTrue(util.IsRegion('eu-central-1'))


class AwsGetRegionFromZoneTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testBadFormat(self):
    with self.assertRaises(ValueError):
      util.GetRegionFromZone('invalid')

  def testZone(self):
    self.assertEqual(util.GetRegionFromZone('us-east-1a'), 'us-east-1')

  def testRegion(self):
    self.assertEqual(util.GetRegionFromZone('eu-central-1'), 'eu-central-1')


class AwsGetBlockDeviceMapTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsGetBlockDeviceMapTestCase, self).setUp()
    p = mock.patch(util.__name__ + '.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__),
                        'data', 'describe_image_output.txt')
    with open(path) as fp:
      self.describe_image_output = fp.read()

  def testInvalidMachineType(self):
    self.assertEqual(aws_virtual_machine.GetBlockDeviceMap('invalid'), None)

  def testValidMachineTypeWithNoRootVolumeSize(self):
    expected = [{'DeviceName': '/dev/xvdb',
                 'VirtualName': 'ephemeral0'}]
    actual = json.loads(aws_virtual_machine.GetBlockDeviceMap('c1.medium'))
    self.assertEqual(actual, expected)

  def testValidMachineTypeWithSpecifiedRootVolumeSize(self):
    util.IssueRetryableCommand.side_effect = [(self.describe_image_output,
                                               None)]
    desired_root_volume_size_gb = 35
    machine_type = 'c1.medium'
    image_id = 'ami-a9d276c9'
    region = 'us-west-2'
    expected = [{'DeviceName': '/dev/sda1',
                 'Ebs': {'SnapshotId': 'snap-826344d5',
                         'DeleteOnTermination': True,
                         'VolumeType': 'gp2',
                         'VolumeSize': 35}},
                {'DeviceName': '/dev/xvdb',
                 'VirtualName': 'ephemeral0'}]
    actual = json.loads(aws_virtual_machine.GetBlockDeviceMap(
        machine_type, desired_root_volume_size_gb, image_id, region))
    self.assertEqual(actual, expected)


class AwsGetRootBlockDeviceSpecForImageTestCase(
    pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsGetRootBlockDeviceSpecForImageTestCase, self).setUp()
    p = mock.patch(util.__name__ + '.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__),
                        'data', 'describe_image_output.txt')
    with open(path) as fp:
      self.describe_image_output = fp.read()

  def testOk(self):
    util.IssueRetryableCommand.side_effect = [(self.describe_image_output,
                                               None)]
    image_id = 'ami-a9d276c9'
    region = 'us-west-2'
    expected = {
        'DeviceName': '/dev/sda1',
        'Ebs': {
            'SnapshotId': 'snap-826344d5',
            'DeleteOnTermination': True,
            'VolumeType': 'gp2',
            'VolumeSize': 8,
            'Encrypted': False
        }
    }
    actual = aws_virtual_machine.GetRootBlockDeviceSpecForImage(image_id,
                                                                region)
    self.assertEqual(actual, expected)


class AwsKeyFileManagerTestCase(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch.object(vm_util, 'IssueRetryableCommand')
  @mock.patch.object(vm_util, 'IssueCommand')
  def testKeyPairLimitExceeded(self, import_cmd, cat_cmd):
    cat_cmd.side_effect = [('key_content', None)]
    import_cmd.side_effect = [
        ('', 'An error occurred (KeyPairLimitExceeded) when calling the '
         'ImportKeyPair operation: Maximum of 5000 keypairs reached.', 255)
    ]
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      aws_virtual_machine.AwsKeyFileManager.ImportKeyfile('region')


if __name__ == '__main__':
  unittest.main()
