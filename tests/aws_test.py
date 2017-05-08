# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util
from tests import mock_flags


_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'uid'
_COMPONENT = 'test_component'


class AwsVolumeExistsTestCase(unittest.TestCase):

  def setUp(self):
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


class AwsVpcExistsTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch('perfkitbenchmarker.providers.aws.'
                   'util.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)
    self.vpc = aws_network.AwsVpc('region')
    self.vpc.id = 'vpc-foo'

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


class AwsVirtualMachineTestCase(unittest.TestCase):

  def openJsonData(self, filename):
    path = os.path.join(os.path.dirname(__file__),
                        'data', filename)
    with open(path) as f:
      return f.read()

  def setUp(self):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)
    mocked_flags.cloud = providers.AWS
    mocked_flags.os_type = os_types.DEBIAN
    mocked_flags.run_uri = 'aaaaaa'
    mocked_flags.temp_dir = 'tmp'
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
        _BENCHMARK_NAME, flag_values=mocked_flags, vm_groups={})
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
    network_mock.placement_group = placement_group
    self.vm.network = network_mock

    self.response = self.openJsonData('aws-describe-instance.json')
    self.sir_response =\
        self.openJsonData('aws-describe-spot-instance-requests.json')

  def testInstancePresent(self):
    util.IssueRetryableCommand.side_effect = [(self.response, None)]
    self.assertTrue(self.vm._Exists())

  def testInstanceDeleted(self):
    response = json.loads(self.response)
    state = response['Reservations'][0]['Instances'][0]['State']
    state['Name'] = 'shutting-down'
    util.IssueRetryableCommand.side_effect = [(json.dumps(response), None)]
    self.assertFalse(self.vm._Exists())

  def testCreateSpot(self):
    vm_util.IssueCommand.side_effect = [(self.sir_response, None, None),
                                        (self.sir_response, None, None)]

    self.vm._CreateSpot()

    vm_util.IssueCommand.assert_any_call(
        ['aws',
         '--output',
         'json',
         'ec2',
         'request-spot-instances',
         '--region=us-east-1',
         '--spot-price=123.45',
         '--client-token=00000000-1111-2222-3333-444444444444',
         '--launch-specification='
         '{"ImageId":"ami-12345","InstanceType":"c3.large",'
         '"KeyName":"perfkit-key-aaaaaa",'
         '"Placement":{'
         '"AvailabilityZone":"us-east-1a",'
         '"GroupName":"placement_group_name"},'
         '"BlockDeviceMappings":['
         '{"VirtualName":"ephemeral0","DeviceName":"/dev/xvdb"},'
         '{"VirtualName":"ephemeral1","DeviceName":"/dev/xvdc"}],'
         '"NetworkInterfaces":[{"DeviceIndex":0,'
         '"AssociatePublicIpAddress":true,"SubnetId":"subnet-id"}]}'])
    vm_util.IssueCommand.assert_called_with(
        ['aws',
         '--output',
         'json',
         '--region=us-east-1',
         'ec2',
         'describe-spot-instance-requests',
         '--spot-instance-request-ids=sir-3wri5sgk'])

  def testCreateSpotLowPriceFails(self):
    response_low = json.loads(self.sir_response)
    response_low['SpotInstanceRequests'][0]['Status']['Code'] = 'price-too-low'
    response_low['SpotInstanceRequests'][0]['Status']['Message'] = \
        'Your price is too low.'
    response_cancel = (
        '{"CancelledSpotInstanceRequests":'
        '[{"State": "cancelled","SpotInstanceRequestId":"sir-2mcg43gk"}]}')
    vm_util.IssueCommand.side_effect = [(self.sir_response, None, None),
                                        (json.dumps(response_low), None, None),
                                        (response_cancel, None, None)]

    self.assertRaises(errors.Resource.CreationError, self.vm._CreateSpot)

    def testDeleteCancelsSpotInstanceRequest(self):
      self.vm.spot_instance_request_id = 'sir-abc'

      self.vm._Delete()

      vm_util.IssueCommand.assert_called_with(
          ['aws',
           '--output',
           'json',
           '--region=us-east-1',
           'ec2',
           'cancel-spot-instance-requests',
           '--spot-instance-request-ids=sir-abc'])


class AwsIsRegionTestCase(unittest.TestCase):

  def testBadFormat(self):
    with self.assertRaises(ValueError):
      util.IsRegion('us-east')

  def testZone(self):
    self.assertFalse(util.IsRegion('us-east-1a'))

  def testRegion(self):
    self.assertTrue(util.IsRegion('eu-central-1'))


class AwsGetRegionFromZoneTestCase(unittest.TestCase):

  def testBadFormat(self):
    with self.assertRaises(ValueError):
      util.GetRegionFromZone('invalid')

  def testZone(self):
    self.assertEqual(util.GetRegionFromZone('us-east-1a'), 'us-east-1')

  def testRegion(self):
    self.assertEqual(util.GetRegionFromZone('eu-central-1'), 'eu-central-1')


class AwsGetBlockDeviceMapTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(util.__name__ + '.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__),
                        'data', 'describe_image_output.txt')
    with open(path) as fp:
      self.describeImageOutput = fp.read()

  def testInvalidMachineType(self):
    self.assertEqual(aws_virtual_machine.GetBlockDeviceMap('invalid'), None)

  def testValidMachineTypeWithNoRootVolumeSize(self):
    expected = [{"DeviceName": "/dev/xvdb",
                 "VirtualName": "ephemeral0"}]
    actual = json.loads(aws_virtual_machine.GetBlockDeviceMap('c1.medium'))
    self.assertEqual(actual, expected)

  def testValidMachineTypeWithSpecifiedRootVolumeSize(self):
    util.IssueRetryableCommand.side_effect = [(self.describeImageOutput, None)]
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


class AwsGetRootBlockDeviceSpecForImageTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(util.__name__ + '.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__),
                        'data', 'describe_image_output.txt')
    with open(path) as fp:
      self.describeImageOutput = fp.read()

  def testOk(self):
    util.IssueRetryableCommand.side_effect = [(self.describeImageOutput, None)]
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
