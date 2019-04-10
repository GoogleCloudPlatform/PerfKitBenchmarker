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


class AwsVpcExistsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsVpcExistsTestCase, self).setUp()
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
    network_mock.placement_group = placement_group
    self.vm.network = network_mock

    self.response = self.open_json_data('aws-describe-instance.json')
    self.sir_response = self.open_json_data(
        'aws-describe-spot-instance-requests.json')

  def testInstancePresent(self):
    util.IssueRetryableCommand.side_effect = [(self.response, None)]
    self.assertTrue(self.vm._Exists())

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

  def testInstanceDeleted(self):
    response = json.loads(self.response)
    state = response['Reservations'][0]['Instances'][0]['State']
    state['Name'] = 'shutting-down'
    util.IssueRetryableCommand.side_effect = [(json.dumps(response), None)]
    self.assertFalse(self.vm._Exists())

  def testCreateSpot(self):
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
        '--associate-public-ip-address',
        '--client-token=00000000-1111-2222-3333-444444444444',
        '--image-id=ami-12345',
        '--instance-type=c3.large',
        '--key-name=perfkit-key-aaaaaa',
        '--block-device-mappings=[{"VirtualName": "ephemeral0", '
        '"DeviceName": "/dev/xvdb"}, {"VirtualName": "ephemeral1", '
        '"DeviceName": "/dev/xvdc"}]',
        '--placement=AvailabilityZone=us-east-1a,'
        'GroupName=placement_group_name',
        '--instance-market-options={"MarketType": "spot", '
        '"SpotOptions": {"SpotInstanceType": "one-time", '
        '"InstanceInterruptionBehavior": "terminate", "MaxPrice": "123.45"}}'
    ])
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
         '--spot-instance-request-ids=sir-abc'])
    self.vm.use_spot_instance = False


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


if __name__ == '__main__':
  unittest.main()
