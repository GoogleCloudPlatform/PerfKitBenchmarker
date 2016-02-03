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
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
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


class AwsVirtualMachineExistsTestCase(unittest.TestCase):

  def setUp(self):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)
    mocked_flags.cloud = providers.AWS
    mocked_flags.os_type = os_types.DEBIAN
    mocked_flags.run_uri = 'aaaaaa'
    p = mock.patch('perfkitbenchmarker.providers.aws.'
                   'util.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)

    # VM Creation depends on there being a BenchmarkSpec.
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        _BENCHMARK_NAME, flag_values=mocked_flags, vm_groups={})
    self.spec = benchmark_spec.BenchmarkSpec(config_spec, _BENCHMARK_NAME,
                                             _BENCHMARK_UID)
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

    self.vm = aws_virtual_machine.AwsVirtualMachine(
        virtual_machine.BaseVmSpec('test_vm_spec.AWS', zone='us-east-1a',
                                   machine_type='c3.large'))
    self.vm.id = 'i-foo'
    path = os.path.join(os.path.dirname(__file__),
                        'data', 'aws-describe-instance.json')
    with open(path) as f:
      self.response = f.read()

  def testInstancePresent(self):
    util.IssueRetryableCommand.side_effect = [(self.response, None)]
    self.assertTrue(self.vm._Exists())

  def testInstanceDeleted(self):
    response = json.loads(self.response)
    state = response['Reservations'][0]['Instances'][0]['State']
    state['Name'] = 'shutting-down'
    util.IssueRetryableCommand.side_effect = [(json.dumps(response), None)]
    self.assertFalse(self.vm._Exists())


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
