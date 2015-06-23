# Copyright 2015 Google Inc. All rights reserved.
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

"""Tests for perfkitbenchmarker.aws."""

import json
import os.path
import unittest

import mock

from perfkitbenchmarker import disk
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.aws import aws_disk
from perfkitbenchmarker.aws import aws_network
from perfkitbenchmarker.aws import aws_virtual_machine
from perfkitbenchmarker.aws import util


class AwsVolumeExistsTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(util.__name__ + '.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)
    self.disk = aws_disk.AwsDisk(disk.BaseDiskSpec(None, None, None), 'zone-a')
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
    p = mock.patch('perfkitbenchmarker.aws.util.IssueRetryableCommand')
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
    for module in ('perfkitbenchmarker.virtual_machine',
                   'perfkitbenchmarker.vm_util'):
      p = mock.patch('{0}.FLAGS'.format(module))
      mock_flags = p.start()
      mock_flags.run_uri = 'aaaaaa'
      self.addCleanup(p.stop)
    p = mock.patch('perfkitbenchmarker.aws.util.IssueRetryableCommand')
    p.start()
    self.addCleanup(p.stop)
    self.vm = aws_virtual_machine.AwsVirtualMachine(
        virtual_machine.BaseVirtualMachineSpec(
            None, 'us-east-1a', 'c3.large', None, None))
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

if __name__ == '__main__':
  unittest.main()
