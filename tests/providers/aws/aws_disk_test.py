# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_disk."""

import unittest

from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.aws import aws_disk
from tests import mock_flags


_COMPONENT = 'test_component'


class AwsDiskSpecTestCase(unittest.TestCase):

  def testDefaults(self):
    spec = aws_disk.AwsDiskSpec(_COMPONENT)
    self.assertIsNone(spec.device_path)
    self.assertIsNone(spec.disk_number)
    self.assertIsNone(spec.disk_size)
    self.assertIsNone(spec.disk_type)
    self.assertIsNone(spec.iops)
    self.assertIsNone(spec.mount_point)
    self.assertEqual(spec.num_striped_disks, 1)

  def testProvidedValid(self):
    spec = aws_disk.AwsDiskSpec(
        _COMPONENT, device_path='test_device_path', disk_number=1,
        disk_size=75, disk_type='test_disk_type', iops=1000,
        mount_point='/mountpoint', num_striped_disks=2)
    self.assertEqual(spec.device_path, 'test_device_path')
    self.assertEqual(spec.disk_number, 1)
    self.assertEqual(spec.disk_size, 75)
    self.assertEqual(spec.disk_type, 'test_disk_type')
    self.assertEqual(spec.iops, 1000)
    self.assertEqual(spec.mount_point, '/mountpoint')
    self.assertEqual(spec.num_striped_disks, 2)

  def testProvidedNone(self):
    spec = aws_disk.AwsDiskSpec(_COMPONENT, iops=None)
    self.assertIsNone(spec.iops)

  def testInvalidOptionTypes(self):
    with self.assertRaises(errors.Config.InvalidValue):
      aws_disk.AwsDiskSpec(_COMPONENT, iops='ten')

  def testNonPresentFlagsDoNotOverrideConfigs(self):
    flags = mock_flags.MockFlags()
    flags['aws_provisioned_iops'].value = 2000
    flags['data_disk_size'].value = 100
    spec = aws_disk.AwsDiskSpec(_COMPONENT, flags, disk_size=75, iops=1000)
    self.assertEqual(spec.disk_size, 75)
    self.assertEqual(spec.iops, 1000)

  def testPresentFlagsOverrideConfigs(self):
    flags = mock_flags.MockFlags()
    flags['aws_provisioned_iops'].parse(2000)
    flags['data_disk_size'].parse(100)
    spec = aws_disk.AwsDiskSpec(_COMPONENT, flags, disk_size=75, iops=1000)
    self.assertEqual(spec.disk_size, 100)
    self.assertEqual(spec.iops, 2000)


if __name__ == '__main__':
  unittest.main()
