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
"""Tests for perfkitbenchmarker.disk."""

import unittest

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from tests import mock_flags


_COMPONENT = 'test_component'


class BaseDiskSpecTestCase(unittest.TestCase):

  def testDefaults(self):
    spec = disk.BaseDiskSpec(_COMPONENT)
    self.assertIsNone(spec.device_path)
    self.assertIsNone(spec.disk_number)
    self.assertIsNone(spec.disk_size)
    self.assertIsNone(spec.disk_type)
    self.assertIsNone(spec.mount_point)
    self.assertEqual(spec.num_striped_disks, 1)

  def testProvidedValid(self):
    spec = disk.BaseDiskSpec(
        _COMPONENT, device_path='test_device_path', disk_number=1,
        disk_size=75, disk_type='test_disk_type', mount_point='/mountpoint',
        num_striped_disks=2)
    self.assertEqual(spec.device_path, 'test_device_path')
    self.assertEqual(spec.disk_number, 1)
    self.assertEqual(spec.disk_size, 75)
    self.assertEqual(spec.disk_type, 'test_disk_type')
    self.assertEqual(spec.mount_point, '/mountpoint')
    self.assertEqual(spec.num_striped_disks, 2)

  def testProvidedNone(self):
    spec = disk.BaseDiskSpec(
        _COMPONENT, device_path=None, disk_number=None, disk_size=None,
        disk_type=None, mount_point=None)
    self.assertIsNone(spec.device_path)
    self.assertIsNone(spec.disk_number)
    self.assertIsNone(spec.disk_size)
    self.assertIsNone(spec.disk_type)
    self.assertIsNone(spec.mount_point)
    self.assertEqual(spec.num_striped_disks, 1)

  def testUnrecognizedOptions(self):
    with self.assertRaises(errors.Config.UnrecognizedOption) as cm:
      disk.BaseDiskSpec(_COMPONENT, color='red', flavor='cherry', texture=None)
    self.assertEqual(str(cm.exception), (
        'Unrecognized options were found in test_component: color, flavor, '
        'texture.'))

  def testInvalidOptionTypes(self):
    with self.assertRaises(errors.Config.InvalidValue):
      disk.BaseDiskSpec(_COMPONENT, device_path=0)
    with self.assertRaises(errors.Config.InvalidValue):
      disk.BaseDiskSpec(_COMPONENT, disk_number='ten')
    with self.assertRaises(errors.Config.InvalidValue):
      disk.BaseDiskSpec(_COMPONENT, disk_size='ten')
    with self.assertRaises(errors.Config.InvalidValue):
      disk.BaseDiskSpec(_COMPONENT, disk_type=0)
    with self.assertRaises(errors.Config.InvalidValue):
      disk.BaseDiskSpec(_COMPONENT, mount_point=0)
    with self.assertRaises(errors.Config.InvalidValue):
      disk.BaseDiskSpec(_COMPONENT, num_striped_disks=None)

  def testOutOfRangeOptionValues(self):
    with self.assertRaises(errors.Config.InvalidValue):
      disk.BaseDiskSpec(_COMPONENT, num_striped_disks=0)

  def testNonPresentFlagsDoNotOverrideConfigs(self):
    flags = mock_flags.MockFlags()
    flags['data_disk_size'].value = 100
    flags['data_disk_type'].value = 'flag_disk_type'
    flags['num_striped_disks'].value = 3
    flags['scratch_dir'].value = '/flag_scratch_dir'
    spec = disk.BaseDiskSpec(
        _COMPONENT, flags, device_path='config_device_path', disk_number=1,
        disk_size=75, disk_type='config_disk_type', mount_point='/mountpoint',
        num_striped_disks=2)
    self.assertEqual(spec.device_path, 'config_device_path')
    self.assertEqual(spec.disk_number, 1)
    self.assertEqual(spec.disk_size, 75)
    self.assertEqual(spec.disk_type, 'config_disk_type')
    self.assertEqual(spec.mount_point, '/mountpoint')
    self.assertEqual(spec.num_striped_disks, 2)

  def testPresentFlagsOverrideConfigs(self):
    flags = mock_flags.MockFlags()
    flags['data_disk_size'].parse(100)
    flags['data_disk_type'].parse('flag_disk_type')
    flags['num_striped_disks'].parse(3)
    flags['scratch_dir'].parse('/flag_scratch_dir')
    spec = disk.BaseDiskSpec(
        _COMPONENT, flags, device_path='config_device_path', disk_number=1,
        disk_size=75, disk_type='config_disk_type', mount_point='/mountpoint',
        num_striped_disks=2)
    self.assertEqual(spec.device_path, 'config_device_path')
    self.assertEqual(spec.disk_number, 1)
    self.assertEqual(spec.disk_size, 100)
    self.assertEqual(spec.disk_type, 'flag_disk_type')
    self.assertEqual(spec.mount_point, '/flag_scratch_dir')
    self.assertEqual(spec.num_striped_disks, 3)


if __name__ == '__main__':
  unittest.main()
