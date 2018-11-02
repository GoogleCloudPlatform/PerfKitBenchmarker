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
"""Tests for perfkitbenchmarker.disk."""

import unittest
import mock

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_COMPONENT = 'test_component'


class BaseDiskSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

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
    FLAGS['data_disk_size'].value = 100
    FLAGS['data_disk_type'].value = 'flag_disk_type'
    FLAGS['num_striped_disks'].value = 3
    FLAGS['scratch_dir'].value = '/flag_scratch_dir'
    spec = disk.BaseDiskSpec(
        _COMPONENT,
        FLAGS,
        device_path='config_device_path',
        disk_number=1,
        disk_size=75,
        disk_type='config_disk_type',
        mount_point='/mountpoint',
        num_striped_disks=2)
    self.assertEqual(spec.device_path, 'config_device_path')
    self.assertEqual(spec.disk_number, 1)
    self.assertEqual(spec.disk_size, 75)
    self.assertEqual(spec.disk_type, 'config_disk_type')
    self.assertEqual(spec.mount_point, '/mountpoint')
    self.assertEqual(spec.num_striped_disks, 2)

  def testPresentFlagsOverrideConfigs(self):
    FLAGS['data_disk_size'].parse(100)
    FLAGS['data_disk_type'].parse('flag_disk_type')
    FLAGS['num_striped_disks'].parse(3)
    FLAGS['scratch_dir'].parse('/flag_scratch_dir')
    spec = disk.BaseDiskSpec(
        _COMPONENT,
        FLAGS,
        device_path='config_device_path',
        disk_number=1,
        disk_size=75,
        disk_type='config_disk_type',
        mount_point='/mountpoint',
        num_striped_disks=2)
    self.assertEqual(spec.device_path, 'config_device_path')
    self.assertEqual(spec.disk_number, 1)
    self.assertEqual(spec.disk_size, 100)
    self.assertEqual(spec.disk_type, 'flag_disk_type')
    self.assertEqual(spec.mount_point, '/flag_scratch_dir')
    self.assertEqual(spec.num_striped_disks, 3)


class _NfsDisk(disk.NfsDisk):

  def __init__(self, flags=None, default_nfs_version=None):
    if flags:
      disk_spec = disk.BaseDiskSpec(_COMPONENT, flags)
    else:
      disk_spec = disk.BaseDiskSpec(_COMPONENT)
    super(_NfsDisk, self).__init__(disk_spec, 'host1:/volume1',
                                   default_nfs_version)


class NfsDiskTestCase(pkb_common_test_case.PkbCommonTestCase):

  def MountOptions(self, **overrides):
    mount_options = {
        'hard': None,
        'retrans': 2,
        'rsize': 1048576,
        'timeo': 600,
        'wsize': 1048576
    }
    mount_options.update(overrides)
    return mount_options

  def MountOptionsAsDict(self, mount_options_str):
    options = dict()
    int_values = set(['retrans', 'rsize', 'timeo', 'wsize'])
    for entry in mount_options_str.split(','):
      parts = entry.split('=', 1)
      key = parts[0]
      value = None if len(parts) == 1 else parts[1]
      options[key] = int(value) if key in int_values else value
    return options

  def testDefaults(self):
    nfs_disk = _NfsDisk()
    self.assertEqual('host1:/volume1', nfs_disk.GetDevicePath())
    self.assertEqual(self.MountOptions(),
                     self.MountOptionsAsDict(nfs_disk.mount_options))
    self.assertEqual(nfs_disk.mount_options, nfs_disk.fstab_options)
    disk_meta = {}
    for key, value in self.MountOptions().iteritems():
      disk_meta['nfs_{}'.format(key)] = value
    disk_meta.update({'num_stripes': 1, 'size': None, 'type': None})
    self.assertEqual(disk_meta, nfs_disk.metadata)
    self.assertTrue(nfs_disk._IsReady())

  def testNfsFlags(self):
    FLAGS['nfs_version'].parse('4.1')
    FLAGS['nfs_rsize'].parse(1)
    FLAGS['nfs_wsize'].parse(2)
    FLAGS['nfs_timeout'].parse(3)
    FLAGS['nfs_timeout_hard'].parse(False)
    FLAGS['nfs_retries'].parse(4)
    nfs_disk = _NfsDisk(FLAGS)
    mount_options = self.MountOptions(soft=None, retrans=4, rsize=1, timeo=30,
                                      wsize=2, nfsvers='4.1')
    mount_options.pop('hard')
    self.assertEqual(mount_options,
                     self.MountOptionsAsDict(nfs_disk.mount_options))

  def testDefaultNfsVersion(self):
    nfs_disk = _NfsDisk(default_nfs_version='4.1')
    self.assertEqual('4.1', nfs_disk.nfs_version)

  def testFlagsOverrideDefaultNfsVersion(self):
    FLAGS['nfs_version'].parse('3.0')
    nfs_disk = _NfsDisk(flags=FLAGS, default_nfs_version='4.1')
    self.assertEqual('3.0', nfs_disk.nfs_version)

  def testAttach(self):
    vm = mock.Mock()
    nfs_disk = _NfsDisk()
    nfs_disk.Attach(vm)
    vm.Install.assert_called_with('nfs_utils')

  def testDetach(self):
    vm = mock.Mock()
    FLAGS['scratch_dir'].parse('/mnt')
    nfs_disk = _NfsDisk(FLAGS)
    nfs_disk.Attach(vm)  # to set the vm on the disk
    nfs_disk.Detach()
    vm.RemoteCommand.assert_called_with('sudo umount /mnt')


if __name__ == '__main__':
  unittest.main()
