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

"""Tests for PerfKitBenchmarker' StaticVirtualMachine."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import mock

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.static_virtual_machine import StaticVirtualMachine
from perfkitbenchmarker.static_virtual_machine import StaticVmSpec
from tests import pkb_common_test_case
from six import StringIO
from six.moves import zip

FLAGS = flags.FLAGS

_COMPONENT = 'test_static_vm_spec'
_DISK_SPEC_DICTS = [{'device_path': '/test_device_path'},
                    {'mount_point': '/test_mount_point'}]


class StaticVmSpecTest(pkb_common_test_case.PkbCommonTestCase):

  def testDefaults(self):
    spec = StaticVmSpec(_COMPONENT)
    self.assertIsNone(spec.ip_address)
    self.assertIsNone(spec.user_name)
    self.assertIsNone(spec.ssh_private_key)
    self.assertIsNone(spec.internal_ip)
    self.assertEqual(spec.ssh_port, 22)
    self.assertIsNone(spec.password)
    self.assertIsNone(spec.os_type)
    self.assertEqual(spec.disk_specs, [])

  def testDiskSpecs(self):
    spec = StaticVmSpec(_COMPONENT, disk_specs=_DISK_SPEC_DICTS)
    self.assertEqual(len(spec.disk_specs), 2)
    for disk_spec in spec.disk_specs:
      self.assertIsInstance(disk_spec, disk.BaseDiskSpec)
    self.assertEqual(spec.disk_specs[0].device_path, '/test_device_path')
    self.assertIsNone(spec.disk_specs[0].mount_point)
    self.assertIsNone(spec.disk_specs[1].device_path)
    self.assertEqual(spec.disk_specs[1].mount_point, '/test_mount_point')


class StaticVirtualMachineTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(StaticVirtualMachineTest, self).setUp()
    self._initial_pool = StaticVirtualMachine.vm_pool
    StaticVirtualMachine.vm_pool.clear()
    p = mock.patch(vm_util.__name__ + '.GetTempDir', return_value='/tmp/dir')
    p.start()
    self.addCleanup(p.stop)
    FLAGS.image = 'test_image'
    FLAGS.os_type = 'debian'

  def tearDown(self):
    super(StaticVirtualMachineTest, self).tearDown()
    StaticVirtualMachine.vm_pool = self._initial_pool

  def _AssertStaticVMsEqual(self, vm1, vm2):
    self.assertEqual(vm1.ip_address, vm2.ip_address)
    self.assertEqual(vm1.internal_ip, vm2.internal_ip)
    self.assertEqual(vm1.user_name, vm2.user_name)
    self.assertEqual(vm1.zone, vm2.zone)
    self.assertEqual(vm1.ssh_private_key, vm2.ssh_private_key)

  def testReadFromFile_WrongFormat(self):
    fp = StringIO('{}')
    self.assertRaises(ValueError,
                      StaticVirtualMachine.ReadStaticVirtualMachineFile,
                      fp)

  def testReadFromFile_MissingKey(self):
    fp = StringIO('[{"ip_address": "10.10.10.3"}]')
    self.assertRaises(ValueError,
                      StaticVirtualMachine.ReadStaticVirtualMachineFile,
                      fp)

  def testReadFromFile_Empty(self):
    fp = StringIO('[]')
    StaticVirtualMachine.ReadStaticVirtualMachineFile(fp)
    self.assertEqual([], list(StaticVirtualMachine.vm_pool))

  def testReadFromFile_NoErr(self):
    s = ('[{'
         '  "ip_address": "174.12.14.1", '
         '  "user_name": "perfkitbenchmarker", '
         '  "keyfile_path": "perfkitbenchmarker.pem" '
         '}, '
         '{ '
         '   "ip_address": "174.12.14.121", '
         '   "user_name": "ubuntu", '
         '   "keyfile_path": "rackspace.pem", '
         '   "internal_ip": "10.10.10.2", '
         '   "zone": "rackspace_dallas" '
         '}] ')
    fp = StringIO(s)
    StaticVirtualMachine.ReadStaticVirtualMachineFile(fp)

    vm_pool = StaticVirtualMachine.vm_pool
    self.assertEqual(2, len(vm_pool))
    self._AssertStaticVMsEqual(
        StaticVirtualMachine(StaticVmSpec(
            _COMPONENT, ip_address='174.12.14.1',
            user_name='perfkitbenchmarker',
            ssh_private_key='perfkitbenchmarker.pem')), vm_pool[0])
    self._AssertStaticVMsEqual(
        StaticVirtualMachine(
            StaticVmSpec(_COMPONENT, ip_address='174.12.14.121',
                         user_name='ubuntu', ssh_private_key='rackspace.pem',
                         internal_ip='10.10.10.2', zone='rackspace_dallas')),
        vm_pool[1])

  def testReadFromFile_InvalidScratchDisksType(self):
    s = ('[{'
         '  "ip_address": "174.12.14.1", '
         '  "user_name": "perfkitbenchmarker", '
         '  "keyfile_path": "perfkitbenchmarker.pem", '
         '  "scratch_disk_mountpoints": "/tmp/google-pkb" '
         '}]')
    fp = StringIO(s)
    self.assertRaises(ValueError,
                      StaticVirtualMachine.ReadStaticVirtualMachineFile,
                      fp)

  def testReadFromFile_UnknownOsTypeDefaultsToLinuxRequiredKeys(self):
    FLAGS.os_type = 'unknown_os_type'
    s = ('[{'
         '  "ip_address": "174.12.14.1", '
         '  "user_name": "perfkitbenchmarker", '
         '  "keyfile_path": "perfkitbenchmarker.pem"'
         '}]')
    fp = StringIO(s)
    StaticVirtualMachine.ReadStaticVirtualMachineFile(fp)

    vm_pool = StaticVirtualMachine.vm_pool
    self.assertEqual(1, len(vm_pool))
    self._AssertStaticVMsEqual(
        StaticVirtualMachine(
            StaticVmSpec(_COMPONENT,
                         ip_address='174.12.14.1',
                         user_name='perfkitbenchmarker',
                         ssh_private_key='perfkitbenchmarker.pem')),
        vm_pool[0])

  def testCreateReturn(self):
    s = ('[{'
         '  "ip_address": "174.12.14.1", '
         '  "user_name": "perfkitbenchmarker", '
         '  "keyfile_path": "perfkitbenchmarker.pem" '
         '}, '
         '{ '
         '   "ip_address": "174.12.14.121", '
         '   "user_name": "ubuntu", '
         '   "keyfile_path": "rackspace.pem", '
         '   "internal_ip": "10.10.10.2", '
         '   "zone": "rackspace_dallas" '
         '}] ')
    fp = StringIO(s)
    StaticVirtualMachine.ReadStaticVirtualMachineFile(fp)
    self.assertEqual(2, len(StaticVirtualMachine.vm_pool))
    vm0 = StaticVirtualMachine.GetStaticVirtualMachine()
    self.assertTrue(vm0.from_pool)
    self.assertEqual(1, len(StaticVirtualMachine.vm_pool))
    vm0.Delete()
    self.assertEqual(2, len(StaticVirtualMachine.vm_pool))
    vm1 = StaticVirtualMachine.GetStaticVirtualMachine()
    self.assertIs(vm0, vm1)

  def testDiskSpecs(self):
    s = """
    [{
        "ip_address": "174.12.14.1",
        "user_name": "ubuntu",
        "keyfile_path": "test_keyfile_path",
        "local_disks": ["/test_local_disk_0", "/test_local_disk_1"],
        "scratch_disk_mountpoints": ["/test_scratch_disk_0",
                                     "/test_scratch_disk_1"]
    }]
    """
    expected_paths_and_mount_points = (
        (None, '/test_scratch_disk_0'), (None, '/test_scratch_disk_1'),
        ('/test_local_disk_0', None), ('/test_local_disk_1', None))
    fp = StringIO(s)
    StaticVirtualMachine.ReadStaticVirtualMachineFile(fp)
    self.assertEqual(1, len(StaticVirtualMachine.vm_pool))
    vm = StaticVirtualMachine.GetStaticVirtualMachine()
    self.assertTrue(vm.from_pool)
    self.assertEqual(len(vm.disk_specs), 4)
    for disk_spec, expected_paths in zip(vm.disk_specs,
                                         expected_paths_and_mount_points):
      expected_device_path, expected_mount_point = expected_paths
      self.assertEqual(disk_spec.device_path, expected_device_path)
      self.assertEqual(disk_spec.mount_point, expected_mount_point)


if __name__ == '__main__':
  unittest.main()
