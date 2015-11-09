# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

from io import BytesIO
import unittest

import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.static_virtual_machine import StaticVirtualMachine
from perfkitbenchmarker.static_virtual_machine import StaticVmSpec


class StaticVirtualMachineTest(unittest.TestCase):

  def setUp(self):
    self._initial_pool = StaticVirtualMachine.vm_pool
    StaticVirtualMachine.vm_pool.clear()
    p = mock.patch(vm_util.__name__ + '.GetTempDir')
    p.start()
    self.addCleanup(p.stop)

  def tearDown(self):
    StaticVirtualMachine.vm_pool = self._initial_pool

  def _AssertStaticVMsEqual(self, vm1, vm2):
    self.assertEqual(vm1.ip_address, vm2.ip_address)
    self.assertEqual(vm1.internal_ip, vm2.internal_ip)
    self.assertEqual(vm1.user_name, vm2.user_name)
    self.assertEqual(vm1.zone, vm2.zone)
    self.assertEqual(vm1.ssh_private_key, vm2.ssh_private_key)

  def testReadFromFile_WrongFormat(self):
    fp = BytesIO('{}')
    self.assertRaises(ValueError,
                      StaticVirtualMachine.ReadStaticVirtualMachineFile,
                      fp)

  def testReadFromFile_MissingKey(self):
    fp = BytesIO('[{"ip_address": "10.10.10.3"}]')
    self.assertRaises(ValueError,
                      StaticVirtualMachine.ReadStaticVirtualMachineFile,
                      fp)

  def testReadFromFile_Empty(self):
    fp = BytesIO('[]')
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
    fp = BytesIO(s)
    StaticVirtualMachine.ReadStaticVirtualMachineFile(fp)

    vm_pool = StaticVirtualMachine.vm_pool
    self.assertEqual(2, len(vm_pool))
    self._AssertStaticVMsEqual(
        StaticVirtualMachine(StaticVmSpec(
            ip_address='174.12.14.1', user_name='perfkitbenchmarker',
            ssh_private_key='perfkitbenchmarker.pem')), vm_pool[0])
    self._AssertStaticVMsEqual(
        StaticVirtualMachine(
            StaticVmSpec(ip_address='174.12.14.121', user_name='ubuntu',
                         ssh_private_key='rackspace.pem',
                         internal_ip='10.10.10.2', zone='rackspace_dallas')),
        vm_pool[1])

  def testReadFromFile_InvalidScratchDisksType(self):
    s = ('[{'
         '  "ip_address": "174.12.14.1", '
         '  "user_name": "perfkitbenchmarker", '
         '  "keyfile_path": "perfkitbenchmarker.pem", '
         '  "scratch_disk_mountpoints": "/tmp/google-pkb" '
         '}]')
    fp = BytesIO(s)
    self.assertRaises(ValueError,
                      StaticVirtualMachine.ReadStaticVirtualMachineFile,
                      fp)

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
    fp = BytesIO(s)
    StaticVirtualMachine.ReadStaticVirtualMachineFile(fp)
    self.assertEqual(2, len(StaticVirtualMachine.vm_pool))
    vm0 = StaticVirtualMachine.GetStaticVirtualMachine()
    self.assertTrue(vm0.from_pool)
    self.assertEqual(1, len(StaticVirtualMachine.vm_pool))
    vm0.Delete()
    self.assertEqual(2, len(StaticVirtualMachine.vm_pool))
    vm1 = StaticVirtualMachine.GetStaticVirtualMachine()
    self.assertIs(vm0, vm1)


if __name__ == '__main__':
  unittest.main()
