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

"""Tests for linux_virtual_machine.py."""

import unittest

import mock

from perfkitbenchmarker import linux_virtual_machine
from tests import mock_flags


# Need to provide implementations for all of the abstract methods in
# order to instantiate linux_virtual_machine.BaseLinuxMixin.
class LinuxVM(linux_virtual_machine.BaseLinuxMixin):

  def Install(self):
    pass

  def Uninstall(self):
    pass


class TestSetFiles(unittest.TestCase):

  def runTest(self, set_files, calls):
    """Run a SetFiles test.

    Args:
      set_files: the value of FLAGS.set_files
      calls: a list of mock.call() objects giving the expected calls to
        vm.RemoteCommand() for the test.
    """

    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags['set_files'].parse(set_files)

    vm = LinuxVM()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.SetFiles()

    self.assertItemsEqual(  # use assertItemsEqual because order is undefined
        remote_command.call_args_list,
        calls)

  def testNoFiles(self):
    self.runTest([],
                 [])

  def testOneFile(self):
    self.runTest(['/sys/kernel/mm/transparent_hugepage/enabled=always'],
                 [mock.call('echo "always" | sudo tee '
                            '/sys/kernel/mm/transparent_hugepage/enabled')])

  def testMultipleFiles(self):
    self.runTest(['/sys/kernel/mm/transparent_hugepage/enabled=always',
                  '/sys/kernel/mm/transparent_hugepage/defrag=never'],
                 [mock.call('echo "always" | sudo tee '
                            '/sys/kernel/mm/transparent_hugepage/enabled'),
                  mock.call('echo "never" | sudo tee '
                            '/sys/kernel/mm/transparent_hugepage/defrag')])


class TestSysctl(unittest.TestCase):

  def runTest(self, sysctl, calls):
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags['sysctl'].parse(sysctl)
    vm = LinuxVM()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.DoSysctls()

    self.assertEqual(remote_command.call_args_list, calls)

  def testSysctl(self):
    self.runTest(
        ['vm.dirty_background_ratio=10', 'vm.dirty_ratio=25'],
        [mock.call('sudo bash -c \'echo "vm.dirty_background_ratio=10" >> '
                   '/etc/sysctl.conf\''),
         mock.call('sudo bash -c \'echo "vm.dirty_ratio=25" >> '
                   '/etc/sysctl.conf\'')])

  def testNoSysctl(self):
    self.runTest([],
                 [])


class TestDiskOperations(unittest.TestCase):

  def setUp(self):
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags['default_timeout'].parse(0)  # due to @retry
    patcher = mock.patch.object(LinuxVM, 'RemoteHostCommand')
    self.remote_command = patcher.start()
    self.addCleanup(patcher.stop)
    self.remote_command.side_effect = [('', None, 0), ('', None, 0)]
    self.vm = LinuxVM()

  def assertRemoteHostCalled(self, *calls):
    self.assertEqual([mock.call(call) for call in calls],
                     self.remote_command.call_args_list)

  def testMountDisk(self):
    mkdir_cmd = ('sudo mkdir -p mp;'
                 'sudo mount -o discard dp mp && '
                 'sudo chown -R $USER:$USER mp;')
    fstab_cmd = 'echo "dp mp ext4 defaults" | sudo tee -a /etc/fstab'
    self.vm.MountDisk('dp', 'mp')
    self.assertRemoteHostCalled(mkdir_cmd, fstab_cmd)

  def testFormatDisk(self):
    expected_command = ('[[ -d /mnt ]] && sudo umount /mnt; '
                        'sudo mke2fs -F -E lazy_itable_init=0,discard '
                        '-O ^has_journal -t ext4 -b 4096 dp')
    self.vm.FormatDisk('dp')
    self.assertRemoteHostCalled(expected_command)


if __name__ == '__main__':
  unittest.main()
