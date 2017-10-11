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

"""Tests for linux_virtual_machine.py"""

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
    self.mocked_flags.set_files = set_files

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
  def testSysctl(self):
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags.sysctl = ['vm.dirty_background_ratio=10',
                                'vm.dirty_ratio=25']
    vm = LinuxVM()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.DoSysctls()

    self.assertEqual(
        remote_command.call_args_list,
        [mock.call('sudo bash -c \'echo "vm.dirty_background_ratio=10" >> '
                   '/etc/sysctl.conf\''),
         mock.call('sudo bash -c \'echo "vm.dirty_ratio=25" >> '
                   '/etc/sysctl.conf\'')])

  def testNoSysctl(self):
    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags.sysctl = []
    vm = LinuxVM()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.DoSysctls()

    self.assertEqual(
        remote_command.call_args_list,
        [])


if __name__ == '__main__':
  unittest.main()
