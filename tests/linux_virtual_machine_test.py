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


class TestConfigureVMKernel(unittest.TestCase):
  def runTest(self, procfs, sysfs, procfs_calls, sysfs_calls):
    """Run a ConfigureVMKernel test.

    Args:
      procfs, sysfs: dictionaries in the format of FLAGS.{procfs,sysfs}_config
        giving the configuration options to set.
      procfs_calls, sysfs_calls: lists of mock.call() objects giving the
        expected calls to vm.RemoteCommand() for the test.
    """

    self.mocked_flags = mock_flags.PatchTestCaseFlags(self)
    self.mocked_flags.procfs_config = procfs
    self.mocked_flags.sysfs_config = sysfs

    vm = LinuxVM()

    with mock.patch.object(vm, 'RemoteCommand') as remote_command:
      vm.ConfigureVMKernel()

    self.assertItemsEqual(  # use assertItemsEqual because order is undefined.
      procfs_calls + sysfs_calls,
      remote_command.call_args_list)

  def testConfigureVMKernel(self):
    self.runTest({'sys': {'vm': {'dirty_background_ratio': '10'}}},
                 {},
                 [mock.call('echo "10" | sudo tee '
                            '/proc/sys/vm/dirty_background_ratio')],
                 [])

  def testConvertToString(self):
    self.runTest({'sys': {'vm': {'dirty_background_ratio': 10}}},
                 {},
                 [mock.call('echo "10" | sudo tee '
                            '/proc/sys/vm/dirty_background_ratio')],
                 [])

  def testMultipleFiles(self):
    self.runTest({'sys': {'vm': {'dirty_background_ratio': 10,
                                 'dirty_ratio': 50}}},
                 {},
                 [mock.call('echo "10" | sudo tee '
                            '/proc/sys/vm/dirty_background_ratio'),
                  mock.call('echo "50" | sudo tee '
                            '/proc/sys/vm/dirty_ratio')],
                 [])

  def testSysfs(self):
    self.runTest({},
                 {'kernel': {'mm': {'transparent_hugepage':
                                    {'enabled': 'always'}}}},
                 [],
                 [mock.call('echo "always" | sudo tee '
                            '/sys/kernel/mm/transparent_hugepage/enabled')])


if __name__ == '__main__':
  unittest.main()
