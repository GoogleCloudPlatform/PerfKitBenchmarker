# Copyright 2014 Google Inc. All rights reserved.
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

"""Tests for perfkitbenchmarker.vm_util."""

import unittest
import mock

from perfkitbenchmarker import vm_util


class StartAntagnoistTest(unittest.TestCase):

  def testStartAntagnoist(self):
    vm = mock.MagicMock()
    vm_util.StartAntagnoist(vm, num_cpu_process=10,
                            num_io_process=5,
                            num_memory_process=15,
                            vm_bytes='10M',
                            vm_hang='100M')
    expected = [mock.call.RemoteCommand('pkill stress', ignore_failure=True),
                mock.call.InstallPackage('stress'),
                mock.call.RemoteCommand(
                    'nohup stress --cpu 10 --io 5 --vm 15 --vm-bytes 10M '
                    '--vm-hang 100M 1> /dev/null 2> /dev/null &')]
    self.assertEqual(vm.mock_calls, expected)

  def testStartAntagnoistWithNoFlags(self):
    vm_util.StartAntagnoist(vm=None)

  def testStartAntagnoistWithNoVmArgument(self):
    vm = mock.MagicMock()
    vm_util.StartAntagnoist(vm, num_cpu_process=10,
                            num_io_process=5,
                            vm_bytes='10M',
                            vm_hang='100M')
    expected = [mock.call.RemoteCommand('pkill stress', ignore_failure=True),
                mock.call.InstallPackage('stress'),
                mock.call.RemoteCommand(
                    'nohup stress --cpu 10 --io 5 1> /dev/null 2> /dev/null &')]
    self.assertEqual(vm.mock_calls, expected)


if __name__ == '__main__':
  unittest.main()
