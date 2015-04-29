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
import os

from perfkitbenchmarker import vm_util


class IssueCommandsWithEnvironmentVariablesTestCase(unittest.TestCase):

  def testNoEnvArgument(self):
    with mock.patch.dict('os.environ', {'perfkit': 'benchmarker'}, clear=True):
      with mock.patch('perfkitbenchmarker.vm_util.subprocess') as subprocess:
        process_mock = mock.Mock()
        attributes = {'communicate.return_value': ('output', 'error',),
                      'returncode': 0}
        process_mock.configure_mock(**attributes)
        subprocess.Popen.return_value = process_mock

        vm_util.IssueCommand(['ls', '-la'])
        subprocess.Popen.assert_called_once_with(['ls', '-la'],
                                                 env=os.environ.copy(),
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE)

  def testOverwriteEnvArgument(self):
    with mock.patch.dict('os.environ', {'perfkit': 'old'}, clear=True):
      with mock.patch('perfkitbenchmarker.vm_util.subprocess') as subprocess:
        process_mock = mock.Mock()
        attributes = {'communicate.return_value': ('output', 'error',),
                      'returncode': 0}
        process_mock.configure_mock(**attributes)
        subprocess.Popen.return_value = process_mock

        env = dict({'perfkit': 'old'}, **{'perfkit': 'new'})

        vm_util.IssueCommand(['ls', '-la'], env=env)
        subprocess.Popen.assert_called_once_with(['ls', '-la'],
                                                 env={'perfkit': 'new'},
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE)

  def testAppendEnvArgument(self):
    with mock.patch.dict('os.environ', {'perfkit': 'old'}, clear=True):
      with mock.patch('perfkitbenchmarker.vm_util.subprocess') as subprocess:
        process_mock = mock.Mock()
        attributes = {'communicate.return_value': ('output', 'error',),
                      'returncode': 0}
        process_mock.configure_mock(**attributes)
        subprocess.Popen.return_value = process_mock

        env = dict({'perfkit': 'old'}, **{'newkey': 'newvalue'})

        vm_util.IssueCommand(['ls', '-la'], env=env)
        subprocess.Popen.assert_called_once_with(['ls', '-la'],
                                                 env={'perfkit': 'old',
                                                      'newkey': 'newvalue'},
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE)

  def testUnsetEnvArgument(self):
    with mock.patch.dict('os.environ', {'perfkit': 'old'}, clear=True):
      with mock.patch('perfkitbenchmarker.vm_util.subprocess') as subprocess:
        process_mock = mock.Mock()
        attributes = {'communicate.return_value': ('output', 'error',),
                      'returncode': 0}
        process_mock.configure_mock(**attributes)
        subprocess.Popen.return_value = process_mock

        env = {}

        vm_util.IssueCommand(['ls', '-la'], env=env)
        subprocess.Popen.assert_called_once_with(['ls', '-la'], env={},
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE)


class ShouldRunOnInternalIpAddressTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(vm_util.__name__ + '.FLAGS')
    self.flags = p.start()
    self.flags_patch = p
    self.sending_vm = mock.MagicMock()
    self.receiving_vm = mock.MagicMock()

  def tearDown(self):
    self.flags_patch.stop()

  def _RunTest(self, expectation, ip_addresses, is_reachable=True):
    self.flags.ip_addresses = ip_addresses
    self.sending_vm.IsReachable.return_value = is_reachable
    self.assertEqual(
        expectation,
        vm_util.ShouldRunOnInternalIpAddress(
            self.sending_vm, self.receiving_vm))

  def testExternal_Reachable(self):
    self._RunTest(False, vm_util.IpAddressSubset.EXTERNAL, True)

  def testExternal_Unreachable(self):
    self._RunTest(False, vm_util.IpAddressSubset.EXTERNAL, False)

  def testInternal_Reachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.INTERNAL, True)

  def testInternal_Unreachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.INTERNAL, False)

  def testBoth_Reachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.BOTH, True)

  def testBoth_Unreachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.BOTH, False)

  def testReachable_Reachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.REACHABLE, True)

  def testReachable_Unreachable(self):
    self._RunTest(
        False, vm_util.IpAddressSubset.REACHABLE, False)


if __name__ == '__main__':
  unittest.main()
