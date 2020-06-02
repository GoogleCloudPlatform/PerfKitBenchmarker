# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.python."""

import unittest
from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import python
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

# Responses for error returning remote commands
_RESPONSE_BAD = '', '', 1
PYTHON_MISSING = _RESPONSE_BAD
PYTHON_PATH_MISSING = _RESPONSE_BAD
ALTERNATIVES_MISSING = _RESPONSE_BAD
ALTERNATIVES_CALL_BAD = _RESPONSE_BAD
PYTHON_VERSION_CALL_BAD = _RESPONSE_BAD

# Responses for non-error remote commands
_RESPONSE_GOOD = '', '', 0
PYTHON_VERSION_2 = '', 'Python 2.7', 0
ALTERNATIVES_FOUND = _RESPONSE_GOOD
PYTHON_PATH_FOUND = _RESPONSE_GOOD
ALTERNATIVES_CALL_GOOD = _RESPONSE_GOOD


class PythonTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(PythonTest, self).setUp()
    FLAGS['default_timeout'].parse(0)  # due to @retry

  def RunSetDefault(self,
                    responses,
                    expected_last_call=None):
    vm = mock.Mock()
    vm.PYTHON_PACKAGE = 'python2'
    vm.RemoteCommandWithReturnCode.side_effect = responses
    python._SetDefaultPythonIfNeeded(vm, '/usr/bin/python2')
    if expected_last_call:
      vm.RemoteCommandWithReturnCode.assert_called_with(
          expected_last_call, ignore_failure=True)
    self.assertLen(vm.RemoteCommandWithReturnCode.call_args_list,
                   len(responses))

  @mock.patch.object(python, '_SetDefaultPythonIfNeeded')
  def testYumCall(self, mock_set_default):
    vm = mock.Mock()
    vm.PYTHON_PACKAGE = 'python3'
    python.YumInstall(vm)
    mock_set_default.assert_called_with(vm, '/usr/bin/python3')

  @mock.patch.object(python, '_SetDefaultPythonIfNeeded')
  def testAptCall(self, mock_set_default):
    vm = mock.Mock()
    python.AptInstall(vm)
    mock_set_default.assert_called_with(vm, '/usr/bin/python2')

  def testDefaultPythonAlreadySet(self):
    responses = [PYTHON_VERSION_2]
    expected = 'python --version'
    self.RunSetDefault(responses, expected)

  def testNoAlternativesProgram(self):
    responses = [PYTHON_MISSING, ALTERNATIVES_MISSING]
    expected = 'which update-alternatives'
    self.RunSetDefault(responses, expected)

  def testMissingPythonPath(self):
    responses = [PYTHON_MISSING, ALTERNATIVES_FOUND, PYTHON_PATH_MISSING]
    expected = 'ls /usr/bin/python2'
    self.RunSetDefault(responses, expected)

  def testBadAlternativesResponse(self):
    responses = [
        PYTHON_MISSING, ALTERNATIVES_FOUND, PYTHON_PATH_FOUND,
        ALTERNATIVES_CALL_BAD
    ]
    with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
      self.RunSetDefault(responses)

  def testNoPythonVersionAfterSet(self):
    responses = [
        PYTHON_MISSING, ALTERNATIVES_FOUND, PYTHON_PATH_FOUND,
        ALTERNATIVES_CALL_GOOD, PYTHON_VERSION_CALL_BAD
    ]
    with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
      self.RunSetDefault(responses)

  def testPythonVersionAfterSet(self):
    responses = [
        PYTHON_MISSING, ALTERNATIVES_FOUND, PYTHON_PATH_FOUND,
        ALTERNATIVES_CALL_GOOD, PYTHON_VERSION_2
    ]
    expected = 'python --version'
    self.RunSetDefault(responses, expected)


if __name__ == '__main__':
  unittest.main()
