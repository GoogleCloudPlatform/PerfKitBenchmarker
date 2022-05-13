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
PYTHON2_PATH_MISSING = _RESPONSE_BAD
ALTERNATIVES_CALL_BAD = _RESPONSE_BAD
PYTHON_VERSION_CALL_BAD = _RESPONSE_BAD

# Responses for non-error remote commands
_RESPONSE_GOOD = '', '', 0
PYTHON_VERSION_2 = '', 'Python 2.7', 0
PYTHON2_PATH_FOUND = _RESPONSE_GOOD
ALTERNATIVES_CALL_GOOD = _RESPONSE_GOOD
SYMLINK_SET = _RESPONSE_GOOD


class PythonTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(PythonTest, self).setUp()
    FLAGS['default_timeout'].parse(0)  # due to @retry

  def RunSetDefault(self,
                    responses,
                    expected_last_call=None):
    vm = mock.Mock()
    vm.PYTHON_2_PACKAGE = 'python2'
    vm.RemoteCommandWithReturnCode.side_effect = responses
    python._SetDefaultPythonIfNeeded(vm)
    if expected_last_call:
      vm.RemoteCommandWithReturnCode.assert_called_with(
          expected_last_call, ignore_failure=True)
    self.assertLen(vm.RemoteCommandWithReturnCode.call_args_list,
                   len(responses))

  @mock.patch.object(python, '_SetDefaultPythonIfNeeded')
  def testYumCall(self, mock_set_default):
    vm = mock.Mock()
    vm.PYTHON_2_PACKAGE = 'python-foo'
    python.Install(vm)
    mock_set_default.assert_called_with(vm)

  @mock.patch.object(python, '_SetDefaultPythonIfNeeded')
  def testAptCall(self, mock_set_default):
    vm = mock.Mock()
    python.Install(vm)
    mock_set_default.assert_called_with(vm)

  def testDefaultPythonAlreadySet(self):
    responses = [PYTHON_VERSION_2]
    expected = 'python --version'
    self.RunSetDefault(responses, expected)

  def testBadAlternativesResponse(self):
    responses = [
        PYTHON_MISSING, PYTHON2_PATH_FOUND, ALTERNATIVES_CALL_BAD, SYMLINK_SET,
        PYTHON_VERSION_2
    ]
    expected = 'python --version'
    self.RunSetDefault(responses, expected)

  def testMissingPythonPath(self):
    responses = [PYTHON_MISSING, PYTHON2_PATH_MISSING]
    with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
      self.RunSetDefault(responses)

  def testNoPythonVersionAfterSet(self):
    responses = [
        PYTHON_MISSING, PYTHON2_PATH_FOUND, ALTERNATIVES_CALL_GOOD,
        PYTHON_VERSION_CALL_BAD
    ]
    with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
      self.RunSetDefault(responses)

  def testPythonVersionAfterSet(self):
    responses = [
        PYTHON_MISSING, PYTHON2_PATH_FOUND, ALTERNATIVES_CALL_GOOD,
        PYTHON_VERSION_2
    ]
    expected = 'python --version'
    self.RunSetDefault(responses, expected)


if __name__ == '__main__':
  unittest.main()
