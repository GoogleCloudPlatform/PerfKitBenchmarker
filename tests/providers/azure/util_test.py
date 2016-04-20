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
"""Tests for perfkitbenchmarker.providers.azure.util."""

import unittest

import mock

from perfkitbenchmarker import providers
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import util
from tests import mock_flags


class CheckAzureVersionTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch.object(providers, '_imported_providers', new=set())
    p.start()
    self.addCleanup(p.stop)

  def testNotCalledUponFlagImport(self):
    self.assertFalse(providers._imported_providers)
    with mock.patch.object(util, '_CheckAzureVersion') as mocked_function:
      providers.LoadProviderFlags([providers.AZURE])
      mocked_function.assert_not_called()

  def testCalledUponFullImport(self):
    self.assertFalse(providers._imported_providers)
    with mock.patch.object(util, '_CheckAzureVersion') as mocked_function:
      providers.LoadProvider(providers.AZURE)
      mocked_function.assert_called_once_with()

  def testAzureNotPresent(self):
    self.assertFalse(providers._imported_providers)
    with mock.patch.object(util.vm_util, 'IssueCommand', side_effect=OSError):
      with self.assertRaises(SystemExit):
        providers.LoadProvider(providers.AZURE)

  def testAzureFailed(self):
    ret = '', '', 1
    with mock.patch.object(util.vm_util, 'IssueCommand', return_value=ret):
      with self.assertRaises(SystemExit):
        util._CheckAzureVersion()

  def testIncorrectVersion(self):
    ret = '0.9.3', '', 0
    with mock.patch.object(util.vm_util, 'IssueCommand', return_value=ret):
      with self.assertRaises(SystemExit):
        util._CheckAzureVersion()

  def testIgnoreCliVersionFlag(self):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)
    mocked_flags['azure_ignore_cli_version'].Parse(True)
    ret = '0.9.3', '', 0
    with mock.patch.object(util.vm_util, 'IssueCommand', return_value=ret):
      util._CheckAzureVersion()

  def testCorrectVersion(self):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)
    mocked_flags['azure_ignore_cli_version'].Parse(True)
    ret = azure.EXPECTED_CLI_VERSION, '', 0
    with mock.patch.object(util.vm_util, 'IssueCommand', return_value=ret):
      util._CheckAzureVersion()


if __name__ == '__main__':
  unittest.main()
