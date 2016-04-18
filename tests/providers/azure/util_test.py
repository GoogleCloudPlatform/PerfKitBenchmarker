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
from perfkitbenchmarker.providers.azure import util


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


if __name__ == '__main__':
  unittest.main()
