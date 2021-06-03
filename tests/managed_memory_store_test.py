# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for managed_memory_store functions."""


import unittest
from perfkitbenchmarker import managed_memory_store
from tests import pkb_common_test_case


class ManagedMemoryStoreTest(pkb_common_test_case.PkbCommonTestCase):

  def testReadableVersion(self):
    """Test normal cases work as they should."""
    self.assertEqual(
        managed_memory_store.ParseReadableVersion('6.x'), '6.x')
    self.assertEqual(
        managed_memory_store.ParseReadableVersion('4.0.10'), '4.0')

  def testReadableVersionExtraneous(self):
    """Test weird cases just return the version number as is."""
    self.assertEqual(
        managed_memory_store.ParseReadableVersion('redis.8'), 'redis.8')
    self.assertEqual(
        managed_memory_store.ParseReadableVersion('redis 9_7_5'),
        'redis 9_7_5')


if __name__ == '__main__':
  unittest.main()
