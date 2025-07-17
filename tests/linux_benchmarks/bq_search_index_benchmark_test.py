# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for bq_search_index_benchmark."""

import unittest
from unittest import mock
from perfkitbenchmarker.linux_benchmarks import bq_search_index_benchmark


class BqSearchIndexBenchmarkTest(unittest.TestCase):

  def setUp(self):
    super(BqSearchIndexBenchmarkTest, self).setUp()
    self.mock_bigquery = mock.MagicMock()
    bq_search_index_benchmark.bigquery = self.mock_bigquery

  def testGetConfig(self):
    config = bq_search_index_benchmark.GetConfig({'flags': {}})
    self.assertIn('bq_search_index', config)

  def testRun(self):
    # TODO(user): Add a more complete test here.
    self.assertEqual([], bq_search_index_benchmark.Run(mock.MagicMock()))


if __name__ == '__main__':
  unittest.main()
