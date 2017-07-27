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
"""Tests for perfkitbenchmarker.scripts.database_scripts.plot_sysbench_results.
"""
import unittest
import os
from perfkitbenchmarker.scripts.database_scripts import plot_sysbench_results

TEST_FILE_1 = '../tests/data/sysbench_stderr_output_sample.txt'
TEST_FILE_1_RUN_SECONDS = 480
TEST_FILE_1_REPORT_INTERVAL = 2
TEST_FILE_1_FIRST_TPS_VALUE = 144.13
TEST_RUN_URI = 'abcdefgh'


class PlotterTestCase(unittest.TestCase):

  def setUp(self):
    self.plotter = plot_sysbench_results.Plotter(
        TEST_FILE_1_RUN_SECONDS, TEST_FILE_1_REPORT_INTERVAL, TEST_RUN_URI)

  def testadd_file(self):
    self.assertRaises(plot_sysbench_results.STDERRFileDoesNotExistError,
                      self.plotter.add_file, '')

  def testparse_file(self):
    # TODO(samspano): Implement test that will raise PatternNotFoundError.
    path1 = os.path.join(
        os.path.dirname(__file__), TEST_FILE_1)
    with open(path1) as file1:
      results = self.plotter._parse_file(file1)
      self.assertEqual(len(results), self.plotter.data_entries_per_file)
      self.assertEqual(results[0], TEST_FILE_1_FIRST_TPS_VALUE)


if __name__ == '__main__':
  unittest.main()
