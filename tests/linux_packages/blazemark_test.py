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
"""Tests for perfkitbenchmarker.packages.blazemark."""

import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_packages import blazemark


class BlazemarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  maxDiff = None

  def setUp(self):
    self.data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')

  def testParseResult(self):
    result_path = os.path.join(self.data_dir, 'blazemark-output.txt')
    with open(result_path) as result_file:
      out = result_file.read()
      results = blazemark._ParseResult(out, 'test')
      self.assertEqual(14, len(results))  # 14 results
      self.assertEqual('test_C-like_Throughput', results[0].metric)
      self.assertEqual(1115.44, results[0].value)
      self.assertEqual('MFlop/s', results[0].unit)
      self.assertEqual({'N': 100}, results[0].metadata)
      self.assertEqual('test_Eigen_Throughput', results[-1].metric)
      self.assertEqual(209.899, results[-1].value)
      self.assertEqual('MFlop/s', results[-1].unit)
      self.assertEqual({'N': 10000000}, results[-1].metadata)

  def testParseExpResult(self):
    result_path = os.path.join(self.data_dir, 'blazemark-output2.txt')
    with open(result_path) as result_file:
      out = result_file.read()
      results = blazemark._ParseResult(out, 'test')
      self.assertEqual(10, len(results))  # 10 results
      self.assertEqual('test_Blaze_Throughput', results[0].metric)
      self.assertEqual(float('3.03424e-08'), results[0].value)
      self.assertEqual('Seconds', results[0].unit)
      self.assertEqual({'N': 3}, results[0].metadata)
      self.assertEqual('test_Blaze_Throughput', results[-1].metric)
      self.assertEqual(31.9121, results[-1].value)
      self.assertEqual('Seconds', results[-1].unit)
      self.assertEqual({'N': 2000}, results[-1].metadata)


if __name__ == '__main__':
  unittest.main()
