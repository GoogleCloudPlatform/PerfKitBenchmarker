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
"""Tests for unixbench_benchmark."""

import os
import unittest

import mock

from perfkitbenchmarker.benchmarks import unixbench_benchmark


class UnixBenchBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    path = os.path.join('tests/data',
                        'unix-bench-sample-result.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def tearDown(self):
    pass

  def testParseUnixBench(self):
    result = unixbench_benchmark.UnixBenchParser(self.contents)
    expected_result = [
        ['Double-Precision Whetstone', 4022.0, 'MWIPS',
         {'samples': '7', 'time': '9.9s'}],
        ['Execl Throughput', 4735.8, 'lps',
         {'samples': '2', 'time': '29.8s'}],
        ['File Copy 1024 bufsize 2000 maxblocks', 1294367.0, 'KBps',
         {'samples': '2', 'time': '30.0s'}],
        ['File Copy 256 bufsize 500 maxblocks', 396912.9, 'KBps',
         {'samples': '2', 'time': '30.0s'}],
        ['File Copy 4096 bufsize 8000 maxblocks', 2513158.7, 'KBps',
         {'samples': '2', 'time': '30.0s'}],
        ['Pipe Throughput', 2221775.6, 'lps',
         {'samples': '7', 'time': '10.0s'}],
        ['Pipe-based Context Switching', 369000.7, 'lps',
         {'samples': '7', 'time': '10.0s'}],
        ['Process Creation', 12587.7, 'lps',
         {'samples': '2', 'time': '30.0s'}],
        ['Shell Scripts (1 concurrent)', 8234.3, 'lpm',
         {'samples': '2', 'time': '60.0s'}],
        ['Shell Scripts (8 concurrent)', 1064.5, 'lpm',
         {'samples': '2', 'time': '60.0s'}],
        ['System Call Overhead', 4439274.5, 'lps',
         {'samples': '7', 'time': '10.0s'}],
        ['Dhrystone 2 using register variables:score', 34872897.7, '',
         {'index': 2988.3, 'baseline': 116700.0}],
        ['Double-Precision Whetstone:score', 4022.0, '',
         {'index': 731.3, 'baseline': 55.0}],
        ['Execl Throughput:score', 4735.8, '',
         {'index': 1101.4, 'baseline': 43.0}],
        ['File Copy 1024 bufsize 2000 maxblocks:score', 1294367.0, '',
         {'index': 3268.6, 'baseline': 3960.0}],
        ['File Copy 256 bufsize 500 maxblocks:score', 396912.9, '',
         {'index': 2398.3, 'baseline': 1655.0}],
        ['File Copy 4096 bufsize 8000 maxblocks:score', 2513158.7, '',
         {'index': 4333.0, 'baseline': 5800.0}],
        ['Pipe Throughput:score', 2221775.6, '',
         {'index': 1786.0, 'baseline': 12440.0}],
        ['Pipe-based Context Switching:score', 369000.7, '',
         {'index': 922.5, 'baseline': 4000.0}],
        ['Process Creation:score', 12587.7, '',
         {'index': 999.0, 'baseline': 126.0}],
        ['Shell Scripts (1 concurrent):score', 8234.3, '',
         {'index': 1942.1, 'baseline': 42.4}],
        ['Shell Scripts (8 concurrent):score', 1064.5, '',
         {'index': 1774.2, 'baseline': 6.0}],
        ['System Call Overhead:score', 4439274.5, '',
         {'index': 2959.5, 'baseline': 15000.0}],
        ['System Benchmarks Index Score', 1825.8, '', {}]]
    self.assertEqual(result, expected_result)


if __name__ == '__main__':
  unittest.main()
