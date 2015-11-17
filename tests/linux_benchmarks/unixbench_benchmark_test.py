# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import unixbench_benchmark


class UnixBenchBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  maxDiff = None

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'unix-bench-sample-result.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def tearDown(self):
    pass

  def testParseUnixBench(self):
    result = unixbench_benchmark.ParseResults(self.contents)
    expected_result = [
        ['Dhrystone 2 using register variables', 34872897.7, 'lps',
         {'num_parallel_copies': 1, 'samples': 7, 'time': '10.0s'}],
        ['Double-Precision Whetstone', 4022.0, 'MWIPS',
         {'num_parallel_copies': 1, 'samples': 7, 'time': '9.9s'}],
        ['Execl Throughput', 4735.8, 'lps',
         {'num_parallel_copies': 1, 'samples': 2, 'time': '29.8s'}],
        ['File Copy 1024 bufsize 2000 maxblocks', 1294367.0, 'KBps',
         {'num_parallel_copies': 1, 'samples': 2, 'time': '30.0s'}],
        ['File Copy 256 bufsize 500 maxblocks', 396912.9, 'KBps',
         {'num_parallel_copies': 1, 'samples': 2, 'time': '30.0s'}],
        ['File Copy 4096 bufsize 8000 maxblocks', 2513158.7, 'KBps',
         {'num_parallel_copies': 1, 'samples': 2, 'time': '30.0s'}],
        ['Pipe Throughput', 2221775.6, 'lps',
         {'num_parallel_copies': 1, 'samples': 7, 'time': '10.0s'}],
        ['Pipe-based Context Switching', 369000.7, 'lps',
         {'num_parallel_copies': 1, 'samples': 7, 'time': '10.0s'}],
        ['Process Creation', 12587.7, 'lps',
         {'num_parallel_copies': 1, 'samples': 2, 'time': '30.0s'}],
        ['Shell Scripts (1 concurrent)', 8234.3, 'lpm',
         {'num_parallel_copies': 1, 'samples': 2, 'time': '60.0s'}],
        ['Shell Scripts (8 concurrent)', 1064.5, 'lpm',
         {'num_parallel_copies': 1, 'samples': 2, 'time': '60.0s'}],
        ['System Call Overhead', 4439274.5, 'lps',
         {'num_parallel_copies': 1, 'samples': 7, 'time': '10.0s'}],
        ['Dhrystone 2 using register variables:score', 34872897.7, '',
         {'index': 2988.3, 'baseline': 116700.0, 'num_parallel_copies': 1}],
        ['Double-Precision Whetstone:score', 4022.0, '',
         {'index': 731.3, 'baseline': 55.0, 'num_parallel_copies': 1}],
        ['Execl Throughput:score', 4735.8, '',
         {'index': 1101.4, 'baseline': 43.0, 'num_parallel_copies': 1}],
        ['File Copy 1024 bufsize 2000 maxblocks:score', 1294367.0, '',
         {'index': 3268.6, 'baseline': 3960.0, 'num_parallel_copies': 1}],
        ['File Copy 256 bufsize 500 maxblocks:score', 396912.9, '',
         {'index': 2398.3, 'baseline': 1655.0, 'num_parallel_copies': 1}],
        ['File Copy 4096 bufsize 8000 maxblocks:score', 2513158.7, '',
         {'index': 4333.0, 'baseline': 5800.0, 'num_parallel_copies': 1}],
        ['Pipe Throughput:score', 2221775.6, '',
         {'index': 1786.0, 'baseline': 12440.0, 'num_parallel_copies': 1}],
        ['Pipe-based Context Switching:score', 369000.7, '',
         {'index': 922.5, 'baseline': 4000.0, 'num_parallel_copies': 1}],
        ['Process Creation:score', 12587.7, '',
         {'index': 999.0, 'baseline': 126.0, 'num_parallel_copies': 1}],
        ['Shell Scripts (1 concurrent):score', 8234.3, '',
         {'index': 1942.1, 'baseline': 42.4, 'num_parallel_copies': 1}],
        ['Shell Scripts (8 concurrent):score', 1064.5, '',
         {'index': 1774.2, 'baseline': 6.0, 'num_parallel_copies': 1}],
        ['System Call Overhead:score', 4439274.5, '',
         {'index': 2959.5, 'baseline': 15000.0, 'num_parallel_copies': 1}],
        ['System Benchmarks Index Score', 1825.8, '',
         {'num_parallel_copies': 1}],
        ['Dhrystone 2 using register variables', 155391896.7, 'lps',
         {'num_parallel_copies': 8, 'samples': 7, 'time': '10.0s'}],
        ['Double-Precision Whetstone', 28632.5, 'MWIPS',
         {'num_parallel_copies': 8, 'samples': 7, 'time': '9.8s'}],
        ['Execl Throughput', 15184.0, 'lps',
         {'num_parallel_copies': 8, 'samples': 2, 'time': '30.0s'}],
        ['File Copy 1024 bufsize 2000 maxblocks', 985484.8, 'KBps',
         {'num_parallel_copies': 8, 'samples': 2, 'time': '30.0s'}],
        ['File Copy 256 bufsize 500 maxblocks', 269732.2, 'KBps',
         {'num_parallel_copies': 8, 'samples': 2, 'time': '30.0s'}],
        ['File Copy 4096 bufsize 8000 maxblocks', 2706156.4, 'KBps',
         {'num_parallel_copies': 8, 'samples': 2, 'time': '30.0s'}],
        ['Pipe Throughput', 8525928.8, 'lps',
         {'num_parallel_copies': 8, 'samples': 7, 'time': '10.0s'}],
        ['Pipe-based Context Switching', 1017270.4, 'lps',
         {'num_parallel_copies': 8, 'samples': 7, 'time': '10.0s'}],
        ['Process Creation', 31563.7, 'lps',
         {'num_parallel_copies': 8, 'samples': 2, 'time': '30.0s'}],
        ['Shell Scripts (1 concurrent)', 32516.3, 'lpm',
         {'num_parallel_copies': 8, 'samples': 2, 'time': '60.0s'}],
        ['Shell Scripts (8 concurrent)', 5012.2, 'lpm',
         {'num_parallel_copies': 8, 'samples': 2, 'time': '60.0s'}],
        ['System Call Overhead', 10288762.3, 'lps',
         {'num_parallel_copies': 8, 'samples': 7, 'time': '10.0s'}],
        ['Dhrystone 2 using register variables:score', 155391896.7, '',
         {'index': 13315.5, 'baseline': 116700.0, 'num_parallel_copies': 8}],
        ['Double-Precision Whetstone:score', 28632.5, '',
         {'index': 5205.9, 'baseline': 55.0, 'num_parallel_copies': 8}],
        ['Execl Throughput:score', 15184.0, '',
         {'index': 3531.2, 'baseline': 43.0, 'num_parallel_copies': 8}],
        ['File Copy 1024 bufsize 2000 maxblocks:score', 985484.8, '',
         {'index': 2488.6, 'baseline': 3960.0, 'num_parallel_copies': 8}],
        ['File Copy 256 bufsize 500 maxblocks:score', 269732.2, '',
         {'index': 1629.8, 'baseline': 1655.0, 'num_parallel_copies': 8}],
        ['File Copy 4096 bufsize 8000 maxblocks:score', 2706156.4, '',
         {'index': 4665.8, 'baseline': 5800.0, 'num_parallel_copies': 8}],
        ['Pipe Throughput:score', 8525928.8, '',
         {'index': 6853.6, 'baseline': 12440.0, 'num_parallel_copies': 8}],
        ['Pipe-based Context Switching:score', 1017270.4, '',
         {'index': 2543.2, 'baseline': 4000.0, 'num_parallel_copies': 8}],
        ['Process Creation:score', 31563.7, '',
         {'index': 2505.1, 'baseline': 126.0, 'num_parallel_copies': 8}],
        ['Shell Scripts (1 concurrent):score', 32516.3, '',
         {'index': 7668.9, 'baseline': 42.4, 'num_parallel_copies': 8}],
        ['Shell Scripts (8 concurrent):score', 5012.2, '',
         {'index': 8353.6, 'baseline': 6.0, 'num_parallel_copies': 8}],
        ['System Call Overhead:score', 10288762.3, '',
         {'index': 6859.2, 'baseline': 15000.0, 'num_parallel_copies': 8}],
        ['System Benchmarks Index Score', 4596.2, '',
         {'num_parallel_copies': 8}]]
    expected_result = [sample.Sample(*exp) for exp in expected_result]
    self.assertSampleListsEqualUpToTimestamp(result, expected_result)


if __name__ == '__main__':
  unittest.main()
