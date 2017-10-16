# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for mnist_benchmark."""
import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark


class MnistBenchmarkTestCase(unittest.TestCase,
                             test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'mnist_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseSysbenchResult(self):
    result = mnist_benchmark.ExtractThroughput(self.contents)
    self.assertEqual(result, 37.786650)


if __name__ == '__main__':
  unittest.main()
