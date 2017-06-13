# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for HPCG benchmark."""
import os
import unittest

import mock

from perfkitbenchmarker.linux_benchmarks import hpcg_benchmark


class HpcgBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(hpcg_benchmark.__name__ + '.FLAGS')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__), '../data',
                        'hpcg_results.txt')
    with open(path) as fp:
      self.test_output = fp.read()

    path = os.path.join(os.path.dirname(__file__), '../data',
                        'hpcg_results2.txt')
    with open(path) as fp:
      self.test_output2 = fp.read()

  def testExtractThroughput(self):
    throughput = hpcg_benchmark._ExtractThroughput(self.test_output)
    self.assertEqual(202.6, throughput)

  def testExtractThroughput2(self):
    throughput = hpcg_benchmark._ExtractThroughput(self.test_output2)
    self.assertEqual(62.3, throughput)

  def testExtractProblemSize(self):
    self.assertEqual([64, 128, 256],
                     hpcg_benchmark._ExtractProblemSize(self.test_output))


if __name__ == '__main__':
  unittest.main()
