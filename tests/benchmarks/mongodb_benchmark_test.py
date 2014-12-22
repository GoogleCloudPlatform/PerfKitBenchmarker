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
"""Tests for mongodb_benchmark."""

import os
import unittest

from perfkitbenchmarker.benchmarks import mongodb_benchmark


class MongoDbBenchmarkTestCase(unittest.TestCase):

  maxDiff = None

  def setUp(self):
    path = os.path.join('tests/data',
                        'mongodb-sample-result.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def tearDown(self):
    pass

  def testParseResult(self):
    result = mongodb_benchmark.ParseResult(self.contents)
    expected_result = [
        ['RunTime', 723.0, 'ms', {'stage': 'OVERALL'}],
        ['Throughput', 1383.1258644536654, 'ops/sec', {'stage': 'OVERALL'}],
        ['AverageLatency', 5596.689516129032, 'us', {'stage': 'UPDATE'}],
        ['MinLatency', 2028.0, 'us', {'stage': 'UPDATE'}],
        ['MaxLatency', 46240.0, 'us', {'stage': 'UPDATE'}],
        ['95thPercentileLatency', 10.0, 'ms', {'stage': 'UPDATE'}],
        ['99thPercentileLatency', 43.0, 'ms', {'stage': 'UPDATE'}],
        ['AverageLatency', 4658.033730158731, 'us', {'stage': 'READ'}],
        ['MinLatency', 1605.0, 'us', {'stage': 'READ'}],
        ['MaxLatency', 43447.0, 'us', {'stage': 'READ'}],
        ['95thPercentileLatency', 10.0, 'ms', {'stage': 'READ'}],
        ['99thPercentileLatency', 12.0, 'ms', {'stage': 'READ'}],
        ['AverageLatency', 372.8, 'us', {'stage': 'CLEANUP'}],
        ['MinLatency', 0.0, 'us', {'stage': 'CLEANUP'}],
        ['MaxLatency', 3720.0, 'us', {'stage': 'CLEANUP'}],
        ['95thPercentileLatency', 3.0, 'ms', {'stage': 'CLEANUP'}],
        ['99thPercentileLatency', 3.0, 'ms', {'stage': 'CLEANUP'}],
        ['Operations', 496.0, '', {'stage': 'UPDATE'}],
        ['Operations', 504.0, '', {'stage': 'READ'}],
        ['Operations', 10.0, '', {'stage': 'CLEANUP'}]]
    self.assertEqual(result, expected_result)


if __name__ == '__main__':
  unittest.main()
