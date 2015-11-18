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
"""Tests for perfkitbenchmarker.packages.wrk."""

import os
import unittest


from perfkitbenchmarker.linux_packages import wrk


class WrkParseOutputTestCase(unittest.TestCase):

  def setUp(self):
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    result_path = os.path.join(data_dir, 'wrk_result.txt')
    with open(result_path) as result_file:
      self.wrk_results = result_file.read()

  def testParsesSample(self):
    expected = [('p5 latency', 0.162, 'ms'),
                ('p50 latency', 0.187, 'ms'),
                ('p90 latency', 0.256, 'ms'),
                ('p99 latency', 0.519, 'ms'),
                ('p99.9 latency', 5.196, 'ms'),
                ('bytes transferred', 150068000.0, 'bytes'),
                ('errors', 0.0, 'n'),
                ('requests', 577297.0, 'n'),
                ('throughput', 9605.69, 'requests/sec')]

    actual = list(wrk._ParseOutput(self.wrk_results))
    self.assertItemsEqual(expected, actual)

  def testFailsForEmptyString(self):
    with self.assertRaisesRegexp(ValueError, 'bar'):
      list(wrk._ParseOutput('bar'))


if __name__ == '__main__':
  unittest.main()
