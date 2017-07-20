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

"""Tests for HPCC benchmark."""
import os
import unittest

import mock

from perfkitbenchmarker.linux_benchmarks import hpcc_benchmark


class HPCCTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(hpcc_benchmark.__name__ + '.FLAGS')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__), '../data', 'hpcc-sample.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseHpcc(self):
    benchmark_spec = mock.MagicMock()
    result = hpcc_benchmark.ParseOutput(self.contents, benchmark_spec)
    self.assertEqual(150, len(result))
    results = {i[0]: i[1] for i in result}
    self.assertAlmostEqual(0.0331844, results['HPL Throughput'])
    self.assertAlmostEqual(0.0151415, results['Random Access Throughput'])
    self.assertAlmostEqual(11.5032, results['STREAM Copy Throughput'])
    self.assertAlmostEqual(11.6338, results['STREAM Scale Throughput'])
    self.assertAlmostEqual(12.7265, results['STREAM Add Throughput'])
    self.assertAlmostEqual(12.2433, results['STREAM Triad Throughput'])
    self.assertAlmostEqual(0.338561, results['PTRANS Throughput'])

    # Spot check a few of the metrics without units.
    self.assertAlmostEqual(4, results['sizeof_float'])
    self.assertAlmostEqual(5.80539, results['StarSTREAM_Add'])


if __name__ == '__main__':
  unittest.main()
