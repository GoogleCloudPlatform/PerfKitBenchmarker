# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for stress-ng benchmark."""
import os
import unittest

import mock

from perfkitbenchmarker.linux_benchmarks import stress_ng_benchmark


class StressngTestCase(unittest.TestCase):

  def setUp(self):
    super(StressngTestCase, self).setUp()
    p = mock.patch(stress_ng_benchmark.__name__)
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(
        os.path.dirname(__file__), '../data', 'stress_ng_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseStressngResult(self):
    metadata = {
        'duration_sec': 10,
        'threads': 16
    }

    samples = stress_ng_benchmark._ParseStressngResult(
        metadata, self.contents)

    # Test metadata
    metadata = samples[0].metadata

    # Test metric and value
    expected = {
        'af-alg': 1065334.160000,
        'bsearch': 1828.650000,
        'cache': 5.600000,
        'context': 13817.930000,
        'cpu': 1255.950000,
        'crypt': 504.780000,
        'getrandom': 11916.240000,
        'heapsort': 19.590000,
        'hsearch': 15985.010000,
        'icache': 2445723.620000,
        'lockbus': 11702022.020000,
        'longjmp': 563937.940000,
        'lsearch': 42.580000,
        'malloc': 5882553.050000,
        'matrix': 14385.370000,
        'membarrier': 127.020000,
        'memcpy': 7338.270000,
        'memfd': 713.050000,
        'mergesort': 827.450000,
        'mincore': 134112.480000,
        'null': 17278290.670000,
        'pipe': 1018656.690000,
        'qsort': 45.190000,
        'rdrand': 1454533.980000,
        'remap-file-pages': 881.440000,
        'str': 165460.410000,
        'stream': 196.600000,
        'tsc': 4365945.050000,
        'tsearch': 70.770000,
        'vecmath': 100118.030000,
        'vm': 455508.400000,
        'vm-rw': 880.190000,
        'wcs': 95852.410000,
        'zero': 16653247.430000,
        'zlib': 168.930000
    }

    for sample in samples:
      self.assertEqual(expected[sample.metric], sample.value)
      self.assertEqual(10, sample.metadata['duration_sec'])
      self.assertEqual(16, sample.metadata['threads'])
    self.assertEqual(len(samples), 35)

if __name__ == '__main__':
  unittest.main()
