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

    samples = []
    samples.append(stress_ng_benchmark._ParseStressngResult(
        metadata, self.contents))

    # Test metadata
    metadata = samples[0].metadata

    # Test metric and value
    expected = {
        'context': 4485.820000
    }

    for sample in samples:
      self.assertEqual(expected[sample.metric], sample.value)
      self.assertEqual(10, sample.metadata['duration_sec'])
      self.assertEqual(16, sample.metadata['threads'])
    self.assertEqual(len(samples), 1)

  def testGeoMean(self):
    floats = [1.0, 3.0, 5.0]
    self.assertAlmostEqual(stress_ng_benchmark._GeoMeanOverflow(floats),
                           2.466212074)
if __name__ == '__main__':
  unittest.main()
