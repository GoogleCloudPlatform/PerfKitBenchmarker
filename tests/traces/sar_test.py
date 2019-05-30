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
"""Tests for sar trace utility."""
import os
import unittest


from perfkitbenchmarker.traces import sar


class SarTestCase(unittest.TestCase):

  def setUp(self):
    super(SarTestCase, self).setUp()
    path = os.path.join(
        os.path.dirname(__file__), '../data', 'sar_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseSarResult(self):
    metadata = {
        'event': 'sar',
        'sender': 'run',
        'sar_interval': 5,
    }

    samples = []
    sar._AddStealResults(metadata, self.contents, samples)

    # Test metadata
    metadata = samples[0].metadata

    expected_steal_values = [
        0.150000, 0.220000, 0.300000, 0.190000, 0.370000, 0.300000, 0.250000,
        0.350000, 0.210000, 0.170000, 17.990000
    ]

    for i in range(0, 11):
      sample = samples[i]
      self.assertEqual(expected_steal_values[i], sample.value)
      self.assertEqual('sar', sample.metadata['event'])
      self.assertEqual('run', sample.metadata['sender'])
    self.assertEqual(len(samples), 11)

    last_sample = samples[-1]
    self.assertEqual('average_steal', last_sample.metric)

if __name__ == '__main__':
  unittest.main()
