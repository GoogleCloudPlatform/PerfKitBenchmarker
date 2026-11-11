# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for Nvidia power trace utility."""
import csv
import io
import os
import unittest

from perfkitbenchmarker.traces import nvidia_power


class NvidiaPowerTestCase(unittest.TestCase):

  def setUp(self):
    super().setUp()
    path = os.path.join(
        os.path.dirname(__file__), '../data', 'nvidia_power_output.txt'
    )
    with open(path) as fp:
      self.contents = fp.read()
    memory_path = os.path.join(
        os.path.dirname(__file__), '../data', 'nvidia_memory_output.txt'
    )
    with open(memory_path) as fp:
      self.memory_contents = fp.read()

  def testParseNvidiaPowerResult(self):
    metadata = {
        'event': 'nvidia_power',
        'sender': 'run',
        'nvidia_power_interval': '1',
        'role': 'default_0',
    }

    samples = []
    nvidia_power._NvidiaPowerResults(
        metadata,
        csv.DictReader(io.StringIO(self.contents)),
        samples,
        ['index', 'timestamp', 'power.draw'],
    )

    expected_values = [
        16.48,
        27.32,
        27.31,
        27.32,
        27.31,
        27.32,
        27.40,
        49.98,
        39.53,
        71.24,
        71.83,
        70.41,
        69.43,
        70.85,
        69.38,
        71.24,
        68.50,
        71.14,
        72.36,
        70.75,
        68.89,
        69.92,
        69.73,
        70.70,
        71.29,
        68.15,
        69.23,
        70.02,
        70.22,
        71.19,
        67.23,
        69.09,
        68.21,
        70.46,
        70.06,
        65.90,
        71.19,
        70.61,
        71.58,
        69.73,
        67.90,
        70.94,
        66.25,
        66.98,
        70.51,
        69.18,
        69.28,
        71.39,
        69.57,
        64.91,
        66.88,
        70.61,
        70.02,
        71.19,
        69.87,
        69.14,
        67.86,
        70.80,
        70.02,
        70.90,
        68.40,
        71.88,
        65.37,
        71.10,
        66.19,
        71.39,
        71.29,
        71.49,
        66.84,
        70.61,
        67.37,
        67.56,
        70.61,
        69.38,
        69.34,
        69.18,
        70.26,
        69.72,
        71.88,
        71.63,
        73.24,
        68.00,
        68.78,
        70.26,
        69.28,
        70.40,
        65.69,
        69.42,
        67.70,
        68.74,
        70.01,
    ]

    for i in range(0, 91):
      sample = samples[i]
      self.assertEqual('power', sample.metric)
      self.assertAlmostEqual(expected_values[i], sample.value)
      self.assertEqual('nvidia_power', sample.metadata['event'])
      self.assertEqual('run', sample.metadata['sender'])
      self.assertEqual('1', sample.metadata['nvidia_power_interval'])
      self.assertEqual('default_0', sample.metadata['role'])
    self.assertEqual(len(samples), 91)

  def testParseNvidiaMemoryResult(self):
    metadata = {
        'event': 'nvidia_power',
        'sender': 'run',
        'nvidia_power_interval': '1',
        'role': 'default_0',
    }

    samples = []
    nvidia_power._NvidiaPowerResults(
        metadata,
        csv.DictReader(io.StringIO(self.memory_contents)),
        samples,
        ['index', 'timestamp', 'memory.used', 'memory.total'],
    )

    expected_used = [1024.0, 4096.0, 12288.0, 40960.0, 40960.0, 9216.0]

    used_samples = [s for s in samples if s.metric == 'memory_used']
    total_samples = [s for s in samples if s.metric == 'memory_total']

    self.assertEqual(len(used_samples), len(expected_used))
    self.assertEqual(len(total_samples), len(expected_used))

    for sample_, expected_value in zip(used_samples, expected_used):
      self.assertAlmostEqual(expected_value, sample_.value)
      self.assertEqual('MiB', sample_.unit)
      self.assertEqual('nvidia_power', sample_.metadata['event'])

    for sample_ in total_samples:
      self.assertAlmostEqual(81559.0, sample_.value)
      self.assertEqual('MiB', sample_.unit)


if __name__ == '__main__':
  unittest.main()
