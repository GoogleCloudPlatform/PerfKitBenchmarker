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

import unittest

from perfkitbenchmarker import sample


class SampleTestCase(unittest.TestCase):

  def testMetadataOptional(self):
    instance = sample.Sample(metric='Test', value=1.0, unit='Mbps')
    self.assertDictEqual({}, instance.metadata)

  def testProvidedMetadataSet(self):
    metadata = {'origin': 'unit test'}
    instance = sample.Sample(metric='Test', value=1.0, unit='Mbps',
                             metadata=metadata.copy())
    self.assertDictEqual(metadata, instance.metadata)


class TestPercentileCalculator(unittest.TestCase):
  def testPercentileCalculator(self):
    numbers = range(0, 1001)
    percentiles = sample.PercentileCalculator(numbers,
                                              percentiles=[0, 1, 99.9, 100])

    self.assertEqual(percentiles['p0'], 0)
    self.assertEqual(percentiles['p1'], 10)
    self.assertEqual(percentiles['p99.9'], 999)
    self.assertEqual(percentiles['p100'], 1000)
    self.assertEqual(percentiles['average'], 500)

    # 4 percentiles we requested, plus average and stddev
    self.assertEqual(len(percentiles), 6)

  def testNoNumbers(self):
    with self.assertRaises(ValueError):
      sample.PercentileCalculator([], percentiles=[0, 1, 99])

  def testOutOfRangePercentile(self):
    with self.assertRaises(ValueError):
      sample.PercentileCalculator([3], percentiles=[-1])

  def testWrongTypePercentile(self):
    with self.assertRaises(ValueError):
      sample.PercentileCalculator([3], percentiles=["a"])
