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
    instance = sample.Sample(
        metric='Test', value=1.0, unit='Mbps', metadata=metadata
    )
    self.assertDictEqual(metadata, instance.metadata)

  def testProvidedMetadataUnchanged(self):
    metadata = {'origin': 'unit test'}
    instance = sample.Sample(
        metric='Test', value=1.0, unit='Mbps', metadata=metadata
    )
    metadata['new_key'] = 'new_value'
    self.assertNotEqual(metadata, instance.metadata)

  def testNoneValueShouldBeZero(self):
    instance = sample.Sample(metric='Test', value=None, unit='Mbps')
    self.assertIsInstance(instance.value, float)
    self.assertEqual(0.0, instance.value)

  def testValuesShouldBeFloats(self):
    instance = sample.Sample(metric='Test', value=1, unit='Mbps')
    self.assertIsInstance(instance.value, float)
    self.assertEqual(1.0, instance.value)

    instance = sample.Sample(metric='Test', value=1.0, unit='Mbps')
    self.assertIsInstance(instance.value, float)
    self.assertEqual(1.0, instance.value)

    instance = sample.Sample(metric='Test', value='1', unit='Mbps')
    self.assertIsInstance(instance.value, float)
    self.assertEqual(1.0, instance.value)

    instance = sample.Sample(metric='Test', value='1.0', unit='Mbps')
    self.assertIsInstance(instance.value, float)
    self.assertEqual(1.0, instance.value)


class TestPercentileCalculator(unittest.TestCase):

  def testPercentileCalculator(self):
    numbers = list(range(0, 1001))
    percentiles = sample.PercentileCalculator(
        numbers, percentiles=[0, 1, 99.9, 100]
    )

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
      sample.PercentileCalculator([3], percentiles=['a'])


class TestSampleGroupCollector(unittest.TestCase):

  def _MakeRunSamples(self, tps, latency, group_key):
    metadata = {'group': group_key}
    return [
        sample.Sample('tps', tps, 'tps', dict(metadata)),
        sample.Sample('latency', latency, 'ms', dict(metadata)),
    ]

  def testAddGroupsSamples(self):
    collector = sample.SampleGroupCollector()
    run_1 = self._MakeRunSamples(tps=100.0, latency=5.0, group_key='a')
    run_2 = self._MakeRunSamples(tps=200.0, latency=2.5, group_key='b')
    collector.Add('a', run_1)
    collector.Add('b', run_2)

    self.assertEqual(collector.all_samples, run_1 + run_2)
    grouped = collector.SamplesByGroup()
    self.assertEqual(set(grouped.keys()), {'a', 'b'})
    self.assertEqual(grouped['a'], run_1)
    self.assertEqual(grouped['b'], run_2)

  def testMaxAggregation(self):
    collector = sample.SampleGroupCollector()
    collector.Add(
        'a', self._MakeRunSamples(tps=100.0, latency=5.0, group_key='a')
    )
    collector.Add(
        'b', self._MakeRunSamples(tps=300.0, latency=2.0, group_key='b')
    )
    collector.Add(
        'c', self._MakeRunSamples(tps=200.0, latency=3.0, group_key='c')
    )

    best_samples = collector.GetBestSamples(
        'tps', 'max_tps', sample.Aggregation.MAX
    )
    metrics = sorted(s.metric for s in best_samples)
    self.assertEqual(metrics, ['max_tps_latency', 'max_tps_tps'])

    tps_sample = [s for s in best_samples if s.metric == 'max_tps_tps'][0]
    self.assertEqual(tps_sample.value, 300.0)
    self.assertEqual(tps_sample.metadata['group'], 'b')
    self.assertNotIn('max_tps', tps_sample.metadata)

  def testMinAggregation(self):
    collector = sample.SampleGroupCollector()
    collector.Add(
        'a', self._MakeRunSamples(tps=100.0, latency=5.0, group_key='a')
    )
    collector.Add(
        'b', self._MakeRunSamples(tps=300.0, latency=2.0, group_key='b')
    )
    collector.Add(
        'c', self._MakeRunSamples(tps=200.0, latency=3.0, group_key='c')
    )

    best_samples = collector.GetBestSamples(
        'latency', 'min_latency', sample.Aggregation.MIN
    )
    lat_sample = [s for s in best_samples if s.metric == 'min_latency_latency'][
        0
    ]
    self.assertEqual(lat_sample.value, 2.0)
    self.assertEqual(lat_sample.metadata['group'], 'b')
    self.assertNotIn('min_latency', lat_sample.metadata)

  def testAnnotatesWinningGroup(self):
    collector = sample.SampleGroupCollector()
    losing_run = self._MakeRunSamples(tps=100.0, latency=5.0, group_key='a')
    winning_run = self._MakeRunSamples(tps=300.0, latency=2.0, group_key='b')
    collector.Add('a', losing_run)
    collector.Add('b', winning_run)

    collector.GetBestSamples('tps', 'max_tps', sample.Aggregation.MAX)
    for s in winning_run:
      self.assertTrue(s.metadata.get('max_tps'))
    for s in losing_run:
      self.assertNotIn('max_tps', s.metadata)

  def testNoMetricFoundRaises(self):
    collector = sample.SampleGroupCollector()
    collector.Add(
        'a', self._MakeRunSamples(tps=100.0, latency=5.0, group_key='a')
    )
    with self.assertRaisesRegex(
        ValueError, 'No sample group produced the metric "nonexistent"'
    ):
      collector.GetBestSamples('nonexistent', 'max_tps', sample.Aggregation.MAX)

  def testEmptyCollectorRaises(self):
    collector = sample.SampleGroupCollector()
    with self.assertRaisesRegex(
        ValueError, 'No sample group produced the metric "tps"'
    ):
      collector.GetBestSamples('tps', 'max_tps', sample.Aggregation.MAX)


if __name__ == '__main__':
  unittest.main()
