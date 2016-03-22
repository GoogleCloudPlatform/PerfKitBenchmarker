# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.analyze"""

import unittest

import mock
import pandas as pd

from perfkitbenchmarker import analyze
from perfkitbenchmarker import sample

# All streams were active from time 4.0 to time 8.0.
SAMPLE_TABLE = pd.DataFrame([
    {'start_time': 0.0, 'duration': 2.0,   # completely before
     'stream_num': 0, 'size': 1},
    {'start_time': 3.0, 'duration': 2.0,   # overlaps left
     'stream_num': 0, 'size': 4},
    {'start_time': 6.0, 'duration': 1.0,   # completely within
     'stream_num': 0, 'size': 2},
    {'start_time': 7.0, 'duration': 4.0,   # overlaps right
     'stream_num': 0, 'size': 8},
    {'start_time': 12.0, 'duration': 2.0,  # completely after
     'stream_num': 0, 'size': 2},
    {'start_time': 4.0, 'duration': 4.0,   # determines interval
     'stream_num': 1, 'size': 4}])


class TestAllStreamsInterval(unittest.TestCase):
  def testAllStreamsInterval(self):
    start_time, duration = analyze.AllStreamsInterval(
        SAMPLE_TABLE['start_time'],
        SAMPLE_TABLE['duration'],
        SAMPLE_TABLE['stream_num'])
    self.assertEquals(start_time, 4.0)
    self.assertEquals(duration, 4.0)


class TestFullyInInterval(unittest.TestCase):
  def testFullyInInterval(self):
    overlaps = analyze.FullyInInterval(
        SAMPLE_TABLE['start_time'],
        SAMPLE_TABLE['duration'],
        4.0, 4.0)

    self.assertTrue(
        (overlaps == pd.Series([False, False, True, False, False, True])).all())


class TestAllStreamsNetThroughput(unittest.TestCase):
  def testAllStreamsNetThroughput(self):
    self.assertAlmostEqual(
        analyze.AllStreamsNetThroughput(
            SAMPLE_TABLE['duration'],
            SAMPLE_TABLE['size'],
            SAMPLE_TABLE['stream_num']),
        17.0 / 11.0 + 4.0 / 4.0)


class TestSummaryStats(unittest.TestCase):
  def testSummaryStats(self):
    series = pd.Series(range(0, 1001))
    stats = analyze.SummaryStats(series, name_prefix='foo ')

    self.assertEqual(stats['foo p0'], 0)
    self.assertEqual(stats['foo p1'], 10)
    self.assertEqual(stats['foo p99.9'], 999)
    self.assertEqual(stats['foo p100'], 1000)
    self.assertEqual(stats['foo mean'], 500)


class TestAppendStatsAsSamples(unittest.TestCase):
  def testAppendStatsAsSamples(self):
    with mock.patch(analyze.__name__ + '.SummaryStats',
                    return_value=pd.Series({'a': 1, 'b': 2, 'c': 3})):
      samples_list = []
      analyze.AppendStatsAsSamples(
          [], 'unit', samples_list,
          timestamps=pd.Series({'a': 11, 'b': 12, 'c': 13}))

      self.assertEqual(
          samples_list[0],
          sample.Sample('a', 1, 'unit', timestamp=11))

      self.assertEqual(
          samples_list[1],
          sample.Sample('b', 2, 'unit', timestamp=12))

      self.assertEqual(
          samples_list[2],
          sample.Sample('c', 3, 'unit', timestamp=13))


if __name__ == '__main__':
  unittest.main()
