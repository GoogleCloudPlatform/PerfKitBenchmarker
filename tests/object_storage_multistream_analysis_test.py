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

import pandas as pd

from perfkitbenchmarker import object_storage_multistream_analysis as analysis
from perfkitbenchmarker import units

# The first stream starts at 0.0 and the last one ends at 14.0. All
# streams were active from time 4.0 to time 8.0.
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


class TestInterval(unittest.TestCase):
  def testStartAndDuration(self):
    interval = analysis.Interval(2, duration=5)
    self.assertEqual(interval.end, 7)

  def testStartAndEnd(self):
    interval = analysis.Interval(2, end=7)
    self.assertEqual(interval.duration, 5)

  def testThreeArgsGood(self):
    # Test that the constructor doesn't raise an exception
    analysis.Interval(2, duration=5, end=7)

  def testThreeArgsBad(self):
    with self.assertRaises(ValueError):
      analysis.Interval(2, duration=5, end=8)

  def testEndBeforeStart(self):
    with self.assertRaises(ValueError):
      analysis.Interval(2, end=1)


class TestGetStreamActiveIntervals(unittest.TestCase):
  def testGetStreamActiveIntervals(self):
    any_stream, all_streams = analysis.GetStreamActiveIntervals(
        SAMPLE_TABLE['start_time'],
        SAMPLE_TABLE['duration'],
        SAMPLE_TABLE['stream_num'])

    print "any_stream", any_stream, "all_streams", all_streams
    self.assertEqual(any_stream, analysis.Interval(0, end=14.0))
    self.assertEqual(all_streams, analysis.Interval(4.0, end=8.0))


class TestStreamStartAndEndGaps(unittest.TestCase):
  def testStreamStartAndEndGaps(self):
    start_gap, stop_gap = analysis.StreamStartAndEndGaps(
        SAMPLE_TABLE['start_time'], SAMPLE_TABLE['duration'],
        analysis.Interval(4.0, duration=4.0))

    self.assertEqual(start_gap, 4.0)
    self.assertEqual(stop_gap, 6.0)


class TestFullyInInterval(unittest.TestCase):
  def testFullyInInterval(self):
    overlaps = analysis.FullyInInterval(
        SAMPLE_TABLE['start_time'],
        SAMPLE_TABLE['duration'],
        analysis.Interval(4.0, duration=4.0))

    self.assertTrue(
        (overlaps == pd.Series([False, False, True, False, False, True])).all())


class TestThroughputStats(unittest.TestCase):

  def setUp(self):
    self.byte = units.byte
    self.sec = units.second
    self.percent = units.percent

  def doTest(self, data, num_streams, correct_answer):
    output = analysis.ThroughputStats(
        data['start_time'], data['duration'], data['size'], data['stream_num'],
        num_streams)
    print 'output', output

    for name, value in output.iteritems():
      if name not in correct_answer:
        raise KeyError('ThroughputStats produced key %s not in correct '
                       'output %s' % (name, str(correct_answer)))

      self.assertEqual(value, correct_answer[name])

  def testOneObject(self):
    # Base case: one object.
    one_op = pd.DataFrame({'start_time': [0],
                           'duration': [1],
                           'size': [2],
                           'stream_num': [0]})
    self.doTest(one_op, 1,
                {'net throughput': 2.0 * self.byte / self.sec,
                 'net throughput (with gap)': 2.0 * self.byte / self.sec})

  def testSecondObjectSameSpeed(self):
    # Adding a second object at same speed has no effect on any metric.
    no_gap = pd.DataFrame({'start_time': [0, 1],
                           'duration': [1, 1],
                           'size': [2, 2],
                           'stream_num': [0, 0]})
    self.doTest(no_gap, 1,
                {'net throughput': 2.0 * self.byte / self.sec,
                 'net throughput (with gap)': 2.0 * self.byte / self.sec})

  def testSecondObjectDifferentSpeed(self):
    # Adding a second object at a different speed yields a different throughput.
    different_speeds = pd.DataFrame({'start_time': [0, 1],
                                     'duration': [1, 3],  # 4 seconds total
                                     'size': [2, 8],      # 10 bytes total
                                     'stream_num': [0, 0]})
    self.doTest(different_speeds, 1,
                {'net throughput': 2.5 * self.byte / self.sec,
                 'net throughput (with gap)': 2.5 * self.byte / self.sec})

  def testGapBetweenObjects(self):
    # Adding a gap affects throughput with overheads, but not without.
    with_gap = pd.DataFrame({'start_time': [0, 3],
                             'duration': [1, 1],
                             'size': [2, 2],
                             'stream_num': [0, 0]})
    self.doTest(with_gap, 1,
                {'net throughput': 2.0 * self.byte / self.sec,
                 'net throughput (with gap)': 1.0 * self.byte / self.sec})

  def testSimultaneousObjects(self):
    # With two simultaneous objects, throughput adds.
    two_streams = pd.DataFrame({'start_time': [0, 0],
                                'duration': [1, 1],
                                'size': [2, 2],
                                'stream_num': [0, 1]})
    self.doTest(two_streams, 2,
                {'net throughput': 4.0 * self.byte / self.sec,
                 'net throughput (with gap)': 4.0 * self.byte / self.sec})

  def testTwoStreamGaps(self):
    # With two streams, overhead is compared to 2 * interval length.
    two_streams_with_gap = pd.DataFrame({'start_time': [0, 3, 0, 3],
                                         'duration': [1, 1, 1, 1],
                                         'size': [2, 2, 2, 2],
                                         'stream_num': [0, 0, 1, 1]})
    self.doTest(two_streams_with_gap, 2,
                {'net throughput': 4.0 * self.byte / self.sec,
                 'net throughput (with gap)': 2.0 * self.byte / self.sec})


class TestGapStats(unittest.TestCase):

  def setUp(self):
    self.sec = units.second
    self.percent = units.percent

  def doTest(self, data, num_streams, interval, correct_answer):
    output = analysis.GapStats(
        data['start_time'], data['duration'], data['stream_num'],
        interval, num_streams)

    for name, value in output.iteritems():
      if name not in correct_answer:
        raise KeyError('GapStats produced key %s not in correct output %s' %
                       (name, str(correct_answer)))
      self.assertEqual(value, correct_answer[name])

  def testOneRecord(self):
    one_record = pd.DataFrame({'start_time': [0],
                               'duration': [1],
                               'stream_num': [0]})
    self.doTest(one_record, 1, analysis.Interval(0, duration=1),
                {'total gap time': 0.0 * self.sec,
                 'gap time proportion': 0.0 * self.percent})

  def testOneStream(self):
    one_stream = pd.DataFrame({'start_time': [0, 2, 4],
                               'duration': [1, 1, 1],
                               'stream_num': [0, 0, 0]})
    self.doTest(one_stream, 1, analysis.Interval(0, duration=5),
                {'total gap time': 2.0 * self.sec,
                 'gap time proportion': 40.0 * self.percent})

  def testOverlapInterval(self):
    overlap = pd.DataFrame({'start_time': [0, 2, 5, 10, 13],
                            'duration': [1, 2, 4, 2, 1],
                            'stream_num': [0, 0, 0, 0, 0]})
    self.doTest(overlap, 1, analysis.Interval(3, duration=8),
                {'total gap time': 2.0 * self.sec,
                 'gap time proportion': 25.0 * self.percent})


if __name__ == '__main__':
  unittest.main()
