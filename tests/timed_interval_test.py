# Copyright 2014 Google Inc. All rights reserved.
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

from perfkitbenchmarker import timed_interval


class TimedIntervalTestCase(unittest.TestCase):
  """Tests exercising TimedInterval."""

  def testGenerateSamplesMeasureNotCalled(self):
    """GenerateSamples should return an empty list if Measure was not called."""
    interval = timed_interval.TimedInterval('Test Interval')
    self.assertEqual(interval.GenerateSamples(), [])

  def testGenerateSamplesNoTimestamps(self):
    """Verify GenerateSamples behavior when include_timestamps is False."""
    interval = timed_interval.TimedInterval('Test Interval')
    self.assertEqual(interval.name, 'Test Interval')
    self.assertIsNone(interval.start_time)
    self.assertIsNone(interval.start_time)
    with interval.Measure():
      pass
    self.assertIsNotNone(interval.start_time)
    self.assertIsNotNone(interval.stop_time)
    self.assertTrue(interval.start_time <= interval.stop_time)
    samples = interval.GenerateSamples()
    self.assertEqual(len(samples), 1)
    self.assertEqual(samples[0].metric, 'Test Interval Runtime')
    self.assertEqual(samples[0].unit, 'seconds')
    self.assertEqual(samples[0].metadata, {})
    self.assertEqual(samples[0].value, interval.stop_time - interval.start_time)

  def testGenerateSamplesIncludeRuntime(self):
    """Verify GenerateSamples behavior when include_timestamps is True."""
    interval = timed_interval.TimedInterval('Test Interval')
    self.assertEqual(interval.name, 'Test Interval')
    self.assertIsNone(interval.start_time)
    self.assertIsNone(interval.start_time)
    with interval.Measure():
      pass
    self.assertIsNotNone(interval.start_time)
    self.assertIsNotNone(interval.stop_time)
    self.assertTrue(interval.start_time <= interval.stop_time)
    samples = interval.GenerateSamples(include_timestamps=True)
    self.assertEqual(len(samples), 3)
    self.assertEqual(samples[0].metric, 'Test Interval Runtime')
    self.assertEqual(samples[0].unit, 'seconds')
    self.assertEqual(samples[0].metadata, {})
    self.assertEqual(samples[0].value, interval.stop_time - interval.start_time)
    self.assertEqual(samples[1].metric, 'Test Interval Start Timestamp')
    self.assertEqual(samples[1].unit, 'seconds')
    self.assertEqual(samples[1].metadata, {})
    self.assertEqual(samples[1].value, interval.start_time)
    self.assertEqual(samples[2].metric, 'Test Interval Stop Timestamp')
    self.assertEqual(samples[2].unit, 'seconds')
    self.assertEqual(samples[2].metadata, {})
    self.assertEqual(samples[2].value, interval.stop_time)


if __name__ == '__main__':
  unittest.main()
