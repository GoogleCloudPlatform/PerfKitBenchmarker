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

import gflags as flags
import unittest

from perfkitbenchmarker import timing_util


class TimingMeasurementsFlagTestCase(unittest.TestCase):
  """Tests exercising TimingMeasurementsFlag."""

  def testEmptyList(self):
    """Passing an empty list is not allowed."""
    e = None
    try:
      timing_util.TimingMeasurementsFlag.Initialize([])
    except flags.IllegalFlagValue as e:
      pass
    self.assertIsNotNone(e)
    self.assertEqual(str(e), 'option --timing_measurements requires argument')

  def testInvalidValue(self):
    """Passing an unrecognized value is not allowed."""
    e = None
    try:
      timing_util.TimingMeasurementsFlag.Initialize(['TEST'])
    except flags.IllegalFlagValue as e:
      pass
    self.assertIsNotNone(e)
    self.assertEqual(str(e), 'TEST: Invalid value for --timing_measurements')

  def testNoneWithAnother(self):
    """Passing NONE with another value is not allowed."""
    e = None
    try:
      timing_util.TimingMeasurementsFlag.Initialize(['NONE', 'RUNTIMES'])
    except flags.IllegalFlagValue as e:
      pass
    self.assertIsNotNone(e)
    self.assertEqual(
        str(e), 'NONE: Cannot combine with other --timing_measurements options')

  def testValid(self):
    """Test various valid combinations."""
    tm_flag = timing_util.TimingMeasurementsFlag
    e = None
    try:
      tm_flag.Initialize(['NONE'])
      self.assertEqual(tm_flag.none, True)
      self.assertEqual(tm_flag.end_to_end_runtime, False)
      self.assertEqual(tm_flag.runtimes, False)
      self.assertEqual(tm_flag.timestamps, False)

      tm_flag.Initialize(['END_TO_END_RUNTIME'])
      self.assertEqual(tm_flag.none, False)
      self.assertEqual(tm_flag.end_to_end_runtime, True)
      self.assertEqual(tm_flag.runtimes, False)
      self.assertEqual(tm_flag.timestamps, False)

      tm_flag.Initialize(['RUNTIMES'])
      self.assertEqual(tm_flag.none, False)
      self.assertEqual(tm_flag.end_to_end_runtime, False)
      self.assertEqual(tm_flag.runtimes, True)
      self.assertEqual(tm_flag.timestamps, False)

      tm_flag.Initialize(['TIMESTAMPS'])
      self.assertEqual(tm_flag.none, False)
      self.assertEqual(tm_flag.end_to_end_runtime, False)
      self.assertEqual(tm_flag.runtimes, False)
      self.assertEqual(tm_flag.timestamps, True)

      tm_flag.Initialize(['END_TO_END_RUNTIME', 'RUNTIMES'])
      self.assertEqual(tm_flag.none, False)
      self.assertEqual(tm_flag.end_to_end_runtime, True)
      self.assertEqual(tm_flag.runtimes, True)
      self.assertEqual(tm_flag.timestamps, False)

      tm_flag.Initialize(['END_TO_END_RUNTIME', 'TIMESTAMPS'])
      self.assertEqual(tm_flag.none, False)
      self.assertEqual(tm_flag.end_to_end_runtime, True)
      self.assertEqual(tm_flag.runtimes, False)
      self.assertEqual(tm_flag.timestamps, True)

      tm_flag.Initialize(['RUNTIMES', 'TIMESTAMPS'])
      self.assertEqual(tm_flag.none, False)
      self.assertEqual(tm_flag.end_to_end_runtime, False)
      self.assertEqual(tm_flag.runtimes, True)
      self.assertEqual(tm_flag.timestamps, True)

      tm_flag.Initialize(['END_TO_END_RUNTIME', 'RUNTIMES', 'TIMESTAMPS'])
      self.assertEqual(tm_flag.none, False)
      self.assertEqual(tm_flag.end_to_end_runtime, True)
      self.assertEqual(tm_flag.runtimes, True)
      self.assertEqual(tm_flag.timestamps, True)
    except flags.IllegalFlagValue as e:
      pass
    self.assertIsNone(e)


class TimedIntervalTestCase(unittest.TestCase):
  """Tests exercising TimedInterval."""

  def testGenerateSamplesMeasureNotCalled(self):
    """GenerateSamples should return an empty list if Measure was not called."""
    interval = timing_util.TimedInterval('Test Interval')
    samples = interval.GenerateSamples(
        include_runtime=True, include_timestamps=True)
    self.assertEqual(samples, [])

  def testGenerateSamplesNoRuntimeNoTimestamps(self):
    """No samples when include_runtime and include_timestamps are False."""
    interval = timing_util.TimedInterval('Test Interval')
    self.assertEqual(interval.name, 'Test Interval')
    self.assertIsNone(interval.start_time)
    self.assertIsNone(interval.stop_time)
    with interval.Measure():
      pass
    self.assertIsNotNone(interval.start_time)
    self.assertIsNotNone(interval.stop_time)
    self.assertTrue(interval.start_time <= interval.stop_time)
    samples = interval.GenerateSamples(
        include_runtime=False, include_timestamps=False)
    self.assertEqual(samples, [])

  def testGenerateSamplesRuntimeNoTimestamps(self):
    """Test generating runtime sample but no timestamp samples."""
    interval = timing_util.TimedInterval('Test Interval')
    self.assertEqual(interval.name, 'Test Interval')
    self.assertIsNone(interval.start_time)
    self.assertIsNone(interval.stop_time)
    with interval.Measure():
      pass
    self.assertIsNotNone(interval.start_time)
    self.assertIsNotNone(interval.stop_time)
    self.assertTrue(interval.start_time <= interval.stop_time)
    samples = interval.GenerateSamples(
        include_runtime=True, include_timestamps=False)
    self.assertEqual(len(samples), 1)
    self.assertEqual(samples[0].metric, 'Test Interval Runtime')
    self.assertEqual(samples[0].unit, 'seconds')
    self.assertEqual(samples[0].metadata, {})
    self.assertEqual(samples[0].value, interval.stop_time - interval.start_time)

  def testGenerateSamplesTimestampsNoRuntime(self):
    """Test generating timestamp samples but no runtime sample."""
    interval = timing_util.TimedInterval('Test Interval')
    self.assertEqual(interval.name, 'Test Interval')
    self.assertIsNone(interval.start_time)
    self.assertIsNone(interval.stop_time)
    with interval.Measure():
      pass
    self.assertIsNotNone(interval.start_time)
    self.assertIsNotNone(interval.stop_time)
    self.assertTrue(interval.start_time <= interval.stop_time)
    samples = interval.GenerateSamples(
        include_runtime=False, include_timestamps=True)
    self.assertEqual(len(samples), 2)
    self.assertEqual(samples[0].metric, 'Test Interval Start Timestamp')
    self.assertEqual(samples[0].unit, 'seconds')
    self.assertEqual(samples[0].metadata, {})
    self.assertEqual(samples[0].value, interval.start_time)
    self.assertEqual(samples[1].metric, 'Test Interval Stop Timestamp')
    self.assertEqual(samples[1].unit, 'seconds')
    self.assertEqual(samples[1].metadata, {})
    self.assertEqual(samples[1].value, interval.stop_time)

  def testGenerateSamplesRuntimeAndTimestamps(self):
    """Test generating both runtime and timestamp samples."""
    interval = timing_util.TimedInterval('Test Interval')
    self.assertEqual(interval.name, 'Test Interval')
    self.assertIsNone(interval.start_time)
    self.assertIsNone(interval.stop_time)
    with interval.Measure():
      pass
    self.assertIsNotNone(interval.start_time)
    self.assertIsNotNone(interval.stop_time)
    self.assertTrue(interval.start_time <= interval.stop_time)
    samples = interval.GenerateSamples(
        include_runtime=True, include_timestamps=True)
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
