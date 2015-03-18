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

from perfkitbenchmarker import flags_validators
from perfkitbenchmarker import timing_util


class ValidateMeasurementsFlagTestCase(unittest.TestCase):
  """Tests exercising ValidateMeasurementsFlag."""

  def testEmptyList(self):
    """Passing an empty list is not allowed."""
    e = None
    try:
      timing_util.ValidateMeasurementsFlag([])
    except flags_validators.Error as e:
      pass
    self.assertIsNotNone(e)
    self.assertEqual(str(e), 'option --timing_measurements requires argument')

  def testInvalidValue(self):
    """Passing an unrecognized value is not allowed."""
    e = None
    try:
      timing_util.ValidateMeasurementsFlag(['TEST'])
    except flags_validators.Error as e:
      pass
    self.assertIsNotNone(e)
    self.assertEqual(str(e), 'TEST: Invalid value for --timing_measurements')

  def testNoneWithAnother(self):
    """Passing NONE with another value is not allowed."""
    e = None
    try:
      timing_util.ValidateMeasurementsFlag(['NONE', 'RUNTIMES'])
    except flags_validators.Error as e:
      pass
    self.assertIsNotNone(e)
    self.assertEqual(
        str(e), 'NONE: Cannot combine with other --timing_measurements options')

  def testValid(self):
    """Test various valid combinations."""
    validate = timing_util.ValidateMeasurementsFlag
    e = None
    try:
      self.assertEqual(validate(['NONE']), True)
      self.assertEqual(validate(['END_TO_END_RUNTIME']), True)
      self.assertEqual(validate(['RUNTIMES']), True)
      self.assertEqual(validate(['TIMESTAMPS']), True)
      self.assertEqual(validate(['END_TO_END_RUNTIME', 'RUNTIMES']), True)
      self.assertEqual(validate(['END_TO_END_RUNTIME', 'TIMESTAMPS']), True)
      self.assertEqual(validate(['RUNTIMES', 'TIMESTAMPS']), True)
      self.assertEqual(
          validate(['END_TO_END_RUNTIME', 'RUNTIMES', 'TIMESTAMPS']),
          True)
    except flags_validators.Error as e:
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
