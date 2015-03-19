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

import re
import unittest

from perfkitbenchmarker import flags_validators
from perfkitbenchmarker import sample
from perfkitbenchmarker import timing_util


class ValidateMeasurementsFlagTestCase(unittest.TestCase):
  """Tests exercising ValidateMeasurementsFlag."""

  def testInvalidValue(self):
    """Passing an unrecognized value is not allowed."""
    exp_str = 'test: Invalid value for --timing_measurements'
    exp_regex = r'^%s$' % re.escape(exp_str)
    with self.assertRaisesRegexp(flags_validators.Error, exp_regex):
      timing_util.ValidateMeasurementsFlag(['test'])

  def testNoneWithAnother(self):
    """Passing none with another value is not allowed."""
    exp_str = 'none: Cannot combine with other --timing_measurements options'
    exp_regex = r'^%s$' % re.escape(exp_str)
    with self.assertRaisesRegexp(flags_validators.Error, exp_regex):
      timing_util.ValidateMeasurementsFlag(['none', 'runtimes'])

  def testValid(self):
    """Test various valid combinations."""
    validate = timing_util.ValidateMeasurementsFlag
    self.assertIs(validate([]), True)
    self.assertIs(validate(['none']), True)
    self.assertIs(validate(['end_to_end_runtime']), True)
    self.assertIs(validate(['runtimes']), True)
    self.assertIs(validate(['timestamps']), True)
    self.assertIs(validate(['end_to_end_runtime', 'runtimes']), True)
    self.assertIs(validate(['end_to_end_runtime', 'timestamps']), True)
    self.assertIs(validate(['runtimes', 'timestamps']), True)
    self.assertIs(
        validate(['end_to_end_runtime', 'runtimes', 'timestamps']), True)


class IntervalTimerTestCase(unittest.TestCase):
  """Tests exercising IntervalTimer."""

  def testMeasureSequential(self):
    """Verify correct interval tuple generation in sequential measurements."""
    timer = timing_util.IntervalTimer()
    self.assertEqual(timer.intervals, [])
    with timer.Measure('First Interval'):
      pass
    with timer.Measure('Second Interval'):
      pass
    self.assertEqual(len(timer.intervals), 2)
    firstInterval = timer.intervals[0]
    self.assertEqual(len(firstInterval), 3)
    firstName = firstInterval[0]
    firstStart = firstInterval[1]
    firstStop = firstInterval[2]
    self.assertEqual(firstName, 'First Interval')
    secondInterval = timer.intervals[1]
    self.assertEqual(len(secondInterval), 3)
    secondName = secondInterval[0]
    secondStart = secondInterval[1]
    secondStop = secondInterval[2]
    self.assertEqual(secondName, 'Second Interval')
    self.assertTrue(firstStart <= firstStop)
    self.assertTrue(firstStop <= secondStart)
    self.assertTrue(secondStart <= secondStop)

  def testMeasureNested(self):
    """Verify correct interval tuple generation in nested measurements."""
    timer = timing_util.IntervalTimer()
    self.assertEqual(timer.intervals, [])
    with timer.Measure('Outer Interval'):
      with timer.Measure('Inner Interval'):
        pass
    self.assertEqual(len(timer.intervals), 2)
    innerInterval = timer.intervals[0]
    self.assertEqual(len(innerInterval), 3)
    innerName = innerInterval[0]
    innerStart = innerInterval[1]
    innerStop = innerInterval[2]
    self.assertEqual(innerName, 'Inner Interval')
    outerInterval = timer.intervals[1]
    self.assertEqual(len(outerInterval), 3)
    outerName = outerInterval[0]
    outerStart = outerInterval[1]
    outerStop = outerInterval[2]
    self.assertEqual(outerName, 'Outer Interval')
    self.assertTrue(outerStart <= innerStart)
    self.assertTrue(innerStart <= innerStop)
    self.assertTrue(innerStop <= outerStop)

  def testGenerateSamplesMeasureNotCalled(self):
    """GenerateSamples should return an empty list if Measure was not called."""
    timer = timing_util.IntervalTimer()
    self.assertEqual(timer.intervals, [])
    samples = timer.GenerateSamples(
        include_runtime=True, include_timestamps=True)
    self.assertEqual(timer.intervals, [])
    self.assertEqual(samples, [])

  def testGenerateSamplesNoRuntimeNoTimestamps(self):
    """No samples when include_runtime and include_timestamps are False."""
    timer = timing_util.IntervalTimer()
    with timer.Measure('First Interval'):
      pass
    with timer.Measure('Second Interval'):
      pass
    samples = timer.GenerateSamples(
        include_runtime=False, include_timestamps=False)
    self.assertEqual(samples, [])

  def testGenerateSamplesRuntimeNoTimestamps(self):
    """Test generating runtime sample but no timestamp samples."""
    timer = timing_util.IntervalTimer()
    with timer.Measure('First'):
      pass
    with timer.Measure('Second'):
      pass
    start0 = timer.intervals[0][1]
    stop0 = timer.intervals[0][2]
    start1 = timer.intervals[1][1]
    stop1 = timer.intervals[1][2]
    samples = timer.GenerateSamples(
        include_runtime=True, include_timestamps=False)
    exp_samples = [
        sample.Sample('First Runtime', stop0 - start0, 'seconds'),
        sample.Sample('Second Runtime', stop1 - start1, 'seconds')]
    self.assertEqual(samples, exp_samples)

  def testGenerateSamplesTimestampsNoRuntime(self):
    """Test generating timestamp samples but no runtime sample."""
    timer = timing_util.IntervalTimer()
    with timer.Measure('First'):
      pass
    with timer.Measure('Second'):
      pass
    start0 = timer.intervals[0][1]
    stop0 = timer.intervals[0][2]
    start1 = timer.intervals[1][1]
    stop1 = timer.intervals[1][2]
    samples = timer.GenerateSamples(
        include_runtime=False, include_timestamps=True)
    exp_samples = [
        sample.Sample('First Start Timestamp', start0, 'seconds'),
        sample.Sample('First Stop Timestamp', stop0, 'seconds'),
        sample.Sample('Second Start Timestamp', start1, 'seconds'),
        sample.Sample('Second Stop Timestamp', stop1, 'seconds')]
    self.assertEqual(samples, exp_samples)

  def testGenerateSamplesRuntimeAndTimestamps(self):
    """Test generating both runtime and timestamp samples."""
    timer = timing_util.IntervalTimer()
    with timer.Measure('First'):
      pass
    with timer.Measure('Second'):
      pass
    start0 = timer.intervals[0][1]
    stop0 = timer.intervals[0][2]
    start1 = timer.intervals[1][1]
    stop1 = timer.intervals[1][2]
    samples = timer.GenerateSamples(
        include_runtime=True, include_timestamps=True)
    exp_samples = [
        sample.Sample('First Runtime', stop0 - start0, 'seconds'),
        sample.Sample('First Start Timestamp', start0, 'seconds'),
        sample.Sample('First Stop Timestamp', stop0, 'seconds'),
        sample.Sample('Second Runtime', stop1 - start1, 'seconds'),
        sample.Sample('Second Start Timestamp', start1, 'seconds'),
        sample.Sample('Second Stop Timestamp', stop1, 'seconds')]
    self.assertEqual(samples, exp_samples)


if __name__ == '__main__':
  unittest.main()
