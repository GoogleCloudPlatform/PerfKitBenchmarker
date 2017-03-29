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

import mock
import re
import unittest

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker import timing_util


class ValidateMeasurementsFlagTestCase(unittest.TestCase):
  """Tests exercising ValidateMeasurementsFlag."""

  def testInvalidValue(self):
    """Passing an unrecognized value is not allowed."""
    exp_str = 'test: Invalid value for --timing_measurements'
    exp_regex = r'^%s$' % re.escape(exp_str)
    with self.assertRaisesRegexp(flags.ValidationError, exp_regex):
      timing_util.ValidateMeasurementsFlag(['test'])

  def testNoneWithAnother(self):
    """Passing none with another value is not allowed."""
    exp_str = 'none: Cannot combine with other --timing_measurements options'
    exp_regex = r'^%s$' % re.escape(exp_str)
    with self.assertRaisesRegexp(flags.ValidationError, exp_regex):
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


class IntervalTimerTestCase(unittest.TestCase, test_util.SamplesTestMixin):
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
    first_interval = timer.intervals[0]
    self.assertEqual(len(first_interval), 3)
    first_name = first_interval[0]
    first_start = first_interval[1]
    first_stop = first_interval[2]
    self.assertEqual(first_name, 'First Interval')
    second_interval = timer.intervals[1]
    self.assertEqual(len(second_interval), 3)
    second_name = second_interval[0]
    second_start = second_interval[1]
    second_stop = second_interval[2]
    self.assertEqual(second_name, 'Second Interval')
    self.assertLessEqual(first_start, first_stop)
    self.assertLessEqual(first_stop, second_start)
    self.assertLessEqual(second_start, second_stop)

  def testMeasureNested(self):
    """Verify correct interval tuple generation in nested measurements."""
    timer = timing_util.IntervalTimer()
    self.assertEqual(timer.intervals, [])
    with timer.Measure('Outer Interval'):
      with timer.Measure('Inner Interval'):
        pass
    self.assertEqual(len(timer.intervals), 2)
    inner_interval = timer.intervals[0]
    self.assertEqual(len(inner_interval), 3)
    inner_name = inner_interval[0]
    inner_start = inner_interval[1]
    inner_stop = inner_interval[2]
    self.assertEqual(inner_name, 'Inner Interval')
    outer_interval = timer.intervals[1]
    self.assertEqual(len(outer_interval), 3)
    outer_name = outer_interval[0]
    outer_start = outer_interval[1]
    outer_stop = outer_interval[2]
    self.assertEqual(outer_name, 'Outer Interval')
    self.assertLessEqual(outer_start, inner_start)
    self.assertLessEqual(inner_start, inner_stop)
    self.assertLessEqual(inner_stop, outer_stop)

  def testGenerateSamplesMeasureNotCalled(self):
    """GenerateSamples should return an empty list if Measure was not called."""
    timer = timing_util.IntervalTimer()
    self.assertEqual(timer.intervals, [])
    samples = timer.GenerateSamples()
    self.assertEqual(timer.intervals, [])
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
    samples = timer.GenerateSamples()
    exp_samples = [
        sample.Sample('First Runtime', stop0 - start0, 'seconds'),
        sample.Sample('Second Runtime', stop1 - start1, 'seconds')]
    self.assertSampleListsEqualUpToTimestamp(samples, exp_samples)

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
    with mock.patch(
        'perfkitbenchmarker.timing_util.TimestampMeasurementsEnabled',
        return_value=True):
      samples = timer.GenerateSamples()
    exp_samples = [
        sample.Sample('First Runtime', stop0 - start0, 'seconds'),
        sample.Sample('First Start Timestamp', start0, 'seconds'),
        sample.Sample('First Stop Timestamp', stop0, 'seconds'),
        sample.Sample('Second Runtime', stop1 - start1, 'seconds'),
        sample.Sample('Second Start Timestamp', start1, 'seconds'),
        sample.Sample('Second Stop Timestamp', stop1, 'seconds')]
    self.assertSampleListsEqualUpToTimestamp(samples, exp_samples)


if __name__ == '__main__':
  unittest.main()
