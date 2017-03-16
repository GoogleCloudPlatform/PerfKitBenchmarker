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
"""Utilities for generating timing samples."""

from collections import OrderedDict
from contextlib import contextmanager
import time

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample


MEASUREMENTS_FLAG_NAME = 'timing_measurements'
# Valid options that can be included in the flag's list value.
MEASUREMENTS_NONE = 'none'
MEASUREMENTS_END_TO_END_RUNTIME = 'end_to_end_runtime'
MEASUREMENTS_RUNTIMES = 'runtimes'
MEASUREMENTS_TIMESTAMPS = 'timestamps'
MEASUREMENTS_ALL = OrderedDict([
    (MEASUREMENTS_NONE, (
        'No measurements included (same as providing an empty list, and cannot '
        'be combined with other options).')),
    (MEASUREMENTS_END_TO_END_RUNTIME, (
        'Includes an end-to-end runtime measurement.')),
    (MEASUREMENTS_RUNTIMES, (
        'Includes runtimes of all measured intervals, including the end-to-end '
        'runtime, the time taken by the benchmark module Prepare, Run, and '
        'Cleanup functions, and other important intervals.')),
    (MEASUREMENTS_TIMESTAMPS, (
        'Includes start and stop timestamps of all measured intervals.'))])


def EndToEndRuntimeMeasurementEnabled():
  """Returns whether end-to-end runtime measurement is globally enabled."""
  return (MEASUREMENTS_END_TO_END_RUNTIME in flags.FLAGS.timing_measurements or
          RuntimeMeasurementsEnabled())


def RuntimeMeasurementsEnabled():
  """Returns whether runtime measurements are globally enabled."""
  return MEASUREMENTS_RUNTIMES in flags.FLAGS.timing_measurements


def TimestampMeasurementsEnabled():
  """Returns whether timestamps measurements are globally enabled."""
  return MEASUREMENTS_TIMESTAMPS in flags.FLAGS.timing_measurements


def ValidateMeasurementsFlag(options_list):
  """Verifies correct usage of the measurements configuration flag.

  The user of the flag must provide at least one option. All provided options
  must be valid. The NONE option cannot be combined with other options.

  Args:
    options_list: A list of strings parsed from the provided value for the
      flag.

  Returns:
    True if the list of options provided as the value for the flag meets all
    the documented requirements.

  Raises:
    flags.ValidationError: If the list of options provided as the value for
      the flag does not meet the documented requirements.
  """
  for option in options_list:
    if option not in MEASUREMENTS_ALL:
      raise flags.ValidationError(
          '%s: Invalid value for --%s' % (option, MEASUREMENTS_FLAG_NAME))
    if option == MEASUREMENTS_NONE and len(options_list) != 1:
      raise flags.ValidationError(
          '%s: Cannot combine with other --%s options' % (
              option, MEASUREMENTS_FLAG_NAME))
  return True


flags.DEFINE_list(
    MEASUREMENTS_FLAG_NAME, MEASUREMENTS_END_TO_END_RUNTIME,
    'Comma-separated list of values from <%s> that selects which timing '
    'measurements to enable. Measurements will be included as samples in the '
    'benchmark results. %s' % (
        '|'.join(MEASUREMENTS_ALL),
        ' '.join(['%s: %s' % (option, description) for option, description in
                  MEASUREMENTS_ALL.iteritems()])))
flags.register_validator(
    MEASUREMENTS_FLAG_NAME, ValidateMeasurementsFlag)


def _GenerateIntervalSamples(interval, include_timestamps):
  """Generates Samples for a single interval timed by IntervalTimer.Measure.

  Args:
    interval: A (name, start_time, stop_time) tuple from a call to
      IntervalTimer.Measure.
    include_timestamps: A Boolean that controls whether Samples containing the
      start and stop timestamps are added to the generated list.

  Returns:
    A list of 0 to 3 Samples as specified by the args. When included, the
    Samples appear in the order of runtime, start timestamp, stop timestamp.
  """
  samples = []
  name = interval[0]
  start_time = interval[1]
  stop_time = interval[2]
  elapsed_time = stop_time - start_time
  samples.append(sample.Sample(name + ' Runtime', elapsed_time, 'seconds'))
  if include_timestamps:
    samples.append(sample.Sample(
        name + ' Start Timestamp', start_time, 'seconds'))
    samples.append(sample.Sample(
        name + ' Stop Timestamp', stop_time, 'seconds'))
  return samples


class IntervalTimer(object):
  """Class that can measure time and generate samples for each measurement.

  Attributes:
    intervals: A list of one 3-tuple per measured interval. Each tuple is of the
      form (name string, start_time float, stop_time float).
  """

  def __init__(self):
    self.intervals = []

  @contextmanager
  def Measure(self, name):
    """Records the start and stop times of the enclosed interval.

    Args:
      name: A string that names the interval.
    """
    start_time = time.time()
    yield
    stop_time = time.time()
    self.intervals.append((name, start_time, stop_time))

  def GenerateSamples(self):
    """Generates Samples based on the times recorded in all calls to Measure.

    Returns:
      A list of Samples. The list contains Samples for each interval that was
      wrapped by a call to Measure, with per-interval Samples generated as
      specified by the args in the order of runtime, start timestamp, stop
      timestamp. All Samples for one interval appear before any Samples from the
      next interval.
    """
    include_timestamps = TimestampMeasurementsEnabled()
    return [
        sample for interval in self.intervals for sample in
        _GenerateIntervalSamples(interval, include_timestamps)]
