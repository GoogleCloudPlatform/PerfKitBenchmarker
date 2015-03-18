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
"""Utilities for generating timing samples."""

from contextlib import contextmanager
import time

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample


class TimingMeasurementsFlag(object):
  """Global settings decoded from the --timing_measurements flag."""

  # Valid options for --timing_measurements.
  NONE = 'NONE'
  END_TO_END_RUNTIME = 'END_TO_END_RUNTIME'
  RUNTIMES = 'RUNTIMES'
  TIMESTAMPS = 'TIMESTAMPS'

  ALL = (NONE, END_TO_END_RUNTIME, RUNTIMES, TIMESTAMPS)

  @staticmethod
  def Initialize(options_list):
    """Verifies correct usage of the flag and initializes global settings.

    Previous values of the global settings are replaced with the new settings.

    Args:
      options_list: A list of strings parsed from the provided value of the
        --timing_measurements flag.

    Raises:
      flags.IllegalFlagValue: If an illegal value was included in the
        comma-separated list provided to the --timing-measurements flag.
    """
    for option in TimingMeasurementsFlag.ALL:
      setattr(TimingMeasurementsFlag, option.lower(), False)
    if len(options_list) is 0:
      raise flags.IllegalFlagValue(
          'option --timing_measurements requires argument')
    for option in options_list:
      if option not in TimingMeasurementsFlag.ALL:
        raise flags.IllegalFlagValue(
            '%s: Invalid value for --timing_measurements' % option)
      if option == TimingMeasurementsFlag.NONE and len(options_list) != 1:
        raise flags.IllegalFlagValue(
            '%s: Cannot combine with other --timing_measurements options' %
            option)
      setattr(TimingMeasurementsFlag, option.lower(), True)


flags.DEFINE_list(
    'timing_measurements', TimingMeasurementsFlag.END_TO_END_RUNTIME,
    'Comma-separated list of values from <%s> that selects which timing '
    'measurements to enable.' % '|'.join(TimingMeasurementsFlag.ALL))


class TimedInterval(object):
  """A measured interval that generates timing samples.

  Attributes:
    name: A string used to prefix generated samples' names.
    start_time: A float containing the time in seconds since the epoch, recorded
      at the beginning of a call to Measure, or None if Measure was not called.
    stop_time: A float containing the time in seconds since the epoch, recorded
      at the end of a call to Measure, or None if Measure was not called.
  """

  def __init__(self, name):
    """Create a named interval.

    Args:
      name: A string used to prefix the generated samples' names.
    """
    self.name = name
    self.start_time = None
    self.stop_time = None

  @contextmanager
  def Measure(self):
    """Records the start and stop time of the enclosed interval."""
    self.start_time = time.time()
    yield
    self.stop_time = time.time()

  def GenerateSamples(self, include_runtime, include_timestamps):
    """Generates samples based on the times recorded in Measure.

    Args:
      include_runtime: A Boolean that controls whether an elapsed time sample is
        included in the generated list.
      include_timestamps: A Boolean that controls whether samples containing the
        start and stop timestamps are added to the generated list.

    Returns:
      A list of Samples. If Measure has not been called, the list is empty.
      Otherwise, the list contains samples as specified by the args, in the
      order of runtime, start timestamp, stop timestamp.
    """
    samples = []
    if self.stop_time is not None:
      if include_runtime:
        elapsed_time = self.stop_time - self.start_time
        samples.append(sample.Sample(
            self.name + ' Runtime', elapsed_time, 'seconds'))
      if include_timestamps:
        samples.append(sample.Sample(
            self.name + ' Start Timestamp', self.start_time, 'seconds'))
        samples.append(sample.Sample(
            self.name + ' Stop Timestamp', self.stop_time, 'seconds'))
    return samples
