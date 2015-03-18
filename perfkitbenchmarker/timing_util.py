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
from perfkitbenchmarker import flags_validators
from perfkitbenchmarker import sample


class TimingMeasurementsFlag(object):
  """Functions and values related to the --timing_measurements flag."""

  # Name of the flag as a user would type it.
  FLAG_NAME = 'timing_measurements'

  # Valid options that can be included in the flag's list value.
  NONE = 'NONE'
  END_TO_END_RUNTIME = 'END_TO_END_RUNTIME'
  RUNTIMES = 'RUNTIMES'
  TIMESTAMPS = 'TIMESTAMPS'
  ALL = (NONE, END_TO_END_RUNTIME, RUNTIMES, TIMESTAMPS)

  @staticmethod
  def Validate(options_list):
    """Verifies correct usage of the flag.

    The user of the flag must provide at least one option. All provided options
    must be valid. The NONE option cannot be combined with other options.

    Args:
      options_list: A list of strings parsed from the provided value for the
        flag.

    Returns:
      True if the list of options provided as the value for the flag meets all
      the documented requirements.

    Raises:
      flags_validators.Error: If the list of options provided as the value for
        the flag does not meet the documented requirements.
    """
    if len(options_list) is 0:
      raise flags_validators.Error(
          'option --%s requires argument' % TimingMeasurementsFlag.FLAG_NAME)
    for option in options_list:
      if option not in TimingMeasurementsFlag.ALL:
        raise flags_validators.Error(
            '%s: Invalid value for --%s' % (
                option, TimingMeasurementsFlag.FLAG_NAME))
      if option == TimingMeasurementsFlag.NONE and len(options_list) != 1:
        raise flags_validators.Error(
            '%s: Cannot combine with other --%s options' % (
                option, TimingMeasurementsFlag.FLAG_NAME))
    return True

  @staticmethod
  def OptionIncluded(option):
    """Determines if a specified option was included in the flag list value.

    Args:
      option: A string containing one of the flag's valid options.

    Returns:
      A Boolean that is True if the specified option is present in the global
      flag value, or False if it is not present.
    """
    return option in getattr(flags.FLAGS, TimingMeasurementsFlag.FLAG_NAME)


flags.DEFINE_list(
    TimingMeasurementsFlag.FLAG_NAME, TimingMeasurementsFlag.END_TO_END_RUNTIME,
    'Comma-separated list of values from <%s> that selects which timing '
    'measurements to enable.' % '|'.join(TimingMeasurementsFlag.ALL))
flags.RegisterValidator(
    TimingMeasurementsFlag.FLAG_NAME, TimingMeasurementsFlag.Validate)


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
