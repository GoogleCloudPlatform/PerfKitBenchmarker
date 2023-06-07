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
"""A performance sample class."""

import collections
import datetime
import math
import time
from typing import Any, Dict, List, NewType

import numpy as np
from perfkitbenchmarker import errors

PERCENTILES_LIST = 0.1, 1, 5, 10, 50, 90, 95, 99, 99.9

# Add this flag to the metadata to hide logging to console.
DISABLE_CONSOLE_LOG = 'disable_console_log'

_SAMPLE_FIELDS = 'metric', 'value', 'unit', 'metadata', 'timestamp'

# Metric names for time series
TPM_TIME_SERIES = 'TPM_time_series'
OPS_TIME_SERIES = 'OPS_time_series'
LATENCY_TIME_SERIES = 'Latency_time_series'

# Metadata for time series
VALUES = 'values'
RAMP_UP_ENDS = 'ramp_up_ends'
RAMP_DOWN_STARTS = 'ramp_down_starts'
TIMESTAMPS = 'timestamps'
INTERVAL = 'interval'
TIME_SERIES_METADATA = [
    RAMP_UP_ENDS, RAMP_DOWN_STARTS, VALUES, TIMESTAMPS, INTERVAL
]


def PercentileCalculator(numbers, percentiles=PERCENTILES_LIST):
  """Computes percentiles, stddev and mean on a set of numbers.

  Args:
    numbers: A sequence of numbers to compute percentiles for.
    percentiles: If given, a list of percentiles to compute. Can be floats, ints
      or longs.

  Returns:
    A dictionary of percentiles.

  Raises:
    ValueError, if numbers is empty or if a percentile is outside of
    [0, 100].

  """

  # 'if not numbers' will fail if numbers is an np.Array or pd.Series.
  if not len(numbers):
    raise ValueError("Can't compute percentiles of empty list.")

  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  for percentile in percentiles:
    float(percentile)  # verify type
    if percentile < 0.0 or percentile > 100.0:
      raise ValueError('Invalid percentile %s' % percentile)

    percentile_string = 'p%s' % str(percentile)
    index = int(count * float(percentile) / 100.0)
    index = min(index, count - 1)  # Correction to handle 100th percentile.
    result[percentile_string] = numbers_sorted[index]

  average = total / float(count)
  result['average'] = average
  if count > 1:
    total_of_squares = sum([(i - average)**2 for i in numbers])
    result['stddev'] = (total_of_squares / (count - 1))**0.5
  else:
    result['stddev'] = 0

  return result


def GeoMean(iterable):
  """Calculate the geometric mean of a collection of numbers.

  Args:
    iterable: A sequence of numbers.

  Returns:
    The geometric mean

  Raises:
    ValueError, if numbers is empty.
  """
  arr = np.fromiter(iterable, dtype='float')
  if not arr.size:
    raise ValueError("Can't compute geomean of empty list.")
  return arr.prod()**(1 / len(arr))


# The Sample is converted via collections.namedtuple._asdict for publishing
SampleDict = NewType('SampleDict', Dict[str, Any])


class Sample(collections.namedtuple('Sample', _SAMPLE_FIELDS)):
  """A performance sample.

  Attributes:
    metric: string. Name of the metric within the benchmark.
    value: float. Result for 'metric'.
    unit: string. Units for 'value'.
    metadata: dict. Additional metadata to include with the sample.
    timestamp: float. Unix timestamp.
  """

  def __new__(cls,
              metric,
              value,
              unit,
              metadata=None,
              timestamp=None,
              **kwargs):
    if timestamp is None:
      timestamp = time.time()

    return super(Sample, cls).__new__(
        cls,
        metric,
        float(value or 0.0),
        unit,
        metadata=metadata or {},
        timestamp=timestamp,
        **kwargs)

  def __eq__(self, other) -> bool:
    if not isinstance(other, Sample):
      # don't attempt to compare against unrelated types
      return NotImplemented
    if self.value != other.value:
      return False
    if self.metric != other.metric:
      return False
    if self.timestamp != other.timestamp:
      return False
    for key, value in other.metadata.items():
      if key not in self.metadata or self.metadata[key] != value:
        return False
    return True

  def DisableConsoleLog(self) -> bool:
    """Disable log to console when this return True."""

    # Disable Console log is set as a metadata rather than a field
    # is due to the current structure of samples class.
    # Adding extra field to a sample might break serialization of some publisher
    # pipeline as they expect certain format.
    # Modyfing asdict function is also not enough because when we pickle
    # the samples,
    return (
        DISABLE_CONSOLE_LOG in self.metadata
        and self.metadata[DISABLE_CONSOLE_LOG]
    )

  def asdict(self)-> Dict[str, Any]:  # pylint:disable=invalid-name
    """Converts the Sample to a dictionary."""
    return self._asdict()


_Histogram = collections.OrderedDict


def MakeHistogram(values: List[float],
                  round_bottom: float = 0.0,
                  round_to_sig_fig: int = 3) -> _Histogram[float, int]:
  """Take a list of float values and returns a ordered dict of values and frequency.

  Args:
    values: a list of float values
    round_bottom: A float between 0 and 1 indicating a percentile of values that
      should be rounded. Any values below this percentile will be rounded
      according to the precision specified by round_to_sig_fig. Values equal to
      and above this percentile will not be rounded. (included with full
      precision). (e.g. 0.95 will round all values below the 95th percentile and
      keep full precision of values above the 95th percentile.) 0 by default,
      rounds no values, 1 would round all values.
    round_to_sig_fig: The number of significant figures kept when rounding
      values. 3 by default.

  Returns:
    An ordered dictionary of the values and their frequency
  """
  histogram = _Histogram()
  for iteration, value in enumerate(sorted(values)):
    percentile = iteration / len(values)
    if percentile < round_bottom:
      if value > 0:
        rounded_value = round(
            value,
            round_to_sig_fig - int(math.floor(math.log10(abs(value)))) - 1)
      else:
        rounded_value = 0.0
      histogram[rounded_value] = histogram.get(rounded_value, 0) + 1
    else:
      histogram[value] = histogram.get(value, 0) + 1
  return histogram


def _ConvertHistogramToString(histogram: _Histogram[float, int]) -> str:
  histogram_label_values = ','.join(
      f'"{key}": {value}' for (key, value) in histogram.items())
  histogram_labels = '{%s}' % histogram_label_values
  return histogram_labels


def CreateHistogramSample(histogram: _Histogram[float, int],
                          name: str,
                          subname: str,
                          units: str,
                          additional_metadata=None,
                          metric='') -> Sample:
  """Given a histogram of values, create a sample.

  Args:
    histogram: an ordered dict of objects
    name: name of histogram
    subname: subname of histogram
    units: the units of measure used in the sample
    additional_metadata: any additional metadata to add
    metric: metric in the sample

  Returns:
    sample: One sample object that reports the histogram passed in.

  """
  metadata = {
      'histogram': _ConvertHistogramToString(histogram),
      'Name': name,
      'Subname': subname
  }
  if additional_metadata:
    metadata.update(additional_metadata)
  return Sample(metric, 0, units, metadata)


def CreateTimeSeriesSample(values: List[Any],
                           timestamps: List[float],
                           metric: str,
                           units: str,
                           interval: float,
                           ramp_up_ends=None,
                           ramp_down_starts=None,
                           additional_metadata=None) -> Sample:
  """Create time series samples.

  Given  a list of values and the timestamp the values
  created at create a time series samples. Each value correspond to the
  timestamp that the value is collected. The size of the values and
  timestamps have to be equal.

  Args:
    values: an value orderd based on time series
    timestamps: an value orderd based on time series in Epoch micro timestamp
    metric: name of time series samples
    units: the units of measure of values
    interval: interval of the metrics in seconds
    ramp_up_ends: The timestamp when ramp up ends in Epoch micro timestamp
    ramp_down_starts: The timestamp when ramp down starts in Epoch nano
      timestamp
    additional_metadata: any additional metadata to add

  Returns:
    sample: One sample object that reports the time series passed in.

  """
  if len(values) != len(timestamps):
    raise errors.Error('Length of values is different to length of timestamps')
  metadata = {VALUES: values, TIMESTAMPS: timestamps, INTERVAL: interval}
  if additional_metadata:
    metadata.update(additional_metadata)

  if ramp_up_ends:
    metadata[RAMP_UP_ENDS] = ramp_up_ends

  if ramp_down_starts:
    metadata[RAMP_DOWN_STARTS] = ramp_down_starts
  return Sample(metric, 0, units, metadata)


def ConvertDateTimeToUnixMs(date: datetime.datetime):
  return time.mktime(date.timetuple()) * 1000
