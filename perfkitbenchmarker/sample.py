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
import math
import time
from typing import Any, Dict, List, NewType

import numpy as np

PERCENTILES_LIST = 0.1, 1, 5, 10, 50, 90, 95, 99, 99.9

_SAMPLE_FIELDS = 'metric', 'value', 'unit', 'metadata', 'timestamp'


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

  def asdict(self)-> Dict[str, Any]:  # pylint:disable=invalid-name
    """Converts the Sample to a dictionary."""
    return self._asdict()


_Histogram = collections.OrderedDict


def MakeHistogram(values: List[float],
                  round_bottom: float = 0.0,
                  round_to_sig_fig: int = 3) -> _Histogram:
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


def _ConvertHistogramToString(histogram: _Histogram) -> str:
  histogram_label_values = ','.join(
      f'"{key}": {value}' for (key, value) in histogram.items())
  histogram_labels = '{%s}' % histogram_label_values
  return histogram_labels


def CreateHistogramSample(histogram: _Histogram,
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
