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

import calendar
import collections
from collections.abc import Sequence
import dataclasses
import datetime
import enum
import math
import time
from typing import Any, NewType

import numpy as np
from perfkitbenchmarker import errors
import pytz

PERCENTILES_LIST = 0.1, 1, 5, 10, 50, 90, 95, 99, 99.9

# Add this flag to the metadata to hide logging to console.
DISABLE_CONSOLE_LOG = 'disable_console_log'

_SAMPLE_FIELDS = 'metric', 'value', 'unit', 'metadata', 'timestamp'

# Metric names for time series
TPM_TIME_SERIES = 'TPM_time_series'
OPS_TIME_SERIES = 'OPS_time_series'
LATENCY_TIME_SERIES = 'Latency_time_series'
QPS_TIME_SERIES = 'QPS_time_series'

# Metadata for time series
VALUES = 'values'
RAMP_UP_ENDS = 'ramp_up_ends'
RAMP_DOWN_STARTS = 'ramp_down_starts'
TIMESTAMPS = 'timestamps'
INTERVAL = 'interval'
TIME_SERIES_METADATA = [
    RAMP_UP_ENDS,
    RAMP_DOWN_STARTS,
    VALUES,
    TIMESTAMPS,
    INTERVAL,
]


class NpAggregation(enum.Enum):
  MEAN = 'mean'


@dataclasses.dataclass
class Percentile:
  """Represents a percentile value (0-100)."""

  value: float
  label: str

  def __init__(self, val: float):
    try:
      self.value = float(val)
    except ValueError as e:
      raise ValueError(f'{val} is not a valid percentile.') from e
    if not (0 <= self.value <= 100):
      raise ValueError(f'{val} must be between 0 and 100.')

    if self.value.is_integer():
      self.label = 'p' + str(int(self.value))
    else:
      self.label = 'p' + str(self.value)


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
    total_of_squares = sum([(i - average) ** 2 for i in numbers])
    result['stddev'] = (total_of_squares / (count - 1)) ** 0.5
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
  return arr.prod() ** (1 / len(arr))


# The Sample is converted via collections.namedtuple._asdict for publishing
SampleDict = NewType('SampleDict', dict[str, Any])


class Sample(collections.namedtuple('Sample', _SAMPLE_FIELDS)):  # pyrefly: ignore[bad-class-definition]
  """A performance sample.

  Attributes:
    metric: string. Name of the metric within the benchmark.
    value: float. Result for 'metric'.
    unit: string. Units for 'value'.
    metadata: dict. Additional metadata to include with the sample.
    timestamp: float. Unix timestamp.
  """

  def __new__(
      cls, metric, value, unit, metadata=None, timestamp=None, **kwargs
  ):
    if timestamp is None:
      timestamp = time.time()

    metadata = metadata or {}
    metadata = dict(metadata)
    return super().__new__(
        cls,
        metric,
        float(value or 0.0),
        unit,
        metadata=metadata,
        timestamp=timestamp,
        **kwargs,
    )

  def asdict(self) -> dict[str, Any]:  # pylint:disable=invalid-name
    """Converts the Sample to a dictionary."""
    return self._asdict()


def SummarizePercentiles(
    numbers: Sequence[float],
    base_sample: Sample,
    metrics: Sequence[NpAggregation | Percentile],
) -> list[Sample]:
  """Returns a few summary statistics samples about a list of numbers.

  Args:
    numbers: A list of numbers to aggregate.
    base_sample: A Sample object to pull metric (as prefix), unit, and metadata
      from.
    metrics: A list of metrics to include (e.g., NpAggregation.MEAN,
      Percentile(50)).

  Returns:
    A list of Sample objects representing the summary statistics.
  """
  if not numbers:
    return []
  samples = []
  prefix = base_sample.metric

  if NpAggregation.MEAN in metrics:
    samples.append(
        Sample(
            prefix + 'mean',
            np.mean(numbers),
            base_sample.unit,
            base_sample.metadata,
        )
    )

  percentiles = [m for m in metrics if isinstance(m, Percentile)]
  if percentiles:
    float_percentiles = [p.value for p in percentiles]
    results = np.percentile(numbers, float_percentiles)
    for i, p in enumerate(percentiles):
      samples.append(
          Sample(
              prefix + p.label,
              results[i],
              base_sample.unit,
              base_sample.metadata,
          )
      )

  return samples


_Histogram = collections.OrderedDict


def MakeHistogram(
    values: list[float], round_bottom: float = 0.0, round_to_sig_fig: int = 3
) -> _Histogram[float, int]:
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
            round_to_sig_fig - int(math.floor(math.log10(abs(value)))) - 1,
        )
      else:
        rounded_value = 0.0
      histogram[rounded_value] = histogram.get(rounded_value, 0) + 1
    else:
      histogram[value] = histogram.get(value, 0) + 1
  return histogram


def _ConvertHistogramToString(histogram: _Histogram[float, int]) -> str:
  histogram_label_values = ','.join(
      f'"{key}": {value}' for (key, value) in histogram.items()
  )
  histogram_labels = '{%s}' % histogram_label_values
  return histogram_labels


def CreateHistogramSample(
    histogram: _Histogram[float, int],
    name: str,
    subname: str,
    units: str,
    additional_metadata=None,
    metric='',
) -> Sample:
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
      'Subname': subname,
  }
  if additional_metadata:
    metadata.update(additional_metadata)
  return Sample(metric, 0, units, metadata)


def CreateTimeSeriesSample(
    values: list[Any],
    timestamps: list[float],
    metric: str,
    units: str,
    interval: float,
    ramp_up_ends=None,
    ramp_down_starts=None,
    additional_metadata=None,
) -> Sample:
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
  # calendar.timegm assumes the time is from UTC.
  # Convert the datetime to UTC timezone first.
  date_utc = date.astimezone(pytz.utc)
  return calendar.timegm(date_utc.timetuple()) * 1000


class Aggregation(str, enum.Enum):
  """Aggregation strategy for sample group selection."""

  MAX = 'max'
  MIN = 'min'


class SampleGroupCollector:
  """Encapsulates samples grouped by a key (e.g., parameter sweep iterations).

  Provides utilities to collect samples across multiple groups/runs, preserve
  their execution order, and extract/annotate best sample sets (e.g., the
  group achieving maximum throughput or minimum latency).

  Attributes:
    all_samples: Flat list of all samples across every group, in execution
      order.
  """

  def __init__(self):
    """Initializes a SampleGroupCollector."""
    self.all_samples: list[Sample] = []
    self._samples_by_group: dict[Any, list[Sample]] = collections.OrderedDict()

  def Add(self, group_key: Any, samples: list[Sample]) -> None:
    """Records samples for a single group iteration."""
    self.all_samples.extend(samples)
    self._samples_by_group.setdefault(group_key, []).extend(samples)

  def SamplesByGroup(self) -> dict[Any, list[Sample]]:
    """Returns samples grouped by key."""
    return {
        group_key: list(samples)
        for group_key, samples in self._samples_by_group.items()
    }

  def _GetMetricValue(self, samples: list[Sample], metric: str) -> float | None:
    """Returns the metric value from a list of samples, if present.

    Subclasses can override this method to support custom value extraction
    (e.g., computing a mean from a metadata list).

    Args:
      samples: List of samples to search for the target metric.
      metric: Target metric name to find.

    Returns:
      The metric value as a float, or None if the metric is not present.
    """
    for s in samples:
      if s.metric == metric:
        return s.value
    return None

  def GetBestSamples(
      self,
      metric: str,
      prefix: str,
      aggregation: Aggregation,
  ) -> list[Sample]:
    """Returns prefixed duplicates of the best (e.g., max/min) group's samples.

    The winning group's original samples are annotated with
    metadata[metadata_key] = True. A duplicate of each sample in that group is
    returned with its metric name prefixed by the prefix and without the
    metadata_key entry in its metadata.

    Args:
      metric: Target metric name to evaluate.
      prefix: Prefix applied to duplicated sample metrics and used as the
        boolean metadata key.
      aggregation: Aggregation strategy (see Aggregation enum).

    Returns:
      A list of prefixed duplicate samples from the winning group.

    Raises:
      ValueError: If no sample group produced the target metric.
    """
    best_samples = None
    best_value = None
    for group_samples in self._samples_by_group.values():
      val = self._GetMetricValue(group_samples, metric)
      if val is None:
        continue
      if best_value is None:
        best_value = val
        best_samples = group_samples
      elif aggregation == Aggregation.MAX and val > best_value:
        best_value = val
        best_samples = group_samples
      elif aggregation == Aggregation.MIN and val < best_value:
        best_value = val
        best_samples = group_samples

    if best_samples is None:
      raise ValueError(f'No sample group produced the metric "{metric}".')

    metadata_key = prefix.rstrip('_')
    metric_prefix = prefix if prefix.endswith('_') else f'{prefix}_'

    prefixed_samples = []
    for s in best_samples:
      sample_metadata = dict(s.metadata)
      sample_metadata.pop(metadata_key, None)
      s.metadata[metadata_key] = True
      prefixed_samples.append(
          Sample(
              f'{metric_prefix}{s.metric}',
              s.value,
              s.unit,
              sample_metadata,
          )
      )
    return prefixed_samples
