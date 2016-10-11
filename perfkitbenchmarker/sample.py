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
import time
PERCENTILES_LIST = [0.1, 1, 5, 10, 50, 90, 95, 99, 99.9]

_SAMPLE_FIELDS = 'metric', 'value', 'unit', 'metadata', 'timestamp'


def PercentileCalculator(numbers, percentiles=PERCENTILES_LIST):
  """Computes percentiles, stddev and mean on a set of numbers.

  Args:
    numbers: A sequence of numbers to compute percentiles for.
    percentiles: If given, a list of percentiles to compute. Can be
      floats, ints or longs.

  Returns:
    A dictionary of percentiles.

  Raises:
    ValueError, if numbers is empty or if a percentile is outside of
    [0, 100].

  """

  if not len(numbers):  # 'if not numbers' will fail if numbers is a pd.Series.
    raise ValueError("Can't compute percentiles of empty list.")

  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  for percentile in percentiles:
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


class Sample(collections.namedtuple('Sample', _SAMPLE_FIELDS)):
  """A performance sample.

  Attributes:
    metric: string. Name of the metric within the benchmark.
    value: float. Result for 'metric'.
    unit: string. Units for 'value'.
    metadata: dict. Additional metadata to include with the sample.
    timestamp: float. Unix timestamp.
  """

  def __new__(cls, metric, value, unit, metadata=None, timestamp=None,
              **kwargs):
    if timestamp is None:
      timestamp = time.time()

    return super(Sample, cls).__new__(cls, metric, value, unit,
                                      metadata=metadata or {},
                                      timestamp=timestamp,
                                      **kwargs)

  def asdict(self):
    """Converts the Sample to a dictionary."""
    return self._asdict()
