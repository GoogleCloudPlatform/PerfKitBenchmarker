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
"""A performance sample class."""

import collections

_SAMPLE_FIELDS = 'metric', 'value', 'unit', 'metadata'

def PercentileCalculator(numbers):
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  result['p1'] = numbers_sorted[int(count * 0.01)]
  result['p5'] = numbers_sorted[int(count * 0.05)]
  result['p50'] = numbers_sorted[int(count * 0.5)]
  result['p90'] = numbers_sorted[int(count * 0.9)]
  result['p99'] = numbers_sorted[int(count * 0.99)]
  result['p99.9'] = numbers_sorted[int(count * 0.999)]
  if count > 0:
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
  """

  def __new__(cls, metric, value, unit, metadata=None, **kwargs):
    return super(Sample, cls).__new__(cls, metric, value, unit,
                                      metadata=metadata or {}, **kwargs)

  def asdict(self):
    """Converts the Sample to a dictionary."""
    return self._asdict()
