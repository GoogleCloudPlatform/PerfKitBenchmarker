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
import time
PERCENTILES_LIST = [1, 5, 50, 90, 99, 99.9]

_SAMPLE_FIELDS = 'metric', 'value', 'unit', 'metadata', 'timestamp'


def PercentileCalculator(numbers):
  """Computes percentiles, stddev and mean on a set of numbers

  Args:
    numbers: The set of numbers to compute percentiles for.

  Returns:
    A dictionary of percentiles.
  """
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  for percentile in PERCENTILES_LIST:
    percentile_string = 'p%s' % str(percentile)
    result[percentile_string] = numbers_sorted[
        int(count * float(percentile) / 100)]

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


class SamplesTestMixin(object):
  """A mixin for unittest.TestCase that adds a type-specific equality
  predicate for samples.
  """

  def __init__(self, *args, **kwargs):
    super(SamplesTestMixin, self).__init__(self, *args, **kwargs)

    self.addTypeEqualityFunc(Sample, self.assertSamplesEqual)

  def assertSamplesEqual(self, a, b, msg=None):
    self.assertEqual(a.metric, b.metric,
                     msg or 'Samples %s and %s have different metrics' % (a, b))
    self.assertEqual(a.value, b.value,
                     msg or 'Samples %s and %s have different values' % (a, b))
    self.assertEqual(a.unit, b.unit,
                     msg or 'Samples %s and %s have different units' % (a, b))
    self.assertEqual(a.metadata, b.metadata,
                     msg or 'Samples %s and %s have different metadata' %
                     (a, b))
    # Deliberately don't compare the timestamp fields of the samples.

  def assertSampleListsEqual(self, a, b, msg=None):
    """Compare two lists of samples.

    Sadly, the builtin assertListsEqual will only use Python's
    built-in equality predicate for testing the equality of elements
    in a list. Since we compare lists of samples a lot, we need a
    custom test for that.
    """

    self.assertEqual(len(a), len(b), msg=msg)
    for i in xrange(len(a)):
      self.assertIsInstance(a[i], Sample)
      self.assertIsInstance(b[i], Sample)
      self.assertSamplesEqual(a[i], b[i], msg=msg)
