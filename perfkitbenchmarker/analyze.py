# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Functions for analyzing output from benchmarks.

In general, this module works on data in the form of Pandas data
frames, because that seems to be the most standard Python statistical
data format.
"""

import pandas as pd

from perfkitbenchmarker import sample


def AllStreamsInterval(start_times, durations, stream_ids):
  """Compute when all streams were active from multistream records.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats.
    durations: a pd.Series of durations, as floats measured in seconds.
    stream_ids: a pd.Series of any type.

  All three arguments must have matching indices. Each (start_time,
  duration, stream_id) triple is considered a record of a single
  operation performed by the given stream. This function computes the
  interval from when the last stream to start began its first
  operation to when the first stream to stop finished its last
  operation.

  Returns: a tuple of
    - a float holding the POSIX timestamp when the last stream to
      start began its first operation
    - a float holding the length of time in seconds from the first
      return value until the first proceess to end stopped its last
      operation
  """

  stream_start_times = start_times.groupby(stream_ids).min()
  stream_end_times = (start_times + durations).groupby(stream_ids).max()
  interval_start = stream_start_times.max()
  interval_end = stream_end_times.min()

  return interval_start, interval_end - interval_start


def StreamStartAndEndGaps(start_times, durations,
                          interval_start, interval_duration):
  """Compute the stream start and stream end timing gaps.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats.
    durations: a pd.Series of durations, as floats measured in seconds.
    interval_start: float. The POSIX timestamp when the last stream started.
    interval_duration: float. The time in seconds that all streams were active.

  Returns: a tuple of
    - The time between when the first and last streams started.
    - The time between when the first and last streams ended.
  """

  interval_end = interval_start + interval_duration
  first_start = start_times.min()
  last_end = (start_times + durations).max()

  return interval_start - first_start, last_end - interval_end


def FullyInInterval(start_times, durations, interval_start, interval_duration):
  """Compute which records are completely inside an interval.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats
    durations: a pd.Series of durations in seconds, as floats
    interval_start: the POSIX timestamp of the interval start, as a float
    interval_duration: the duration of the interval in seconds, as a float

  start_times and durations must have matching indices. Each
  (start_time, duration) pair is considered a record of an operation
  that began at the given start time and continued for the given
  duration.

  Returns: a pd.Series of booleans. An element is True if the
    corresponding record lies completely within the given interval,
    and false otherwise.
  """

  interval_end = interval_start + interval_duration
  record_ends = start_times + durations

  return (start_times >= interval_start) & (record_ends <= interval_end)


def AllStreamsThroughputStats(durations, sizes, stream_ids,
                              num_streams, interval_duration):
  """Compute the net throughput of multiple streams doing operations.

  Args:
    durations: a pd.Series of durations in seconds, as floats.
    sizes: a pd.Series of bytes.
    stream_ids: a pd.Series of any type.
    num_streams: int. The number of streams.
    interval_duration: a float. The time all streams were active, in seconds.

  durations, sizes, and stream_ids must have matching indices. This
  function computes the per-stream net throughput (sum of bytes
  transferred / total transfer time) and then adds the results to find
  the overall net throughput. Operations from the same stream cannot
  overlap, but operations from different streams may overlap.

  Returns: a tuple of
    - The net throughput of all streams, excluding times they were inactive.
    - The throughput of all streams, including times they were inactive.
    - The total time that all streams were inactive.
    - The total time all streams were inactive, as a proportion of the
      total benchmark time.
  """

  total_bytes_by_stream = sizes.groupby(stream_ids).sum()
  active_time_by_stream = durations.groupby(stream_ids).sum()
  total_overhead = interval_duration * num_streams - active_time_by_stream.sum()

  return ((total_bytes_by_stream / active_time_by_stream).sum(),
          total_bytes_by_stream.sum() / interval_duration,
          total_overhead,
          total_overhead / (interval_duration * num_streams))


def SummaryStats(series, name_prefix=''):

  """Compute some summary statistics for a series.

  Args:
    series: a pd.Series of floats.
    name_prefix: if given, a prefix for the statistic names.

  Returns: a pd.Series with summary statistics.
  """

  # Percentiles 0, 1, 2, ..., 100
  percentiles = range(0, 101, 1)
  # range() and xrange() don't accept floating-point arguments, so
  # add 99.1, 99.2, ..., 99.9 ourselves.
  for i in xrange(1, 10):
    percentiles.append(99 + i / 10.0)

  # TODO: use series.describe() to simplify this once we upgrade to a
  # version of pandas where describe() has the 'percentiles' keyword
  # argument.
  result = {}
  values = sorted(series)
  count = len(values)

  for percentile in percentiles:
    name = name_prefix + 'p' + str(percentile)
    val = values[int((count - 1) * float(percentile) / 100.0)]
    result[name] = val

  result[name_prefix + 'min'] = values[0]
  result[name_prefix + 'max'] = values[-1]
  result[name_prefix + 'median'] = result[name_prefix + 'p50']
  result[name_prefix + 'mean'] = sum(values) / float(count)

  sum_of_squares = sum([num ** 2 for num in values])
  result[name_prefix + 'stddev'] = (sum_of_squares / (count - 1)) ** 0.5

  return pd.Series(result)


def AppendStatsAsSamples(series, unit, samples_list,
                         name_prefix=None, timestamps=None, metadata=None):
  """Append statistics about a series to a list as sample.Samples.

  Args:
    series: a pd.Series of floats.
    unit: string. Passed to sample.Sample.
    samples_list: the list of sample.Samples to append to.
    name_prefix: if given, a prefix for the statistic names.
    timestamps: if given, a pd.Series of floats, used as Sample timestamps.
    metadata: if given, extra metadata for the sample.Samples.

  If timestamps is given, its index must match the index of series.
  """

  stats = SummaryStats(series, name_prefix=name_prefix)

  for name, value in stats.iteritems():
    samples_list.append(sample.Sample(
        name, value, unit,
        timestamp=timestamps[name] if timestamps is not None else None,
        metadata=metadata))
