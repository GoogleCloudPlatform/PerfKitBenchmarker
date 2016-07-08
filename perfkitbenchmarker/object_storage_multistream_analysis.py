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

import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import units


class ProcessSynchronizationError(errors.Error):
  """Exception raised when we fail to synchronize processes."""
  pass


class Interval(object):
  """Represents an interval in time.

  Args:
    start: float. A POSIX timestamp.
    duration: float. A length of time, in seconds.
    end: float. A POSIX timestamp.

  Either duration or end must be passed to the constructor.
  """

  def __init__(self, start, duration=None, end=None):
    self.start = start

    if duration is not None:
      if end is not None and start + duration != end:
        raise ValueError(
            'Invalid arguments to interval constructor: %s + %s != %s' %
            (start, duration, end))
      self.duration = duration
    elif end is not None:
      if end < start:
        raise ValueError('Interval (%s, %s) is not valid' % (start, end))
      self.duration = end - start


  @property
  def end(self):
    return self.start + self.duration


  def __eq__(self, other):
    return (isinstance(other, Interval) and
            self.start == other.start and
            self.duration == other.duration)


  def __repr__(self):
    return 'Interval(%s, %s, %s)' % (self.start, self.duration, self.end)


def GetStreamActiveIntervals(start_times, durations, stream_ids):
  """Compute when all streams were active and when any streams were active.

  This function computes two intervals based on the input: 1) the
  interval when *all* streams were doing work, and 2) the interval
  where *any one* of the streams was doing work.

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
    - an Interval describing when any streams were active
    - an Interval describing when all streams were active
  """

  assert start_times.index.equals(durations.index)
  assert start_times.index.equals(stream_ids.index)

  first_start = start_times.min()
  last_end = (start_times + durations).max()

  stream_start_times = start_times.groupby(stream_ids).min()
  stream_end_times = (start_times + durations).groupby(stream_ids).max()
  interval_start = stream_start_times.max()
  interval_end = stream_end_times.min()

  logging.info('Stream ends after stream starts: %s',
               (stream_end_times > stream_start_times).all())
  if interval_end <= interval_start:
    raise ProcessSynchronizationError(
        'No interval when all streams were active.')

  any_stream = Interval(first_start, end=last_end)
  all_streams = Interval(interval_start, end=interval_end)

  return (any_stream, all_streams)


def StreamStartAndEndGaps(start_times, durations, interval):
  """Compute the stream start and stream end timing gaps.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats.
    durations: a pd.Series of durations, as floats measured in seconds.
    interval: Interval. The interval when all streams were active.

  Returns: a tuple of
    - The time between when the first and last streams started.
    - The time between when the first and last streams ended.
  """

  assert start_times.index.equals(durations.index)

  first_start = start_times.min()
  last_end = (start_times + durations).max()

  return interval.start - first_start, last_end - interval.end


def FullyInInterval(start_times, durations, interval):
  """Compute which records are completely inside an interval.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats
    durations: a pd.Series of durations in seconds, as floats
    interval: Interval. The interval to check membership in.

  start_times and durations must have matching indices. Each
  (start_time, duration) pair is considered a record of an operation
  that began at the given start time and continued for the given
  duration.

  Returns: a pd.Series of booleans. An element is True if the
    corresponding record lies completely within the given interval,
    and false otherwise.
  """

  assert start_times.index.equals(durations.index)

  record_ends = start_times + durations
  return (start_times >= interval.start) & (record_ends <= interval.end)


def ThroughputStats(start_times, durations, sizes, stream_ids, num_streams):
  """Compute throughput stats of multiple streams doing operations.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats.
    durations: a pd.Series of durations in seconds, as floats.
    sizes: a pd.Series of bytes.
    stream_ids: a pd.Series of any type.
    num_streams: int. The number of streams.

  start_times, durations, sizes, and stream_ids must have matching indices.
  This function computes the per-stream net throughput (sum of bytes
  transferred / total transfer time) and then adds the results to find
  the overall net throughput. Operations from the same stream cannot
  overlap, but operations from different streams may overlap.

  Returns: a dictionary with keys and values
    - 'net throughput': the net throughput of all streams in the input
    - 'net throughput (with gap)':  net throughput including some benchmark
      overhead

  The values are Quantity objects with appropriate units.
  """

  assert start_times.index.equals(durations.index)
  assert start_times.index.equals(sizes.index)
  assert start_times.index.equals(stream_ids.index)

  bit = units.bit
  sec = units.second

  end_times = start_times + durations

  stream_starts = start_times.groupby(stream_ids).min()
  stream_ends = end_times.groupby(stream_ids).max()

  total_bytes_by_stream = sizes.groupby(stream_ids).sum()
  active_time_by_stream = durations.groupby(stream_ids).sum()
  overall_duration_by_stream = stream_ends - stream_starts

  return {
      'net throughput':
      (total_bytes_by_stream / active_time_by_stream).sum() * 8 * bit / sec,
      'net throughput (with gap)':
      (total_bytes_by_stream /
       overall_duration_by_stream).sum() * 8 * bit / sec}


def GapStats(start_times, durations, stream_ids, interval, num_streams):
  """Compute statistics about operation gaps in an interval.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats.
    durations: a pd.Series of durations in seconds, as floats.
    stream_ids: a pd.Series of any type.
    interval: the interval to compute statistics for.
    num_streams: the total number of streams.

  Returns: a dictionary with keys and values
    - 'total gap time': total time spent not transmitting or receiving data
    - 'gap time proportion': time spent in gaps / total time in benchmark

  The values are Quantity objects with appropriate units.
  """

  assert start_times.index.equals(durations.index)
  assert start_times.index.equals(stream_ids.index)

  sec = units.second
  percent = units.percent

  end_times = start_times + durations

  # True if record overlaps the interval at all, False if not.
  overlaps_interval = ((start_times < interval.end) &
                       (end_times > interval.start))

  # The records that overlap the interval, shunk to be completely in
  # the interval.
  start_or_interval = start_times[overlaps_interval].apply(
      lambda x: max(x, interval.start))
  end_or_interval = end_times[overlaps_interval].apply(
      lambda x: min(x, interval.end))

  total_active_time = (end_or_interval - start_or_interval).sum()
  total_gap_time = interval.duration * num_streams - total_active_time

  return {'total gap time': total_gap_time * sec,
          'gap time proportion':
          float(total_gap_time) /
          (interval.duration * num_streams) *
          100.0 * percent}
