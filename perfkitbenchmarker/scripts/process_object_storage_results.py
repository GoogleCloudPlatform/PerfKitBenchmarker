#!/usr/bin/python

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

"""Process the output of the object storage benchmarking script."""

import logging
import json
import sys

import gflags
import pandas as pd

FLAGS = gflags.FLAGS

gflags.DEFINE_enum('operation', None, ['upload', 'download'],
                   'Whether the results were for uploads or downloads.')
gflags.DEFINE_list('sizes', None,
                   'The sizes of the objects in the benchmark, in bytes.')
gflags.DEFINE_integer('num_vms', None,
                      'The number of VMs used for the benchmark.')
gflags.DEFINE_integer('streams_per_vm', None,
                      'The number of independent streams per VM.')
gflags.DEFINE_integer('objects_per_stream', None,
                      'The number of objects that each stream sent or '
                      'received.')

# If the gap between different stream starts and ends is above a
# certain proportion of the total time, we log a warning because we
# are throwing out a lot of information. We also put the warning in
# the sample metadata.
MULTISTREAM_STREAM_GAP_THRESHOLD = 0.2

LATENCY_UNIT = 'seconds'

# Percentiles to output
PERCENTILES_LIST = [0.1, 1, 5, 10, 50, 90, 95, 99, 99.9]


class ProcessSynchronizationError(Exception):
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


def _AppendPercentilesToResults(output_results, input_results, metric_name,
                                metric_unit, metadata):
  """Compute percentiles of the input and append them to the output list.

  Args:
    output_results: a list. Results will be appended here.
    input_results: a pandas.Series.
    metric_name: a string.
    metric_unit: a string.
    metadata: a dictionary.
  """

  # Allowing empty input makes semantic sense here.
  if len(input_results) == 0:
    return

  for percentile in PERCENTILES_LIST:
    value = input_results.quantile(percentile / 100.0)
    output_results.append({
        'metric': '%s p%s' % (metric_name, percentile),
        'value': value,
        'unit': metric_unit,
        'metadata': metadata})


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

  The values are numbers measuring bits per second.
  """

  assert start_times.index.equals(durations.index)
  assert start_times.index.equals(sizes.index)
  assert start_times.index.equals(stream_ids.index)

  end_times = start_times + durations

  stream_starts = start_times.groupby(stream_ids).min()
  stream_ends = end_times.groupby(stream_ids).max()

  total_bytes_by_stream = sizes.groupby(stream_ids).sum()
  active_time_by_stream = durations.groupby(stream_ids).sum()
  overall_duration_by_stream = stream_ends - stream_starts

  return {
      'net throughput':
      (total_bytes_by_stream / active_time_by_stream).sum() * 8,
      'net throughput (with gap)':
      (total_bytes_by_stream /
       overall_duration_by_stream).sum() * 8}


def GapStats(start_times, durations, stream_ids, interval, num_streams):
  """Compute statistics about operation gaps in an interval.

  Args:
    start_times: a pd.Series of POSIX timestamps, as floats.
    durations: a pd.Series of durations in seconds, as floats.
    stream_ids: a pd.Series of any type.
    interval: the interval to compute statistics for.
    num_streams: the total number of streams.

  Returns: a tuple of
    - total time spent not transmitting or receiving, in seconds
    - time in gaps / total benchmark time
  """

  assert start_times.index.equals(durations.index)
  assert start_times.index.equals(stream_ids.index)

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

  return (total_gap_time,
          float(total_gap_time) /
          (interval.duration * num_streams))


def Main(argv):
  """Read and process results from the api_multistream worker process.

  Results will be reported per-object size and combined for all
  objects.

  Args:
    argv: all remaining arguments from the command line. These should
    be this script, followed by a list of files to process.
  """

  if len(argv) < 2:
    raise ValueError('Must specify at least one results file.')

  num_streams = FLAGS.streams_per_vm * FLAGS.num_vms
  operation = FLAGS.operation
  sizes = [int(size) for size in FLAGS.sizes]

  metadata = {}
  metadata['num_streams'] = num_streams
  metadata['objects_per_stream'] = (
      FLAGS.objects_per_stream)

  records = pd.DataFrame({'operation': [],
                          'start_time': [],
                          'latency': [],
                          'size': [],
                          'stream_num': []})
  for file in argv[1:]:
    proc_json = json.load(open(file, 'r'))
    records = records.append(pd.DataFrame(proc_json))
  records = records.reset_index()

  logging.info('Records:\n%s', records)
  logging.info('All latencies positive:%s',
               (records['latency'] > 0).all())

  # Results will be a list of dictionaries with fields metric
  # (string), value (float), unit (string), and metadata (dict).
  results = []

  any_streams_active, all_streams_active = GetStreamActiveIntervals(
      records['start_time'], records['latency'], records['stream_num'])
  start_gap, stop_gap = StreamStartAndEndGaps(
      records['start_time'], records['latency'], all_streams_active)
  if ((start_gap + stop_gap) / any_streams_active.duration <
      MULTISTREAM_STREAM_GAP_THRESHOLD):
    logging.info(
        'First stream started %s seconds before last stream started', start_gap)
    logging.info(
        'Last stream ended %s seconds after first stream ended', stop_gap)
  else:
    logging.warning(
        'Difference between first and last stream start/end times was %s and '
        '%s, which is more than %s of the benchmark time %s.',
        start_gap, stop_gap, MULTISTREAM_STREAM_GAP_THRESHOLD,
        any_streams_active.duration)
    metadata['stream_gap_above_threshold'] = True

  records_in_interval = records[
      FullyInInterval(records['start_time'],
                      records['latency'],
                      all_streams_active)]

  # Don't publish the full distribution in the metadata because doing
  # so might break regexp-based parsers that assume that all metadata
  # values are simple Python objects. However, do add an
  # 'object_size_B' metadata field even for the full results because
  # searching metadata is easier when all records with the same metric
  # name have the same set of metadata fields.
  distribution_metadata = metadata.copy()
  distribution_metadata['object_size_B'] = 'distribution'

  latency_prefix = 'Multi-stream %s latency' % operation
  logging.info('Processing %s multi-stream %s results for the full '
               'distribution.', len(records_in_interval), operation)
  _AppendPercentilesToResults(
      results,
      records_in_interval['latency'],
      latency_prefix,
      LATENCY_UNIT,
      distribution_metadata)

  logging.info('Processing %s multi-stream %s results for net throughput',
               len(records), operation)
  throughput_stats = ThroughputStats(
      records_in_interval['start_time'],
      records_in_interval['latency'],
      records_in_interval['size'],
      records_in_interval['stream_num'],
      num_streams)
  # A special throughput statistic that uses all the records, not
  # restricted to the interval.
  throughput_stats['net throughput (simplified)'] = (
      records['size'].sum() * 8 / any_streams_active.duration)
  for name, value in throughput_stats.iteritems():
    results.append({
        'metric': 'Multi-stream ' + operation + ' ' + name,
        'value': value,
        'unit': 'bits / second',
        'metadata': distribution_metadata})

  gap_stats = GapStats(
      records['start_time'],
      records['latency'],
      records['stream_num'],
      all_streams_active,
      num_streams)
  logging.info('Benchmark overhead was %s percent of total benchmark time',
               gap_stats[1])

  results.append({
      'metric': 'Multi-stream ' + operation + ' total gap time',
      'value': gap_stats[0],
      'unit': 'seconds',
      'metadata': distribution_metadata})
  results.append({
      'metric': 'Multi-stream ' + operation + ' gap time percent',
      'value': gap_stats[1] * 100,
      'unit': 'percent',
      'metadata': distribution_metadata})

  # QPS metrics
  results.append({
      'metric': 'Multi-stream ' + operation + ' QPS (any stream active)',
      'value': len(records) / any_streams_active.duration,
      'unit': 'operations / second',
      'metadata': distribution_metadata})
  results.append({
      'metric': 'Multi-stream ' + operation + ' QPS (all streams active)',
      'value': len(records_in_interval) / all_streams_active.duration,
      'unit': 'operations / second',
      'metadata': distribution_metadata})

  # Publish by-size and full-distribution stats even if there's only
  # one size in the distribution, because it simplifies postprocessing
  # of results.
  for size in sizes:
    this_size_records = records_in_interval[records_in_interval['size'] == size]
    this_size_metadata = metadata.copy()
    this_size_metadata['object_size_B'] = size
    logging.info('Processing %s multi-stream %s results for object size %s',
                 len(records), operation, size)
    _AppendPercentilesToResults(
        results,
        this_size_records['latency'],
        latency_prefix,
        LATENCY_UNIT,
        this_size_metadata)

  json.dump(results, sys.stdout, indent=0)


if __name__ == '__main__':
  Main(FLAGS(sys.argv))
