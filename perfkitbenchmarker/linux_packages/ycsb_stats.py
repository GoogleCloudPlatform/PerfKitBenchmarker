# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Parsing results from YCSB output into samples."""

import bisect
import collections
from collections.abc import Iterable, Mapping
import copy
import csv
import dataclasses
import io
import itertools
import json
import logging
import math
import operator
import posixpath
import re
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine

FLAGS = flags.FLAGS

YCSB_URL_TEMPLATE = (
    'https://github.com/brianfrankcooper/YCSB/releases/'
    'download/{0}/ycsb-{0}.tar.gz'
)
YCSB_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'ycsb')
YCSB_EXE = posixpath.join(YCSB_DIR, 'bin', 'ycsb')
HDRHISTOGRAM_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'hdrhistogram')
HDRHISTOGRAM_TAR_URL = (
    'https://github.com/HdrHistogram/HdrHistogram/archive/'
    'HdrHistogram-2.1.10.tar.gz'
)
HDRHISTOGRAM_GROUPS = ['READ', 'UPDATE']

_DEFAULT_PERCENTILES = 50, 75, 90, 95, 99, 99.9

HISTOGRAM = 'histogram'
HDRHISTOGRAM = 'hdrhistogram'
TIMESERIES = 'timeseries'
YCSB_MEASUREMENT_TYPES = [HISTOGRAM, HDRHISTOGRAM, TIMESERIES]

# Binary operators to aggregate reported statistics.
# Statistics with operator 'None' will be dropped.
AGGREGATE_OPERATORS = {
    'Operations': operator.add,
    'RunTime(ms)': max,
    'Return=0': operator.add,
    'Return=-1': operator.add,
    'Return=-2': operator.add,
    'Return=-3': operator.add,
    'Return=OK': operator.add,
    'Return=ERROR': operator.add,
    'Return=NOT_FOUND': operator.add,
    'LatencyVariance(ms)': None,
    'AverageLatency(ms)': None,  # Requires both average and # of ops.
    'Throughput(ops/sec)': operator.add,
    '95thPercentileLatency(ms)': None,  # Calculated across clients.
    '99thPercentileLatency(ms)': None,  # Calculated across clients.
    'MinLatency(ms)': min,
    'MaxLatency(ms)': max,
}

# Status line pattern
_STATUS_PATTERN = r'(\d+) sec: \d+ operations; (\d+.\d+) current ops\/sec'
# Status interval default is 10 sec, change to 1 sec.
_STATUS_INTERVAL_SEC = 1

# Default loading thread count for non-batching backends.
DEFAULT_PRELOAD_THREADS = 32

# Customer YCSB tar url. If not set, the official YCSB release will be used.
_ycsb_tar_url = None

# Parameters for incremental workload. Can be made into flags in the future.
_INCREMENTAL_STARTING_QPS = 500
_INCREMENTAL_TIMELIMIT_SEC = 60 * 5

_ThroughputTimeSeries = dict[int, float]
# Tuple of (percentile, latency, count)
_HdrHistogramTuple = tuple[float, float, int]


@dataclasses.dataclass
class _OpResult:
  """Individual results for a single operation.

  Attributes:
    group: group name (e.g., update, insert, overall)
    statistics: dict mapping from statistic name to value
    data_type: Corresponds to --ycsb_measurement_type.
    data: For HISTOGRAM/HDRHISTOGRAM: list of (ms_lower_bound, count) tuples,
      e.g. [(0, 530), (19, 1)] indicates that 530 ops took between 0ms and 1ms,
      and 1 took between 19ms and 20ms. Empty bins are not reported. For
      TIMESERIES: list of (time, latency us) tuples.
  """

  group: str = ''
  data_type: str = ''
  data: list[tuple[int, float]] = dataclasses.field(default_factory=list)
  statistics: dict[str, float] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class YcsbResult:
  """Aggregate results for the YCSB run.

  Attributes:
    client: Contains YCSB version information.
    command_line: Command line executed.
    throughput_time_series: Time series of throughputs (interval, QPS).
    groups: dict of operation group name to results for that operation.
  """

  client: str = ''
  command_line: str = ''
  throughput_time_series: _ThroughputTimeSeries = dataclasses.field(
      default_factory=dict
  )
  groups: dict[str, _OpResult] = dataclasses.field(default_factory=dict)


def _ValidateErrorRate(result: YcsbResult, threshold: float) -> None:
  """Raises an error if results contains entries with too high error rate.

  Computes the error rate for each operation, example output looks like:

    [INSERT], Operations, 100
    [INSERT], AverageLatency(us), 74.92
    [INSERT], MinLatency(us), 5
    [INSERT], MaxLatency(us), 98495
    [INSERT], 95thPercentileLatency(us), 42
    [INSERT], 99thPercentileLatency(us), 1411
    [INSERT], Return=OK, 90
    [INSERT], Return=ERROR, 10

  This function will then compute 10/100 = 0.1 error rate.

  Args:
    result: The result of running ParseResults()
    threshold: The error rate before throwing an exception. 1.0 means no
      exception will be thrown, 0.0 means an exception is always thrown.

  Raises:
    errors.Benchmarks.RunError: If the computed error rate is higher than the
      threshold.
  """
  for operation in result.groups.values():
    name, stats = operation.group, operation.statistics
    # The operation count can be 0
    count = stats.get('Operations', 0)
    if count == 0:
      continue
    # These keys may be missing from the output.
    error_rate = stats.get('Return=ERROR', 0) / count
    if error_rate > threshold:
      raise errors.Benchmarks.RunError(
          f'YCSB had a {error_rate} error rate for {name}, higher than '
          f'threshold {threshold}'
      )


def ParseResults(
    ycsb_result_string: str,
    data_type: str = 'histogram',
    error_rate_threshold: float = 1.0,
) -> 'YcsbResult':
  """Parse YCSB results.

  Example input for histogram datatype:

    YCSB Client 0.1
    Command line: -db com.yahoo.ycsb.db.HBaseClient -P /tmp/pkb/workloada
    [OVERALL], RunTime(ms), 1800413.0
    [OVERALL], Throughput(ops/sec), 2740.503428935472
    [UPDATE], Operations, 2468054
    [UPDATE], AverageLatency(us), 2218.8513395574005
    [UPDATE], MinLatency(us), 554
    [UPDATE], MaxLatency(us), 352634
    [UPDATE], 95thPercentileLatency(ms), 4
    [UPDATE], 99thPercentileLatency(ms), 7
    [UPDATE], Return=0, 2468054
    [UPDATE], 0, 398998
    [UPDATE], 1, 1015682
    [UPDATE], 2, 532078
    ...

  Example input for hdrhistogram datatype:

    YCSB Client 0.17.0
    Command line: -db com.yahoo.ycsb.db.RedisClient -P /opt/pkb/workloadb
    [OVERALL], RunTime(ms), 29770.0
    [OVERALL], Throughput(ops/sec), 33590.86328518643
    [UPDATE], Operations, 49856.0
    [UPDATE], AverageLatency(us), 1478.0115532734276
    [UPDATE], MinLatency(us), 312.0
    [UPDATE], MaxLatency(us), 24623.0
    [UPDATE], 95thPercentileLatency(us), 3501.0
    [UPDATE], 99thPercentileLatency(us), 6747.0
    [UPDATE], Return=OK, 49856
    ...

  Example input for ycsb version 0.17.0+:

    ...
    Command line: -db com.yahoo.ycsb.db.HBaseClient10 ... -load
    YCSB Client 0.17.0

    Loading workload...
    Starting test.
    ...
    [OVERALL], RunTime(ms), 11411
    [OVERALL], Throughput(ops/sec), 8763.473841030585
    [INSERT], Operations, 100000
    [INSERT], AverageLatency(us), 74.92
    [INSERT], MinLatency(us), 5
    [INSERT], MaxLatency(us), 98495
    [INSERT], 95thPercentileLatency(us), 42
    [INSERT], 99thPercentileLatency(us), 1411
    [INSERT], Return=OK, 100000
    ...

  Example input for timeseries datatype:

    ...
    [OVERALL], RunTime(ms), 240007.0
    [OVERALL], Throughput(ops/sec), 10664.605615669543
    ...
    [READ], Operations, 1279253
    [READ], AverageLatency(us), 3002.7057071587874
    [READ], MinLatency(us), 63
    [READ], MaxLatency(us), 93584
    [READ], Return=OK, 1279281
    [READ], 0, 528.6142757498257
    [READ], 500, 360.95347448674966
    [READ], 1000, 667.7379547689283
    [READ], 1500, 731.5389357265888
    [READ], 2000, 778.7992281717318
    ...

  Args:
    ycsb_result_string: str. Text output from YCSB.
    data_type: Either 'histogram' or 'timeseries' or 'hdrhistogram'. 'histogram'
      and 'hdrhistogram' datasets are in the same format, with the difference
      being lacking the (millisec, count) histogram component. Hence are parsed
      similarly.
    error_rate_threshold: Error statistics in the output should not exceed this
      ratio.

  Returns:
    A YcsbResult object that contains the results from parsing YCSB output.
  Raises:
    IOError: If the results contained unexpected lines.
  """
  if (
      'redis.clients.jedis.exceptions.JedisConnectionException'
      in ycsb_result_string
  ):
    # This error is cause by ycsb using an old version of redis client 2.9.0
    # https://github.com/xetorthio/jedis/issues/1977
    raise errors.Benchmarks.KnownIntermittentError(
        'errors.Benchmarks.KnownIntermittentError'
    )

  lines = []
  client_string = 'YCSB'
  command_line = 'unknown'
  throughput_time_series = {}
  fp = io.StringIO(ycsb_result_string)
  result_string = next(fp).strip()

  def IsHeadOfResults(line):
    return line.startswith('[OVERALL]')

  while not IsHeadOfResults(result_string):
    if result_string.startswith('YCSB Client 0.'):
      client_string = result_string
    if result_string.startswith('Command line:'):
      command_line = result_string
    # Look for status lines which include throughput on a 1-sec basis.
    match = re.search(_STATUS_PATTERN, result_string)
    if match is not None:
      timestamp, qps = int(match.group(1)), float(match.group(2))
      # Repeats in the printed status are erroneous, ignore.
      if timestamp not in throughput_time_series:
        throughput_time_series[timestamp] = qps
    try:
      result_string = next(fp).strip()
    except StopIteration:
      raise IOError(
          f'Could not parse YCSB output: {ycsb_result_string}'
      ) from None

  if result_string.startswith('[OVERALL]'):  # YCSB > 0.7.0.
    lines.append(result_string)
  else:
    # Received unexpected header
    raise IOError(f'Unexpected header: {client_string}')

  # Some databases print additional output to stdout.
  # YCSB results start with [<OPERATION_NAME>];
  # filter to just those lines.
  def LineFilter(line):
    return re.search(r'^\[[A-Z]+\]', line) is not None

  lines = itertools.chain(lines, filter(LineFilter, fp))

  r = csv.reader(lines)

  by_operation = itertools.groupby(r, operator.itemgetter(0))

  result = YcsbResult(
      client=client_string,
      command_line=command_line,
      throughput_time_series=throughput_time_series,
  )

  for operation, lines in by_operation:
    operation = operation[1:-1].lower()

    if operation == 'cleanup':
      continue

    op_result = _OpResult(group=operation, data_type=data_type)
    latency_unit = 'ms'
    for _, name, val in lines:
      name = name.strip()
      val = val.strip()
      # Drop ">" from ">1000"
      if name.startswith('>'):
        name = name[1:]
      val = float(val) if '.' in val or 'nan' in val.lower() else int(val)
      if name.isdigit():
        if val:
          if data_type == TIMESERIES and latency_unit == 'us':
            val /= 1000.0
          op_result.data.append((int(name), val))
      else:
        if '(us)' in name:
          name = name.replace('(us)', '(ms)')
          val /= 1000.0
          latency_unit = 'us'
        op_result.statistics[name] = val

    result.groups[operation] = op_result
  _ValidateErrorRate(result, error_rate_threshold)
  return result


def ParseHdrLogFile(logfile: str) -> list[_HdrHistogramTuple]:
  """Parse a hdrhistogram log file into a list of (percentile, latency, count).

  Example decrypted hdrhistogram logfile (value measures latency in microsec):

  #[StartTime: 1523565997 (seconds since epoch), Thu Apr 12 20:46:37 UTC 2018]
       Value     Percentile TotalCount 1/(1-Percentile)

     314.000 0.000000000000          2           1.00
     853.000 0.100000000000      49955           1.11
     949.000 0.200000000000     100351           1.25
     1033.000 0.300000000000     150110           1.43
     ...
     134271.000 0.999998664856    1000008      748982.86
     134271.000 0.999998855591    1000008      873813.33
     201983.000 0.999999046326    1000009     1048576.00
  #[Mean    =     1287.159, StdDeviation   =      667.560]
  #[Max     =   201983.000, Total count    =      1000009]
  #[Buckets =            8, SubBuckets     =         2048]

  Example of output:
     [(0, 0.314, 2), (10, 0.853, 49953), (20, 0.949, 50396), ...]

  Args:
    logfile: Hdrhistogram log file.

  Returns:
    List of (percentile, value, count) tuples
  """
  result = []
  last_percent_value = -1
  prev_total_count = 0
  for row in logfile.split('\n'):
    if re.match(r'( *)(\d|\.)( *)', row):
      row_vals = row.split()
      # convert percentile to 100 based and round up to 3 decimal places
      percentile = math.floor(float(row_vals[1]) * 100000) / 1000.0
      current_total_count = int(row_vals[2])
      if (
          percentile > last_percent_value
          and current_total_count > prev_total_count
      ):
        # convert latency to millisec based and percentile to 100 based.
        latency = float(row_vals[0]) / 1000
        count = current_total_count - prev_total_count
        result.append((percentile, latency, count))
        last_percent_value = percentile
        prev_total_count = current_total_count
  return result


def ParseHdrLogs(
    hdrlogs: Mapping[str, str]
) -> dict[str, list[_HdrHistogramTuple]]:
  """Parse a dict of group to hdr logs into a dict of group to histogram tuples.

  Args:
    hdrlogs: Dict of group (read or update) to hdr logs for that group.

  Returns:
    Dict of group to histogram tuples of reportable percentile values.
  """
  parsed_hdr_histograms = {}
  for group, logfile in hdrlogs.items():
    values = ParseHdrLogFile(logfile)
    parsed_hdr_histograms[group] = values
  return parsed_hdr_histograms


def _CumulativeSum(xs):
  total = 0
  for x in xs:
    total += x
    yield total


def _WeightedQuantile(x, weights, p):
  """Weighted quantile measurement for an ordered list.

  This method interpolates to the higher value when the quantile is not a direct
  member of the list. This works well for YCSB, since latencies are floored.

  Args:
    x: List of values.
    weights: List of numeric weights.
    p: float. Desired quantile in the interval [0, 1].

  Returns:
    float.

  Raises:
    ValueError: When 'x' and 'weights' are not the same length, or 'p' is not in
      the interval [0, 1].
  """
  if len(x) != len(weights):
    raise ValueError(
        'Lengths do not match: {0} != {1}'.format(len(x), len(weights))
    )
  if p < 0 or p > 1:
    raise ValueError('Invalid quantile: {0}'.format(p))
  n = sum(weights)
  target = n * float(p)
  cumulative = list(_CumulativeSum(weights))

  # Find the first cumulative weight >= target
  i = bisect.bisect_left(cumulative, target)
  if i == len(x):
    return x[-1]
  else:
    return x[i]


def _PercentilesFromHistogram(ycsb_histogram, percentiles=_DEFAULT_PERCENTILES):
  """Calculate percentiles for from a YCSB histogram.

  Args:
    ycsb_histogram: List of (time_ms, frequency) tuples.
    percentiles: iterable of floats, in the interval [0, 100].

  Returns:
    dict, mapping from percentile to value.
  Raises:
    ValueError: If one or more percentiles are outside [0, 100].
  """
  result = collections.OrderedDict()
  histogram = sorted(ycsb_histogram)
  for percentile in percentiles:
    if percentile < 0 or percentile > 100:
      raise ValueError('Invalid percentile: {0}'.format(percentile))
    if math.modf(percentile)[0] < 1e-7:
      percentile = int(percentile)
    label = 'p{0}'.format(percentile)
    latencies, freqs = list(zip(*histogram))
    time_ms = _WeightedQuantile(latencies, freqs, percentile * 0.01)
    result[label] = time_ms
  return result


def CombineResults(
    result_list: Iterable[YcsbResult],
    measurement_type: str,
    combined_hdr: Mapping[str, list[_HdrHistogramTuple]],
):
  """Combine results from multiple YCSB clients.

  Reduces a list of YCSB results (the output of ParseResults)
  into a single result. Histogram bin counts, operation counts, and throughput
  are summed; RunTime is replaced by the maximum runtime of any result.

  Args:
    result_list: Iterable of ParseResults outputs.
    measurement_type: Measurement type used. If measurement type is histogram,
      histogram bins are summed across results. If measurement type is
      hdrhistogram, an aggregated hdrhistogram (combined_hdr) is expected.
    combined_hdr: Dict of already aggregated histogram.

  Returns:
    A dictionary, as returned by ParseResults.
  """

  def DropUnaggregated(result: YcsbResult) -> None:
    """Remove statistics which 'operators' specify should not be combined."""
    drop_keys = {k for k, v in AGGREGATE_OPERATORS.items() if v is None}
    for group in result.groups.values():
      for k in drop_keys:
        group.statistics.pop(k, None)

  def CombineHistograms(hist1, hist2):
    h1 = dict(hist1)
    h2 = dict(hist2)
    keys = sorted(frozenset(h1) | frozenset(h2))
    result = []
    for k in keys:
      result.append((k, h1.get(k, 0) + h2.get(k, 0)))
    return result

  combined_weights = {}

  def _CombineLatencyTimeSeries(
      combined_series: list[tuple[int, float]],
      individual_series: list[tuple[int, float]],
  ) -> list[tuple[int, float]]:
    """Combines two timeseries of average latencies.

    Args:
      combined_series: A list representing the timeseries with which the
        individual series is being merged.
      individual_series: A list representing the timeseries being merged with
        the combined series.

    Returns:
      A list representing the new combined series.

    Note that this assumes that each individual timeseries spent an equal
    amount of time executing requests for each timeslice. This should hold for
    runs without -target where each client has an equal number of threads, but
    may not hold otherwise.
    """
    combined_series = dict(combined_series)
    individual_series = dict(individual_series)
    timestamps = set(combined_series) | set(individual_series)

    result = []
    for timestamp in sorted(timestamps):
      if timestamp not in individual_series:
        continue
      if timestamp not in combined_weights:
        combined_weights[timestamp] = 1.0
      if timestamp not in combined_series:
        result.append((timestamp, individual_series[timestamp]))
        continue

      # This computes a new combined average latency by dividing the sum of
      # request latencies by the sum of request counts for the time period.
      # The sum of latencies for an individual series is assumed to be "1",
      # so the sum of latencies for the combined series is the total number of
      # series i.e. "combined_weight".
      # The request count for an individual series is 1 / average latency.
      # This means the request count for the combined series is
      # combined_weight * 1 / average latency.
      combined_weight = combined_weights[timestamp]
      average_latency = (combined_weight + 1.0) / (
          (combined_weight / combined_series[timestamp])
          + (1.0 / individual_series[timestamp])
      )
      result.append((timestamp, average_latency))
      combined_weights[timestamp] += 1.0
    return result

  def _CombineThroughputTimeSeries(
      series1: _ThroughputTimeSeries, series2: _ThroughputTimeSeries
  ) -> _ThroughputTimeSeries:
    """Returns a combined dict of [timestamp, total QPS] from the two series."""
    timestamps1 = set(series1)
    timestamps2 = set(series2)
    all_timestamps = timestamps1 | timestamps2
    diff_timestamps = timestamps1 ^ timestamps2
    if diff_timestamps:
      # This case is rare but does happen occassionally, so log a warning
      # instead of raising an exception.
      logging.warning(
          'Expected combined timestamps to be the same, got different '
          'timestamps: %s',
          diff_timestamps,
      )
    result = {}
    for timestamp in all_timestamps:
      result[timestamp] = series1.get(timestamp, 0) + series2.get(timestamp, 0)
    return result

  result_list = list(result_list)
  result = copy.deepcopy(result_list[0])
  DropUnaggregated(result)

  for indiv in result_list[1:]:
    for group_name, group in indiv.groups.items():
      if group_name not in result.groups:
        logging.warning(
            'Found result group "%s" in individual YCSB result, '
            'but not in accumulator.',
            group_name,
        )
        result.groups[group_name] = copy.deepcopy(group)
        continue

      # Combine reported statistics.
      # If no combining operator is defined, the statistic is skipped.
      # Otherwise, the aggregated value is either:
      # * The value in 'indiv', if the statistic is not present in 'result' or
      # * AGGREGATE_OPERATORS[statistic](result_value, indiv_value)
      for k, v in group.statistics.items():
        if k not in AGGREGATE_OPERATORS:
          logging.warning('No operator for "%s". Skipping aggregation.', k)
          continue
        elif AGGREGATE_OPERATORS[k] is None:  # Drop
          result.groups[group_name].statistics.pop(k, None)
          continue
        elif k not in result.groups[group_name].statistics:
          logging.warning(
              'Found statistic "%s.%s" in individual YCSB result, '
              'but not in accumulator.',
              group_name,
              k,
          )
          result.groups[group_name].statistics[k] = copy.deepcopy(v)
          continue

        op = AGGREGATE_OPERATORS[k]
        result.groups[group_name].statistics[k] = op(
            result.groups[group_name].statistics[k], v
        )

      if measurement_type == HISTOGRAM:
        result.groups[group_name].data = CombineHistograms(
            result.groups[group_name].data, group.data
        )
      elif measurement_type == TIMESERIES:
        result.groups[group_name].data = _CombineLatencyTimeSeries(
            result.groups[group_name].data, group.data
        )
    result.client = ' '.join((result.client, indiv.client))
    result.command_line = ';'.join((result.command_line, indiv.command_line))

    # if _THROUGHPUT_TIME_SERIES.value:
    result.throughput_time_series = _CombineThroughputTimeSeries(
        result.throughput_time_series, indiv.throughput_time_series
    )

  if measurement_type == HDRHISTOGRAM:
    for group_name in combined_hdr:
      if group_name in result.groups:
        result.groups[group_name].data = combined_hdr[group_name]

  return result


def CombineHdrHistogramLogFiles(
    hdr_install_dir: str,
    hdr_files_dir: str,
    vms: Iterable[virtual_machine.VirtualMachine],
) -> dict[str, str]:
  """Combine multiple hdr histograms by group type.

  Combine multiple hdr histograms in hdr log files format into 1 human
  readable hdr histogram log file.
  This is done by
  1) copying hdrhistogram log files to a single file on a worker vm;
  2) aggregating file containing multiple %-tile histogram into
     a single %-tile histogram using HistogramLogProcessor from the
     hdrhistogram package that is installed on the vms. Refer to https://
     github.com/HdrHistogram/HdrHistogram/blob/master/HistogramLogProcessor

  Args:
    hdr_install_dir: directory where HistogramLogProcessor is located.
    hdr_files_dir: directory on the remote vms where hdr files are stored.
    vms: remote vms

  Returns:
    dict of hdrhistograms keyed by group type
  """
  vms = list(vms)
  hdrhistograms = {}
  for grouptype in HDRHISTOGRAM_GROUPS:

    def _GetHdrHistogramLog(vm, group=grouptype):
      filename = f'{hdr_files_dir}{group}.hdr'
      return vm.RemoteCommand(f'touch {filename} && tail -1 {filename}')[0]

    results = background_tasks.RunThreaded(_GetHdrHistogramLog, vms)

    # It's possible that there is no result for certain group, e.g., read
    # only, update only.
    if not all(results):
      continue

    worker_vm = vms[0]
    for hdr in results[1:]:
      worker_vm.RemoteCommand(
          'sudo chmod 755 {1}{2}.hdr && echo "{0}" >> {1}{2}.hdr'.format(
              hdr[:-1], hdr_files_dir, grouptype
          )
      )
    hdrhistogram, _ = worker_vm.RemoteCommand(
        'cd {0} && ./HistogramLogProcessor -i {1}{2}.hdr'
        ' -outputValueUnitRatio 1'.format(
            hdr_install_dir, hdr_files_dir, grouptype
        )
    )
    hdrhistograms[grouptype.lower()] = hdrhistogram
  return hdrhistograms


def CreateSamples(
    ycsb_result: YcsbResult,
    ycsb_version: str,
    include_histogram: bool = False,
    include_command_line=True,
    **kwargs,
) -> list[sample.Sample]:
  """Create PKB samples from a YCSB result.

  Args:
    ycsb_result: Result of ParseResults.
    ycsb_version: The version of YCSB used to run the tests.
    include_histogram: If True, include records for each histogram bin. Note
      that this will increase the output volume significantly.
    include_command_line: If True, include command line in metadata. Note that
      this makes sample output much longer if there are multiple client VMs.
    **kwargs: Base metadata for each sample.

  Yields:
    List of sample.Sample objects.
  """
  command_line = ycsb_result.command_line
  stage = 'load' if command_line.endswith('-load') else 'run'
  base_metadata = {
      'stage': stage,
      'ycsb_tar_url': _ycsb_tar_url,
      'ycsb_version': ycsb_version,
  }
  if include_command_line:
    base_metadata['command_line'] = command_line
  base_metadata.update(kwargs)

  throughput_time_series = ycsb_result.throughput_time_series
  if throughput_time_series:
    yield sample.Sample(
        'Throughput Time Series',
        0,
        '',
        {'throughput_time_series': sorted(throughput_time_series.items())},
    )

  for group_name, group in ycsb_result.groups.items():
    meta = base_metadata.copy()
    meta['operation'] = group_name
    for statistic, value in group.statistics.items():
      if value is None:
        continue

      unit = ''
      m = re.match(r'^(.*) *\((us|ms|ops/sec)\)$', statistic)
      if m:
        statistic = m.group(1)
        unit = m.group(2)
      yield sample.Sample(' '.join([group_name, statistic]), value, unit, meta)

    if group.data and group.data_type == HISTOGRAM:
      percentiles = _PercentilesFromHistogram(group.data)
      for label, value in percentiles.items():
        yield sample.Sample(
            ' '.join([group_name, label, 'latency']), value, 'ms', meta
        )
      if include_histogram:
        for time_ms, count in group.data:
          yield sample.Sample(
              '{0}_latency_histogram_{1}_ms'.format(group_name, time_ms),
              count,
              'count',
              meta,
          )

    if group.data and group.data_type == HDRHISTOGRAM:
      # Strip percentile from the three-element tuples.
      histogram = [value_count[-2:] for value_count in group.data]
      percentiles = _PercentilesFromHistogram(histogram)
      for label, value in percentiles.items():
        yield sample.Sample(
            ' '.join([group_name, label, 'latency']), value, 'ms', meta
        )
      if include_histogram:
        histogram = []
        for _, value, bucket_count in group.data:
          histogram.append(
              {'microsec_latency': int(value * 1000), 'count': bucket_count}
          )
        hist_meta = meta.copy()
        hist_meta.update({'histogram': json.dumps(histogram)})
        yield sample.Sample(
            '{0} latency histogram'.format(group_name), 0, '', hist_meta
        )

    if group.data and group.data_type == TIMESERIES:
      for sample_time, average_latency in group.data:
        timeseries_meta = meta.copy()
        timeseries_meta['sample_time'] = sample_time
        yield sample.Sample(
            ' '.join([group_name, 'AverageLatency (timeseries)']),
            average_latency,
            'ms',
            timeseries_meta,
        )
      yield sample.Sample(
          'Average Latency Time Series',
          0,
          '',
          {'latency_time_series': group.data},
      )
