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

"""Install, execute, and parse results from YCSB.

YCSB (the Yahoo! Cloud Serving Benchmark) is a common method of comparing NoSQL
database performance.
https://github.com/brianfrankcooper/YCSB

For PerfKitBenchmarker, we wrap YCSB to:

  * Pre-load a database with a fixed number of records.
  * Execute a collection of workloads under a staircase load.
  * Parse the results into PerfKitBenchmarker samples.

The 'YCSBExecutor' class handles executing YCSB on a collection of client VMs.
Generally, clients just need this class. For example, to run against
HBase 1.0:

  >>> executor = ycsb.YCSBExecutor('hbase-10')
  >>> samples = executor.LoadAndRun(loader_vms)

By default, this runs YCSB workloads A and B against the database, 32 threads
per client VM, with an initial database size of 1GB (1k records).
Each workload runs for at most 30 minutes.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import bisect
import collections
import copy
import csv
import itertools
import json
import logging
import math
import operator
import os
import posixpath
import re
import time

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import INSTALL_DIR
import six
from six.moves import filter
from six.moves import range
from six.moves import zip

FLAGS = flags.FLAGS

YCSB_URL_TEMPLATE = ('https://github.com/brianfrankcooper/YCSB/releases/'
                     'download/{0}/ycsb-{0}.tar.gz')
YCSB_DIR = posixpath.join(INSTALL_DIR, 'ycsb')
YCSB_EXE = posixpath.join(YCSB_DIR, 'bin', 'ycsb')
HDRHISTOGRAM_DIR = posixpath.join(INSTALL_DIR, 'hdrhistogram')
HDRHISTOGRAM_TAR_URL = ('https://github.com/HdrHistogram/HdrHistogram/archive/'
                        'HdrHistogram-2.1.10.tar.gz')
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
    'LatencyVariance(ms)': None,
    'AverageLatency(ms)': None,  # Requires both average and # of ops.
    'Throughput(ops/sec)': operator.add,
    '95thPercentileLatency(ms)': None,  # Calculated across clients.
    '99thPercentileLatency(ms)': None,  # Calculated across clients.
    'MinLatency(ms)': min,
    'MaxLatency(ms)': max}


flags.DEFINE_string('ycsb_version', '0.9.0', 'YCSB version to use. Defaults to '
                    'version 0.9.0.')
flags.DEFINE_string('ycsb_tar_url', None, 'URL to a YCSB tarball to use '
                    'instead of the releases located on github.')
flags.DEFINE_enum('ycsb_measurement_type', HISTOGRAM,
                  YCSB_MEASUREMENT_TYPES,
                  'Measurement type to use for ycsb. Defaults to histogram.')
flags.DEFINE_enum('ycsb_measurement_interval', 'op',
                  ['op', 'intended', 'both'],
                  'Measurement interval to use for ycsb. Defaults to op.')
flags.DEFINE_boolean('ycsb_histogram', False, 'Include individual '
                     'histogram results from YCSB (will increase sample '
                     'count).')
flags.DEFINE_boolean('ycsb_load_samples', True, 'Include samples '
                     'from pre-populating database.')
flags.DEFINE_boolean('ycsb_skip_load_stage', False, 'If True, skip the data '
                     'loading staging. It can be used when the database target '
                     'already exists with pre-populated data.')
flags.DEFINE_boolean('ycsb_include_individual_results', False,
                     'Include results from each client VM, rather than just '
                     'combined results.')
flags.DEFINE_boolean('ycsb_reload_database', True,
                     'Reload database, othewise skip load stage. '
                     'Note, this flag is only used if the database '
                     'is already loaded.')
flags.DEFINE_integer('ycsb_client_vms', 1, 'Number of YCSB client VMs.')
flags.DEFINE_list('ycsb_workload_files', ['workloada', 'workloadb'],
                  'Path to YCSB workload file to use during *run* '
                  'stage only. Comma-separated list')
flags.DEFINE_list('ycsb_load_parameters', [],
                  'Passed to YCSB during the load stage. Comma-separated list '
                  'of "key=value" pairs.')
flags.DEFINE_list('ycsb_run_parameters', [],
                  'Passed to YCSB during the load stage. Comma-separated list '
                  'of "key=value" pairs.')
flags.DEFINE_list('ycsb_threads_per_client', ['32'], 'Number of threads per '
                  'loader during the benchmark run. Specify a list to vary the '
                  'number of clients.')
flags.DEFINE_integer('ycsb_preload_threads', None, 'Number of threads per '
                     'loader during the initial data population stage. '
                     'Default value depends on the target DB.')
flags.DEFINE_integer('ycsb_record_count', None, 'Pre-load with a total '
                     'dataset of records total. Overrides recordcount value in '
                     'all workloads of this run. Defaults to None, where '
                     'recordcount value in each workload is used. If neither '
                     'is not set, ycsb default of 0 is used.')
flags.DEFINE_integer('ycsb_operation_count', None, 'Number of operations '
                     '*per client VM*.')
flags.DEFINE_integer('ycsb_timelimit', 1800, 'Maximum amount of time to run '
                     'each workload / client count combination. Set to 0 for '
                     'unlimited time.')
flags.DEFINE_integer('ycsb_field_count', None, 'Number of fields in a record. '
                     'Defaults to None which uses the ycsb default of 10.')
flags.DEFINE_integer('ycsb_field_length', None, 'Size of each field. Defaults '
                     'to None which uses the ycsb default of 100.')
flags.DEFINE_enum('ycsb_requestdistribution',
                  None, ['uniform', 'zipfian', 'latest'],
                  'Type of request distribution.  '
                  'This will overwrite workload file parameter')
flags.DEFINE_float('ycsb_readproportion',
                   None,
                   'The read proportion, '
                   'Default is 0.5 in workloada and 0.95 in YCSB.')
flags.DEFINE_float('ycsb_updateproportion',
                   None,
                   'The update proportion, '
                   'Default is 0.5 in workloada and 0.05 in YCSB.')
flags.DEFINE_float('ycsb_scanproportion',
                   None,
                   'The scan proportion, '
                   'Default is 0 in workloada and 0 in YCSB.')

# Default loading thread count for non-batching backends.
DEFAULT_PRELOAD_THREADS = 32

# Customer YCSB tar url. If not set, the official YCSB release will be used.
_ycsb_tar_url = None


def SetYcsbTarUrl(url):
  global _ycsb_tar_url
  _ycsb_tar_url = url


def _GetVersionIndex(version_str):
  """Returns the version index from ycsb version string.

  Args:
    version_str: ycsb version string with format '0.<version index>.0'.

  Returns:
    (int) version index.
  """
  return int(version_str.split('.')[1])


def _GetThreadsPerLoaderList():
  """Returns the list of client counts per VM to use in staircase load."""
  return [int(thread_count) for thread_count in FLAGS.ycsb_threads_per_client]


def _GetWorkloadFileList():
  """Returns the list of workload files to run.

  Returns:
    In order of preference:
      * The argument to --ycsb_workload_files.
      * Bundled YCSB workloads A and B.
  """
  return [data.ResourcePath(workload)
          for workload in FLAGS.ycsb_workload_files]


def CheckPrerequisites():
  for workload_file in _GetWorkloadFileList():
    if not os.path.exists(workload_file):
      raise IOError('Missing workload file: {0}'.format(workload_file))
  if FLAGS.ycsb_measurement_type == HDRHISTOGRAM:
    if _GetVersionIndex(FLAGS.ycsb_version) < 11:
      raise errors.Config.InvalidValue('hdrhistogram not supported on earlier '
                                       'ycsb versions.')


def _Install(vm):
  """Installs the YCSB and, if needed, hdrhistogram package on the VM."""
  vm.Install('openjdk')
  vm.Install('curl')
  ycsb_url = (_ycsb_tar_url or FLAGS.ycsb_tar_url or
              YCSB_URL_TEMPLATE.format(FLAGS.ycsb_version))
  install_cmd = ('mkdir -p {0} && curl -L {1} | '
                 'tar -C {0} --strip-components=1 -xzf -')
  vm.RemoteCommand(install_cmd.format(YCSB_DIR, ycsb_url))
  if _GetVersionIndex(FLAGS.ycsb_version) >= 11:
    vm.RemoteCommand(install_cmd.format(HDRHISTOGRAM_DIR, HDRHISTOGRAM_TAR_URL))
    vm.RemoteCommand('sudo apt-get --assume-yes install maven > /dev/null 2>&1')
    # _JAVA_OPTIONS needed to work around this issue:
    # https://stackoverflow.com/questions/53010200/maven-surefire-could-not-find-forkedbooter-class
    vm.RemoteCommand('cd {0}; _JAVA_OPTIONS=-Djdk.net.URLClassPath.'
                     'disableClassPathURLCheck=true  '
                     'mvn install > /dev/null 2>&1'.format(
                         HDRHISTOGRAM_DIR))


def YumInstall(vm):
  """Installs the YCSB package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the YCSB package on the VM."""
  _Install(vm)


def ParseResults(ycsb_result_string, data_type='histogram'):
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

    YCSB Client 0.12.0
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

  Example input for ycsb version after 0.13.0:

    ...
    Command line: -db com.yahoo.ycsb.db.HBaseClient10 ... -load
    YCSB Client 0.14.0

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
    data_type: Either 'histogram' or 'timeseries' or 'hdrhistogram'.
      'histogram' and 'hdrhistogram' datasets are in the same format, with the
      difference being lacking the (millisec, count) histogram component. Hence
      are parsed similarly.

  Returns:
    A dictionary with keys:
      client: containing YCSB version information.
      command_line: Command line executed.
      groups: list of operation group descriptions, each with schema:
        group: group name (e.g., update, insert, overall)
        statistics: dict mapping from statistic name to value
        histogram: list of (ms_lower_bound, count) tuples, e.g.:
          [(0, 530), (19, 1)]
        indicates that 530 ops took between 0ms and 1ms, and 1 took between
        19ms and 20ms. Empty bins are not reported.
  Raises:
    IOError: If the results contained unexpected lines.
  """
  # TODO: YCSB 0.9.0 output client and command line string to stderr, so
  # we need to support it in the future.
  lines = []
  client_string = 'YCSB'
  command_line = 'unknown'
  fp = six.StringIO(ycsb_result_string)
  result_string = next(fp).strip()

  def IsHeadOfResults(line):
    return line.startswith('[OVERALL]')

  while not IsHeadOfResults(result_string):
    if result_string.startswith('YCSB Client 0.'):
      client_string = result_string
    if result_string.startswith('Command line:'):
      command_line = result_string
    try:
      result_string = next(fp).strip()
    except StopIteration:
      raise IOError(
          'Could not parse YCSB output: {}'.format(ycsb_result_string))

  if result_string.startswith('[OVERALL]'):  # YCSB > 0.7.0.
    lines.append(result_string)
  else:
    # Received unexpected header
    raise IOError('Unexpected header: {0}'.format(client_string))

  # Some databases print additional output to stdout.
  # YCSB results start with [<OPERATION_NAME>];
  # filter to just those lines.
  def LineFilter(line):
    return re.search(r'^\[[A-Z]+\]', line) is not None

  lines = itertools.chain(lines, filter(LineFilter, fp))

  r = csv.reader(lines)

  by_operation = itertools.groupby(r, operator.itemgetter(0))

  result = collections.OrderedDict([
      ('client', client_string),
      ('command_line', command_line),
      ('groups', collections.OrderedDict())])

  for operation, lines in by_operation:
    operation = operation[1:-1].lower()

    if operation == 'cleanup':
      continue

    op_result = {
        'group': operation,
        data_type: [],
        'statistics': {}
    }
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
          op_result[data_type].append((int(name), val))
      else:
        if '(us)' in name:
          name = name.replace('(us)', '(ms)')
          val /= 1000.0
          latency_unit = 'us'
        op_result['statistics'][name] = val

    result['groups'][operation] = op_result
  return result


def ParseHdrLogFile(logfile):
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
    List of (percent, value, count) tuples
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
      if (percentile > last_percent_value and
          current_total_count > prev_total_count):
        # convert latency to millisec based and percentile to 100 based.
        latency = float(row_vals[0]) / 1000
        count = current_total_count - prev_total_count
        result.append((percentile, latency, count))
        last_percent_value = percentile
        prev_total_count = current_total_count
  return result


def ParseHdrLogs(hdrlogs):
  """Parse a dict of group to hdr logs into a dict of group to histogram tuples.

  Args:
    hdrlogs: Dict of group (read or update) to hdr logs for that group.

  Returns:
    Dict of group to histogram tuples of reportable percentile values.
  """
  parsed_hdr_histograms = {}
  for group, logfile in six.iteritems(hdrlogs):
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
    raise ValueError('Lengths do not match: {0} != {1}'.format(
        len(x), len(weights)))
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


def _CombineResults(result_list, measurement_type, combined_hdr):
  """Combine results from multiple YCSB clients.

  Reduces a list of YCSB results (the output of ParseResults)
  into a single result. Histogram bin counts, operation counts, and throughput
  are summed; RunTime is replaced by the maximum runtime of any result.

  Args:
    result_list: List of ParseResults outputs.
    measurement_type: Measurement type used. If measurement type is histogram,
      histogram bins are summed across results. If measurement type is
      hdrhistogram, an aggregated hdrhistogram (combined_hdr) is expected.
    combined_hdr: Dict of already aggregated histogram.
  Returns:
    A dictionary, as returned by ParseResults.
  """
  def DropUnaggregated(result):
    """Remove statistics which 'operators' specify should not be combined."""
    drop_keys = {k for k, v in six.iteritems(AGGREGATE_OPERATORS) if v is None}
    for group in six.itervalues(result['groups']):
      for k in drop_keys:
        group['statistics'].pop(k, None)

  def CombineHistograms(hist1, hist2):
    h1 = dict(hist1)
    h2 = dict(hist2)
    keys = sorted(frozenset(h1) | frozenset(h2))
    result = []
    for k in keys:
      result.append((k, h1.get(k, 0) + h2.get(k, 0)))
    return result

  combined_weights = {}

  def CombineTimeseries(combined_series, individual_series):
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
          (combined_weight / combined_series[timestamp]) +
          (1.0 / individual_series[timestamp]))
      result.append((timestamp, average_latency))
      combined_weights[timestamp] += 1.0
    return result

  result = copy.deepcopy(result_list[0])
  DropUnaggregated(result)

  for indiv in result_list[1:]:
    for group_name, group in six.iteritems(indiv['groups']):
      if group_name not in result['groups']:
        logging.warn('Found result group "%s" in individual YCSB result, '
                     'but not in accumulator.', group_name)
        result['groups'][group_name] = copy.deepcopy(group)
        continue

      # Combine reported statistics.
      # If no combining operator is defined, the statistic is skipped.
      # Otherwise, the aggregated value is either:
      # * The value in 'indiv', if the statistic is not present in 'result' or
      # * AGGREGATE_OPERATORS[statistic](result_value, indiv_value)
      for k, v in six.iteritems(group['statistics']):
        if k not in AGGREGATE_OPERATORS:
          logging.warn('No operator for "%s". Skipping aggregation.', k)
          continue
        elif AGGREGATE_OPERATORS[k] is None:  # Drop
          result['groups'][group_name]['statistics'].pop(k, None)
          continue
        elif k not in result['groups'][group_name]['statistics']:
          logging.warn('Found statistic "%s.%s" in individual YCSB result, '
                       'but not in accumulator.', group_name, k)
          result['groups'][group_name]['statistics'][k] = copy.deepcopy(v)
          continue

        op = AGGREGATE_OPERATORS[k]
        result['groups'][group_name]['statistics'][k] = (
            op(result['groups'][group_name]['statistics'][k], v))

      if measurement_type == HISTOGRAM:
        result['groups'][group_name][HISTOGRAM] = CombineHistograms(
            result['groups'][group_name][HISTOGRAM],
            group[HISTOGRAM])
      elif measurement_type == TIMESERIES:
        result['groups'][group_name][TIMESERIES] = CombineTimeseries(
            result['groups'][group_name][TIMESERIES],
            group[TIMESERIES])
      else:
        result['groups'][group_name].pop(HISTOGRAM, None)
    result['client'] = ' '.join((result['client'], indiv['client']))
    result['command_line'] = ';'.join((result['command_line'],
                                       indiv['command_line']))
    if 'target' in result and 'target' in indiv:
      result['target'] += indiv['target']

  if measurement_type == HDRHISTOGRAM:
    for group_name in combined_hdr:
      if group_name in result['groups']:
        result['groups'][group_name][HDRHISTOGRAM] = combined_hdr[group_name]

  return result


def _ParseWorkload(contents):
  """Parse a YCSB workload file.

  YCSB workloads are Java .properties format.
  http://en.wikipedia.org/wiki/.properties
  This function does not support all .properties syntax, in particular escaped
  newlines.

  Args:
    contents: str. Contents of the file.

  Returns:
    dict mapping from property key to property value for each property found in
    'contents'.
  """
  fp = six.StringIO(contents)
  result = {}
  for line in fp:
    if (line.strip() and not line.lstrip().startswith('#') and
        not line.lstrip().startswith('!')):
      k, v = re.split(r'\s*[:=]\s*', line, maxsplit=1)
      result[k] = v.strip()
  return result


def _CreateSamples(ycsb_result, include_histogram=False, **kwargs):
  """Create PKB samples from a YCSB result.

  Args:
    ycsb_result: dict. Result of ParseResults.
    include_histogram: bool. If True, include records for each histogram bin.
        Note that this will increase the output volume significantly.
    **kwargs: Base metadata for each sample.

  Yields:
    List of sample.Sample objects.
  """
  stage = 'load' if ycsb_result['command_line'].endswith('-load') else 'run'
  base_metadata = {
      'command_line': ycsb_result['command_line'],
      'stage': stage,
      'ycsb_tar_url': _ycsb_tar_url,
      'ycsb_version': FLAGS.ycsb_version
  }
  base_metadata.update(kwargs)

  for group_name, group in six.iteritems(ycsb_result['groups']):
    meta = base_metadata.copy()
    meta['operation'] = group_name
    for statistic, value in six.iteritems(group['statistics']):
      if value is None:
        continue

      unit = ''
      m = re.match(r'^(.*) *\((us|ms|ops/sec)\)$', statistic)
      if m:
        statistic = m.group(1)
        unit = m.group(2)
      yield sample.Sample(' '.join([group_name, statistic]), value, unit, meta)

    if group.get(HISTOGRAM, []):
      percentiles = _PercentilesFromHistogram(group[HISTOGRAM])
      for label, value in six.iteritems(percentiles):
        yield sample.Sample(' '.join([group_name, label, 'latency']),
                            value, 'ms', meta)
      if include_histogram:
        for time_ms, count in group[HISTOGRAM]:
          yield sample.Sample(
              '{0}_latency_histogram_{1}_ms'.format(group_name, time_ms),
              count, 'count', meta)

    if group.get(HDRHISTOGRAM, []):
      # Strip percentile from the three-element tuples.
      histogram = [value_count[-2:] for value_count in group[HDRHISTOGRAM]]
      percentiles = _PercentilesFromHistogram(histogram)
      for label, value in six.iteritems(percentiles):
        yield sample.Sample(' '.join([group_name, label, 'latency']),
                            value, 'ms', meta)
      if include_histogram:
        histogram = []
        for _, value, bucket_count in group[HDRHISTOGRAM]:
          histogram.append({'microsec_latency': int(value * 1000),
                            'count': bucket_count})
        hist_meta = meta.copy()
        hist_meta.update({'histogram': json.dumps(histogram)})
        yield sample.Sample('{0} latency histogram'.format(group_name),
                            0, '', hist_meta)

    if group.get(TIMESERIES):
      for sample_time, average_latency in group[TIMESERIES]:
        timeseries_meta = meta.copy()
        timeseries_meta['sample_time'] = sample_time
        yield sample.Sample(' '.join([group_name,
                                      'AverageLatency (timeseries)']),
                            average_latency, 'ms', timeseries_meta)


class YCSBExecutor(object):
  """Load data and run benchmarks using YCSB.

  See core/src/main/java/com/yahoo/ycsb/workloads/CoreWorkload.java for
  attribute descriptions.

  Attributes:
    database: str.
    loaded: boolean. If the database is already loaded.
    parameters: dict. May contain the following, plus database-specific fields
      (e.g., columnfamily for HBase).

      threads: int.
      target: int.
      fieldcount: int.
      fieldlengthdistribution: str.
      readallfields: boolean.
      writeallfields: boolean.
      readproportion: float.
      updateproportion: float.
      scanproportion: float.
      readmodifywriteproportion: float.
      requestdistribution: str.
      maxscanlength: int. Number of records to scan.
      scanlengthdistribution: str.
      insertorder: str.
      hotspotdatafraction: float.
      perclientparam: list.
      shardkeyspace: boolean. Default to False, indicates if clients should
      have their own keyspace.
  """

  FLAG_ATTRIBUTES = 'cp', 'jvm-args', 'target', 'threads'

  def __init__(self, database, parameter_files=None, **kwargs):
    self.database = database
    self.loaded = False
    self.measurement_type = FLAGS.ycsb_measurement_type
    self.hdr_dir = HDRHISTOGRAM_DIR

    self.parameter_files = parameter_files or []
    self.parameters = kwargs.copy()
    self.parameters['measurementtype'] = self.measurement_type
    self.parameters['measurement.interval'] = FLAGS.ycsb_measurement_interval

    # Self-defined parameters, pop them out of self.parameters, so they
    # are not passed to ycsb commands
    self.perclientparam = self.parameters.pop('perclientparam', None)
    self.shardkeyspace = self.parameters.pop('shardkeyspace', False)

  def _BuildCommand(self, command_name, parameter_files=None, **kwargs):
    """Builds the YCSB command line."""
    command = [YCSB_EXE, command_name, self.database]

    parameters = self.parameters.copy()
    parameters.update(kwargs)

    # These are passed as flags rather than properties, so they
    # are handled differently.
    for flag in self.FLAG_ATTRIBUTES:
      value = parameters.pop(flag, None)
      if value is not None:
        command.extend(('-{0}'.format(flag), str(value)))

    for param_file in list(self.parameter_files) + list(parameter_files or []):
      command.extend(('-P', param_file))

    for parameter, value in six.iteritems(parameters):
      command.extend(('-p', '{0}={1}'.format(parameter, value)))

    return 'cd %s; %s' % (YCSB_DIR, ' '.join(command))

  @property
  def _default_preload_threads(self):
    """The default number of threads to use for pre-populating the DB."""
    if FLAGS['ycsb_preload_threads'].present:
      return FLAGS.ycsb_preload_threads
    return DEFAULT_PRELOAD_THREADS

  def _Load(self, vm, **kwargs):
    """Execute 'ycsb load' on 'vm'."""
    kwargs.setdefault('threads', self._default_preload_threads)
    if FLAGS.ycsb_record_count:
      kwargs.setdefault('recordcount', FLAGS.ycsb_record_count)
    for pv in FLAGS.ycsb_load_parameters:
      param, value = pv.split('=', 1)
      kwargs[param] = value
    command = self._BuildCommand('load', **kwargs)
    stdout, stderr = vm.RobustRemoteCommand(command)
    return ParseResults(str(stderr + stdout), self.measurement_type)

  def _LoadThreaded(self, vms, workload_file, **kwargs):
    """Runs "Load" in parallel for each VM in VMs.

    Args:
      vms: List of virtual machine instances. client nodes.
      workload_file: YCSB Workload file to use.
      **kwargs: Additional key-value parameters to pass to YCSB.

    Returns:
      List of sample.Sample objects.
    Raises:
      IOError: If number of results is not equal to the number of VMs.
    """
    results = []

    kwargs.setdefault('threads', self._default_preload_threads)
    if FLAGS.ycsb_record_count:
      kwargs.setdefault('recordcount', FLAGS.ycsb_record_count)
    if FLAGS.ycsb_field_count:
      kwargs.setdefault('fieldcount', FLAGS.ycsb_field_count)
    if FLAGS.ycsb_field_length:
      kwargs.setdefault('fieldlength', FLAGS.ycsb_field_length)

    with open(workload_file) as fp:
      workload_meta = _ParseWorkload(fp.read())
      workload_meta.update(kwargs)
      workload_meta.update(stage='load',
                           clients=len(vms) * kwargs['threads'],
                           threads_per_client_vm=kwargs['threads'],
                           workload_name=os.path.basename(workload_file))
      self.workload_meta = workload_meta
    record_count = int(workload_meta.get('recordcount', '1000'))
    n_per_client = int(record_count) // len(vms)
    loader_counts = [
        n_per_client + (1 if i < (record_count % len(vms)) else 0)
        for i in range(len(vms))
    ]

    remote_path = posixpath.join(INSTALL_DIR,
                                 os.path.basename(workload_file))

    def PushWorkload(vm):
      if os.path.basename(remote_path):
        vm.RemoteCommand('sudo rm -f ' + remote_path)
      vm.PushFile(workload_file, remote_path)
    vm_util.RunThreaded(PushWorkload, list(set(vms)))

    kwargs['parameter_files'] = [remote_path]

    def _Load(loader_index):
      start = sum(loader_counts[:loader_index])
      kw = copy.deepcopy(kwargs)
      kw.update(insertstart=start,
                insertcount=loader_counts[loader_index])
      if self.perclientparam is not None:
        kw.update(self.perclientparam[loader_index])
      results.append(self._Load(vms[loader_index], **kw))
      logging.info('VM %d (%s) finished', loader_index, vms[loader_index])

    start = time.time()
    vm_util.RunThreaded(_Load, list(range(len(vms))))
    events.record_event.send(
        type(self).__name__, event='load', start_timestamp=start,
        end_timestamp=time.time(), metadata=copy.deepcopy(kwargs))

    if len(results) != len(vms):
      raise IOError('Missing results: only {0}/{1} reported\n{2}'.format(
          len(results), len(vms), results))

    samples = []
    if FLAGS.ycsb_include_individual_results and len(results) > 1:
      for i, result in enumerate(results):
        samples.extend(_CreateSamples(
            result, result_type='individual', result_index=i,
            include_histogram=FLAGS.ycsb_histogram,
            **workload_meta))

    # hdr histograms not collected upon load, only upon run
    combined = _CombineResults(results, self.measurement_type, {})
    samples.extend(_CreateSamples(
        combined, result_type='combined',
        include_histogram=FLAGS.ycsb_histogram,
        **workload_meta))

    return samples

  def _Run(self, vm, **kwargs):
    """Run a single workload from a client vm."""
    for pv in FLAGS.ycsb_run_parameters:
      param, value = pv.split('=', 1)
      kwargs[param] = value
    command = self._BuildCommand('run', **kwargs)
    # YCSB version greater than 0.7.0 output some of the
    # info we need to stderr. So we have to combine these 2
    # output to get expected results.
    hdr_files_dir = kwargs.get('hdrhistogram.output.path', None)
    if hdr_files_dir:
      vm.RemoteCommand('mkdir -p {0}'.format(hdr_files_dir))
    stdout, stderr = vm.RobustRemoteCommand(command)
    return ParseResults(str(stderr + stdout), self.measurement_type)

  def _RunThreaded(self, vms, **kwargs):
    """Run a single workload using `vms`."""
    target = kwargs.pop('target', None)
    if target is not None:
      target_per_client = target // len(vms)
      targets = [
          target_per_client + (1 if i < (target % len(vms)) else 0)
          for i in range(len(vms))
      ]
    else:
      targets = [target for _ in vms]

    results = []

    if self.shardkeyspace:
      record_count = int(self.workload_meta.get('recordcount', '1000'))
      n_per_client = int(record_count) // len(vms)
      loader_counts = [
          n_per_client + (1 if i < (record_count % len(vms)) else 0)
          for i in range(len(vms))
      ]

    def _Run(loader_index):
      """Run YCSB on an individual VM."""
      vm = vms[loader_index]
      params = copy.deepcopy(kwargs)
      params['target'] = targets[loader_index]
      if self.perclientparam is not None:
        params.update(self.perclientparam[loader_index])
      if self.shardkeyspace:
        start = sum(loader_counts[:loader_index])
        end = start + loader_counts[loader_index]
        params.update(insertstart=start,
                      recordcount=end)
      results.append(self._Run(vm, **params))
      logging.info('VM %d (%s) finished', loader_index, vm)

    vm_util.RunThreaded(_Run, list(range(len(vms))))

    if len(results) != len(vms):
      raise IOError('Missing results: only {0}/{1} reported\n{2}'.format(
          len(results), len(vms), results))

    return results

  def RunStaircaseLoads(self, vms, workloads, **kwargs):
    """Run each workload in 'workloads' in succession.

    A staircase load is applied for each workload file, for each entry in
    ycsb_threads_per_client.

    Args:
      vms: List of VirtualMachine objects to generate load from.
      workloads: List of workload file names.
      **kwargs: Additional parameters to pass to each run.  See constructor for
      options.

    Returns:
      List of sample.Sample objects.
    """
    all_results = []
    parameters = {}
    for workload_index, workload_file in enumerate(workloads):
      if FLAGS.ycsb_operation_count:
        parameters = {'operationcount': FLAGS.ycsb_operation_count}
      if FLAGS.ycsb_record_count:
        parameters['recordcount'] = FLAGS.ycsb_record_count
      if FLAGS.ycsb_field_count:
        parameters['fieldcount'] = FLAGS.ycsb_field_count
      if FLAGS.ycsb_field_length:
        parameters['fieldlength'] = FLAGS.ycsb_field_length
      if FLAGS.ycsb_timelimit:
        parameters['maxexecutiontime'] = FLAGS.ycsb_timelimit
      hdr_files_dir = posixpath.join(self.hdr_dir, str(workload_index))
      if FLAGS.ycsb_measurement_type == HDRHISTOGRAM:
        parameters['hdrhistogram.fileoutput'] = True
        parameters['hdrhistogram.output.path'] = hdr_files_dir
      if FLAGS.ycsb_requestdistribution:
        parameters['requestdistribution'] = FLAGS.ycsb_requestdistribution
      if FLAGS.ycsb_readproportion:
        parameters['readproportion'] = FLAGS.ycsb_readproportion
      if FLAGS.ycsb_updateproportion:
        parameters['updateproportion'] = FLAGS.ycsb_updateproportion
      if FLAGS.ycsb_scanproportion:
        parameters['scanproportion'] = FLAGS.ycsb_scanproportion
      parameters.update(kwargs)
      remote_path = posixpath.join(INSTALL_DIR,
                                   os.path.basename(workload_file))

      with open(workload_file) as fp:
        workload_meta = _ParseWorkload(fp.read())
        workload_meta.update(kwargs)
        workload_meta.update(workload_name=os.path.basename(workload_file),
                             workload_index=workload_index,
                             stage='run')

      def PushWorkload(vm, workload_file, remote_path):
        vm.RemoteCommand('sudo rm -f ' + remote_path)
        vm.PushFile(workload_file, remote_path)
      vm_util.RunThreaded(PushWorkload, [((vm, workload_file, remote_path), {})
                                         for vm in dict.fromkeys(vms)])

      parameters['parameter_files'] = [remote_path]
      for client_count in _GetThreadsPerLoaderList():
        parameters['threads'] = client_count
        start = time.time()
        results = self._RunThreaded(vms, **parameters)
        events.record_event.send(
            type(self).__name__, event='run', start_timestamp=start,
            end_timestamp=time.time(), metadata=copy.deepcopy(parameters))
        client_meta = workload_meta.copy()
        client_meta.update(parameters)
        client_meta.update(clients=len(vms) * client_count,
                           threads_per_client_vm=client_count)

        if FLAGS.ycsb_include_individual_results and len(results) > 1:
          for i, result in enumerate(results):
            all_results.extend(_CreateSamples(
                result,
                result_type='individual',
                result_index=i,
                include_histogram=FLAGS.ycsb_histogram,
                **client_meta))

        if self.measurement_type == HDRHISTOGRAM:
          combined_log = self.CombineHdrHistogramLogFiles(hdr_files_dir, vms)
          parsed_hdr = ParseHdrLogs(combined_log)
          combined = _CombineResults(results, self.measurement_type, parsed_hdr)
        else:
          combined = _CombineResults(results, self.measurement_type, {})
        all_results.extend(_CreateSamples(
            combined, result_type='combined',
            include_histogram=FLAGS.ycsb_histogram,
            **client_meta))

    return all_results

  def CombineHdrHistogramLogFiles(self, hdr_files_dir, vms):
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
      hdr_files_dir: directory on the remote vms where hdr files are stored.
      vms: remote vms

    Returns:
      dict of hdrhistograms keyed by group type
    """
    hdrhistograms = {}
    for grouptype in HDRHISTOGRAM_GROUPS:
      worker_vm = vms[0]
      hdr, _ = worker_vm.RemoteCommand(
          'touch {0}{1}.hdr && tail -1 {0}{1}.hdr'.format(
              hdr_files_dir, grouptype))
      # It's possible that there is no result for certain group, e.g., read
      # only, update only.
      if not hdr:
        continue

      for vm in vms[1:]:
        hdr, _ = vm.RemoteCommand(
            'touch {0}{1}.hdr && tail -1 {0}{1}.hdr'.format(
                hdr_files_dir, grouptype))
        worker_vm.RemoteCommand(
            'sudo chmod 777 {1}{2}.hdr && echo "{0}" >> {1}{2}.hdr'.format(
                hdr[:-1], hdr_files_dir, grouptype))
      hdrhistogram, _ = worker_vm.RemoteCommand(
          'cd {0}; ./HistogramLogProcessor -i {1}{2}.hdr -outputValueUnitRatio '
          '1'.format(self.hdr_dir, hdr_files_dir, grouptype))
      hdrhistograms[grouptype.lower()] = hdrhistogram
    return hdrhistograms

  def Load(self, vms, workloads=None, load_kwargs=None):
    """Load data using YCSB."""
    workloads = workloads or _GetWorkloadFileList()
    load_samples = []
    assert workloads, 'no workloads'
    if FLAGS.ycsb_reload_database or not self.loaded:
      load_samples += list(self._LoadThreaded(
          vms, workloads[0], **(load_kwargs or {})))
      self.loaded = True
    if FLAGS.ycsb_load_samples:
      return load_samples
    else:
      return []

  def Run(self, vms, workloads=None, run_kwargs=None):
    """Runs each workload/client count combination."""
    workloads = workloads or _GetWorkloadFileList()
    assert workloads, 'no workloads'
    return list(self.RunStaircaseLoads(vms, workloads,
                                       **(run_kwargs or {})))

  def LoadAndRun(self, vms, workloads=None, load_kwargs=None, run_kwargs=None):
    """Load data using YCSB, then run each workload/client count combination.

    Loads data using the workload defined by 'workloads', then
    executes YCSB for each workload file in 'workloads', for each
    client count defined in FLAGS.ycsb_threads_per_client.

    Generally database benchmarks using YCSB should only need to call this
    method.

    Args:
      vms: List of virtual machines. VMs to use to generate load.
      workloads: List of strings. Workload files to use. If unspecified,
        _GetWorkloadFileList() is used.
      load_kwargs: dict. Additional arguments to pass to the load stage.
      run_kwargs: dict. Additional arguments to pass to the run stage.
    Returns:
      List of sample.Sample objects.
    """
    load_samples = []
    if not FLAGS.ycsb_skip_load_stage:
      load_samples = self.Load(vms, workloads=workloads,
                               load_kwargs=load_kwargs)
    run_samples = self.Run(vms, workloads=workloads, run_kwargs=run_kwargs)
    return load_samples + run_samples
