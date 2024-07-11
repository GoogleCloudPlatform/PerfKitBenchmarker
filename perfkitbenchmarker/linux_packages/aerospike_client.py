# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing aerospike tools installation and cleanup functions."""

import collections
import copy
import dataclasses
import datetime
import re
from typing import Any, List

from absl import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

# https://download.aerospike.com/artifacts/aerospike-tools/7.0.5/
AEROSPIKE_TOOL_VERSION_NAME_FOR_OS = {
    os_types.UBUNTU2004: 'ubuntu20.04',
    os_types.RHEL8: 'el8',
}

DEFAULT_PKG_VERSION = '10.2.1'

_AEROSPIKE_TOOLS_VERSION = flags.DEFINE_string(
    'aerospike_tools_version',
    DEFAULT_PKG_VERSION,
    'Aerospike tools version to use'
)

PATH_FORMATTER = 'aerospike-tools_%s_%s_x86_64'
TAR_FILE_FORMATTER = '%s.tgz'

DOWNLOAD_URL_PREFIX = (
    'https://download.aerospike.com/artifacts/aerospike-tools/%s'
)

STDOUT_START = 'Stage 1: default config'
SUM = lambda x, y: x + y
METADATA_AGGREGATOR = {
    'start_timestamp': min,
    'window': max,
    'min': min,
    'max': max,
    'tps': SUM,
    'timeouts': SUM,
    'errors': SUM,
}
RAMPUP_TIME_IN_MS = 60 * 1000

# Randomly picked number, where the line of namespace field should locate.
ASBENCH_HEADER_LINES = 20


@dataclasses.dataclass(frozen=True)
class HistogramLine:
  """Class for keeping track of a Histogram Line."""

  operation: str  # Write / read
  buckets: List[int]
  counts: List[int]
  time: datetime.datetime
  duration: float = 0
  windows: int = 0


def _Install(vm):
  """Installs the aerospike client on the VM."""
  if FLAGS.os_type not in AEROSPIKE_TOOL_VERSION_NAME_FOR_OS:
    raise ValueError(
        f'Unsupported OS type: {FLAGS.os_type}. Supported OS types are: '
        + ', '.join(AEROSPIKE_TOOL_VERSION_NAME_FOR_OS.keys())
    )
  path = PATH_FORMATTER % (
      FLAGS.aerospike_tools_version,
      AEROSPIKE_TOOL_VERSION_NAME_FOR_OS[FLAGS.os_type],
  )
  tar_file = TAR_FILE_FORMATTER % path
  url = '/'.join(
      [DOWNLOAD_URL_PREFIX % FLAGS.aerospike_tools_version, tar_file]
  )
  vm.Install('wget')
  vm.RemoteCommand(f'wget {url}')
  vm.RemoteCommand('tar xvzf ' + tar_file)
  vm.RemoteCommand(f'cd {path}; sudo ./asinstall')


def AptInstall(vm):
  """Installs the aerospike tools on the VM."""
  _Install(vm)


def YumInstall(vm):
  """Installs the aerospike tools on the VM."""
  _Install(vm)


def _ExtractResultLines(lines):
  for line_idx in range(len(lines)):
    if lines[line_idx].startswith(STDOUT_START):  # ignore everyhing before.
      return lines[line_idx + 1 :]


def ParseDate(date: str) -> datetime.datetime:
  """Parse the datetime object."""
  return datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ').replace(
      tzinfo=datetime.timezone.utc
  )


def ParseAsbenchStdout(output):
  """Parse output produced by asbench.

  Output format:
  port:                   3000
  user:                   (null)
  services-alternate:     false
  ...
  2022-07-26 21:27:59.941 INFO Add node BB97300800A0142 10.128.0.115:3000
  2022-07-26 21:27:59.943 INFO Start 128 transaction threads
  Stage 1: default config (specify your own with --workload-stages)

  2022-07-26 21:28:00.943 INFO write(tps=10209 timeouts=0 errors=0)
  read(tps=91085 timeouts=0 errors=0) total(tps=101294 timeouts=0 errors=0)
  hdr: write 2022-07-26T21:28:00Z 1, 10210, 299, 42207, 1221, 1470, 1729, 2925,
  23023
  hdr: read  2022-07-26T21:28:00Z 1, 91120, 142, 41471, 1217, 1475, 1730, 2097,
  22575

  Args:
    output: string. Stdout from asbench command.

  Returns:
    A list of sample.Sample object.
  """
  lines = output.splitlines()
  namespace = None
  for line_idx in range(min(ASBENCH_HEADER_LINES, len(lines))):
    if lines[line_idx].lower().startswith('namespace'):
      namespace = lines[line_idx].split(':')[1].strip()
      break
  if not namespace:
    raise ValueError('Namespace not found.')

  samples = []
  result_lines = _ExtractResultLines(lines)

  metadata = {}
  line_idx = 0
  while line_idx < len(result_lines):
    line = result_lines[line_idx]
    if (
        'AEROSPIKE_ERR_TIMEOUT' in line
        or 'AEROSPIKE_ERR_CONNECTION' in line
        or 'AEROSPIKE_ERR_SERVER_FULL' in line
    ):
      continue
    elif 'Add node' in line or 'Remove node' in line:
      continue
    find_read_hdr = False
    find_write_hdr = False
    if ' read(' in line:
      _ParseHdrLines(
          result_lines, line_idx, samples, metadata, namespace, 'read'
      )
    if ' write(' in line:
      _ParseHdrLines(
          result_lines, line_idx, samples, metadata, namespace, 'write'
      )
    line_idx += find_read_hdr + find_write_hdr + 1
  return samples


def _ParseHdrLines(
    lines: str,
    line_idx: int,
    samples: List[sample.Sample],
    metadata: dict[str, float],
    namespace: str,
    op: str,
):
  """Parse a hdr line generated by asbench."""
  # Each line formatted as:
  # hdr: operation datetime time_window (or seconds-running in the newer version
  # ), #requests, min, max, p50, p90, p99, p99.9, p99.99
  aggregation_metrics = regex_util.ExtractAllFloatMetrics(
      regex_util.ExtractExactlyOneMatch(
          rf'{op}\(.*?\) (?:total|read|write)', lines[line_idx]
      )
  )
  metadata.update(aggregation_metrics)
  find_op_hdr = False
  for hdr_idx in range(2):
    hdr_line = lines[line_idx + hdr_idx + 1]
    if f'hdr: {op}' in hdr_line:
      metadata.update(_ParseMetadata(hdr_line))
      find_op_hdr = True
      break
  if find_op_hdr:
    samples.append(
        sample.Sample(
            namespace + f'_{op}',
            aggregation_metrics['tps'],
            'transaction_per_second',
            metadata=copy.deepcopy(metadata),
        )
    )
  metadata.clear()


def _ParseMetadata(line: str):
  # Each line formatted as:
  # hdr: operation datetime time_window (or seconds-running in the newer version
  # ), #requests, min, max, p50, p90, p99, p99.9, p99.99
  values = line.split()
  start_datetime = ParseDate(values[2])
  op = values[1]
  return {
      'start_timestamp': start_datetime.timestamp(),
      'window': 1,  # default to 1 seconds
      f'{op}_min': float(values[5][:-1]),
      f'{op}_max': float(values[6][:-1]),
      f'{op}_p50': float(values[7][:-1]),
      f'{op}_p90': float(values[8][:-1]),
      f'{op}_p99': float(values[9][:-1]),
      f'{op}_p99.9': float(values[10][:-1]),
      f'{op}_p99.99': float(values[11]),
  }


def AggregateAsbenchSamples(raw_samples):
  """Aggregate samples across client VMs.

  Args:
    raw_samples: List of sample.Sample object. Each sample produced by a
      different vm for a particular window.

  Returns:
    A list of sample.Sample objects.
  """
  aggregated_samples = {}
  for s in raw_samples:
    # In case 2 vms had slightly different start_timestamp, making sure
    # samples merged always have same start_timestamp + window
    timestamp = s.metadata['start_timestamp']
    agg_key = '%s-%s' % (timestamp, s.metric)
    if agg_key not in aggregated_samples:
      aggregated_samples[agg_key] = s
    else:
      new_value = aggregated_samples[agg_key].value + s.value
      current_sample = aggregated_samples[agg_key]
      aggregated_samples[agg_key] = sample.Sample(
          current_sample.metric,
          new_value,
          current_sample.unit,
          current_sample.metadata,
          current_sample.timestamp,
      )

      def _AggregateMetadata(agg_metadata, metadata):
        # Iterate on the copy of metadata, so we can drop keys at runtime.
        for key, value in copy.deepcopy(agg_metadata).items():
          # find aggregator
          for regex in METADATA_AGGREGATOR:
            if re.search(regex, key):
              agg_metadata[key] = METADATA_AGGREGATOR[regex](
                  value, metadata[key]
              )
              break

      _AggregateMetadata(aggregated_samples[agg_key].metadata, s.metadata)

  return list(aggregated_samples.values())


def ParseHistogramLine(line: str) -> HistogramLine:
  """Parse a single histogram line generated by asbench."""
  # A sample line
  # write_hist 2023-03-22T23:33:13Z, 1.0001s, 1040, 0:330, 100:262, 500:9
  values = line.split()
  op = values[0].split('_')[0]
  time = ParseDate(values[1][:-1])
  duration = float(values[2][:-2])
  latency_histograms = values[4:]
  buckets = []
  counts = []
  for latency in latency_histograms:
    bucket, count = latency.split(':')
    buckets.append(int(bucket))
    counts.append(int(count.split(',')[0]))
  return HistogramLine(op, buckets, counts, time, duration, 0)


def ParseHistogramFile(
    output: str,
    histograms: collections.OrderedDict[Any, Any],
    timestamps: dict[int, datetime.date],
) -> None:
  """Parse a single histogram file generated by asbench.

  Args:
    output: String. Output from histogram file.
    histograms: Dict. A dictionary of histograms parsed.
    timestamps: Dict, A dictionary to keep track of the timestamps.
  """
  result_lines = _ExtractResultLines(output.splitlines())
  window_count_for_operation = collections.defaultdict(int)
  for line in result_lines:
    histogram_line = ParseHistogramLine(line)
    if not histogram_line.counts or histogram_line.duration < 0.01:
      continue
    op = histogram_line.operation
    # Index starts at 1
    window_count_for_operation[op] += 1
    window = window_count_for_operation[op]
    if (window, op) not in histograms:
      histograms[(window, op)] = collections.OrderedDict()

    if window in timestamps:
      timestamps[window] = min(timestamps[window], histogram_line.time)
    else:
      timestamps[window] = histogram_line.time

    for i in range(len(histogram_line.buckets)):
      latency = histogram_line.buckets[i]
      histograms[(window, op)][latency] = (
          histograms[(window, op)].get(int(latency), 0)
          + histogram_line.counts[i]
      )


def ParseAsbenchHistogramTimeSeries(output: str) -> List[HistogramLine]:
  """Generate Percentile Time Series from histogram output.

  Args:
    output: Output from histogram file.

  Returns:
   List of histogram.
  """
  result_lines = _ExtractResultLines(output.splitlines())
  histogram_time_series = []
  for line in result_lines:
    histogram_time_series.append(ParseHistogramLine(line))
  return histogram_time_series


def GenerateDetailHistogramSamples(
    histograms: collections.OrderedDict[Any, Any],
    start_datetime: datetime.datetime,
) -> List[sample.Sample]:
  """Generate Histogram samples based on histograms."""
  samples = []
  for window, op in histograms:
    metric = f'{op}_histogram'
    samples.append(
        sample.CreateHistogramSample(
            histogram=histograms[(window, op)],
            name=metric,
            subname=f'{metric}_{window}',
            units='usec',
            additional_metadata={
                'window': window,
                'op': op,
                'start_timestamp': datetime.datetime.timestamp(start_datetime),
            },
            metric=metric,
        )
    )
  return samples


def GetMaxLatencyGivenBucket(bucket: int) -> int:
  """Get the maximum latency of a given bucket."""
  # https://docs.aerospike.com/tools/asbench
  if bucket < 4000:
    return bucket + 100
  elif bucket < 64000:
    return bucket + 1000
  else:
    return bucket + 4000


def CalculatePercentileFromHistogram(
    histogram: collections.OrderedDict[Any, Any], percentile: float
) -> int:
  """Get the latency from a histogram on a given percentile."""
  total_count = sum(histogram.values())
  index_given_percentile = int(percentile * total_count / 100.0 + 0.5)
  latency_buckets = sorted(histogram.keys())
  if index_given_percentile >= total_count:
    return GetMaxLatencyGivenBucket(max(histogram.keys()))

  current_index = 0
  bucket_index = 0
  while current_index < index_given_percentile:
    current_index += histogram[latency_buckets[bucket_index]]
    bucket_index += 1

  return GetMaxLatencyGivenBucket(latency_buckets[bucket_index - 1])


def GeneratePercentileTimeSeriesSamples(
    histograms: collections.OrderedDict[Any, Any],
    percentiles: List[str],
    timestamps: dict[int, datetime.datetime],
) -> List[sample.Sample]:
  """Generate Percentile Time Series samples based on histograms."""
  percentile_results = collections.defaultdict(list)
  timestamps_in_us = []
  for i in sorted(timestamps.keys()):
    timestamps_in_us.append(datetime.datetime.timestamp(timestamps[i]) * 1000)
  samples = []
  for window, op in histograms:
    for percentile in percentiles:
      percentile_results[(op, percentile)].append(
          CalculatePercentileFromHistogram(
              histograms[(window, op)], float(percentile)
          )
          / 1000,
      )
  for op, percentile in percentile_results:
    samples.append(
        sample.CreateTimeSeriesSample(
            percentile_results[(op, percentile)],
            timestamps_in_us,
            op + '_' + percentile + '_percentile_latency_time_series',
            'ms',
            1,
            additional_metadata={},
        )
    )
  return samples


def ParseAsbenchHistogram(result_files: List[str]) -> List[sample.Sample]:
  """Parse histogram files generated by asbench.

  Args:
    result_files: List of filenames. Each file contains a periodic latency
      histogram from a client vm.

  Returns:
    A list of sample.Sample object. Each representing a latency histogram.
  """
  histograms = collections.OrderedDict()
  timestamps = {}
  samples = []

  for result_file in result_files:
    with open(vm_util.PrependTempDir(result_file)) as f:
      ParseHistogramFile(f.read(), histograms, timestamps)
  if FLAGS.aerospike_publish_detailed_samples:
    samples += GenerateDetailHistogramSamples(histograms, timestamps[0])
  if FLAGS.aerospike_publish_percentile_time_series:
    percentiles = FLAGS.aerospike_percentiles_to_capture
    samples += GeneratePercentileTimeSeriesSamples(
        histograms, percentiles, timestamps
    )
  return samples


@dataclasses.dataclass
class AsbenchResult:
  """Class that represents Asbench results."""

  ops: float
  timestamp: float
  read_min: float
  read_max: float
  write_min: float
  write_max: float

  def __init__(self, s: sample.Sample):
    super(AsbenchResult, self).__init__()
    self.ops = s.value
    self.timestamp = (
        s.metadata['start_timestamp'] + s.metadata['window'] - 1
    ) * 1000
    self.read_min = s.metadata.get('read_min', None)
    self.read_max = s.metadata.get('read_max', None)
    self.write_min = s.metadata.get('write_min', None)
    self.write_max = s.metadata.get('write_max', None)


def CreateTimeSeriesSample(samples: List[sample.Sample]) -> List[sample.Sample]:
  """Create time series samples from a list of per time window sample.

  Args:
    samples: A list of sample.Sample. Each sample represent throughput, latency
      collected in that time window.

  Returns:
    A list of time series samples, where each sample encodes a list of values
      for the entire run.
  """
  sample_results = {}
  for s in samples:
    ns_op = s.metric
    sample_results.setdefault(ns_op, []).append(AsbenchResult(s))

  ts_samples = []
  total_ops = []
  total_ops_value = 0.0
  for ns_op in sample_results:
    sample_results[ns_op] = sorted(
        sample_results[ns_op], key=lambda r: r.timestamp
    )
    results = sample_results[ns_op]
    rampup_end_time = min(r.timestamp for r in results) + RAMPUP_TIME_IN_MS
    ts_samples.append(
        sample.CreateTimeSeriesSample(
            [r.ops for r in results],
            [r.timestamp for r in results],
            ns_op + '_client_tps',
            'ops',
            1,
            ramp_up_ends=rampup_end_time,
            additional_metadata={},
        )
    )
    if results[0].read_min:
      # the workload does read operations
      ts_samples.extend([
          sample.CreateTimeSeriesSample(
              [r.read_min for r in results],
              [r.timestamp for r in results],
              f'Read_Min_{sample.LATENCY_TIME_SERIES}',
              'us',
              1,
              ramp_up_ends=rampup_end_time,
              additional_metadata={},
          ),
          sample.CreateTimeSeriesSample(
              [r.read_max for r in results],
              [r.timestamp for r in results],
              f'Read_Max_{sample.LATENCY_TIME_SERIES}',
              'us',
              1,
              ramp_up_ends=rampup_end_time,
              additional_metadata={},
          ),
      ])
    total_ops_value += sum([r.ops for r in results]) / len(results)
    if results[0].write_min:
      # The workload has write operations
      ts_samples.extend([
          sample.CreateTimeSeriesSample(
              [r.write_min for r in results],
              [r.timestamp for r in results],
              f'Write_Min_{sample.LATENCY_TIME_SERIES}',
              'us',
              1,
              ramp_up_ends=rampup_end_time,
              additional_metadata={},
          ),
          sample.CreateTimeSeriesSample(
              [r.write_max for r in results],
              [r.timestamp for r in results],
              f'Write_Max_{sample.LATENCY_TIME_SERIES}',
              'us',
              1,
              ramp_up_ends=rampup_end_time,
              additional_metadata={},
          ),
      ])
  total_ops.append(sample.Sample(
      'total_ops', total_ops_value, 'ops', {}
  ))
  return ts_samples + total_ops
