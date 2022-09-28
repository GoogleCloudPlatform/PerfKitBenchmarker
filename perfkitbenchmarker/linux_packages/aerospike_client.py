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
import os
import re
from typing import List

from absl import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
import pytz

FLAGS = flags.FLAGS
PATH = 'aerospike-tools-7.0.5-ubuntu20.04'
TAR_FILE = f'{PATH}.tgz'
DOWNLOAD_URL = ('https://download.aerospike.com/'
                f'artifacts/aerospike-tools/7.0.5/{TAR_FILE}')
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


def _Install(vm):
  """Installs the aerospike client on the VM."""
  vm.RemoteCommand(f'wget {DOWNLOAD_URL}')
  vm.RemoteCommand('tar xvzf ' + TAR_FILE)
  vm.RemoteCommand(f'cd {PATH}; sudo ./asinstall')


def AptInstall(vm):
  """Installs the aerospike tools on the VM."""
  _Install(vm)


def YumInstall(vm):
  """Installs the aerospike tools on the VM."""
  _Install(vm)


def _ExtractResultLines(lines):
  for line_idx in range(len(lines)):
    if lines[line_idx].startswith(STDOUT_START):  # ignore everyhing before.
      return lines[line_idx + 1:]


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
  result_lines = []
  start_datetime = None
  samples = []
  result_lines = _ExtractResultLines(lines)

  metadata = {}

  for line in result_lines:
    if line.startswith('hdr:'):
      # Each line formatted as:
      # hdr: operation datetime time_window, #requests, min, max, p50, p90, p99,
      # p99.9, p99.99
      values = line.split()
      if not start_datetime:
        start_datetime = datetime.datetime.fromisoformat(  # pylint:disable=g-tzinfo-replace
            values[2][:-1]).replace(tzinfo=pytz.utc)
      op = values[1]
      samples[-1].metadata.update({
          'start_timestamp': start_datetime.timestamp(),
          'window': int(values[3][:-1]),
          f'{op}_min': float(values[5][:-1]),
          f'{op}_max': float(values[6][:-1]),
          f'{op}_p50': float(values[7][:-1]),
          f'{op}_p90': float(values[8][:-1]),
          f'{op}_p99': float(values[9][:-1]),
          f'{op}_p99.9': float(values[10][:-1]),
          f'{op}_p99.99': float(values[11])
      })
    elif 'AEROSPIKE_ERR_TIMEOUT' in line or 'AEROSPIKE_ERR_CONNECTION' in line:
      continue
    else:
      aggregation_metrics = regex_util.ExtractAllFloatMetrics(
          regex_util.ExtractExactlyOneMatch(r'total\(.*\)', line))

      metadata.update(aggregation_metrics)
      samples.append(
          sample.Sample(
              'throughput',
              aggregation_metrics['tps'],
              'transaction_per_second',
              metadata=copy.deepcopy(metadata)))
      metadata.clear()
  return samples


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
    timestamp = s.metadata['window'] + s.metadata['start_timestamp']
    if timestamp not in aggregated_samples:
      aggregated_samples[timestamp] = s
    else:
      new_value = aggregated_samples[timestamp].value + s.value
      current_sample = aggregated_samples[timestamp]
      aggregated_samples[timestamp] = sample.Sample(current_sample.metric,
                                                    new_value,
                                                    current_sample.unit,
                                                    current_sample.metadata,
                                                    current_sample.timestamp)

      def _AggregateMetadata(agg_metadata, metadata):
        # Iterate on the copy of metadata, so we can drop keys at runtime.
        for key, value in copy.deepcopy(agg_metadata).items():
          # find aggregator
          for regex in METADATA_AGGREGATOR:
            if re.search(regex, key):
              agg_metadata[key] = METADATA_AGGREGATOR[regex](value,
                                                             metadata[key])
              break
            else:
              # drop metadata if we do not know how to aggregate
              agg_metadata.pop(key, None)

      _AggregateMetadata(aggregated_samples[timestamp].metadata, s.metadata)

  return list(aggregated_samples.values())


def ParseHistogramFile(output, histograms):
  """Parse a single histogram file generated by asbench.

  Args:
    output: String. Output from histogram file.
    histograms: Dict. A dictionary of histograms parsed.

  Returns:
    A datetime object representing the earliest datetime from the file.
  """
  start_datetime = None
  result_lines = _ExtractResultLines(output.splitlines())
  last_datetime = None
  window = 0
  for line in result_lines:
    values = line.split()
    op = values[0].split('_')[0]
    current_datetime = datetime.datetime.fromisoformat(values[1][:-2])
    if not start_datetime:
      start_datetime = current_datetime
    else:
      start_datetime = min(start_datetime, current_datetime)
    if current_datetime != last_datetime:
      last_datetime = current_datetime
      window += 1
    if (window, op) not in histograms:
      histograms[(window, op)] = collections.OrderedDict()
    latency_histograms = values[4:]
    for latency in latency_histograms:
      bucket, count = latency.split(':')
      bucket = int(bucket)
      count = int(count.split(',')[0])
      histograms[(window, op)][int(bucket)] = histograms[(window, op)].get(
          int(bucket), 0) + int(count)
  return start_datetime


def ParseAsbenchHistogram(result_files):
  """Parse histogram files generated by asbench.

  Args:
    result_files: List of filenames. Each file contains a periodic latency
      histogram from a client vm.

  Returns:
    A list of sample.Sample object. Each representing a latency histogram.
  """
  histograms = {}
  samples = []
  start_datetime = None

  for result_file in result_files:
    with open(os.path.join(vm_util.GetTempDir(), result_file)) as f:
      earliest_datetime = ParseHistogramFile(f.read(), histograms)
      start_datetime = min(
          earliest_datetime,
          start_datetime) if start_datetime else earliest_datetime

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
                'start_timestamp': datetime.datetime.timestamp(start_datetime)
            },
            metric=metric))
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
    self.timestamp = (s.metadata['start_timestamp'] + s.metadata['window'] -
                      1) * 1000
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
  results = []
  for s in samples:
    results.append(AsbenchResult(s))
  results = sorted(results, key=lambda r: r.timestamp)
  rampup_end_time = min(r.timestamp for r in results) + RAMPUP_TIME_IN_MS
  ts_samples = [
      sample.CreateTimeSeriesSample([r.ops for r in results],
                                    [r.timestamp for r in results],
                                    sample.OPS_TIME_SERIES,
                                    'ops',
                                    1,
                                    ramp_up_ends=rampup_end_time,
                                    additional_metadata={})
  ]
  total_ops = sample.Sample('total_ops',
                            sum([r.ops for r in results]) / len(results), 'ops',
                            {})
  if results[0].read_min:
    # the workload does read operations
    ts_samples.extend([
        sample.CreateTimeSeriesSample([r.read_min for r in results],
                                      [r.timestamp for r in results],
                                      f'Read_Min_{sample.LATENCY_TIME_SERIES}',
                                      'us',
                                      1,
                                      ramp_up_ends=rampup_end_time,
                                      additional_metadata={}),
        sample.CreateTimeSeriesSample([r.read_max for r in results],
                                      [r.timestamp for r in results],
                                      f'Read_Max_{sample.LATENCY_TIME_SERIES}',
                                      'us',
                                      1,
                                      ramp_up_ends=rampup_end_time,
                                      additional_metadata={})
    ])
  if results[0].write_min:
    # The workload has write operations
    ts_samples.extend([
        sample.CreateTimeSeriesSample([r.write_min for r in results],
                                      [r.timestamp for r in results],
                                      f'Write_Min_{sample.LATENCY_TIME_SERIES}',
                                      'us',
                                      1,
                                      ramp_up_ends=rampup_end_time,
                                      additional_metadata={}),
        sample.CreateTimeSeriesSample([r.write_max for r in results],
                                      [r.timestamp for r in results],
                                      f'Write_Max_{sample.LATENCY_TIME_SERIES}',
                                      'us',
                                      1,
                                      ramp_up_ends=rampup_end_time,
                                      additional_metadata={})
    ])
  return ts_samples + [total_ops]
