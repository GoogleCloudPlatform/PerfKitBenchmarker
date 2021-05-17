# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Records system performance counters during benchmark runs using mpstat.

This collector collects activities for processors using mpstat.
Samples are reported in the form of mpstat_{metric} or mpstat_avg_{metric}.
mpstat_{metric} is the reported {metric} for the given mpstat_interval and
mpstat_count for a specific cpu. The cpu id is reported in the sample metadata.
mpstat_avg_{metric} is the average of {metric} over all cpus.
Currently, only aggregated statistics are reported. Specifically, intr/s, %usr,
%nice, %sys, %iowait, %irq, %soft, %steal, %guest, %idle. Individual stats can
be added later if needed.

Currently reported stats:
%usr: % CPU utilization that occurred while executing at the user level
(application).
%nice: % CPU utilization that occurred while executing at the user level with
nice priority.
%sys: % CPU utilization that occurred while executing at the system level
(kernel). Note that this does not include time spent servicing hardware and
software interrupts.
%iowait: % of time that the CPU or CPUs were idle during which the system had an
outstanding disk I/O request.
%irq: % of time spent by the CPU or CPUs to service hardware interrupts.
%soft: % of time spent by the CPU or CPUs to service software interrupts.
%steal: % of time spent in involuntary wait by the virtual CPU or CPUs while the
hypervisor was servicing another virtual processor.
%guest: % of time spent by the CPU or CPUs to run a virtual processor.
%idle: % of time that the CPU or CPUs were idle and the system did not have an
outstanding disk I/O request.

For more details, see https://linux.die.net/man/1/mpstat.

"""

# TODO(user) Refactor to output and read JSON

import datetime
import logging
import os
from typing import Any, Callable, Dict, List, Optional

from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.traces import base_collector
import six

_MPSTAT = flags.DEFINE_boolean(
    'mpstat', False, 'Run mpstat (https://linux.die.net/man/1/mpstat) '
    'to collect system performance metrics during benchmark run.')
_MPSTAT_BREAKDOWN = flags.DEFINE_enum(
    'mpstat_breakdown', 'SUM', ['SUM', 'CPU', 'ALL'],
    'Level of aggregation for statistics. Accepted '
    'values are "SUM", "CPU", "ALL". Defaults to SUM. See '
    'https://linux.die.net/man/1/mpstat for details.')
_MPSTAT_CPUS = flags.DEFINE_string(
    'mpstat_cpus', 'ALL', 'Comma delimited string of CPU ids or ALL. '
    'Defaults to ALL.')
_MPSTAT_INTERVAL = flags.DEFINE_integer(
    'mpstat_interval', 1,
    'The amount of time in seconds between each mpstat report.'
    'Defaults to 1.')
_MPSTAT_COUNT = flags.DEFINE_integer(
    'mpstat_count', 1, 'The number of reports generated at interval apart.'
    'Defaults to 1.')
_MPSTAT_PUBLISH = flags.DEFINE_boolean(
    'mpstat_publish', False,
    'Whether to publish mpstat statistics.')
_MPSTAT_PUBLISH_PER_INTERVAL_SAMPLES = flags.DEFINE_boolean(
    'mpstat_publish_per_interval_samples', False,
    'Whether to publish a separate mpstat statistics sample '
    'for each interval. If True, --mpstat_publish must be True.')

FLAGS = flags.FLAGS

_TWENTY_THREE_HOURS_IN_SECONDS = 23 * 60 * 60

flags.register_validator(
    _MPSTAT_INTERVAL.name,
    lambda value: value < _TWENTY_THREE_HOURS_IN_SECONDS,
    message=('If --mpstat_interval must be less than 23 hours (if it\'s set '
             'near or above 24 hours, it becomes hard to infer sample '
             'timestamp from mpstat output.'))

flags.register_validator(
    _MPSTAT_PUBLISH_PER_INTERVAL_SAMPLES.name,
    lambda value: FLAGS.mpstat_publish or not value,
    message=('If --mpstat_publish_per_interval is True, --mpstat_publish must '
             'be True.'))


def _ParseStartTime(output: str) -> float:
  """Parse the start time of the mpstat report.

  Args:
    output: output of mpstat

  Returns:
    An integer representing the unix time at which the first sample in the
      report was run.

  Example input:
  Linux 5.10.26-1rodete1-amd64 (wlifferth.c.googlers.com)         2021-05-13
      _x86_64_        (8 CPU)

  16:44:16     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal
      %guest  %gnice   %idle
  16:44:17     all   13.03    0.73   10.96    0.85    0.00    4.99    0.24
      0.00    0.00   69.18

  """
  lines = output.split('\n')
  date = lines[0].split()[3]
  time = lines[2].split()[0]
  start_datetime_string = ' '.join([date, time])
  # As a sysstat utility, this is printed in UTC by default
  start_datetime = datetime.datetime.strptime(
      start_datetime_string,
      '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc)
  return start_datetime.timestamp()


def _ParsePercentageUse(
    rows: List[str],
    metadata: Dict[str, Any],
    timestamp: Optional[float] = None):
  """Parse a CPU percentage use data chunk.

  Args:
    rows: List of mpstat CPU percentage lines.
    metadata: metadata of the sample.
    timestamp: timestamp of the sample.

  Yields:
    List of samples

  input data:
  Average: CPU %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle
  Average: all 1.82 0.11  0.84 0.05    0.00  0.31  0.00   0.00   0.00   96.88
  Average:   0 1.77 0.09  0.82 0.07    0.00  2.21  0.00   0.00   0.00   95.04
  Average:   1 1.85 0.12  0.83 0.06    0.00  0.65  0.00   0.00   0.00   96.49
  ...
  """
  header_row = rows[0]
  headers = [header.strip('%') for header in header_row.split()]
  for row in rows[1:]:
    data = row.split()
    name_value_pairs = list(zip(headers, data))
    cpu_id_pair = name_value_pairs[1]
    for header, value in name_value_pairs[2:]:
      meta = metadata.copy()
      if 'all' in cpu_id_pair:
        metric_name = 'mpstat_avg_' + header
        cpu_id = -1
      else:
        metric_name = 'mpstat_' + header
        cpu_id = int(cpu_id_pair[1])
      meta['mpstat_cpu_id'] = cpu_id
      yield sample.Sample(
          metric=metric_name,
          value=float(value),
          unit='%',
          metadata=meta,
          timestamp=timestamp,
          )


def _ParseInterruptsPerSec(
    rows: List[str],
    metadata: Dict[str, Any],
    timestamp: Optional[float] = None):
  """Parse a interrput/sec data chunk.

  Args:
    rows: List of mpstat interrupts per second lines.
    metadata: metadata of the sample.
    timestamp: timestamp of the sample.

  Yields:
    List of samples

  input data:
  Average:  CPU    intr/s
  Average:  all   3371.98
  Average:    0    268.54
  Average:    1    265.59
  ...
  """
  for row in rows[1:]:  # skipping first header row
    data = row.split()
    meta = metadata.copy()
    if 'all' in data:
      metric_name = 'mpstat_avg_intr'
      cpu_id = -1
    else:
      metric_name = 'mpstat_intr'
      cpu_id = int(data[1])
    meta['mpstat_cpu_id'] = cpu_id
    yield sample.Sample(
        metric=metric_name,
        value=float(data[2]),
        unit='interrupts/sec',
        metadata=meta,
        timestamp=timestamp,
        )


def _GetPerIntervalSamples(
    per_interval_paragraphs: List[List[str]],
    metadata: Dict[str, Any],
    start_timestamp: int,
    interval: int,
    parse_function: Callable[[List[str], Dict[str, Any]],
                             sample.Sample]) -> List[sample.Sample]:
  """Generate samples from a list of list of lines of mpstat output.

  Args:
    per_interval_paragraphs: A list of lists of strings, where each string
      is a line of mpstat output, and each list of strings is a single mpstat
      report.
    metadata: a dictionary of metadata for the sample.
    start_timestamp: a unix timestamp representing the start of the first
      reporting period.
    interval: the interval between mpstat reports
    parse_function: a function that accepts a single mpstat report (list of
      strings) along with metadata and returns a sample generate from that
      report. Should be one of {_ParsePercentageUse, _ParseInterruptsPerSec}.

  Returns:
    a list of samples to publish

  Because individual reports only have time (without a date), here we generate
  the timestamp based on the number of intervals that have passed in order to
  guarantee correct behavior if mpstat is run for more than 1 day.
  """
  samples = []
  # TODO(user) Refactor to read times from mpstat output
  for ordinal, paragraph in enumerate(per_interval_paragraphs):
    sample_timestamp = start_timestamp + (ordinal * interval)
    metadata = metadata.copy()
    metadata['ordinal'] = ordinal
    samples += parse_function(
        paragraph,
        metadata,
        timestamp=sample_timestamp)
  return samples


def _MpstatResults(
    metadata: Dict[str, Any],
    output: str,
    interval: int,
    per_interval_samples: bool = False,
    ):
  """Parses and appends mpstat results to the samples list.

  Args:
    metadata: metadata of the sample.
    output: output of mpstat
    interval: the interval between mpstat reports; required if
      per_interval_samples is True
    per_interval_samples: whether a sample per interval should be published

  Returns:
    List of samples.
  """

  start_timestamp = _ParseStartTime(output)
  samples = []
  percentage_usage_lines_list = []
  interrupts_per_sec_lines_list = []
  paragraphs = output.split('\n\n')

  for paragraph in paragraphs:
    lines = paragraph.rstrip().split('\n')
    if lines and '%irq' in lines[0]:
      if 'Average' in lines[0]:
        samples += _ParsePercentageUse(lines, metadata)
      elif per_interval_samples:
        percentage_usage_lines_list.append(lines)
    elif lines and 'Average' in lines[0] and 'intr/s' in lines[0]:
      if 'Average' in lines[0]:
        samples += _ParseInterruptsPerSec(lines, metadata)
      elif per_interval_samples:
        interrupts_per_sec_lines_list.append(lines)
    elif lines and 'Average' in lines[0]:
      logging.debug('Skipping aggregated metrics: %s', lines[0])

  samples += _GetPerIntervalSamples(
      percentage_usage_lines_list,
      metadata=metadata,
      start_timestamp=start_timestamp,
      interval=interval,
      parse_function=_ParsePercentageUse)

  samples += _GetPerIntervalSamples(
      interrupts_per_sec_lines_list,
      metadata=metadata,
      start_timestamp=start_timestamp,
      interval=interval,
      parse_function=_ParseInterruptsPerSec)

  return samples


class MpstatCollector(base_collector.BaseCollector):
  """mpstat collector.

  Installs and runs mpstat on a collection of VMs.
  """

  def __init__(
      self,
      interval=None,
      output_directory=None,
      per_interval_samples=False):
    super().__init__(interval, output_directory=output_directory)
    self.per_interval_samples = per_interval_samples

  def _CollectorName(self):
    return 'mpstat'

  def _InstallCollector(self, vm):
    vm.InstallPackages('sysstat')

  def _CollectorRunCommand(self, vm, collector_file):
    # We set the environment variable S_TIME_FORMAT=ISO to ensure consistent
    # time formatting from mpstat
    return ('export S_TIME_FORMAT=ISO; mpstat -I {breakdown} -u -P '
            '{processor_number} {interval} {count} > {output} 2>&1 &'.format(
                breakdown=FLAGS.mpstat_breakdown,
                processor_number=FLAGS.mpstat_cpus,
                interval=self.interval,
                count=FLAGS.mpstat_count,
                output=collector_file))

  def Analyze(self, sender, benchmark_spec, samples):
    """Analyze mpstat file and record samples.

    Args:
      sender: event sender for collecting stats.
      benchmark_spec: benchmark_spec of this run.
      samples: samples to add stats to.
    """

    def _Analyze(role, output):
      """Parse file and record samples."""
      with open(
          os.path.join(self.output_directory, os.path.basename(output)),
          'r') as fp:
        output = fp.read()
        metadata = {
            'event': 'mpstat',
            'sender': 'run',
            'role': role,
        }
        samples.extend(
            _MpstatResults(
                metadata,
                output,
                self.interval,
                per_interval_samples=self.per_interval_samples,
                ))

    vm_util.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)])


def Register(parsed_flags):
  """Registers the mpstat collector if FLAGS.mpstat is set."""
  if not parsed_flags.mpstat:
    return

  logging.debug('Registering mpstat collector.')

  collector = MpstatCollector(
      interval=parsed_flags.mpstat_interval,
      per_interval_samples=parsed_flags.mpstat_publish_per_interval_samples)
  events.before_phase.connect(collector.Start, events.RUN_PHASE, weak=False)
  events.after_phase.connect(collector.Stop, events.RUN_PHASE, weak=False)
  if parsed_flags.mpstat_publish:
    events.samples_created.connect(
        collector.Analyze, events.RUN_PHASE, weak=False)
