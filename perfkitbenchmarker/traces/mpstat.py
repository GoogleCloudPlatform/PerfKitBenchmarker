# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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


import datetime
import json
import logging
import os
from typing import Any, Dict, List, Optional

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
    third_party/py/perfkitbenchmarker/tests/data/mpstat_output.json

  """
  hosts = output['sysstat']['hosts']
  date = hosts[0]['date']
  time = hosts[0]['statistics'][0]['timestamp']
  # TODO(user): handle malformed json output from mpstat
  start_datetime_string = ' '.join([date, time])
  # As a sysstat utility, this is printed in UTC by default
  start_datetime = datetime.datetime.strptime(
      start_datetime_string,
      '%Y-%m-%d %H:%M:%S').replace(tzinfo=datetime.timezone.utc)
  return start_datetime.timestamp()


def _GetCPUMetrics(host_stats):
  """Generate list of metrics that we want to publish.

  Args:
    host_stats: List of mpstat reports.

  Returns:
    List of metrics that we want to publish.
  """
  cpu_metrics = []
  for cpu_metric in host_stats[0]['cpu-load'][0]:
    # we don't want to generate a sample for cpu - cpu_id.
    if cpu_metric == 'cpu':
      continue
    cpu_metrics.append(cpu_metric)
  return cpu_metrics


def _GetCPUAverageMetrics(
    host_stats: List[Dict[str, Any]],
    number_of_cpus: int,
    metadata: Dict[str, Any],
    timestamp: Optional[float] = None):
  """Get average metrics for all CPUs.

  Args:
    host_stats: List of mpstat reports.
    number_of_cpus: how many CPUs are being used.
    metadata: metadata of the sample.
    timestamp: timestamp of the sample.

  Returns:
    List of samples - containing the average metrics for all CPUs.

  input data:
  [
    {
      "timestamp": "22:05:29",
      "cpu-load": [
        {"cpu": "-1", "usr": 100.00, "nice": 0.00, "sys": 0.00, "iowait": 0.00,
        "irq": 0.00, "soft": 0.00, "steal": 0.00, "guest": 0.00, "gnice": 0.00,
        "idle": 0.00},
        {"cpu": "0", "usr": 100.00, "nice": 0.00, "sys": 0.00, "iowait": 0.00,
        "irq": 0.00, "soft": 0.00, "steal": 0.00, "guest": 0.00, "gnice": 0.00,
        "idle": 0.00},
        {"cpu": "1", "usr": 100.00, "nice": 0.00, "sys": 0.00, "iowait": 0.00,
        "irq": 0.00, "soft": 0.00, "steal": 0.00, "guest": 0.00, "gnice": 0.00,
        "idle": 0.00}
      ]
      ...
    }, {
      "timestamp": "22:05:31",
      "cpu-load": [
        {"cpu": "-1", "usr": 100.00, "nice": 0.00, "sys": 0.00, "iowait": 0.00,
        "irq": 0.00, "soft": 0.00, "steal": 0.00, "guest": 0.00, "gnice": 0.00,
        "idle": 0.00},
        {"cpu": "0", "usr": 100.00, "nice": 0.00, "sys": 0.00, "iowait": 0.00,
        "irq": 0.00, "soft": 0.00, "steal": 0.00, "guest": 0.00, "gnice": 0.00,
        "idle": 0.00},
        {"cpu": "1", "usr": 100.00, "nice": 0.00, "sys": 0.00, "iowait": 0.00,
        "irq": 0.00, "soft": 0.00, "steal": 0.00, "guest": 0.00, "gnice": 0.00,
        "idle": 0.00}
      ]
      ...
    }
  ]
  """
  samples = []
  cpu_metrics = _GetCPUMetrics(host_stats)
  for cpu_id in range(-1, number_of_cpus):
    for cpu_metric in cpu_metrics:
      measurements = []
      for report in host_stats:
        value = report['cpu-load'][cpu_id + 1][cpu_metric]
        measurements.append(value)
      average = sum(measurements) / len(measurements)
      metric_name = 'mpstat_avg_' + cpu_metric
      meta = metadata.copy()
      meta['mpstat_cpu_id'] = cpu_id
      samples.append(sample.Sample(
          metric=metric_name,
          value=average,
          unit='%',
          metadata=meta,
          timestamp=timestamp))
  return samples


def _GetCPUAverageInterruptions(
    host_stats: List[Dict[str, Any]],
    number_of_cpus: int,
    metadata: Dict[str, Any],
    timestamp: Optional[float] = None):
  """Get average interruption for all CPUs.

  Args:
    host_stats: List of mpstat reports.
    number_of_cpus: how many CPUs are being used.
    metadata: metadata of the sample.
    timestamp: timestamp of the sample.

  Returns:
    List of samples - containing the average metrics for all CPUs.

  input data:
  [
    {
      "timestamp": "22:05:29",
      "sum-interrupts": [
        {"cpu": "all", "intr": 274.77},
        {"cpu": "0", "intr": 264.27},
        {"cpu": "1", "intr": 15.45}
      ],
      ...
    }, {
      "timestamp": "22:05:31",
      "sum-interrupts": [
        {"cpu": "all", "intr": 273.75},
        {"cpu": "0", "intr": 264.73},
        {"cpu": "1", "intr": 13.30}
      ],
      ...
    }
  ]
  """
  samples = []
  for cpu_id in range(number_of_cpus+1):
    measurements = []
    for report in host_stats:
      value = report['sum-interrupts'][cpu_id]['intr']
      measurements.append(value)
    average = sum(measurements)/len(measurements)
    metric_name = 'mpstat_avg_intr'
    meta = metadata.copy()
    meta['mpstat_cpu_id'] = cpu_id-1
    samples.append(sample.Sample(
        metric=metric_name,
        value=average,
        unit='interrupts/sec',
        metadata=meta,
        timestamp=timestamp))
  return samples


def _GetPerIntervalSamples(
    host_stats: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    start_timestamp: int,
    interval: int) -> List[sample.Sample]:
  """Generate samples for all CPU related metrics in every run of mpstat.

  Args:
    host_stats: List of mpstat reports.
    metadata: metadata of the sample.
    start_timestamp: a unix timestamp representing the start of the first
      reporting period.
    interval: the interval between mpstat reports

  Returns:
    a list of samples to publish

  Because individual reports only have time (without a date), here we generate
  the timestamp based on the number of intervals that have passed in order to
  guarantee correct behavior if mpstat is run for more than 1 day.
  """
  samples = []
  cpu_metrics = _GetCPUMetrics(host_stats)
  for ordinal, host_stat in enumerate(host_stats):
    sample_timestamp = start_timestamp + (ordinal * interval)
    for cpu_metric in cpu_metrics:
      for cpu in host_stat['cpu-load']:
        metric_name = 'mpstat_avg_' + cpu_metric
        cpu_id = int(cpu['cpu'])
        metric_value = cpu[cpu_metric]
        meta = metadata.copy()
        meta['mpstat_cpu_id'] = cpu_id
        meta['ordinal'] = ordinal
        samples.append(sample.Sample(
            metric=metric_name,
            value=metric_value,
            unit='%',
            metadata=meta,
            timestamp=sample_timestamp))
  return samples


def _MpstatResults(
    metadata: Dict[str, Any],
    output: Dict[str, Any],
    interval: int,
    per_interval_samples: bool = False,
    ):
  """Parses and appends mpstat results to the samples list.

  Args:
    metadata: metadata of the sample.
    output: output of mpstat in JSON format
    interval: the interval between mpstat reports; required if
      per_interval_samples is True
    per_interval_samples: whether a sample per interval should be published

  Returns:
    List of samples.
  """
  start_timestamp = _ParseStartTime(output)
  samples = []
  hosts = output['sysstat']['hosts']

  for host in hosts:
    host_stats = host['statistics']
    number_of_cpus = host['number-of-cpus']
    metadata['nodename'] = host['nodename']

    samples += _GetCPUAverageMetrics(
        host_stats,
        number_of_cpus,
        metadata,
        start_timestamp)

    samples += _GetCPUAverageInterruptions(
        host_stats,
        number_of_cpus,
        metadata,
        start_timestamp)

    if per_interval_samples:
      samples += _GetPerIntervalSamples(
          host_stats,
          metadata,
          start_timestamp,
          interval)

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
            '{processor_number} {interval} {count} -o JSON > {output} 2>&1 &'
            .format(
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
        output = json.loads(fp.read())
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
