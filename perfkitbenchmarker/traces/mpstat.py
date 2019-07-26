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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os

from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.traces import base_collector
import six

flags.DEFINE_boolean(
    'mpstat', False, 'Run mpstat (https://linux.die.net/man/1/mpstat) '
    'to collect system performance metrics during benchmark run.')
flags.DEFINE_enum(
    'mpstat_breakdown', 'SUM', ['SUM', 'CPU', 'ALL'],
    'Level of aggregation for statistics. Accepted '
    'values are "SUM", "CPU", "ALL". Defaults to SUM. See '
    'https://linux.die.net/man/1/mpstat for details.')
flags.DEFINE_string(
    'mpstat_cpus', 'ALL', 'Comma delimited string of CPU ids or ALL. '
    'Defaults to ALL.')
flags.DEFINE_integer(
    'mpstat_interval', 1,
    'The amount of time in seconds between each mpstat report.'
    'Defaults to 1.')
flags.DEFINE_integer(
    'mpstat_count', 1, 'The number of reports generated at interval apart.'
    'Defaults to 1.')
flags.DEFINE_boolean('mpstat_publish', False,
                     'Whether to publish mpstat statistics.')
FLAGS = flags.FLAGS


def _ParsePercentageUse(rows, metadata):
  """Parse a CPU percentage use data chunk.

  Args:
    rows: List of mpstat CPU percentage lines.
    metadata: metadata of the sample.

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
          metric=metric_name, value=float(value), unit='%', metadata=meta)


def _ParseInterruptsPerSec(rows, metadata):
  """Parse a interrput/sec data chunk.

  Args:
    rows: List of mpstat interrupts per second lines.
    metadata: metadata of the sample.

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
    yield sample.Sample(metric=metric_name, value=float(data[2]),
                        unit='interrupts/sec', metadata=meta)


def _MpstatResults(metadata, output):
  """Parses and appends mpstat results to the samples list.

  Args:
    metadata: metadata of the sample.
    output: output of mpstat

  Returns:
    List of samples.
  """
  samples = []
  paragraphs = output.split('\n\n')

  for paragraph in paragraphs:
    lines = paragraph.rstrip().split('\n')
    if lines and 'Average' in lines[0] and '%irq' in lines[0]:
      samples += _ParsePercentageUse(lines, metadata)
    elif lines and 'Average' in lines[0] and 'intr/s' in lines[0]:
      samples += _ParseInterruptsPerSec(lines, metadata)
    elif lines and 'Average' in lines[0]:
      logging.debug('Skipping aggregated metrics: %s', lines[0])

  return samples


class MpstatCollector(base_collector.BaseCollector):
  """mpstat collector.

  Installs and runs mpstat on a collection of VMs.
  """

  def _CollectorName(self):
    return 'mpstat'

  def _InstallCollector(self, vm):
    vm.InstallPackages('sysstat')

  def _CollectorRunCommand(self, vm, collector_file):
    return ('mpstat -I {breakdown} -u -P {processor_number} {interval} {count} '
            '> {output} 2>&1 &'.format(
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
        samples.extend(_MpstatResults(metadata, output))

    vm_util.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)])


def Register(parsed_flags):
  """Registers the mpstat collector if FLAGS.mpstat is set."""
  if not parsed_flags.mpstat:
    return

  logging.debug('Registering mpstat collector.')

  collector = MpstatCollector(interval=parsed_flags.mpstat_interval)
  events.before_phase.connect(collector.Start, events.RUN_PHASE, weak=False)
  events.after_phase.connect(collector.Stop, events.RUN_PHASE, weak=False)
  if parsed_flags.mpstat_publish:
    events.samples_created.connect(
        collector.Analyze, events.RUN_PHASE, weak=False)
