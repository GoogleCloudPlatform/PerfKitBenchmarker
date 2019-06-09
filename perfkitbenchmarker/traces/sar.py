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
"""Records cpu performance counters during benchmark runs using sar."""

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
    'sar', False, 'Run sar (https://linux.die.net/man/1/sar) '
    'on each VM to collect system performance metrics during '
    'each benchmark run.')
flags.DEFINE_integer(
    'sar_interval', 5, 'sar sample collection frequency, in seconds. Only '
    'applicable when --sar is specified.')
flags.DEFINE_integer(
    'sar_samples', None,
    'Number of sar samples to collect. When undefined sar is '
    'ran indefinitely. This must be set to record average '
    'statistics. Only applicable when --sar is specified.')
flags.DEFINE_string(
    'sar_output', None, 'Output directory for sar output. '
    'Only applicable when --sar is specified. '
    'Default: run temporary directory.')
flags.DEFINE_boolean('sar_publish', True,
                     'Whether to publish average sar statistics.')
FLAGS = flags.FLAGS


def _AddStealResults(metadata, output, samples):
  """Appends average Steal Time %'s to the samples list.

  Sample data e.g.
  ...
  Linux 4.4.0-1083-aws (ip-10-0-0-217)   05/21/2019   _x86_64_  (8 CPU)

  12:12:36 AM     CPU     %user     %nice   %system   %iowait    %steal    %idle
  12:17:17 AM     all     18.09      0.00      0.00      0.00     81.91     0.00
  12:17:22 AM     all     21.96      0.00      0.00      0.00     78.04     0.00
  12:17:27 AM     all     36.47      0.00      0.00      0.00     63.53     0.00
  Average:        all     33.73      0.00      0.00      0.00     66.27     0.00

  Args:
    metadata: metadata of the sample.
    output: the output of the stress-ng benchmark.
    samples: list of samples to return.
  """
  output_lines = output.splitlines()

  for line in output_lines:
    line_split = line.split()
    if not line_split:
      continue
    if line_split[0] == 'Linux':
      continue
    if line_split[-2] == '%steal':
      continue
    if line_split[0] == 'Average:':
      metric = 'average_steal'
    else:
      metric = 'steal'
    value = float(line_split[-2])  # parse %steal time

    my_metadata = {'user_percent': float(line_split[3])}
    my_metadata.update(metadata)

    samples.append(
        sample.Sample(
            metric=metric, value=value, unit='%', metadata=my_metadata))


class _SarCollector(base_collector.BaseCollector):
  """sar collector.

  Installs sysstat and runs sar on a collection of VMs.
  """

  def _CollectorName(self):
    return 'sar'

  def _InstallCollector(self, vm):
    vm.InstallPackages('sysstat')

  def _CollectorRunCommand(self, vm, collector_file):
    cmd = ('sar -u {sar_interval} {sar_samples} > {output} 2>&1 & '
           'echo $!').format(
               output=collector_file,
               sar_interval=FLAGS.sar_interval,
               sar_samples=FLAGS.sar_samples if FLAGS.sar_samples else '')
    return cmd

  def Analyze(self, sender, benchmark_spec, samples):
    """Analyze sar file and record samples."""

    def _Analyze(role, f):
      """Parse file and record samples."""
      with open(os.path.join(self.output_directory, os.path.basename(f)),
                'r') as fp:
        output = fp.read()
        metadata = {
            'event': 'sar',
            'sender': 'run',
            'sar_interval': self.interval,
            'role': role,
        }
        _AddStealResults(metadata, output, samples)

    vm_util.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)])


def Register(parsed_flags):
  """Registers the sar collector if FLAGS.sar is set."""
  if not parsed_flags.sar:
    return

  output_directory = (
      parsed_flags.sar_output
      if parsed_flags['sar_output'].present else vm_util.GetTempDir())

  logging.debug('Registering sar collector with interval %s, output to %s.',
                parsed_flags.sar_interval, output_directory)

  if not os.path.isdir(output_directory):
    os.makedirs(output_directory)
  collector = _SarCollector(
      interval=parsed_flags.sar_interval, output_directory=output_directory)
  events.before_phase.connect(collector.Start, events.RUN_PHASE, weak=False)
  events.after_phase.connect(collector.Stop, events.RUN_PHASE, weak=False)
  if parsed_flags.sar_publish:
    events.samples_created.connect(
        collector.Analyze, events.RUN_PHASE, weak=False)
