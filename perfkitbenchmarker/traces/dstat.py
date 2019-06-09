# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Records system performance counters during benchmark runs using dstat.

http://dag.wiee.rs/home-made/dstat/
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import logging
import os
import re
import numpy as np

from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import dstat
from perfkitbenchmarker.traces import base_collector
import six

flags.DEFINE_boolean('dstat', False,
                     'Run dstat (http://dag.wiee.rs/home-made/dstat/) '
                     'on each VM to collect system performance metrics during '
                     'each benchmark run.')
flags.DEFINE_integer('dstat_interval', None,
                     'dstat sample collection frequency, in seconds. Only '
                     'applicable when --dstat is specified.')
flags.DEFINE_string('dstat_output', None,
                    'Output directory for dstat output. '
                    'Only applicable when --dstat is specified. '
                    'Default: run temporary directory.')
flags.DEFINE_boolean('dstat_publish', False,
                     'Whether to publish average dstat statistics.')
flags.DEFINE_string('dstat_publish_regex', None, 'Requires setting '
                    'dstat_publish to true. If specified, any dstat statistic '
                    'matching this regular expression will be published such '
                    'that each individual statistic will be in a sample with '
                    'the time since the epoch in the metadata. Examples. Use '
                    '".*" to record all samples. Use "net" to record '
                    'networking statistics.')
FLAGS = flags.FLAGS


class _DStatCollector(base_collector.BaseCollector):
  """dstat collector.

  Installs and runs dstat on a collection of VMs.
  """

  def _CollectorName(self):
    return 'dstat'

  def _InstallCollector(self, vm):
    vm.Install('dstat')

  def _CollectorRunCommand(self, vm, collector_file):
    num_cpus = vm.num_cpus

    # List block devices so that I/O to each block device can be recorded.
    block_devices, _ = vm.RemoteCommand(
        'lsblk --nodeps --output NAME --noheadings')
    block_devices = block_devices.splitlines()
    cmd = ('dstat --epoch -C total,0-{max_cpu} '
           '-D total,{block_devices} '
           '-clrdngyi -pms --fs --ipc --tcp '
           '--udp --raw --socket --unix --vm --rpc '
           '--noheaders --output {output} {dstat_interval} > /dev/null 2>&1 & '
           'echo $!').format(
               max_cpu=num_cpus - 1,
               block_devices=','.join(block_devices),
               output=collector_file,
               dstat_interval=self.interval or '')
    return cmd

  def Analyze(self, sender, benchmark_spec, samples):
    """Analyze dstat file and record samples."""

    def _AnalyzeEvent(role, labels, out, event):
      # Find out index of rows belong to event according to timestamp.
      cond = (out[:, 0] > event.start_timestamp) & (
          out[:, 0] < event.end_timestamp)
      # Skip analyzing event if none of rows falling into time range.
      if not cond.any():
        return
      # Calculate mean of each column.
      avg = np.average(out[:, 1:], weights=cond, axis=0)
      metadata = copy.deepcopy(event.metadata)
      metadata['event'] = event.event
      metadata['sender'] = event.sender
      metadata['vm_role'] = role

      samples.extend([
          sample.Sample(label, avg[idx], '', metadata)
          for idx, label in enumerate(labels[1:])])

      dstat_publish_regex = FLAGS.dstat_publish_regex
      if dstat_publish_regex:
        assert labels[0] == 'epoch__epoch'
        for i, label in enumerate(labels[1:]):
          metric_idx = i + 1  # Skipped first label for the epoch.
          if re.search(dstat_publish_regex, label):
            for sample_idx, value in enumerate(out[:, metric_idx]):
              individual_sample_metadata = copy.deepcopy(metadata)
              individual_sample_metadata['dstat_epoch'] = out[sample_idx, 0]
              samples.append(
                  sample.Sample(label, value, '', individual_sample_metadata))

    def _Analyze(role, file):
      with open(os.path.join(self.output_directory,
                             os.path.basename(file)), 'r') as f:
        fp = iter(f)
        labels, out = dstat.ParseCsvFile(fp)
        vm_util.RunThreaded(
            _AnalyzeEvent,
            [((role, labels, out, e), {}) for e in events.TracingEvent.events])

    vm_util.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)])


def Register(parsed_flags):
  """Registers the dstat collector if FLAGS.dstat is set."""
  if not parsed_flags.dstat:
    return

  output_directory = (parsed_flags.dstat_output
                      if parsed_flags['dstat_output'].present
                      else vm_util.GetTempDir())

  logging.debug('Registering dstat collector with interval %s, output to %s.',
                parsed_flags.dstat_interval, output_directory)

  if not os.path.isdir(output_directory):
    os.makedirs(output_directory)
  collector = _DStatCollector(interval=parsed_flags.dstat_interval,
                              output_directory=output_directory)
  events.before_phase.connect(collector.Start, events.RUN_PHASE, weak=False)
  events.after_phase.connect(collector.Stop, events.RUN_PHASE, weak=False)
  if parsed_flags.dstat_publish:
    events.samples_created.connect(
        collector.Analyze, events.RUN_PHASE, weak=False)
