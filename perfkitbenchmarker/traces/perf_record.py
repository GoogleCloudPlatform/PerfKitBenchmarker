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
"""Collects the full sar archive. No samples will be published."""


import logging
import os
from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.traces import base_collector

FLAG_PERF_RECORD = flags.DEFINE_boolean(
    'perf_record',
    False,
    'Run perf record on each VM to collect profiles during each benchmark run. '
    'Note that this can be VERY INTRUSIVE and change the result. '
    'Also, note that the generated perf.data file must be analyzed on the same'
    ' machine that generated it. It will not be downloaded. '
    'There exists a way to do cross-machine analysis with "perf archive", but'
    ' it was tested to be buggy and not well-supported.',
)
FLAG_PERF_RECORD_INTERVAL = flags.DEFINE_integer(
    'perf_record_interval',
    99,
    'perf record sample collection frequency, in milliseconds. Only '
    'applicable when --perf_record is specified.',
)
FLAG_PERF_RECORD_OPTIONS = flags.DEFINE_string(
    'perf_record_options',
    '-g',
    'Options to pass to perf record.',
)


class _PerfRecordCollector(base_collector.BaseCollector):
  """perf record collector for manual analysis."""

  def _CollectorName(self):
    return 'perf_record'

  def _InstallCollector(self, vm):
    pass

  def _CollectorRunCommand(self, vm, collector_file):
    """Starts perf record in the background and returns the pid."""
    cmd = (
        f'sudo perf record {FLAG_PERF_RECORD_OPTIONS.value}'
        f' -F {FLAG_PERF_RECORD_INTERVAL.value} &>{collector_file} & echo $!'
    )
    return cmd


def Register(parsed_flags):
  """Registers the perf record collector if FLAGS.perf_record is set."""
  if not parsed_flags.perf_record:
    return

  output_directory = vm_util.GetTempDir()

  logging.debug('Registering perf record collector')

  if not os.path.isdir(output_directory):
    os.makedirs(output_directory)
  collector = _PerfRecordCollector(
      interval=FLAG_PERF_RECORD_INTERVAL.value,
      output_directory=output_directory,
  )
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
