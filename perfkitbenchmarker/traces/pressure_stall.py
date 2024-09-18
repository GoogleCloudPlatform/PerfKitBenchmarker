# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Records pressure stall metrics from the system."""


import logging

from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker.traces import base_collector

_PRESSURE = flags.DEFINE_boolean(
    'pressure_stall', False, 'Run get_pressure_stall_stats.py'
)

FLAGS = flags.FLAGS


class PressureStallCollector(base_collector.BaseCollector):
  """pressure stall information collector."""

  def __init__(
      self, interval=None, output_directory=None, per_interval_samples=False
  ):
    super().__init__(interval, output_directory=output_directory)
    self.per_interval_samples = per_interval_samples

  def _CollectorName(self):
    return 'pressure_stall'

  def _InstallCollector(self, vm):
    vm.PushDataFile(
        'get_pressure_stall_stats.py', '~/get_pressure_stall_stats.py'
    )

  def _CollectorRunCommand(self, vm, collector_file):
    return (
        'python3 ~/get_pressure_stall_stats.py > {output} 2>&1 & echo $!'
        .format(
            output=collector_file,
        )
    )


def Register(parsed_flags):
  """Registers the pressure stall collector if FLAGS.pressure_stall is set."""
  if not parsed_flags.pressure_stall:
    return

  logging.debug('Registering pressure stall collector.')

  collector = PressureStallCollector()
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
