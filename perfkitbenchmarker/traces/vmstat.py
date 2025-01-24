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
"""Records system performance counters during benchmark runs using vmstat."""


import logging

from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker.traces import base_collector

_VMSTAT = flags.DEFINE_boolean(
    'vmstat',
    False,
    'Run vmstat to collect system performance metrics during benchmark run.',
)
_VMSTAT_INTERVAL = flags.DEFINE_integer(
    'vmstat_interval',
    5,
    'The amount of time in seconds between each vmstat report.Defaults to 5.',
)

FLAGS = flags.FLAGS


class VmstatCollector(base_collector.BaseCollector):
  """vmstat collector.

  Installs and runs vmstat on a collection of VMs.
  """

  def __init__(self, interval=None, output_directory=None):
    super().__init__(interval, output_directory=output_directory)

  def _CollectorName(self):
    return 'vmstat'

  def _InstallCollector(self, vm):
    vm.InstallPackages('sysstat')

  def _CollectorRunCommand(self, vm, collector_file):
    return 'vmstat {interval} > {output} 2>&1 &'.format(
        interval=FLAGS.vmstat_interval,
        output=collector_file,
    )


def Register(parsed_flags):
  """Registers the vmstat collector if FLAGS.vmstat is set."""
  if not parsed_flags.vmstat:
    return

  logging.debug('Registering vmstat collector.')

  collector = VmstatCollector(interval=parsed_flags.vmstat_interval)
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
