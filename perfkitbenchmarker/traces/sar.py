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

flags.DEFINE_boolean(
    'sar',
    False,
    'Run sar (https://linux.die.net/man/1/sar) '
    'on each VM to collect system performance metrics during '
    'each benchmark run, and then download the full archive for analysis.',
)
flags.DEFINE_integer(
    'sar_interval',
    5,
    'sar sample collection frequency, in seconds. Only '
    'applicable when --sar is specified.',
)
FLAGS = flags.FLAGS


class _SarCollector(base_collector.BaseCollector):
  """sar archive collector for manual analysis.

  Installs sysstat and runs sar on a collection of VMs.
  """

  def _CollectorName(self):
    return 'sar'

  def _InstallCollector(self, vm):
    vm.InstallPackages('sysstat')

  def _CollectorRunCommand(self, vm, collector_file):
    # this starts sar in the background and returns the pid
    cmd = (
        'sar -o {output} {sar_interval} &>/dev/null & echo $!'
    ).format(
        output=collector_file,
        sar_interval=FLAGS.sar_interval,
    )
    return cmd


def Register(parsed_flags):
  """Registers the sar collector if FLAGS.sar is set."""
  if not parsed_flags.sar:
    return

  output_directory = vm_util.GetTempDir()

  logging.debug(
      'Registering sar collector with interval %s, output to %s.',
      parsed_flags.sar_interval,
      output_directory,
  )

  if not os.path.isdir(output_directory):
    os.makedirs(output_directory)
  collector = _SarCollector(
      interval=parsed_flags.sar_interval, output_directory=output_directory
  )
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
