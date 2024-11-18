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
from perfkitbenchmarker import data
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.traces import base_collector

FLAG_SS = flags.DEFINE_boolean(
    'ss',
    False,
    'Run ss (https://linux.die.net/man/8/ss) '
    'on each VM to collect TCP socket performance metrics during '
    'each benchmark run, and then download the full text for analysis.',
)
FLAG_SS_INTERVAL = flags.DEFINE_integer(
    'ss_interval',
    1,
    'ss sample collection frequency, in seconds. Only '
    'applicable when --ss is specified.',
)
FLAG_SS_OPTIONS = flags.DEFINE_string(
    'ss_options',
    '-tiepm "dport == 5001 or sport == 5001 or dport == 20000 or sport =='
    ' 20000"',
    'cmdline options to pass to ss; defaults will show TCP details for'
    ' netperf(20000) and iperf(5001) ports',
)


class _SsCollector(base_collector.BaseCollector):
  """ss collector for manual analysis.

  Runs ss on a collection of VMs.
  """

  def _CollectorName(self):
    return 'ss'

  def _InstallCollector(self, vm):
    vm.RenderTemplate(
        data.ResourcePath('ss.sh.j2'),
        'ss.sh',
        context={
            'options': FLAG_SS_OPTIONS.value,
            'interval': FLAG_SS_INTERVAL.value,
        },
    )
    vm.RemoteCommand('chmod +x ss.sh')

  def _CollectorRunCommand(self, vm, collector_file):
    """Starts ss in the background and returns the pid."""
    # NOTE that &>/dev/null is IMPORTANT, otherwise the command will get stuck
    cmd = f'./ss.sh {collector_file} &>/dev/null & echo $!'
    return cmd


def Register(parsed_flags):
  """Registers the ss collector if flag 'ss' is set."""
  if not parsed_flags.ss:
    return

  output_directory = vm_util.GetTempDir()

  logging.debug(
      'Registering ss collector with interval %s, output to %s.',
      FLAG_SS_INTERVAL.value,
      output_directory,
  )

  if not os.path.isdir(output_directory):
    os.makedirs(output_directory)
  collector = _SsCollector(
      interval=FLAG_SS_INTERVAL.value, output_directory=output_directory
  )
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
