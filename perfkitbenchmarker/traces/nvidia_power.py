# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs nvidia-smi power.draw on VMs."""

import csv
import datetime
import logging
import os
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.benchmark_spec import BenchmarkSpec
from perfkitbenchmarker.traces import base_collector
import six

_NVIDIA_POWER = flags.DEFINE_boolean(
    'nvidia_power', False, 'Run nvidia power on each VM to collect power in '
    'each benchmark run.')
_INTERVAL = flags.DEFINE_integer('nvidia_power_interval', 1,
                                 'Nvidia power sample collection frequency.')
_PUBLISH = flags.DEFINE_boolean('nvidia_power_publish', True,
                                'Whether to publish Nvidia Power.')
_SELECTIONS = flags.DEFINE_string('nvidia_power_selections', 'power',
                                  'Choice(s) of data to collect; all or any '
                                  'combinations of power, pstate, utilization, '
                                  'temperature, clocks, and '
                                  'clocks_throttle_reasons.')
FLAGS = flags.FLAGS

# queries to give to 'nvidia-smi --query-gpu='; refer to the output of
# 'nvidia-smi --help-query-gpu' for details.
QUERY_TABLE = {
    'power.draw': {
        'metric': 'power',
        'header': ' power.draw [W]',
        'unit': 'watts',
        'type': 'float',
    },
    'pstate': {
        'metric': 'pstate',
        'header': ' pstate',
        'unit': '',
        'type': 'string',
    },
    'clocks_throttle_reasons.active': {
        'metric': 'clocks_throttle_reasons',
        'header': ' clocks_throttle_reasons.active',
        'unit': '',
        'type': 'string',
    },
    'utilization.gpu': {
        'metric': 'utilization_gpu',
        'header': ' utilization.gpu [%]',
        'unit': 'percent',
        'type': 'float',
    },
    'utilization.memory': {
        'metric': 'utilization_memory',
        'header': ' utilization.memory [%]',
        'unit': 'percent',
        'type': 'float',
    },
    'temperature.gpu': {
        'metric': 'temperature_gpu',
        'header': ' temperature.gpu',
        'unit': 'C',
        'type': 'float',
    },
    'temperature.memory': {
        'metric': 'temperature_memory',
        'header': ' temperature.memory',
        'unit': 'C',
        'type': 'float',
    },
    'clocks.gr': {
        'metric': 'clocks_graphics',
        'header': ' clocks.current.graphics [MHz]',
        'unit': 'MHz',
        'type': 'float',
    },
    'clocks.sm': {
        'metric': 'clocks_sm',
        'header': ' clocks.current.sm [MHz]',
        'unit': 'MHz',
        'type': 'float',
    },
    'clocks.mem': {
        'metric': 'clocks_mem',
        'header': ' clocks.current.memory [MHz]',
        'unit': 'MHz',
        'type': 'float',
    },
    'clocks.video': {
        'metric': 'clocks_video',
        'header': ' clocks.current.video [MHz]',
        'unit': 'MHz',
        'type': 'float',
    },
}

QUERY_GROUPS = {
    'power': ['power.draw'],
    'pstate': ['pstate'],
    'utilization': ['utilization.gpu', 'utilization.memory'],
    'temperature': ['temperature.gpu', 'temperature.memory'],
    'clocks': ['clocks.gr', 'clocks.sm', 'clocks.mem', 'clocks.video'],
    'clocks_throttle_reasons': ['clocks_throttle_reasons.active'],
}


def _NvidiaPowerResults(
    metadata: Dict[str, Any],
    output: csv.DictReader,
    samples: List[sample.Sample],
    query_items: List[str],
) -> None:
  """Parse output lines to get samples.

  Args:
    metadata: metadata.
    output: csv dict reader pointing to the collector_file.
    samples: list of samples to append parsed results to.
    query_items: list of query items.
  """
  for line in output:
    gpu_metadata = line.copy()
    timestamp = datetime.datetime.timestamp(
        datetime.datetime.strptime(gpu_metadata[' timestamp'],
                                   ' %Y/%m/%d %H:%M:%S.%f'))
    gpu_metadata.update(metadata)
    for query_item in query_items:
      if query_item in ['index', 'timestamp']:
        continue
      table_row = QUERY_TABLE[query_item]
      content = line.get(table_row['header']).split()[0]
      new_metadata = gpu_metadata.copy()
      value = 0.0
      if table_row['type'] == 'string':
        new_metadata[table_row['metric']] = content
      else:
        value = float(content)
      samples.append(
          sample.Sample(
              metric=table_row['metric'],
              value=value,
              unit=table_row['unit'],
              metadata=new_metadata,
              timestamp=timestamp))


class _NvidiaPowerCollector(base_collector.BaseCollector):
  """Nvidia GPU power collector.

  Runs Nvidia on the VMs.
  """

  def __init__(self, selections: str = 'power',
               interval: float = 1.0,
               output_directory: str = '') -> None:
    super(_NvidiaPowerCollector, self).__init__(interval, output_directory)
    self.selections = selections
    self.query_items = ['index', 'timestamp']
    if selections == 'all':
      selection_tokens = QUERY_GROUPS.keys()
    else:
      selection_tokens = selections.split(',')
    for selection in selection_tokens:
      if selection not in QUERY_GROUPS.keys():
        logging.warning('Unreconized selection %s.', selection)
        continue
      self.query_items.extend(QUERY_GROUPS[selection])
    self.query_items_str = ','.join(self.query_items)
    logging.info('query_items_str = %s', self.query_items_str)

  def _CollectorName(self) -> str:
    """See base class."""
    return 'nvidia_power'

  def _CollectorRunCommand(self, vm: virtual_machine.BaseVirtualMachine,
                           collector_file: str) -> str:
    """See base class."""
    interval_ms = int(self.interval * 1000 + 1e-6)
    return (
        f'nvidia-smi --query-gpu={self.query_items_str} --format=csv '
        f'-lms {interval_ms} > {collector_file} 2>&1 & echo $!'
    )

  def Analyze(
      self,
      unused_sender,
      benchmark_spec: BenchmarkSpec,
      samples: List[sample.Sample],
  ) -> None:
    """Analyze Nvidia power and record samples."""

    def _Analyze(role: str, collector_file: str) -> None:
      with open(
          os.path.join(self.output_directory, os.path.basename(collector_file)),
          'r') as fp:
        metadata = {
            'event': 'nvidia_power',
            'nvidia_interval': self.interval,
            'role': role
        }
        _NvidiaPowerResults(metadata, csv.DictReader(fp), samples,
                            self.query_items)

    vm_util.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)])


def Register(parsed_flags: flags) -> None:
  """Registers the Nvidia power collector if FLAGS.nvidia_power is set."""
  if not parsed_flags.nvidia_power:
    return

  collector = _NvidiaPowerCollector(
      selections=_SELECTIONS.value, interval=_INTERVAL.value,
      output_directory=vm_util.GetTempDir())
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
  if _PUBLISH.value:
    events.benchmark_samples_created.connect(collector.Analyze, weak=False)
