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

FLAGS = flags.FLAGS


def _NvidiaPowerResults(metadata: Dict[str, Any], output: csv.DictReader,
                        samples: List[sample.Sample]) -> None:
  for line in output:
    gpu_metadata = line.copy()
    gpu_metadata.update(metadata)
    samples.append(
        sample.Sample(
            metric='power',
            value=float(line.get(' power.draw [W]').split()[0]),
            unit='watts',
            metadata=gpu_metadata))


class _NvidiaPowerCollector(base_collector.BaseCollector):
  """Nvidia GPU power collector.

  Runs Nvidia on the VMs.
  """

  def _CollectorName(self) -> str:
    """See base class."""
    return 'nvidia_power'

  def _CollectorRunCommand(self, vm: virtual_machine.BaseVirtualMachine,
                           collector_file: str) -> str:
    """See base class."""
    return f'nvidia-smi --query-gpu=index,power.draw --format=csv -l {self.interval} > {collector_file} 2>&1 & echo $!'

  def Analyze(self, unused_sender, benchmark_spec: BenchmarkSpec,
              samples: List[sample.Sample]) -> None:
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
        _NvidiaPowerResults(metadata, csv.DictReader(fp), samples)

    vm_util.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)])


def Register(parsed_flags: flags) -> None:
  """Registers the Nvidia power collector if FLAGS.nvidia_power is set."""
  if not parsed_flags.nvidia_power:
    return

  collector = _NvidiaPowerCollector(
      interval=_INTERVAL.value, output_directory=vm_util.GetTempDir())
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
  if _PUBLISH.value:
    events.benchmark_samples_created.connect(collector.Analyze, weak=False)
