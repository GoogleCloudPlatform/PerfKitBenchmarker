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
"""Collects disk io stats during benchmark runs using iostat."""

import collections
import datetime
import json
import os
import re
from typing import Any, List, Optional
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import events
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.traces import base_collector

_IOSTAT = flags.DEFINE_boolean(
    'iostat',
    False,
    'Enable iostat to collect disk io statistics.',
)
_IOSTAT_INTERVAL = flags.DEFINE_integer(
    'iostat_interval',
    1,
    'The interval in seconds between iostat samples.',
)
_IOSTAT_COUNT = flags.DEFINE_integer(
    'iostat_count',
    10,
    'The number of iostat samples to collect.',
)
_IOSTAT_METRICS = flags.DEFINE_list(
    'iostat_metrics',
    [
        'r/s',
        'w/s',
        'rkB/s',
        'wkB/s',
        'r_await',
        'w_await',
        'rareq-sz',
        'wareq-sz',
        'aqu-sz',
        'util',
    ],
    'A list of iostat metrics to collect. Currently this is confined by the'
    ' list of disk metrics emitted from iostat -x',
)
_IOSTAT_INCLUDE_DEVICES_REGEX = flags.DEFINE_string(
    'iostat_include_devices_regex',
    None,
    'A regex to select devices to include.',
)
_IOSTAT_PUBLISH_SAMPLES = flags.DEFINE_boolean(
    'iostat_publish_samples',
    True,
    'Whether to publish iostat samples.',
)

FLAGS = flags.FLAGS

METRIC_UNITS = {
    'r/s': 'ops/s',
    'w/s': 'ops/s',
    'd/s': 'ops/s',
    'f/s': 'ops/s',
    'rkB/s': 'kB/s',
    'wkB/s': 'kB/s',
    'dkB/s': 'kB/s',
    'rrqm/s': 'ops/s',
    'wrqm/s': 'ops/s',
    'drqm/s': 'ops/s',
    'r_await': 'ms',
    'w_await': 'ms',
    'd_await': 'ms',
    'f_await': 'ms',
    'rareq-sz': 'kB',
    'wareq-sz': 'kB',
    'dareq-sz': 'kB',
    'aqu-sz': '',
    'util': '%',
}


def _IostatResults(
    metadata: dict[str, str],
    json_output: Any,
    samples: List[sample.Sample],
    metrics: list[str],
    interval: int,
    count: int,
    device_regex: Optional[str],
) -> None:
  """Parses iostat json data and adds samples."""
  stats = json_output['sysstat']['hosts']
  for per_host in stats:
    per_host_stats = per_host['statistics']
    if not per_host_stats:
      continue
    timestamps = _GetTimestamps(per_host_stats, interval, count)
    per_device_stats = collections.defaultdict(list)
    for stat in per_host_stats:
      for dev_stat in stat['disk']:
        dev = dev_stat['disk_device']
        if device_regex and not re.match(device_regex, dev):
          continue
        per_device_stats[dev].append(dev_stat)
    for disk, disk_stats in per_device_stats.items():
      new_meta = metadata.copy()
      new_meta['nodename'] = per_host['nodename']
      new_meta['disk'] = disk
      for disk_metric in metrics:
        values = [entry[disk_metric] for entry in disk_stats]
        samples.append(
            sample.CreateTimeSeriesSample(
                values=values,
                timestamps=timestamps,
                metric=f'{disk}_{disk_metric}_time_series',
                units=METRIC_UNITS.get(disk_metric, ''),
                interval=interval,
                additional_metadata=new_meta,
            )
        )


def _GetTimestamps(
    per_host_stats: list[dict[str, Any]], interval: int, count: int
) -> list[float]:
  first_ts = datetime.datetime.strptime(
      per_host_stats[0]['timestamp'], '%Y-%m-%dT%H:%M:%S%z'
  ).timestamp()
  return [first_ts + i * interval for i in range(count)]


class IostatCollector(base_collector.BaseCollector):
  """Collects disk io stats during benchmark runs using iostat."""

  def __init__(
      self,
      interval: int = 1,
      output_directory: str = '',
      metrics: Optional[list[str]] = None,
      count: int = 10,
      device_regex: Optional[str] = None,
  ) -> None:
    super().__init__(interval, output_directory=output_directory)
    self.count = count
    self.interval = interval
    self.metrics = metrics or []
    self.device_regex = device_regex

  def _CollectorName(self):
    return 'iostat'

  def _InstallCollector(self, vm: virtual_machine.BaseVirtualMachine):
    vm.InstallPackages('sysstat')

  def _CollectorRunCommand(
      self, vm: virtual_machine.BaseVirtualMachine, collector_file: str
  ):
    if vm.BASE_OS_TYPE == os_types.WINDOWS:
      raise NotImplementedError('iostat is not supported on Windows.')
    return (
        f'export S_TIME_FORMAT=ISO; iostat -xt {self.interval} {self.count} -o'
        f' JSON > {collector_file} 2>&1 &'
    )

  def Analyze(
      self,
      sender,
      benchmark_spec: bm_spec.BenchmarkSpec,
      samples: List[sample.Sample],
  ) -> None:
    del sender

    def _Analyze(role: str, collector_file: str) -> None:
      with open(
          os.path.join(self.output_directory, os.path.basename(collector_file)),
          'r',
      ) as fp:
        metadata = {
            'event': 'iostat',
            'role': role,
        }
        json_output = json.loads(fp.read())
        _IostatResults(
            metadata,
            json_output,
            samples,
            self.metrics,
            self.interval,
            self.count,
            self.device_regex,
        )

    background_tasks.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in self._role_mapping.items()]
    )


def Register(parsed_flags: flags.FlagValues) -> None:
  """Registers the iostat collector if FLAGS.iostat is set."""
  if not parsed_flags.iostat:
    return

  collector = IostatCollector(
      interval=parsed_flags.iostat_interval,
      count=parsed_flags.iostat_count,
      metrics=parsed_flags.iostat_metrics,
      device_regex=parsed_flags.iostat_include_devices_regex,
      output_directory=vm_util.GetTempDir(),
  )
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
  if parsed_flags.iostat_publish_samples:
    events.benchmark_samples_created.connect(collector.Analyze, weak=False)
