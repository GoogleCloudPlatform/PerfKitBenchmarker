# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing DCPerf FeedSim.

FeedSim is an aggregator-like workload that finds the maximum QPS a VM can
sustain while keeping p95 latency within its target.
"""

import json

from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import dcperf

_JOB = 'feedsim_autoscale'
_ARM_JOB = 'feedsim_autoscale_arm'


def Install(vm: virtual_machine.BaseVirtualMachine) -> None:
  vm.Install('dcperf')
  vm.RobustRemoteCommand(
      f'cd {dcperf.DCPERF_DIR} && sudo ./benchpress_cli.py install {_JOB}'
  )


def Run(server: virtual_machine.BaseVirtualMachine) -> list[sample.Sample]:
  """Runs FeedSim and returns its samples."""
  job = _JOB
  if server.is_aarch64:
    job = _ARM_JOB
  output, _ = server.RobustRemoteCommand(
      f'cd {dcperf.DCPERF_DIR} && sudo ./benchpress_cli.py run {job}'
  )
  report = output.rpartition('Results Report:')[2].strip().splitlines()[0]
  metrics = json.loads(report)['metrics']
  if metrics['successful_instances'] != int(metrics['spawned_instances']):
    raise errors.Benchmarks.RunError(f'FeedSim instances failed: {metrics}')
  if not metrics['score']:
    raise errors.Benchmarks.RunError(f'FeedSim reported no score: {metrics}')
  overall = metrics['overall']
  metadata = {
      'target_percentile': metrics['target_percentile'],
      'target_latency_msec': metrics['target_latency_msec'],
  }
  return [
      sample.Sample('qps', overall['final_achieved_qps'], 'qps', metadata),
      sample.Sample('latency', overall['average_latency_msec'], 'ms', metadata),
      sample.Sample('score', metrics['score'], '', metadata),
  ]
