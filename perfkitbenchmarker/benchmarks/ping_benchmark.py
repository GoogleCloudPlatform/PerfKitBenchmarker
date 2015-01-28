# Copyright 2014 Google Inc. All rights reserved.
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

"""Runs ping.

This benchmark runs ping using the internal ips of vms in the same zone.
"""

import logging
from perfkitbenchmarker import sample
import re


BENCHMARKS_INFO = {
    'name': 'ping',
    'description': 'Benchmarks ping latency over internal IP addresses',
    'scratch_disk': False,
    'num_machines': 2}

METRICS = ('Min Latency', 'Average Latency', 'Max Latency', 'Latency Std Dev')


def GetInfo():
  return BENCHMARKS_INFO


def Prepare(benchmark_spec):  # pylint: disable=unused-argument
  """Install ping on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass


def Run(benchmark_spec):
  """Run ping on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  if not vms[0].IsReachable(vms[1]):
    logging.warn('%s is not reachable from %s', vms[1], vms[0])
    return []
  vm = vms[0]
  logging.info('Ping results:')
  ping_cmd = 'ping -c 100 %s' % vms[1].internal_ip
  stdout, _ = vm.RemoteCommand(ping_cmd, should_log=True)
  stats = re.findall('([0-9]*\\.[0-9]*)', stdout.splitlines()[-1])
  assert len(stats) == len(METRICS), stats
  results = []
  metadata = {'ip_type': 'internal'}
  for i, metric in enumerate(METRICS):
    results.append(sample.Sample(metric, float(stats[i]), 'ms', metadata))
  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup ping on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
