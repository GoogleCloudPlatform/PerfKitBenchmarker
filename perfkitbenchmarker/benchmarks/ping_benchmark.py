#!/usr/bin/env python
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

import re

import logging


BENCHMARKS_INFO = {'name': 'ping',
                   'description': 'Run ping',
                   'scratch_disk': False,
                   'num_machines': 2}

METRICS = ['Min Latency', 'Average Latency', 'Max Latency', 'Latency Std Dev']


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
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  if not vms[0].IsReachable(vms[1]):
    return []
  vm = vms[0]
  logging.info('Ping results:')
  ping_cmd = 'ping -c 100 %s' % vms[1].internal_ip
  stdout, _ = vm.RemoteCommand(ping_cmd, should_log=True)
  stats = re.findall('([0-9]*\\.[0-9]*)', stdout.splitlines()[-1])
  assert len(stats) == 4
  results = []
  metadata = {'ip_type': 'internal'}
  for i in range(4):
    results.append((METRICS[i], float(stats[i]), 'ms', metadata))
  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup ping on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
