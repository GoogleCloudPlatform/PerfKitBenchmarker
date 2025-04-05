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
"""A simple encryption benchmark.

https://gitlab.com/cryptsetup/cryptsetup

TODO(user): Implement me
"""

import logging
from typing import Any

from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'cryptsetup'
BENCHMARK_CONFIG = """
cryptsetup:
  description: Simple encryption benchmark.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Installs the cryptsetup utility and benchmark on the VM."""
  vm = benchmark_spec.vms[0]
  vm.InstallPackages('cryptsetup')


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the cryptsetup benchmark and returns a dict of performance data.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  vm = benchmark_spec.vms[0]
  # TODO(user): Implement me
  samples = []
  stdout, _ = vm.RemoteCommand('cryptsetup benchmark')
  logging.info('Benchmark output: %s', stdout)
  line = stdout.splitlines()
  line = line[1:5]
  for line in stdout:
      list = line.split()
      metric = list[0] 
      iteration = list[1]
      unit = ''.join(list[2:4])
      metadata = list[6]
      samples.append(Sample(metric, iteration, unit, metadata))
  return samples


def Cleanup(_):
  """Don't bother cleaning up the benchmark files."""
  pass
