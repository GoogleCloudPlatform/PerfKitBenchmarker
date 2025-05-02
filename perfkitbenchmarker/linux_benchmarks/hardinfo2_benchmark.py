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
"""A multi-purpose Linux utility and benchmark.

https://github.com/hardinfo2/hardinfo2

TODO(user): Implement me
"""

import logging
from typing import Any

from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'hardinfo2'
BENCHMARK_CONFIG = """
hardinfo2:
  description: Hardinfo2 benchmark.
  vm_groups:
    default:
      vm_spec: *default_dual_core
"""


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Installs and builds the hardinfo2 benchmark on the VM."""
  vm = benchmark_spec.vms[0]
  vm.Install('hardinfo2')


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the hardinfo2 benchmark and returns a dict of performance data.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  vm = benchmark_spec.vms[0]
  samples = []
  # TODO(user): Implement me
  stdout, _ = vm.RemoteCommand('echo "Hello World"')
  logging.info('Benchmark output: %s', stdout)
  return samples


def Cleanup(_):
  """Don't bother cleaning up the benchmark files."""
  pass
