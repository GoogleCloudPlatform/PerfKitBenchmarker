# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Sample benchmark which uses a VM."""

import logging
from typing import Any

from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'example_vm'
BENCHMARK_CONFIG = """
example_vm:
  description: Runs a sample benchmark.
  # VM Groups are created in the order they appear in the config, thus
  # bigger VMs (e.g. servers) should be defined before smaller ones (e.g. clients).
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
"""

# temp file
_FILE = '/tmp/example_vm_benchmark.txt'


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the VMs and other resources for running the benchmark.

  This is a good place to download binaries onto the VMs, create any data files
  needed for a benchmark run, etc.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  vm = benchmark_spec.vms[0]

  _, _ = vm.RemoteCommand(f'echo "Hello World" > {_FILE}')
  logging.info('Wrote "Hello World" to %s.', _FILE)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  vm = benchmark_spec.vms[0]
  stdout, _ = vm.RemoteCommand(f'cat {_FILE}')
  logging.info('Read %s from %s.', stdout.strip(), _FILE)
  # This is a sample benchmark that produces no meaningful data, but we return
  # a single sample as an example.
  metadata = dict()
  metadata['sample_metadata'] = 'stdout'
  return [sample.Sample('sample_metric', 123.456, 'sec', metadata)]


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up after the benchmark completes.

  The state of the VMs should be equivalent to the state before Prepare was
  called.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  del benchmark_spec
