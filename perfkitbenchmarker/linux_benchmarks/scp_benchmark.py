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
"""SCP files between VMs."""

import logging
import time
from typing import Any
from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'scp'
BENCHMARK_CONFIG = """
scp:
  description: Runs a sample benchmark.
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
"""

# temp file
# _FILE = '/tmp/example_vm_benchmark.txt'


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
  vms = benchmark_spec.vms
  if len(vms) != 2:
    raise ValueError(
        f'scp benchmark requires exactly two machines, found {len(vms)}'
    )

  # _, _ = vm.RemoteCommand(f'echo "Hello World" > {_FILE}')
  # logging.info('Wrote "Hello World" to %s.', _FILE)
  logging.info('[HELLO] SCP BENCHMARK - Prepare')


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  vms = benchmark_spec.vms
  # stdout, _ = vm.RemoteCommand(f'cat {_FILE}')
  # logging.info('Read %s from %s.', stdout.strip(), _FILE)
  logging.info('[HELLO] SCP BENCHMARK - Run')
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