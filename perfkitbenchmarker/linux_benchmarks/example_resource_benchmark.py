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

"""Runs an example / dummy benchmark that creates an example resource & logs.

This benchmark demonstrates how to initialize & use an example resource, whose
config is defined in example_resource_spec.py & example_resource.py.
"""

import logging
from typing import Any
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'example_resource'
BENCHMARK_CONFIG = """
example_resource:
  description: >
    Example benchmark demonstrating how to write a benchmark.
  example_resource:
    example_type: 'ImplementedExampleResource'
    log_text: 'Hello from Benchmark'
"""


def GetConfig(user_config: dict[Any, Any]) -> dict[Any, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  logging.info('Running Prepare phase of the example benchmark')
  del benchmark_spec


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the example benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  logging.info('Running Run phase of the example benchmark')
  ex_resource = benchmark_spec.example_resource
  # Every resource supplies create times by default.
  samples = ex_resource.GetSamples()
  metadata = ex_resource.GetResourceMetadata()
  samples.append(sample.Sample('Example Sample Latency', 0.5, 'ms', metadata))
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleanup resources to tjeor original state.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  logging.info('Running Cleanup phase of the example benchmark')
  del benchmark_spec
