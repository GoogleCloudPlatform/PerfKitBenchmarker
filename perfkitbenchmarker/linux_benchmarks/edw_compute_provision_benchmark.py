# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Benchmark measuring the latency of provisioning edw compute resources.

This benchmark measures the latency (from initial request until the resource
reports a ready status) for provisioning edw computation resources such as
BigQuery slot capacity commitments, Snowflake warehouses, and Redshift clusters.
"""

from typing import Any, Dict, List

from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample


BENCHMARK_NAME = 'edw_compute_provision_benchmark'

BENCHMARK_CONFIG = """
edw_compute_provision_benchmark:
  description: Benchmarks provisioning time for edw computation resources
  edw_compute_resource:
    type: bigquery_slots
"""


def GetConfig(user_config: Dict[Any, Any]) -> Dict[Any, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  del benchmark_spec  # Prepare is currently a no-op for this benchmark.
  pass


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  results = []
  edw_compute_resource = benchmark_spec.edw_compute_resource
  results.extend(edw_compute_resource.GetLifecycleMetrics())
  return results


def Cleanup(benchmark_spec):
  benchmark_spec.edw_compute_resource.Cleanup()
