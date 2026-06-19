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
"""Benchmark for Kubernetes management plane operations.

TODO: Add comments & implement.
"""

from typing import Any

from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'kubernetes_management'

BENCHMARK_CONFIG = """
kubernetes_management:
  description: >
    Benchmarks GKE/EKS/AKS management plane operations: concurrent node pool
    create/upgrade/delete, overlapping cluster + node-pool ops, and large-scale
    provisioning. Focused on control-plane API responsiveness.
  container_cluster:
    type: Kubernetes
    vm_count: 1
"""


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config: bm_spec.BenchmarkSpec) -> None:
  del benchmark_config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  del benchmark_spec


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  del benchmark_spec
  return []


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  del benchmark_spec
