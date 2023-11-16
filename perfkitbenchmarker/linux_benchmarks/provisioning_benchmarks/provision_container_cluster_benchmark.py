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

"""Benchmark for timing provisioning Kubernetes clusters and pods.

Timing for creation of the cluster and pods are handled by resource.py
(see resource.GetSamples). There is one end-to-end sample created measured from
the beginning of cluster creation through the pod running.
"""

from typing import List

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'provision_container_cluster'

BENCHMARK_CONFIG = """
provision_container_cluster:
  description: >
      Time spinning up and deleting a Kubernetes Cluster
  container_specs:
    hello_world:
      image: hello-world
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec):
  """Provision container, because it's not handled by provision stage."""
  cluster = bm_spec.container_cluster
  assert cluster
  cluster.DeployContainer('hello-world', bm_spec.container_specs['hello_world'])


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Get samples from resource provisioning."""
  cluster = bm_spec.container_cluster
  assert cluster
  container = cluster.containers['hello-world'][0]
  samples = container.GetSamples()
  if not cluster.user_managed:
    samples.append(
        sample.Sample(
            'Time to Create Cluster and Pod',
            container.resource_ready_time - cluster.create_start_time,
            'seconds',
        )
    )
  return samples


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  assert bm_spec.container_cluster
  bm_spec.container_cluster.containers['hello-world'][0].Delete()
