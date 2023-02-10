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

"""Benchmark for timing provisioning Kubernetes clusters.

All timing and reporting is handled by resource.py. This simply gives a
convenient place to define the Benchmark Config
"""

from typing import List

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample


BENCHMARK_NAME = 'provision_container_service'

BENCHMARK_CONFIG = """
provision_container_service:
  description: >
      Time spinning up and deleting a Kubernetes Cluster
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def Run(_) -> List[sample.Sample]:
  # TODO(pclay): Possibly time provisioning K8s resources here.
  return []


def Cleanup(_):
  pass
