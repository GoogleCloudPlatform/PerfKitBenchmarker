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
"""Measures the performance of provisioning a memory store.

Provisioning metrics are automatically collected by benchmark_spec.py, which
is why we do not need to do anything in the prepare/run phases of this file.
"""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import managed_memory_store

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'provision_memory_store'

BENCHMARK_CONFIG = f"""
provision_memory_store:
  description: Measure the performance of provisioning a memory store.
  memory_store:
    service_type: memorystore
    memory_store_type: {managed_memory_store.REDIS}
    version: redis_7_x
  vm_groups:
    clients:
      vm_spec: *default_single_core
      vm_count: 1
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def Run(_):
  return []


def Cleanup(_):
  pass
