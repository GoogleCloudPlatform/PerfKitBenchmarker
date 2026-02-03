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

"""Benchmark for timing provisioning for TPU VMs.

This measures the time it takes to create the TPU.
Example usage:
pkb.py --benchmarks=provision_tpu  --tpu_type=v6e --tpu_topology=1x1
"""

import time
from typing import List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'provision_tpu'
BENCHMARK_CONFIG = """
provision_tpu:
  description: >
      Time spinning up a TPU VMs.
  tpu_groups:
    default:
      cloud: GCP
      tpu_type: v6e
      tpu_topology: 1x1
      tpu_tf_version: v2-alpha-tpuv6e
      tpu_zone: europe-west4-a
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_):
  pass


def CheckPrerequisites(_):
  """Perform flag checks."""
  pass


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark."""
  tpu_vm = bm_spec.tpu_groups['default']
  metadata = tpu_vm.GetResourceMetadata()
  tpu_vm.WaitForSshBecameReady()
  time_to_ssh = time.time() - tpu_vm.create_start_time
  return [
      sample.Sample(
          'Time to Create TPU',
          tpu_vm.create_time,
          'seconds',
          metadata,
      ),
      sample.Sample(
          'Time From Create to SSH',
          time_to_ssh,
          'seconds',
          metadata,
      ),
  ]


def Cleanup(_):
  pass
