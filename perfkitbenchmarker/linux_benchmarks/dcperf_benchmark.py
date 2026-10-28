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
"""Runs a Meta DCPerf test.

https://github.com/facebookresearch/DCPerf
"""

import types
from typing import Any

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import feedsim
from perfkitbenchmarker.linux_packages import taobench

BENCHMARK_NAME = 'dcperf'
BENCHMARK_CONFIG = """
dcperf:
  description: Runs a Meta DCPerf test.
  vm_groups:
    server:
      os_type: ubuntu2204
      vm_spec: &dcperf_vm_spec
        GCP:
          machine_type: c4a-standard-16
          zone: us-central1-b
          boot_disk_size: 200
        AWS:
          machine_type: m9g.4xlarge
          zone: us-east-1b
          boot_disk_size: 200
        Azure:
          machine_type: Standard_D16ps_v6
          zone: eastus2
          boot_disk_size: 200
    clients:
      os_type: ubuntu2204
      vm_spec: *dcperf_vm_spec
      vm_count: 1
"""

_WORKLOADS: dict[str, types.ModuleType] = {
    'taobench_standalone': taobench,
    'taobench_autoscale': taobench,
    'feedsim': feedsim,
}
_CLIENT_SERVER_WORKLOADS = frozenset({'taobench_autoscale'})

_DCPERF_TEST = flags.DEFINE_enum(
    'dcperf_test',
    'taobench_standalone',
    list(_WORKLOADS),
    'The DCPerf workload to run.',
)


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if _DCPERF_TEST.value not in _CLIENT_SERVER_WORKLOADS:
    config['vm_groups']['clients']['vm_count'] = 0
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  background_tasks.RunThreaded(
      _WORKLOADS[_DCPERF_TEST.value].Install, benchmark_spec.vms
  )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  workload = _WORKLOADS[_DCPERF_TEST.value]
  server = benchmark_spec.vm_groups['server'][0]
  if _DCPERF_TEST.value in _CLIENT_SERVER_WORKLOADS:
    return workload.Run(
        server=server, clients=benchmark_spec.vm_groups['clients']
    )
  return workload.Run(server=server)


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  del benchmark_spec
