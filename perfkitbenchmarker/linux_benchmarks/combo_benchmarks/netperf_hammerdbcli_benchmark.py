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
"""Runs Netperf and Hammerdbcli benchmarks in parallel.

Netperf_hammerdbcli benchmark specifies its own benchmark config,
"""

from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker.linux_benchmarks import hammerdbcli_benchmark
from perfkitbenchmarker.linux_benchmarks import netperf_benchmark
from perfkitbenchmarker.linux_packages import hammerdb


BENCHMARK_NAME = 'netperf_hammerdbcli'
BENCHMARK_CONFIG = """
netperf_hammerdbcli:
  description: Runs Netperf and Hammerdbcli in parallel.
  relational_db:
    engine: mysql
    db_spec:
      GCP:
        machine_type:
          cpus: 4
          memory: 7680MiB
        zone: us-central1-c
      AWS:
        machine_type: db.m4.xlarge
        zone: us-east-1a
      Azure:
        machine_type:
          tier: Premium
          compute_units: 500
        zone: eastus
    db_disk_spec:
      GCP:
        disk_size: 1000
        disk_type: pd-ssd
      AWS:
        disk_size: 6144
        disk_type: gp2
      Azure:
        disk_size: 128
    vm_groups:
      servers:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-west-1a
          Azure:
            machine_type: Standard_B4ms
            zone: westus
        disk_spec: *default_500_gb
      replications:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-b
          AWS:
            machine_type: m4.4xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_B4ms
            zone: eastus
        disk_spec: *default_500_gb
      clients:
        os_type: debian9
        vm_spec:
          GCP:
            machine_type: n1-standard-8
            zone: us-central1-c
          AWS:
            machine_type: m4.xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_D4_v3
            zone: eastus
        disk_spec:
          GCP:
            disk_size: 500
            disk_type: pd-ssd
          AWS:
            disk_size: 500
            disk_type: gp2
          Azure:
            disk_size: 500
            disk_type: StandardSSD_LRS
  vm_groups:
    netperf_clients:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
  flags:
    netperf_test_length: 300
    placement_group_style: closest_supported
"""

FLAGS = flags.FLAGS


def GetConfig(user_config: Dict[Any, Any]) -> Dict[Any, Any]:
  """Merge BENCHMARK_CONFIG with user_config to create benchmark_spec.

  Args:
    user_config: user-define configs (through FLAGS.benchmark_config_file or
      FLAGS.config_override).

  Returns:
    merged configs
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepare both benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  # Prepare Netperf benchmark
  netperf_client_vm, netperf_server_vm = (
      benchmark_spec.vm_groups['netperf_clients'][0],
      benchmark_spec.relational_db.server_vm,
  )
  background_tasks.RunThreaded(
      netperf_benchmark.PrepareNetperf, [netperf_client_vm, netperf_server_vm]
  )
  background_tasks.RunParallelThreads(
      [
          (netperf_benchmark.PrepareClientVM, [netperf_client_vm], {}),
          (
              netperf_benchmark.PrepareServerVM,
              [
                  netperf_server_vm,
                  netperf_client_vm.internal_ip,
                  netperf_client_vm.ip_address,
              ],
              {},
          ),
      ],
      2,
  )

  # Prepare Hammerdbcli benchmark
  hammerdbcli_benchmark.Prepare(benchmark_spec)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs both benchmarks in parallel.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

  Raises:
    RunError: A run-stage error raised by an individual benchmark.
  """
  netperf_client_vm, netperf_server_vm = (
      benchmark_spec.vm_groups['netperf_clients'][0],
      benchmark_spec.relational_db.server_vm,
  )
  # TODO(andytzhu) - synchronize the Run phases and add support for
  # targeting TPM and CPU load.
  output_samples_list = background_tasks.RunParallelThreads(
      [
          (hammerdbcli_benchmark.Run, [benchmark_spec], {}),
          (
              netperf_benchmark.RunClientServerVMs,
              [netperf_client_vm, netperf_server_vm],
              {},
          ),
      ],
      max_concurrency=2,
      post_task_delay=hammerdb.HAMMERDB_TPCC_RAMPUP.value,
  )

  return output_samples_list[0] + output_samples_list[1]


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  netperf_client_vm, netperf_server_vm = (
      benchmark_spec.vm_groups['netperf_clients'][0],
      benchmark_spec.relational_db.server_vm,
  )
  netperf_benchmark.CleanupClientServerVMs(netperf_client_vm, netperf_server_vm)
