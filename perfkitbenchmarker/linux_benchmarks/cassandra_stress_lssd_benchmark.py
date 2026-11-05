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

"""Cassandra stress benchmark on VMs with Local SSDs and FIO preconditioning.

This benchmark measures performance of Cassandra clusters backed by Local SSDs
using cassandra-stress, with parallel FIO preconditioning of server disks prior
to Cassandra configuration and preloading.
"""

from perfkitbenchmarker.linux_benchmarks import cassandra_stress_benchmark as base_benchmark
from perfkitbenchmarker.linux_benchmarks import lssd_workloads_util

BENCHMARK_NAME = 'cassandra_stress_lssd'
BENCHMARK_CONFIG = """
cassandra_stress_lssd:
  description: >
    Benchmark Cassandra on Local SSDs using cassandra-stress with FIO
    preconditioning.
  vm_groups:
    workers:
      vm_spec:
        GCP:
          machine_type: z3-highmem-16-highlssd
          zone: us-central1-a
        Azure:
          machine_type: Standard_L16s_v3
          zone: eastus2
        AWS:
          machine_type: i4i.4xlarge
          zone: us-east-1a
      disk_spec:
        GCP:
          disk_type: local
          interface: NVME
          mount_point: /scratch
        Azure:
          disk_type: local
          num_striped_disks: 2
          mount_point: /scratch
        AWS:
          disk_type: local
          num_striped_disks: 1
          mount_point: /scratch
    client:
      vm_spec:
        GCP:
          machine_type: c4-standard-32
          zone: us-central1-a
        Azure:
          machine_type: Standard_D32s_v5
          zone: eastus2
        AWS:
          machine_type: m6i.8xlarge
          zone: us-east-1a
  flags:
    cassandra_stress_run_duration: 30m
    is_row_cache_enabled: false
    cassandra_replication_factor: 3
    cassandra_stress_consistency_level: LOCAL_QUORUM
    cassandra_client_zones: us-central1-a
    cassandra_server_zones: us-central1-a,us-central1-a,us-central1-a
    cassandra_stress_population_distribution: GAUSSIAN
    cassandra_yaml_template_path: cassandra/cassandra-lssd.yaml.j2
    db_machine_type: z3-highmem-16-highlssd
    disk_fs_type: ext4
    os_type: ubuntu2404
    openjdk_version: 17
    lssd_workload_precondition_disk: True
    iostat: True
    sar: False
"""


def GetConfig(user_config):
  """Returns the benchmark config, applying user and flag overrides."""
  return base_benchmark.GetConfig(
      user_config,
      benchmark_config=BENCHMARK_CONFIG,
      benchmark_name=BENCHMARK_NAME,
  )


def CheckPrerequisites(benchmark_config):
  """Verifies that required flags and template resources are present."""
  base_benchmark.CheckPrerequisites(benchmark_config)


def Prepare(benchmark_spec):
  """Preconditions server Local SSDs, installs Cassandra, and loads keys.

  Local SSDs on Cassandra server VMs are preconditioned with FIO and
  reformatted before delegating to `base_benchmark.Prepare` so that
  `cassandra.Configure` creates the data directory on the final filesystem.

  Args:
    benchmark_spec: The benchmark specification.
  """
  cassandra_vms, _ = base_benchmark.ParseVmGroups(benchmark_spec.vm_groups)
  lssd_workloads_util.PreconditionServerDisksIfEnabled(cassandra_vms)
  base_benchmark.Prepare(benchmark_spec)


def Run(benchmark_spec):
  """Runs cassandra-stress and returns collected samples."""
  return base_benchmark.Run(benchmark_spec)


def Cleanup(benchmark_spec):
  """Stops Cassandra and cleans server data directories."""
  base_benchmark.Cleanup(benchmark_spec)
