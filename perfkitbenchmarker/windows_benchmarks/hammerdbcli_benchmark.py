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

"""Benchmarking SQLServer with the Hammerdb benchmark.

This benchmark uses Windows as the OS for both the database server and the
HammerDB client(s).
"""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sql_engine_utils

from perfkitbenchmarker.windows_packages import hammerdb


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'hammerdbcli'
BENCHMARK_CONFIG = """
hammerdbcli:
  description: Runs hammerdb against specified databases.
  relational_db:
    engine: sqlserver
    db_spec:
      GCP:
        machine_type:
          cpus: 4
          memory: 7680MiB
        zone: us-central1-c
      AWS:
        machine_type: db.m5.xlarge
        zone: us-west-1a
      Azure:
        machine_type:
          tier: Premium
          compute_units: 500
        zone: eastus
    db_disk_spec:
        GCP:
          disk_size: 500
          disk_type: pd-ssd
          num_striped_disks: 1
          mount_point: /scratch
        AWS:
          disk_size: 500
          disk_type: gp2
          num_striped_disks: 1
          mount_point: /scratch
        Azure:
          disk_size: 500
          disk_type: Premium_LRS
          num_striped_disks: 1
          mount_point: /scratch
    vm_groups:
      servers:
        os_type: windows2022_desktop_sqlserver_2019_standard
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-central1-c
            boot_disk_size: 50
            boot_disk_type: pd-ssd
          AWS:
            machine_type: m6i.xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_D4s_v5
            zone: eastus
            boot_disk_type: Premium_LRS
        disk_spec:
          GCP:
            disk_size: 500
            disk_type: pd-ssd
            num_striped_disks: 1
            mount_point: /scratch
          AWS:
            disk_size: 500
            disk_type: gp2
            num_striped_disks: 1
            mount_point: /scratch
          Azure:
            disk_size: 500
            disk_type: Premium_LRS
            num_striped_disks: 1
            mount_point: /scratch
      clients:
        os_type: windows2022_desktop
        vm_spec:
          GCP:
            machine_type: n2-standard-16
            zone: us-central1-c
            boot_disk_size: 50
            boot_disk_type: pd-ssd
          AWS:
            machine_type: m6i.4xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_D16s_v5
            zone: eastus
            boot_disk_type: Premium_LRS
        disk_spec:
          GCP:
            disk_size: 500
            disk_type: pd-ssd
            num_striped_disks: 1
            mount_point: /scratch
          AWS:
            disk_size: 500
            disk_type: gp2
            num_striped_disks: 1
            mount_point: /scratch
          Azure:
            disk_size: 500
            disk_type: Premium_LRS
            num_striped_disks: 1
            mount_point: /scratch
"""


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Verifies that benchmark flags is correct."""
  if hammerdb.HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION.value != hammerdb.NON_OPTIMIZED:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Non-optimized hammerdbcli_optimized_server_configuration'
        ' is not implemented.')


def Prepare(benchmark_spec):
  """Prepare the benchmark by installing dependencies.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  hammerdb.SetDefaultConfig()
  relational_db = benchmark_spec.relational_db
  vm = relational_db.client_vm
  vm.Install('hammerdb')
  hammerdb.SetupConfig(vm, sql_engine_utils.SQLSERVER,
                       hammerdb.HAMMERDB_SCRIPT.value, relational_db.endpoint,
                       relational_db.port, relational_db.spec.database_password,
                       relational_db.spec.database_username, False)


def Run(benchmark_spec):
  """Run the benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  client_vms = benchmark_spec.vm_groups['clients']

  samples = hammerdb.Run(client_vms[0], sql_engine_utils.SQLSERVER,
                         hammerdb.HAMMERDB_SCRIPT.value,
                         timeout=None)

  metadata = GetMetadata()
  for sample in samples:
    sample.metadata.update(metadata)
  return samples


def GetMetadata():
  metadata = hammerdb.GetMetadata(sql_engine_utils.SQLSERVER)
  # No reason to support multiple runs in a single benchmark run yet.
  metadata.pop('hammerdbcli_num_run', None)
  # columnar engine not applicable to sqlserver.
  metadata.pop('hammerdbcli_load_tpch_tables_to_columnar_engine', None)
  return metadata


def Cleanup(_):
  """No custom cleanup as the VMs are destroyed after the test."""
  pass
