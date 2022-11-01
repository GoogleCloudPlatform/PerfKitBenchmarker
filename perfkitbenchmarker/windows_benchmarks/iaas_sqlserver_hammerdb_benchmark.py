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

"""Benchmarking SQLServer with the Hammerdb benchmark on IAAS VMs.

This benchmark uses Windows as the OS for both the MySQL Server and the HammerDB
client(s). It benchmarks IAAS VMs and not managed services.
"""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import db_util
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.windows_packages import hammerdb


# Default SQLServer Port
DEFAULT_SQLSERVER_PORT = 1433
# Default SQLServer User
DEFAULT_SQLSERVER_USER = 'sa'


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'iaas_sqlserver_hammerdb'
BENCHMARK_CONFIG = """
iaas_sqlserver_hammerdb:
  description: Runs hammerdb against iaas sqlserver.
  vm_groups:
    servers:
      os_type: windows2022_desktop_sqlserver_2019_standard
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
    clients:
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
"""


def GetConfig(user_config):
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepare the benchmark by installing dependencies.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  hammerdb.SetDefaultConfig()
  server_vms = benchmark_spec.vm_groups['servers']
  client_vms = benchmark_spec.vm_groups['clients']
  assert(len(server_vms)) == 1
  assert(len(client_vms)) == 1
  vm_util.RunThreaded(lambda vm: vm.Install('hammerdb'), client_vms)

  username = DEFAULT_SQLSERVER_USER
  password = db_util.GenerateRandomDbPassword()

  def _ConfigureSQLServer(vm):
    iaas_relational_db.ConfigureSQLServer(vm, username, password)
  vm_util.RunThreaded(_ConfigureSQLServer, server_vms)

  def _SetupConfig(vm):
    hammerdb.SetupConfig(vm, sql_engine_utils.SQLSERVER,
                         hammerdb.HAMMERDB_SCRIPT.value,
                         server_vms[0].internal_ip, DEFAULT_SQLSERVER_PORT,
                         password, username, False)
  vm_util.RunThreaded(_SetupConfig, client_vms)


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
  # add other metadata, merge with existing code.
  return {
      'hammerdbcli_script': hammerdb.HAMMERDB_SCRIPT.value,
  }


def Cleanup(_):
  """No custom cleanup as the VMs are destroyed after the test."""
  pass
