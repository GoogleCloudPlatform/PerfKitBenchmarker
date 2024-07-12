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
from perfkitbenchmarker.linux_packages import hammerdb as linux_hammerdb
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
      controller:
        os_type: windows2022_desktop
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-central1-c
            boot_disk_size: 50
          AWS:
            machine_type: m6i.xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_D4s_v5
            zone: eastus
            boot_disk_type: Premium_LRS
      servers:
        os_type: windows2022_desktop_sqlserver_2019_standard
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-central1-c
            boot_disk_size: 50
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
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['relational_db']['vm_groups']['controller']['vm_count'] = 0
  if FLAGS.db_high_availability:
    # We need two additional vms for sql ha deployment.
    # First vm to act as the second node in sql cluster
    # and additional vm to act as a domain controller.

    config['relational_db']['vm_groups']['servers']['vm_count'] = 2
    config['relational_db']['vm_groups']['controller']['vm_count'] = 1
    config['relational_db']['vm_groups']['controller']['vm_spec'][FLAGS.cloud][
        'zone'
    ] = config['relational_db']['vm_groups']['servers']['vm_spec'][FLAGS.cloud][
        'zone'
    ]

    if FLAGS.db_high_availability_type == 'FCIMW':
      config['relational_db']['vm_groups']['servers']['disk_spec'][FLAGS.cloud][
          'multi_writer_mode'
      ] = True

  return config


def CheckPrerequisites(_):
  """Verifies that benchmark flags is correct."""
  if hammerdb.HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION.value not in [
      hammerdb.NON_OPTIMIZED,
      hammerdb.MINIMUM_RECOVERY,
  ]:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Non-optimized hammerdbcli_optimized_server_configuration'
        ' is not implemented.'
    )


def Prepare(benchmark_spec):
  """Prepare the benchmark by installing dependencies.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  relational_db = benchmark_spec.relational_db
  vm = relational_db.client_vm
  num_cpus = None
  if hasattr(relational_db, 'server_vm'):
    server_vm = relational_db.server_vm
    num_cpus = server_vm.NumCpusForBenchmark()
  hammerdb.SetDefaultConfig(num_cpus)
  vm.Install('hammerdb')

  is_azure = FLAGS.cloud == 'Azure' and FLAGS.use_managed_db
  if (
      benchmark_spec.relational_db.spec.high_availability
      and benchmark_spec.relational_db.spec.high_availability_type == 'AOAG'
  ):
    db_name = linux_hammerdb.MAP_SCRIPT_TO_DATABASE_NAME[
        linux_hammerdb.HAMMERDB_SCRIPT.value
    ]
    relational_db.client_vm_query_tools.IssueSqlCommand(
        """CREATE DATABASE [{0}];
        BACKUP DATABASE [{0}] TO DISK = 'F:\\Backup\\{0}.bak';
        ALTER AVAILABILITY GROUP [{1}] ADD DATABASE [{0}];
        """.format(db_name, sql_engine_utils.SQLSERVER_AOAG_NAME)
    )
  elif is_azure and hammerdb.HAMMERDB_SCRIPT.value == 'tpc_c':
    # Create the database first only Azure requires creating the database.
    relational_db.client_vm_query_tools.IssueSqlCommand('CREATE DATABASE tpcc;')

  hammerdb.SetupConfig(
      vm,
      sql_engine_utils.SQLSERVER,
      hammerdb.HAMMERDB_SCRIPT.value,
      relational_db.endpoint,
      relational_db.port,
      relational_db.spec.database_password,
      relational_db.spec.database_username,
      is_azure,
  )

  # SQL Server exhibits better performance when restarted after prepare step
  if FLAGS.hammerdbcli_restart_before_run:
    relational_db.RestartDatabase()


def SetMinimumRecover(relational_db):
  """Change sql server settings to make TPM nubmers stable."""
  # https://www.mssqltips.com/sqlservertip/4541/adjust-targetrecoverytime-to-reduce-sql-server-io-spikes/
  relational_db.client_vm_query_tools.IssueSqlCommand(
      'ALTER DATABASE tpcc SET TARGET_RECOVERY_TIME = 12000 SECONDS;'
  )
  relational_db.client_vm_query_tools.IssueSqlCommand(
      'ALTER DATABASE tpcc SET AUTO_UPDATE_STATISTICS OFF;'
  )

  relational_db.client_vm_query_tools.IssueSqlCommand(
      'ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 1;'
  )

  relational_db.client_vm_query_tools.IssueSqlCommand(
      'ALTER DATABASE [tpcc] SET DELAYED_DURABILITY = DISABLED WITH NO_WAIT;'
  )

  relational_db.client_vm_query_tools.IssueSqlCommand(
      "ALTER DATABASE [tpcc] MODIFY FILE ( NAME = N'tpcc', SIZE = 500 GB,"
      ' FILEGROWTH = 10%);'
  )

  relational_db.client_vm_query_tools.IssueSqlCommand(
      "dbcc shrinkfile('tpcc_log',truncateonly)"
  )

  relational_db.client_vm_query_tools.IssueSqlCommand(
      "alter database tpcc modify file (name='tpcc_log', size=64000)"
  )

  # Verify the setting changed
  relational_db.client_vm_query_tools.IssueSqlCommand("dbcc loginfo('tpcc')")
  relational_db.client_vm_query_tools.IssueSqlCommand(
      'SELECT name,target_recovery_time_in_seconds FROM sys.databases;'
  )


def Run(benchmark_spec):
  """Run the benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  client_vms = benchmark_spec.vm_groups['clients']
  relational_db = benchmark_spec.relational_db

  if (
      hammerdb.HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION.value
      == hammerdb.MINIMUM_RECOVERY
  ):
    SetMinimumRecover(relational_db)

  samples = hammerdb.Run(
      client_vms[0],
      sql_engine_utils.SQLSERVER,
      hammerdb.HAMMERDB_SCRIPT.value,
      timeout=linux_hammerdb.HAMMERDB_RUN_TIMEOUT.value,
  )

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
