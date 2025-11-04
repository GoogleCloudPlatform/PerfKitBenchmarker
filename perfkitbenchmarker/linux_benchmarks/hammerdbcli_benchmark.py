"""Runs the HammerDB relational database benchmark."""

import posixpath
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import mysql_iaas_relational_db
from perfkitbenchmarker import postgres_iaas_relational_db
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import hammerdb
from perfkitbenchmarker.providers.gcp import gcp_alloy_db  # pylint: disable=unused-import


# Update this version when changing a config
# TODO(chunla) Consider adding checks to make sure this version gets updated.
CONFIG_VERSION = 'v1.0'

# MYSQL Config file path
MYSQL_CONFIG_PATH = '/etc/mysql/mysql.conf.d/mysqld.cnf'
FLAGS = flags.FLAGS

DATABASE_FILE_SIZE = '{{DATABASE_FILE_SIZE}}'
INNODB_BUFFER_POOL_SIZE = '{{INNODB_BUFFER_POOL_SIZE}}'
SHARED_BUFFER_SIZE = '{{SHARED_BUFFER_SIZE}}'
MAX_CONNECTIONS = '{{MAX_CONNECTIONS}}'
PG_VERSION = '{{PG_VERSION}}'
SCRATCH_DIR_PLACEHOLDER = '{{SCRATCH_DIR}}'
BENCHMARK_NAME = 'hammerdbcli'
BENCHMARK_CONFIG = """
hammerdbcli:
  description: Runs hammerdbcli.
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
            boot_disk_size: 200
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
        os_type: debian11
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
"""


def GetConfig(user_config: dict[Any, Any]) -> dict[Any, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _PrepareServer(db: iaas_relational_db.IAASRelationalDb):
  """Applies server configurations if necessary."""
  optimized_server_config = (
      hammerdb.HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION.value
  )
  if (
      optimized_server_config != hammerdb.NON_OPTIMIZED
      and not FLAGS.use_managed_db
  ):
    SetOptimizedServerConfiguration(optimized_server_config, db.server_vm, db)
  elif db.engine == sql_engine_utils.POSTGRES:
    custom_server_config = hammerdb.HAMMERDB_SERVER_CONFIGURATION.value
    if custom_server_config:
      SetPostgresOptimizedServerConfiguration(
          optimized_server_config,
          db.server_vm,
          db,
          custom_server_config,
      )


def _PrepareAlloyDb(db: gcp_alloy_db.GCPAlloyRelationalDb) -> None:
  """Prepares AlloyDB if necessary."""
  db_name = hammerdb.MAP_SCRIPT_TO_DATABASE_NAME[hammerdb.HAMMERDB_SCRIPT.value]
  if not db.enable_columnar_engine:
    return
  db.CreateColumnarEngineExtension(db_name)
  if (
      hammerdb.HAMMERDB_SCRIPT.value == hammerdb.HAMMERDB_SCRIPT_TPC_H
      and hammerdb.LOAD_TPCH_TABLES_TO_COLUMNAR_ENGINE.value
  ):
    for table in hammerdb.TPCH_TABLES:
      db.AddTableToColumnarEngine(table, database_name=db_name)


def _GetNumCpus(db: relational_db.BaseRelationalDb) -> int | None:
  """Returns the number of CPUs for the database."""
  if hasattr(db, 'server_vm'):
    return db.server_vm.NumCpusForBenchmark()
  return None


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepare Hammerdbcli by installing dependencies and uploading binaries.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  client_vms = benchmark_spec.vm_groups['clients']
  assert len(client_vms) == 1
  client_vm = client_vms[0]
  db = benchmark_spec.relational_db

  hammerdb.SetDefaultConfig(_GetNumCpus(db))

  client_vm.Install('hammerdb')

  if not FLAGS.use_managed_db:
    _PrepareServer(db)

  hammerdb.SetupConfig(
      vm=client_vm,
      db_engine=db.engine,
      hammerdb_script=hammerdb.HAMMERDB_SCRIPT.value,
      ip=db.endpoint,
      port=db.port,
      password=db.spec.database_password,
      user=db.spec.database_username,
      is_managed_azure=(FLAGS.cloud == 'Azure' and FLAGS.use_managed_db),
  )

  if db.engine == sql_engine_utils.ALLOYDB:
    _PrepareAlloyDb(db)


def SetOptimizedServerConfiguration(
    optimized_server_config: str,
    server_vm: virtual_machine.BaseVirtualMachine,
    db: relational_db.BaseRelationalDb,
) -> None:
  """Set the optimized configuration for hammerdb.

  Args:
    optimized_server_config: The optimized server configuration type. Currently
      support MINIMUM_RECOVERY and RESTORABLE
    server_vm: Server VM to host the database.
    db: Relational database class.
  """
  if db.engine == sql_engine_utils.MYSQL:
    SetMysqlOptimizedServerConfiguration(optimized_server_config, server_vm, db)
  elif db.engine == sql_engine_utils.POSTGRES:
    SetPostgresOptimizedServerConfiguration(
        optimized_server_config, server_vm, db
    )


def SetMysqlOptimizedServerConfiguration(
    optimized_server_config: str,
    server_vm: virtual_machine.BaseVirtualMachine,
    db: mysql_iaas_relational_db.MysqlIAASRelationalDb,
) -> None:
  """Set the optimized configuration for hammerdb for Mysql.

  Args:
    optimized_server_config: The optimized server configuration type. Currently
      support MINIMUM_RECOVERY and RESTORABLE
    server_vm: Server VM to host the database.
    db: Relational database class.
  """
  config = f'hammerdb_optimized_{optimized_server_config}_mysqld.cnf'
  server_vm.PushFile(
      data.ResourcePath(posixpath.join('relational_db_configs', config)),
      '',
  )
  hammerdb.SearchAndReplaceGuestFile(
      server_vm,
      '~/',
      config,
      INNODB_BUFFER_POOL_SIZE,
      db.innodb_buffer_pool_size,
  )
  server_vm.RemoteCommand(f'sudo mv {config} {MYSQL_CONFIG_PATH}')
  server_vm.RemoteCommand(f'sudo chown mysql:mysql {MYSQL_CONFIG_PATH}')
  server_vm.RemoteCommand(f'sudo cat {MYSQL_CONFIG_PATH}')
  server_vm.RemoteCommand('sudo sudo service mysql restart')

  # Flush table might be refreshed after changing the cnf file.
  db.SetMYSQLClientPrivileges()


def SetPostgresOptimizedServerConfiguration(
    optimized_server_config: str,
    server_vm: virtual_machine.BaseVirtualMachine,
    db: postgres_iaas_relational_db.PostgresIAASRelationalDb,
    custom_server_config: str = '',
) -> None:
  """Set the optimized configuration for hammerdb for postgres.

  Args:
    optimized_server_config: The optimized server configuration type. Currently
      support MINIMUM_RECOVERY and RESTORABLE
    server_vm: Server VM to host the database.
    db: Relational database class.
    custom_server_config: The custom server configuration file name.
  """

  shared_buffer_size = db.postgres_shared_buffer_size
  server_vm.RemoteCommand('sudo systemctl stop postgresql')

  # Set the hugepages size for hammerdb
  # https://www.hammerdb.com/blog/uncategorized/hammerdb-best-practice-for-postgresql-performance-and-scalability/
  # Each huge page is 2MB, the huge page size should be larger
  # than the SHARED_BUFFER_SIZE.
  # Needs extra 20 percent of total memory for other processes.
  # Divided by 2 to covert MB to page number.
  shared_buffer_size_mb = shared_buffer_size * 1000

  huge_page_number = int(shared_buffer_size_mb / 2 * 1.2)
  server_vm.RemoteCommand(
      f'echo "vm.nr_hugepages = {huge_page_number}" '
      '| sudo tee -a'
      ' /etc/sysctl.conf'
  )
  server_vm.RemoteCommand('sudo sysctl -p')

  if optimized_server_config == hammerdb.NON_OPTIMIZED and custom_server_config:
    config = custom_server_config
  else:
    config = (
        f'hammerdb_optimized_{optimized_server_config}_' + 'postgresql.conf'
    )
  server_vm.PushFile(
      data.ResourcePath(posixpath.join('relational_db_configs', config)),
      '',
  )
  hammerdb.SearchAndReplaceGuestFile(
      server_vm,
      '~/',
      config,
      MAX_CONNECTIONS,
      str(hammerdb.HAMMERDB_NUM_VU.value + 10),
  )
  hammerdb.SearchAndReplaceGuestFile(
      server_vm, '~/', config, SHARED_BUFFER_SIZE, str(shared_buffer_size)
  )
  db_version = db.spec.engine_version
  hammerdb.SearchAndReplaceGuestFile(
      server_vm, '~/', config, PG_VERSION, db_version
  )
  hammerdb.SearchAndReplaceGuestFile(
      server_vm,
      '~/',
      config,
      SCRATCH_DIR_PLACEHOLDER,
      server_vm.GetScratchDir(),
  )
  server_vm.RemoteCommand(
      f'sudo bash -c "cat {config} >'
      f' /etc/postgresql/{db_version}/main/postgresql.conf"'
  )
  server_vm.RemoteCommand('sudo systemctl restart postgresql')


def AddMetadata(metadata: dict[str, Any], updates: Any) -> dict[str, Any]:
  """Returns a copy of the metadata with dictionary update applied."""
  result = metadata.copy()
  result.update(updates)
  return result


def _CheckAlloyDbColumnarEngine(
    db: gcp_alloy_db.GCPAlloyRelationalDb,
    client_vm: virtual_machine.BaseVirtualMachine,
    script: str,
    timeout: int | None,
    database_name: str,
) -> list[sample.Sample]:
  """Checks AlloyDB columnar engine recommendation and reruns if needed."""
  columnar_size, relation = db.GetColumnarEngineRecommendation('tpch')
  db.UpdateAlloyDBFlags(columnar_size, True, 'off', relation=relation)
  db.WaitColumnarEnginePopulates(database_name)
  # Another prewarm
  stdout = hammerdb.Run(client_vm, db.engine, script, timeout=timeout)
  return hammerdb.ParseResults(script=script, stdout=stdout, vm=client_vm)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the Hammerdbcli benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.

  Raises:
    Exception: if the script is unknown
  """
  client_vm = benchmark_spec.vm_groups['clients'][0]
  db = benchmark_spec.relational_db
  num_cpus = (
      db.server_vm.NumCpusForBenchmark() if hasattr(db, 'server_vm') else None
  )
  script = hammerdb.HAMMERDB_SCRIPT.value
  timeout = hammerdb.HAMMERDB_RUN_TIMEOUT.value

  hammerdb.SetDefaultConfig(num_cpus)

  metadata = hammerdb.GetMetadata(db.engine)
  # Consider if we should have separate versioning for each config.
  metadata['hammerdbcli_config_version'] = CONFIG_VERSION

  samples = []
  for i in range(1, 1 + hammerdb.NUM_RUN.value):
    metadata['run_iteration'] = i
    stdout = hammerdb.Run(client_vm, db.engine, script, timeout=timeout)
    current_samples = hammerdb.ParseResults(
        script=script, stdout=stdout, vm=client_vm
    )
    if (
        db.engine == sql_engine_utils.ALLOYDB
        and db.enable_columnar_engine_recommendation
        and i == 1
    ):
      database_name = hammerdb.MAP_SCRIPT_TO_DATABASE_NAME[script]
      current_samples = _CheckAlloyDbColumnarEngine(
          db, client_vm, script, timeout, database_name
      )

    for s in current_samples:
      s.metadata.update(metadata)
    samples += current_samples
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup the VM to its original state.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec
