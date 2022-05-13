# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Pgbench benchmark for PostgreSQL databases.

  Pgbench is a TPC-B like database benchmark for Postgres and
  is published by the PostgreSQL group.

  This implementation of pgbench in PKB uses the ManagedRelationalDB
  resource. A client VM is also required. To change the specs of the
  database server, change the vm_spec nested inside
  managed_relational_db spec. To change the specs of the client,
  change the vm_spec nested directly inside the pgbench spec.

  The scale factor can be used to set the size of the test database.
  Additionally, the runtime per step, as well as the number of clients
  at each step can be specified.

  Documentations of pgbench
  https://www.postgresql.org/docs/10/pgbench.html

  This benchmark is written for pgbench 9.5, which is the default
  (as of 10/2017) version installed on Ubuntu 16.04.
"""

import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker.linux_packages import pgbench

flags.DEFINE_integer(
    'pgbench_scale_factor', 1, 'scale factor used to fill the database',
    lower_bound=1)
flags.DEFINE_integer(
    'pgbench_seconds_per_test', 10, 'number of seconds to run each test phase',
    lower_bound=1)
flags.DEFINE_integer(
    'pgbench_seconds_to_pause_before_steps', 30,
    'number of seconds to pause before each client load step')
flag_util.DEFINE_integerlist(
    'pgbench_client_counts',
    flag_util.IntegerList([1]),
    'array of client counts passed to pgbench', module_name=__name__)
flag_util.DEFINE_integerlist(
    'pgbench_job_counts',
    flag_util.IntegerList([]),
    'array of job counts passed to pgbench. Jobs count '
    'is the number of worker threads within pgbench. '
    'When this is empty, Pgbench is run with job counts equals to '
    'client counts. If this is specified, it must have the same length as '
    'pgbench_client_counts.', module_name=__name__)
FLAGS = flags.FLAGS


BENCHMARK_NAME = 'pgbench'
BENCHMARK_CONFIG = """
pgbench:
  description: pgbench benchmark for managed PostgreSQL databases
  relational_db:
    engine: postgres
    db_spec:
      GCP:
        machine_type:
          cpus: 16
          memory: 64GiB
        zone: us-central1-c
      AWS:
        machine_type: db.m4.4xlarge
        zone: us-west-1c
      Azure:
        machine_type:
          tier: Standard
          compute_units: 800
        zone: eastus
    db_disk_spec:
      GCP:
        disk_size: 1000
        disk_type: pd-ssd
      AWS:
        disk_size: 6144
        disk_type: gp2
      Azure:
        #Valid storage sizes range from minimum of 128000 MB and additional increments of 128000 MB up to maximum of 1024000 MB.
        disk_size: 128
    vm_groups:
      clients:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-west-1c
          Azure:
            machine_type: Standard_A4m_v2
            zone: eastus
        disk_spec: *default_500_gb
      servers:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-west-1c
          Azure:
            machine_type: Standard_A4m_v2
            zone: eastus
        disk_spec: *default_500_gb
"""


TEST_DB_NAME = 'perftest'
DEFAULT_DB_NAME = 'postgres'
# TODO(ferneyhough): determine MAX_JOBS from VM NumCpusForBenchmark()
MAX_JOBS = 16


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config:  Benchmark config to verify

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config


def UpdateBenchmarkSpecWithPrepareStageFlags(benchmark_spec):
  """Updates benchmark_spec with flags that are used in the prepare stage."""
  benchmark_spec.scale_factor = FLAGS.pgbench_scale_factor


def UpdateBenchmarkSpecWithRunStageFlags(benchmark_spec):
  """Updates benchmark_spec with flags that are used in the run stage."""
  benchmark_spec.seconds_per_test = FLAGS.pgbench_seconds_per_test
  benchmark_spec.seconds_to_pause = FLAGS.pgbench_seconds_to_pause_before_steps
  benchmark_spec.client_counts = FLAGS.pgbench_client_counts
  benchmark_spec.job_counts = FLAGS.pgbench_job_counts


def GetDbSize(relational_db, db_name):
  """Get the size of the database."""
  stdout, _ = relational_db.client_vm_query_tools.IssueSqlCommand(
      f'SELECT pg_size_pretty(pg_database_size(\'{db_name}\'))',
      database_name=TEST_DB_NAME)
  return ParseSizeFromTable(stdout)


def Prepare(benchmark_spec):
  """Prepares the client and server VM for the pgbench test.

  This function installs pgbench on the client and creates and populates
  the test database on the server.

  If DEFAULT_DB_NAME exists, it will be dropped and recreated,
  else it will be created. pgbench will populate the database
  with sample data using FLAGS.pgbench_scale_factor.

  Args:
    benchmark_spec: benchmark_spec object which contains the database server
                    and client_vm
  """
  vm = benchmark_spec.vms[0]
  vm.Install('pgbench')

  UpdateBenchmarkSpecWithPrepareStageFlags(benchmark_spec)

  db = benchmark_spec.relational_db

  CreateDatabase(benchmark_spec, DEFAULT_DB_NAME, TEST_DB_NAME)

  connection_string = db.client_vm_query_tools.GetConnectionString(TEST_DB_NAME)
  vm.RobustRemoteCommand(f'pgbench {connection_string} -i '
                         f'-s {benchmark_spec.scale_factor}')


def ParseSizeFromTable(stdout):
  """Parse stdout of a table representing size of the database.

  Example stdoutput is:

  pg_size_pretty
  ----------------
  22 MB
  (1 row)

  Args:
     stdout:  stdout from psql query obtaining the table size.
  Returns:
     size in MB that was parsed from the table
  Raises:
     Exception: if unknown how to parse the output.
  """
  size_line = stdout.splitlines()[2]
  match = re.match(r' *(\d+) *(\w*)$', size_line)
  size = float(match.group(1))
  units = match.group(2)

  if units == 'MB':
    return size
  elif units == 'GB':
    return size * 1000
  else:
    raise Exception(f'Unknown how to parse units {size} {units}.')


def DoesDatabaseExist(client_vm, connection_string, database_name):
  """Returns whether or not the specifid database exists on the server.

  Args:
    client_vm: client vm which will run the query
    connection_string: database server connection string understood by psql
    database_name: name of database to check for existense

  Returns:
    True if database_name exists, else False
  """
  command = (f'psql {connection_string} '
             fr'-lqt | cut -d \| -f 1 | grep -qw {database_name}')
  _, _, return_value = client_vm.RemoteCommandWithReturnCode(
      command, ignore_failure=True)
  return return_value == 0


def CreateDatabase(benchmark_spec, default_database_name, new_database_name):
  """Creates a new database on the database server.

  If new_database_name already exists on the server, it will be dropped and
  recreated.

  Args:
    benchmark_spec: benchmark_spec object which contains the database server
      and client_vm
    default_database_name: name of the default database guaranteed to exist on
      the server
    new_database_name: name of the new database to create, or drop and recreate
  """
  client_vm = benchmark_spec.vms[0]
  db = benchmark_spec.relational_db
  connection_string = db.client_vm_query_tools.GetConnectionString(
      database_name=default_database_name)

  if DoesDatabaseExist(client_vm, connection_string, new_database_name):
    db.client_vm_query_tools.IssueSqlCommand(
        f'DROP DATABASE {new_database_name}',
        database_name=default_database_name)

  db.client_vm_query_tools.IssueSqlCommand(
      'CREATE DATABASE {0}'.format(new_database_name),
      database_name=default_database_name,
  )


def Run(benchmark_spec):
  """Runs the pgbench benchark on the client vm, against the db server.

  Args:
    benchmark_spec: benchmark_spec object which contains the database server
      and client_vm

  Returns:
    a list of sample objects
  """
  UpdateBenchmarkSpecWithRunStageFlags(benchmark_spec)

  relational_db = benchmark_spec.relational_db
  db_size = GetDbSize(relational_db, TEST_DB_NAME)
  common_metadata = GetMetaData(db_size, benchmark_spec)

  pgbench.RunPgBench(benchmark_spec, relational_db, benchmark_spec.vms[0],
                     TEST_DB_NAME,
                     benchmark_spec.client_counts,
                     benchmark_spec.job_counts,
                     benchmark_spec.seconds_to_pause,
                     benchmark_spec.seconds_per_test, common_metadata)
  return []


def GetMetaData(db_size, benchmark_spec):
  """Gets the metadata related to pgbench."""
  metadata = {
      'scale_factor': benchmark_spec.scale_factor,
      'postgres_db_size_MB': db_size,
      'seconds_per_test': benchmark_spec.seconds_per_test,
      'seconds_to_pause_before_steps': benchmark_spec.seconds_to_pause,
  }

  return metadata


def Cleanup(benchmark_spec):
  """Uninstalls pgbench from the client vm."""
  vm = benchmark_spec.vms[0]
  vm.Uninstall('pgbench')
