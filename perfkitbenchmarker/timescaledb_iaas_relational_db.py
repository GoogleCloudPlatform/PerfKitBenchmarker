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
"""Responsible for IAAS relational database provisioning.

This class is responsible to provide helper methods for IAAS relational
database for TimescaleDB.
"""

from absl import flags
from perfkitbenchmarker import postgres_iaas_relational_db
from perfkitbenchmarker import sql_engine_utils

KB_TO_GB = 1.0 / 1000000

FLAGS = flags.FLAGS
DEFAULT_TIMESCALEDB_POSTGRES_VERSION = '16'


class TimescaleDbIAASRelationalDb(
    postgres_iaas_relational_db.PostgresIAASRelationalDb
    ):
  """Object representing a IAAS relational database Service."""

  ENGINE = sql_engine_utils.TIMESCALEDB

  def _IsReadyUnmanaged(self):
    """Return true if the underlying resource is ready.

    Returns:
      True if TimescaleDB was installed successfully, False if not.

    Raises:
      Exception: If this method is called when the database is a managed one.
        Shouldn't happen.
    """
    stdout, _ = self.server_vm.RemoteCommand(
        f'PGPASSWORD={self.spec.database_password} psql -h localhost -U'
        ' postgres -c "SELECT 1"',
        ignore_failure=True,
    )
    return '1 row' in stdout

  def _ApplyDbFlags(self):
    """Apply Flags on the database."""
    if FLAGS.db_flags:
      for flag in FLAGS.db_flags:
        self.server_vm.RemoteCommand(
            f'PGPASSWORD={self.spec.database_password} psql -h localhost -U'
            ' postgres -c "ALTER SYSTEM SET {}"'.format(flag)
        )

    self.server_vm.RemoteCommand(
        'sudo docker container restart timescaledb'
    )

  def _SetupLinuxUnmanagedDatabase(self):
    super(
        postgres_iaas_relational_db.PostgresIAASRelationalDb, self
        )._SetupLinuxUnmanagedDatabase()
    self.server_vm.Install('docker')
    self.server_vm.RemoteCommand('sudo mkdir /scratch/timescaledb')

    self.server_vm.RemoteCommand(
        'sudo docker run '
        f' --shm-size={self.server_vm.total_memory_kb * KB_TO_GB}g'
        ' --name timescaledb -e POSTGRES_PASSWORD=perfkitbenchmarker -v'
        ' /scratch/timescaledb:/var/lib/postgresql/data -p 5432:5432 -d'
        ' timescale/timescaledb-ha:pg16'
        f' -c shared_buffers={self.postgres_shared_buffer_size}GB'
    )

    self._WaitUntilReady()

    self.server_vm.RemoteCommand(
        f'PGPASSWORD={self.spec.database_password} psql -h localhost -U'
        ' postgres -c '
        '"CREATE ROLE %s LOGIN SUPERUSER PASSWORD \'%s\';"'
        % (self.spec.database_username, self.spec.database_password)
    )

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    return DEFAULT_TIMESCALEDB_POSTGRES_VERSION

  def PrintUnmanagedDbStats(self):
    """Prints the unmanaged database stats."""
    # Log the wal size
    self.server_vm.RemoteCommand('sudo docker logs timescaledb')
