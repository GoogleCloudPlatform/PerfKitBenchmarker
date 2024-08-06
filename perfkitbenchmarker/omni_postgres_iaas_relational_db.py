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
database for AlloyDB Omni.
"""

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS
DEFAULT_OMNI_POSTGRES_VERSION = '15'


class OmniPostgresIAASRelationalDb(iaas_relational_db.IAASRelationalDb):
  """Object representing a IAAS relational database Service."""

  ENGINE = sql_engine_utils.OMNI

  def _IsReadyUnmanaged(self):
    """Return true if the underlying resource is ready.

    Returns:
      True if Omni was installed successfully, False if not.

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
      raise NotImplementedError('db_flags is not supported for Omni Postgres')

  def _SetupLinuxUnmanagedDatabase(self):
    super()._SetupLinuxUnmanagedDatabase()
    self.server_vm.Install('docker')
    self.server_vm.RemoteCommand('sudo mkdir /scratch/omni')

    self.server_vm.RemoteCommand(
        'sudo docker run --name omni -e POSTGRES_PASSWORD=perfkitbenchmarker -v'
        ' /scratch/omni:/var/lib/postgresql/data -p 5432:5432 -d'
        ' google/alloydbomni'
    )

    @vm_util.Retry(
        poll_interval=5,
        fuzz=0,
        timeout=self.READY_TIMEOUT,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def WaitUntilReady():
      if not self._IsReady():
        raise errors.Resource.RetryableCreationError('Not yet ready')

    WaitUntilReady()

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
    return DEFAULT_OMNI_POSTGRES_VERSION

  def PrintUnmanagedDbStats(self):
    """Prints the unmanaged database stats."""
    # Log the wal size
    self.server_vm.RemoteCommand('sudo docker logs omni')
