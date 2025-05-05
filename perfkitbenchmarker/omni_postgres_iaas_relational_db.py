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
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util

KB_TO_GB = 1.0 / 1000000

FLAGS = flags.FLAGS
DEFAULT_OMNI_POSTGRES_VERSION = '15'


class OmniPostgresIAASRelationalDb(iaas_relational_db.IAASRelationalDb):
  """Object representing a IAAS relational database Service."""

  ENGINE = sql_engine_utils.OMNI

  def __init__(self, relational_db_spec):
    """Initialize the Postgres IAAS relational database object.

    Args:
      relational_db_spec: spec of the managed database.

    Raises:
      UnsupportedError: if high availability is requested for an unmanaged db.
    """
    super().__init__(relational_db_spec)
    if self.spec.high_availability:
      raise relational_db.UnsupportedError(
          'High availability is unsupported for unmanaged databases.'
      )
    self.postgres_shared_buffer_size = FLAGS.postgres_shared_buffer_size

  def SetVms(self, vm_groups):
    super().SetVms(vm_groups)
    if 'servers' in vm_groups:
      if not self.postgres_shared_buffer_size:
        self.postgres_shared_buffer_size = int(
            self.server_vm.total_memory_kb
            * KB_TO_GB
            * FLAGS.postgres_shared_buffer_ratio
        )

  def GetResourceMetadata(self):
    metadata = super().GetResourceMetadata()
    metadata.update(
        {'postgres_shared_buffer_size': self.postgres_shared_buffer_size}
    )
    return metadata

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
      for flag in FLAGS.db_flags:
        self.server_vm.RemoteCommand(
            f'PGPASSWORD={self.spec.database_password} psql -h localhost -U'
            f' postgres -c "ALTER SYSTEM SET {flag}"'
        )

    self.server_vm.RemoteCommand(
        'sudo docker container restart omni'
    )

  def _SetupLinuxUnmanagedDatabase(self):
    super()._SetupLinuxUnmanagedDatabase()
    self.server_vm.Install('docker')
    self.server_vm.RemoteCommand('sudo mkdir /scratch/omni')

    self.server_vm.RemoteCommand(
        'sudo docker run '
        f' --shm-size={self.server_vm.total_memory_kb * KB_TO_GB}g'
        ' --name omni -e POSTGRES_PASSWORD=perfkitbenchmarker -v'
        ' /scratch/omni:/var/lib/postgresql/data -p 5432:5432 -d'
        ' google/alloydbomni'
        f' -c shared_buffers={self.postgres_shared_buffer_size}GB'
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
