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
"""Responsible for IAAS relational database provisioning.

This class is responsible to provide helper methods for IAAS relational
database.
"""
import posixpath

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils

FLAGS = flags.FLAGS

IS_READY_TIMEOUT = 600  # 10 minutes
MSSQL_PID = 'developer'  # Edition of SQL server on Linux

POSTGRES_RESOURCE_PATH = 'database_configurations/postgres'
POSTGRES_13_VERSION = '13'

POSTGRES_HBA_CONFIG = 'pg_hba.conf'
POSTGRES_CONFIG = 'postgresql.conf'
POSTGRES_CONFIG_PATH = '/etc/postgresql/{0}/main/'

DEFAULT_POSTGRES_VERSION = POSTGRES_13_VERSION

DEFAULT_ENGINE_VERSION = '13'


class PostgresIAASRelationalDb(iaas_relational_db.IAASRelationalDb):
  """Object representing a IAAS relational database Service."""

  ENGINE = sql_engine_utils.POSTGRES

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
      kb_to_gb = 1.0 / 1000000
      if not self.postgres_shared_buffer_size:
        self.postgres_shared_buffer_size = int(
            self.server_vm.total_memory_kb
            * kb_to_gb
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
      True if MySQL was installed successfully, False if not.

    Raises:
      Exception: If this method is called when the database is a managed one.
        Shouldn't happen.
    """
    stdout, stderr = self.server_vm.RemoteCommand(
        'sudo service postgresql status'
    )
    return stdout and not stderr

  def _ApplyDbFlags(self):
    """Apply Flags on the database."""
    if FLAGS.db_flags:
      version = self.spec.engine_version
      postgres_conf_path = POSTGRES_CONFIG_PATH.format(version)
      postgres_conf_file = postgres_conf_path + POSTGRES_CONFIG
      for flag in FLAGS.db_flags:
        self.server_vm.RemoteCommand(
            "sudo sh -c 'echo %s >> %s'" % (flag, postgres_conf_file)
        )
      self.server_vm.RemoteCommand('sudo systemctl restart postgresql')

  def _SetupLinuxUnmanagedDatabase(self):
    super()._SetupLinuxUnmanagedDatabase()
    if self.spec.engine_version == POSTGRES_13_VERSION:
      self.server_vm.Install('postgres13')
    else:
      raise relational_db.UnsupportedError(
          'Only postgres version 13 is currently supported'
      )

    vm = self.server_vm
    version = self.spec.engine_version
    postgres_conf_path = POSTGRES_CONFIG_PATH.format(version)
    postgres_conf_file = postgres_conf_path + POSTGRES_CONFIG
    postgres_hba_conf_file = postgres_conf_path + POSTGRES_HBA_CONFIG
    vm.PushFile(
        data.ResourcePath(
            posixpath.join(POSTGRES_RESOURCE_PATH, POSTGRES_HBA_CONFIG)
        )
    )
    vm.RemoteCommand('sudo systemctl restart postgresql')
    vm.RemoteCommand(
        'sudo -u postgres psql postgres -c '
        '"ALTER USER postgres PASSWORD \'%s\';"'
        % self.spec.database_password
    )
    vm.RemoteCommand(
        'sudo -u postgres psql postgres -c '
        '"CREATE ROLE %s LOGIN SUPERUSER PASSWORD \'%s\';"'
        % (self.spec.database_username, self.spec.database_password)
    )
    vm.RemoteCommand('sudo systemctl stop postgresql')

    # Change the directory to scratch
    vm.RemoteCommand(
        'sudo sed -i.bak '
        "\"s:'/var/lib/postgresql/{0}/main':'{1}/postgresql/{0}/main':\" "
        '/etc/postgresql/{0}/main/postgresql.conf'.format(
            version, self.server_vm.GetScratchDir()
        )
    )

    # Accept remote connection
    vm.RemoteCommand(
        'sudo sed -i.bak '
        r'"s:\#listen_addresses ='
        " 'localhost':listen_addresses = '*':\" "
        '{}'.format(postgres_conf_file)
    )

    # Set the size of the shared buffer
    vm.RemoteCommand(
        'sudo sed -i.bak "s:shared_buffers = 128MB:shared_buffers = {}GB:" '
        '{}'.format(self.postgres_shared_buffer_size, postgres_conf_file)
    )
    # Update data path to new location
    vm.RemoteCommand(f'sudo rsync -av /var/lib/postgresql {vm.GetScratchDir()}')

    # # Use cat to move files because mv will override file permissions
    self.server_vm.RemoteCommand(
        "sudo bash -c 'cat pg_hba.conf > {}'".format(postgres_hba_conf_file)
    )

    self.server_vm.RemoteCommand('sudo cat {}'.format(postgres_conf_file))
    self.server_vm.RemoteCommand('sudo cat {}'.format(postgres_hba_conf_file))
    vm.RemoteCommand('sudo systemctl restart postgresql')

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    return DEFAULT_POSTGRES_VERSION
