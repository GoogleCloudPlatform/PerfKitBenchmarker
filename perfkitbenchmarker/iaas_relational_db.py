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

from absl import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils

FLAGS = flags.FLAGS

IS_READY_TIMEOUT = 600  # 10 minutes
MSSQL_PID = 'developer'  # Edition of SQL server on Linux


class IAASRelationalDb(relational_db.BaseRelationalDb):
  """Object representing a IAAS relational database Service."""

  IS_MANAGED = False
  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED', 'ENGINE']
  DEFAULT_ENGINE_VERSION = None

  def __init__(self, relational_db_spec):
    """Initialize the IAAS relational database object.

    Args:
      relational_db_spec: spec of the managed database.

    Raises:
      UnsupportedError: if high availability is requested for an unmanaged db.
    """
    super().__init__(relational_db_spec)

    self.spec.database_username = 'root'
    self.spec.database_password = 'perfkitbenchmarker'

  @property
  def server_vm(self):
    """Server VM for hosting a managed database.

    Raises:
      RelationalDbPropertyNotSetError: if the server_vm is missing.

    Returns:
      The server_vm.
    """
    if not hasattr(self, '_server_vm'):
      raise relational_db.RelationalDbPropertyNotSetError(
          'server_vm is not set'
      )
    return self._server_vm

  @server_vm.setter
  def server_vm(self, server_vm):
    self._server_vm = server_vm

  @property
  def server_vm_query_tools(self):
    if not hasattr(self, '_server_vm_query_tools'):
      connection_properties = sql_engine_utils.DbConnectionProperties(
          self.spec.engine,
          self.spec.engine_version,
          'localhost',
          self.port,
          self.spec.database_username,
          self.spec.database_password,
      )
      self._server_vm_query_tools = sql_engine_utils.GetQueryToolsByEngine(
          self.server_vm, connection_properties
      )
    return self._server_vm_query_tools

  def SetVms(self, vm_groups):
    super().SetVms(vm_groups)
    if 'servers' in vm_groups:
      self.server_vm = vm_groups['servers'][0]

  def _IsReadyUnmanaged(self) -> bool:
    """Return true if the underlying resource is ready.

    Returns:
      True the service is installed successfully, False if not.
    """
    return True

  def _ApplyDbFlags(self):
    """Apply Flags on the database."""
    raise NotImplementedError('Flags is not supported on %s' % self.spec.engine)

  def _SetupUnmanagedDatabase(self):
    """Installs unmanaged databases on server vm."""
    if self.server_vm.OS_TYPE in os_types.WINDOWS_OS_TYPES:
      self._SetupWindowsUnamangedDatabase()
    else:
      self._SetupLinuxUnmanagedDatabase()

  def _SetupWindowsUnamangedDatabase(self):
    """Installs unmanaged databases on server vm for Windows."""
    raise NotImplementedError(
        f'OS Type is not supported {self.server_vm.OS_TYPE}'
    )

  def _SetupLinuxUnmanagedDatabase(self):
    """Installs unmanaged databases on server vm for Linux."""
    self.server_vm_query_tools.InstallPackages()
    self.server_vm.Install('rsync')

    if relational_db.OPTIMIZE_DB_SYSCTL_CONFIG.value:
      if self.client_vm.IS_REBOOTABLE:
        self.client_vm.ApplySysctlPersistent({
            'net.ipv4.tcp_keepalive_time': 100,
            'net.ipv4.tcp_keepalive_intvl': 100,
            'net.ipv4.tcp_keepalive_probes': 10,
        })
      if self.server_vm.IS_REBOOTABLE:
        self.server_vm.ApplySysctlPersistent({
            'net.ipv4.tcp_keepalive_time': 100,
            'net.ipv4.tcp_keepalive_intvl': 100,
            'net.ipv4.tcp_keepalive_probes': 10,
        })

  def PrintUnmanagedDbStats(self):
    """Print unmanaged db stats before delete."""
    pass

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    raise NotImplementedError(
        'Default engine not specified for engine {}'.format(engine)
    )

  def _Create(self):
    """Setup the underlying resource."""
    self._SetupUnmanagedDatabase()
    self.unmanaged_db_exists = True

  def _SetEndpoint(self):
    """Set the DB endpoint for this instance during _PostCreate."""
    self.endpoint = self.server_vm.internal_ip

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    self.unmanaged_db_exists = False
    self.PrintUnmanagedDbStats()

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    return self.unmanaged_db_exists

  def _IsReady(self, timeout=IS_READY_TIMEOUT):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Args:
      timeout: how long to wait when checking if the DB is ready.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    return self._IsReadyUnmanaged()
