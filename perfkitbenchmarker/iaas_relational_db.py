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
from perfkitbenchmarker import db_util
from perfkitbenchmarker import os_types
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils

FLAGS = flags.FLAGS

IS_READY_TIMEOUT = 600  # 10 minutes

POSTGRES_RESOURCE_PATH = 'database_configurations/postgres'
POSTGRES_13_VERSION = '13'

POSTGRES_HBA_CONFIG = 'pg_hba.conf'
POSTGRES_CONFIG = 'postgresql.conf'
POSTGRES_CONFIG_PATH = '/etc/postgresql/{0}/main/'

DEFAULT_MYSQL_VERSION = '8.0'
DEFAULT_POSTGRES_VERSION = POSTGRES_13_VERSION

DEFAULT_ENGINE_VERSIONS = {
    sql_engine_utils.MYSQL: DEFAULT_MYSQL_VERSION,
    sql_engine_utils.POSTGRES: DEFAULT_POSTGRES_VERSION,
}


class IAASRelationalDb(relational_db.BaseRelationalDb):
  """Object representing a IAAS relational database Service."""
  IS_MANAGED = False

  def __init__(self, relational_db_spec):
    """Initialize the IAAS relational database object.

    Args:
      relational_db_spec: spec of the managed database.

    Raises:
      UnsupportedError: if high availability is requested for an unmanaged db.
    """
    super(IAASRelationalDb, self).__init__(relational_db_spec)
    if self.spec.high_availability:
      raise relational_db.UnsupportedError('High availability is unsupported '
                                           'for unmanaged '
                                           'databases.')
    self.endpoint = ''
    self.spec.database_username = 'root'
    self.spec.database_password = 'perfkitbenchmarker'
    self.innodb_buffer_pool_size = FLAGS.innodb_buffer_pool_size
    self.mysql_bin_log = FLAGS.mysql_bin_log
    self.innodb_log_file_size = FLAGS.innodb_log_file_size
    self.postgres_shared_buffer_size = FLAGS.postgres_shared_buffer_size

  @property
  def server_vm(self):
    """Server VM for hosting a managed database.

    Raises:
      RelationalDbPropertyNotSetError: if the server_vm is missing.

    Returns:
      The server_vm.
    """
    if not hasattr(self, '_server_vm'):
      raise relational_db.RelationalDbPropertyNotSetError('server_vm is '
                                                          'not set')
    return self._server_vm

  @server_vm.setter
  def server_vm(self, server_vm):
    self._server_vm = server_vm

  @property
  def server_vm_query_tools(self):
    if not hasattr(self, '_server_vm_query_tools'):
      connection_properties = sql_engine_utils.DbConnectionProperties(
          self.spec.engine, self.spec.engine_version, 'localhost', self.port,
          self.spec.database_username, self.spec.database_password)
      self._server_vm_query_tools = sql_engine_utils.GetQueryToolsByEngine(
          self.server_vm, connection_properties)
    return self._server_vm_query_tools

  def SetVms(self, vm_groups):
    super().SetVms(vm_groups)
    if 'servers' in vm_groups:
      self.server_vm = vm_groups['servers'][0]
      kb_to_gb = 1.0 / 1000000
      if not self.innodb_buffer_pool_size:
        self.innodb_buffer_pool_size = int(self.server_vm.total_memory_kb *
                                           kb_to_gb / 4)

      if not self.postgres_shared_buffer_size:
        self.postgres_shared_buffer_size = int(self.server_vm.total_memory_kb *
                                               kb_to_gb / 4)

  def GetResourceMetadata(self):
    metadata = super().GetResourceMetadata()
    if self.spec.engine == 'mysql':
      metadata.update({
          'unmanaged_db_innodb_buffer_pool_size_gb':
              self.innodb_buffer_pool_size,
          'unmanaged_db_innodb_log_file_size_mb':
              self.innodb_log_file_size,
          'unmanaged_db_mysql_bin_log':
              self.mysql_bin_log
      })

    if self.spec.engine == 'postgres':
      metadata.update(
          {'postgres_shared_buffer_size': self.postgres_shared_buffer_size})
    return metadata

  def _IsReadyUnmanaged(self):
    """Return true if the underlying resource is ready.

    Returns:
      True if MySQL was installed successfully, False if not.

    Raises:
      Exception: If this method is called when the database is a managed one.
        Shouldn't happen.
    """
    if self.spec.engine == sql_engine_utils.MYSQL:
      if (self.spec.engine_version == '5.6' or
          self.spec.engine_version.startswith('5.6.')):
        mysql_name = 'mysql56'
      elif (self.spec.engine_version == '5.7' or
            self.spec.engine_version.startswith('5.7.')):
        mysql_name = 'mysql57'
      elif (self.spec.engine_version == '8.0' or
            self.spec.engine_version.startswith('8.0.')):
        mysql_name = 'mysql80'
      else:
        raise Exception('Invalid database engine version: %s. Only 5.6 and 5.7 '
                        'and 8.0 are supported.' % self.spec.engine_version)
      stdout, stderr = self.server_vm.RemoteCommand(
          'sudo service %s status' % self.server_vm.GetServiceName(mysql_name))
      return stdout and not stderr
    elif self.spec.engine == sql_engine_utils.POSTGRES:
      stdout, stderr = self.server_vm.RemoteCommand(
          'sudo service postgresql status')
      return stdout and not stderr
    elif self.spec.engine == sql_engine_utils.SQLSERVER:
      return True

    raise relational_db.UnsupportedError('%s engine is not supported '
                                         'for unmanaged database.' %
                                         self.spec.engine)

  def _ApplyDbFlags(self):
    """Apply Flags on the database."""
    # TODO(chunla): Refactor this into a separate engine module.
    if self.spec.engine == sql_engine_utils.MYSQL:
      self._ApplyMySqlFlags()
    elif self.spec.engine == sql_engine_utils.POSTGRES:
      self._ApplyPostgresFlags()
    else:
      raise NotImplementedError('Flags is not supported on %s' %
                                self.spec.engine)

  def _ApplyMySqlFlags(self):
    if FLAGS.db_flags:
      for flag in FLAGS.db_flags:
        _, stderr, _ = self.client_vm_query_tools.IssueSqlCommand(
            'SET %s;' % flag, ignore_failure=True)
        if stderr:
          raise Exception('Invalid MySQL flags: %s' % stderr)

  def _ApplyPostgresFlags(self):
    """Add postgres flags to postgres config file."""
    if FLAGS.db_flags:
      version = self.spec.engine_version
      postgres_conf_path = POSTGRES_CONFIG_PATH.format(version)
      postgres_conf_file = postgres_conf_path + POSTGRES_CONFIG
      for flag in FLAGS.db_flags:
        self.server_vm.RemoteCommand('sudo sh -c \'echo %s >> %s\'' %
                                     (flag, postgres_conf_file))
      self.server_vm.RemoteCommand('sudo systemctl restart postgresql')

  def _SetupUnmanagedDatabase(self):
    """Installs unmanaged databases on server vm."""
    if self.server_vm.OS_TYPE in os_types.WINDOWS_OS_TYPES:
      self._SetupWindowsUnamangedDatabase()
    else:
      self._SetupLinuxUnmanagedDatabase()

  def _SetupWindowsUnamangedDatabase(self):
    db_engine = self.spec.engine

    if db_engine == sql_engine_utils.SQLSERVER:
      self.spec.database_username = 'sa'
      self.spec.database_password = db_util.GenerateRandomDbPassword()
      self.server_vm.RemoteCommand('sqlcmd -Q "ALTER LOGIN sa ENABLE;"')
      self.server_vm.RemoteCommand(
          'sqlcmd -Q "ALTER LOGIN sa WITH PASSWORD = \'%s\' ;"' %
          self.spec.database_password)

      # Change the authentication method from windows authentication to
      # SQL Server Authentication
      # https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/change-server-authentication-mode?view=sql-server-ver15
      self.server_vm.RemoteCommand(
          'sqlcmd -Q "EXEC xp_instance_regwrite'
          ' N\'HKEY_LOCAL_MACHINE\','
          ' N\'Software\\Microsoft\\MSSQLServer\\MSSQLServer\', '
          'N\'LoginMode\', REG_DWORD, 2"')

      # Set the default database location to scratch disk
      self.server_vm.RemoteCommand(
          'sqlcmd -Q "EXEC xp_instance_regwrite N\'HKEY_LOCAL_MACHINE\', '
          'N\'Software\\Microsoft\\MSSQLServer\\MSSQLServer\', '
          'N\'BackupDirectory\', REG_SZ, N\'C:\\scratch\'"')
      self.server_vm.RemoteCommand(
          'sqlcmd -Q "EXEC xp_instance_regwrite N\'HKEY_LOCAL_MACHINE\', '
          'N\'Software\\Microsoft\\MSSQLServer\\MSSQLServer\', '
          'N\'DefaultData\', REG_SZ, N\'%s:\\\'"' %
          self.server_vm.assigned_disk_letter)
      self.server_vm.RemoteCommand(
          'sqlcmd -Q "EXEC xp_instance_regwrite N\'HKEY_LOCAL_MACHINE\', '
          'N\'Software\\Microsoft\\MSSQLServer\\MSSQLServer\', '
          'N\'DefaultLog\', REG_SZ, N\'%s:\\\'"' %
          self.server_vm.assigned_disk_letter)
      self.server_vm.RemoteCommand('net stop mssqlserver /y')
      self.server_vm.RemoteCommand('net start mssqlserver')
      return

    raise relational_db.UnsupportedError('Only sql server is currently '
                                         'supported on windows vm')

  def _SetupLinuxUnmanagedDatabase(self):
    db_engine = self.spec.engine
    self.server_vm_query_tools.InstallPackages()

    if self.client_vm.IS_REBOOTABLE:
      self.client_vm.ApplySysctlPersistent({
          'net.ipv4.tcp_keepalive_time': 100,
          'net.ipv4.tcp_keepalive_intvl': 100,
          'net.ipv4.tcp_keepalive_probes': 10
      })
    if self.server_vm.IS_REBOOTABLE:
      self.server_vm.ApplySysctlPersistent({
          'net.ipv4.tcp_keepalive_time': 100,
          'net.ipv4.tcp_keepalive_intvl': 100,
          'net.ipv4.tcp_keepalive_probes': 10
      })

    if db_engine == 'mysql':
      self._InstallMySQLServer()
    elif db_engine == 'postgres':
      self._InstallPostgresServer()
    else:
      raise Exception(
          'Engine {0} not supported for unmanaged databases.'.format(
              self.spec.engine))

  def _PrepareDataDirectories(self, mysql_name):
    # Make the data directories in case they don't already exist.
    self.server_vm.RemoteCommand('sudo mkdir -p /scratch/mysql')
    self.server_vm.RemoteCommand('sudo mkdir -p /scratch/tmp')
    self.server_vm.RemoteCommand('sudo chown mysql:mysql /scratch/mysql')
    self.server_vm.RemoteCommand('sudo chown mysql:mysql /scratch/tmp')
    # Copy all the contents of the default data directories to the new ones.
    self.server_vm.RemoteCommand(
        'sudo rsync -avzh /var/lib/mysql/ /scratch/mysql')
    self.server_vm.RemoteCommand('sudo rsync -avzh /tmp/ /scratch/tmp')
    self.server_vm.RemoteCommand('df', should_log=True)
    # Configure AppArmor.
    self.server_vm.RemoteCommand(
        'echo "alias /var/lib/mysql -> /scratch/mysql," | sudo tee -a '
        '/etc/apparmor.d/tunables/alias')
    self.server_vm.RemoteCommand(
        'echo "alias /tmp -> /scratch/tmp," | sudo tee -a '
        '/etc/apparmor.d/tunables/alias')
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s|# Allow data files dir access|'
        '  /scratch/mysql/ r, /scratch/mysql/** rwk, /scratch/tmp/ r, '
        '/scratch/tmp/** rwk, /proc/*/status r, '
        '/sys/devices/system/node/ r, /sys/devices/system/node/node*/meminfo r,'
        ' /sys/devices/system/node/*/* r, /sys/devices/system/node/* r, '
        '# Allow data files dir access|g" /etc/apparmor.d/usr.sbin.mysqld')
    self.server_vm.RemoteCommand(
        'sudo apparmor_parser -r /etc/apparmor.d/usr.sbin.mysqld')
    self.server_vm.RemoteCommand('sudo systemctl restart apparmor')
    # Finally, change the MySQL data directory.
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s|datadir\t\t= /var/lib/mysql|datadir\t\t= /scratch/mysql|g" '
        '%s' % self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s|tmpdir\t\t= /tmp|tmpdir\t\t= /scratch/tmp|g" '
        '%s' % self.server_vm.GetPathToConfig(mysql_name))

  def _InstallPostgresServer(self):
    if self.spec.engine_version == POSTGRES_13_VERSION:
      self.server_vm.Install('postgres13')
    else:
      raise relational_db.UnsupportedError(
          'Only postgres version 13 is currently supported')

    vm = self.server_vm
    version = self.spec.engine_version
    postgres_conf_path = POSTGRES_CONFIG_PATH.format(version)
    postgres_conf_file = postgres_conf_path + POSTGRES_CONFIG
    postgres_hba_conf_file = postgres_conf_path + POSTGRES_HBA_CONFIG
    vm.PushFile(
        data.ResourcePath(
            posixpath.join(POSTGRES_RESOURCE_PATH, POSTGRES_HBA_CONFIG)))
    vm.RemoteCommand('sudo -u postgres psql postgres -c '
                     '"ALTER USER postgres PASSWORD \'%s\';"' %
                     self.spec.database_password)
    vm.RemoteCommand('sudo -u postgres psql postgres -c '
                     '"CREATE ROLE %s LOGIN SUPERUSER PASSWORD \'%s\';"' %
                     (self.spec.database_username, self.spec.database_password))

    # Change the directory to scratch
    vm.RemoteCommand(
        'sudo sed -i.bak '
        '"s:\'/var/lib/postgresql/{0}/main\':\'{1}/postgresql/{0}/main\':" '
        '/etc/postgresql/{0}/main/postgresql.conf'.format(
            version, self.server_vm.GetScratchDir()))

    # Accept remote connection
    vm.RemoteCommand('sudo sed -i.bak '
                     r'"s:\#listen_addresses ='
                     ' \'localhost\':listen_addresses = \'*\':" '
                     '{}'.format(postgres_conf_file))

    # Set the size of the shared buffer
    vm.RemoteCommand(
        'sudo sed -i.bak "s:shared_buffers = 128MB:shared_buffers = {}GB:" '
        '{}'.format(self.postgres_shared_buffer_size, postgres_conf_file))
    # Update data path to new location
    vm.InstallPackages('rsync')
    vm.RemoteCommand('sudo rsync -av /var/lib/postgresql /scratch')

    # # Use cat to move files because mv will override file permissions
    self.server_vm.RemoteCommand('sudo bash -c '
                                 "'cat pg_hba.conf > "
                                 "{}'".format(postgres_hba_conf_file))

    self.server_vm.RemoteCommand('sudo cat {}'.format(postgres_conf_file))
    self.server_vm.RemoteCommand('sudo cat {}'.format(postgres_hba_conf_file))
    vm.RemoteCommand('sudo systemctl restart postgresql')

  def _InstallMySQLServer(self):
    """Installs MySQL Server on the server vm.

    https://d0.awsstatic.com/whitepapers/Database/optimizing-mysql-running-on-amazon-ec2-using-amazon-ebs.pdf
    for minimal tuning parameters.
    Raises:
      Exception: If the requested engine version is unsupported, or if this
        method is called when the database is a managed one. The latter
        shouldn't happen.
    """
    if (self.spec.engine_version == '5.6' or
        self.spec.engine_version.startswith('5.6.')):
      mysql_name = 'mysql56'
    elif (self.spec.engine_version == '5.7' or
          self.spec.engine_version.startswith('5.7.')):
      mysql_name = 'mysql57'
    elif (self.spec.engine_version == '8.0' or
          self.spec.engine_version.startswith('8.0.')):
      mysql_name = 'mysql80'
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6 and 5.7 '
                      'and 8.0 are supported.' % self.spec.engine_version)
    self.server_vm.Install(mysql_name)
    self.server_vm.RemoteCommand('chmod 755 %s' %
                                 self.server_vm.GetScratchDir())
    self.server_vm.RemoteCommand('sudo service %s stop' %
                                 self.server_vm.GetServiceName(mysql_name))
    self._PrepareDataDirectories(mysql_name)

    # Minimal MySQL tuning; see AWS whitepaper in docstring.
    innodb_buffer_pool_gb = self.innodb_buffer_pool_size
    innodb_log_file_mb = self.innodb_log_file_size
    self.server_vm.RemoteCommand(
        'echo "\n'
        f'innodb_buffer_pool_size = {innodb_buffer_pool_gb}G\n'
        'innodb_flush_method = O_DIRECT\n'
        'innodb_flush_neighbors = 0\n'
        f'innodb_log_file_size = {innodb_log_file_mb}M'
        '" | sudo tee -a %s' % self.server_vm.GetPathToConfig(mysql_name))

    if self.mysql_bin_log:
      bin_log_path = self.server_vm.GetScratchDir() + '/mysql/mysql-bin.log'
      self.server_vm.RemoteCommand(
          'echo "\n'
          'server-id  = 1\n'
          'log_bin = %s\n'
          '" | sudo tee -a %s' %
          (bin_log_path, self.server_vm.GetPathToConfig(mysql_name)))

    # These (and max_connections after restarting) help avoid losing connection.
    self.server_vm.RemoteCommand(
        'echo "\nskip-name-resolve\n'
        'connect_timeout        = 86400\n'
        'wait_timeout        = 86400\n'
        'interactive_timeout        = 86400" | sudo tee -a %s' %
        self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo sed -i "s/^bind-address/#bind-address/g" '
        '%s' % self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo sed -i "s/^mysqlx-bind-address/#mysqlx-bind-address/g" '
        '%s' % self.server_vm.GetPathToConfig(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s/max_allowed_packet\t= 16M/max_allowed_packet\t= 1024M/g" %s' %
        self.server_vm.GetPathToConfig(mysql_name))

    # Configure logging (/var/log/mysql/error.log will print upon db deletion).
    self.server_vm.RemoteCommand(
        'echo "\nlog_error_verbosity        = 3" | sudo tee -a %s' %
        self.server_vm.GetPathToConfig(mysql_name))
    # Restart.
    self.server_vm.RemoteCommand('sudo service %s restart' %
                                 self.server_vm.GetServiceName(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo cat %s' % self.server_vm.GetPathToConfig(mysql_name),
        should_log=True)

    self.server_vm_query_tools.IssueSqlCommand(
        'SET GLOBAL max_connections=8000;', superuser=True)

    self.SetMYSQLClientPrivileges()

  def SetMYSQLClientPrivileges(self):
    client_ip = self.client_vm.internal_ip

    self.server_vm_query_tools.IssueSqlCommand(
        'CREATE USER \'%s\'@\'%s\' IDENTIFIED BY \'%s\';' %
        (self.spec.database_username, client_ip, self.spec.database_password),
        superuser=True,
        ignore_failure=True)

    self.server_vm_query_tools.IssueSqlCommand(
        'GRANT ALL PRIVILEGES ON *.* TO \'%s\'@\'%s\';' %
        (self.spec.database_username, client_ip),
        superuser=True,
        ignore_failure=True)
    self.server_vm_query_tools.IssueSqlCommand(
        'FLUSH PRIVILEGES;', superuser=True, ignore_failure=True)

  def PrintUnmanagedDbStats(self):
    """Print server logs on unmanaged db."""
    if self.spec.engine == 'mysql':
      self.server_vm.RemoteCommand('sudo cat /var/log/mysql/error.log')
      self.server_vm_query_tools.IssueSqlCommand(
          'SHOW GLOBAL STATUS LIKE \'Aborted_connects\';', superuser=True)
      self.server_vm_query_tools.IssueSqlCommand(
          'SHOW GLOBAL STATUS LIKE \'Aborted_clients\';', superuser=True)
      self.server_vm_query_tools.IssueSqlCommand(
          'SHOW GLOBAL STATUS LIKE \'%version%\';', superuser=True)

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    if engine not in DEFAULT_ENGINE_VERSIONS:
      raise NotImplementedError('Default engine not specified for '
                                'engine {0}'.format(engine))
    return DEFAULT_ENGINE_VERSIONS[engine]

  def _Create(self):
    """Creates the Cloud SQL instance and authorizes traffic from anywhere."""
    self._SetupUnmanagedDatabase()
    self.endpoint = self.server_vm.internal_ip
    self.unmanaged_db_exists = True

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

  def _PostCreate(self):
    """Creates the PKB user and sets the password."""
    super()._PostCreate()
    self.client_vm_query_tools.InstallPackages()
