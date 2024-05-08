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
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils

FLAGS = flags.FLAGS

IS_READY_TIMEOUT = 600  # 10 minutes
MSSQL_PID = 'developer'  # Edition of SQL server on Linux

DEFAULT_MYSQL_VERSION = '8.0'

_SETUP_APPARMOR_MYSQL = flags.DEFINE_boolean(
    'setup_apparmor_mysql',
    True,
    'Whether we should setup apparmor '
    'for mysql, some machines do '
    'not have apparmor.',
)


class MysqlIAASRelationalDb(iaas_relational_db.IAASRelationalDb):
  """Object representing a IAAS relational database Service."""

  ENGINE = sql_engine_utils.MYSQL

  def __init__(self, relational_db_spec):
    """Initialize the IAAS relational database object.

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
    self.innodb_buffer_pool_size = FLAGS.innodb_buffer_pool_size
    self.mysql_bin_log = FLAGS.mysql_bin_log
    self.innodb_log_file_size = FLAGS.innodb_log_file_size

  def SetVms(self, vm_groups):
    super().SetVms(vm_groups)
    if 'servers' in vm_groups:
      kb_to_gb = 1.0 / 1000000
      if not self.innodb_buffer_pool_size:
        self.innodb_buffer_pool_size = int(
            self.server_vm.total_memory_kb
            * kb_to_gb
            * FLAGS.innodb_buffer_pool_ratio
        )

  def GetResourceMetadata(self):
    metadata = super().GetResourceMetadata()
    metadata.update({
        'unmanaged_db_innodb_buffer_pool_size_gb': self.innodb_buffer_pool_size,
        'unmanaged_db_innodb_log_file_size_mb': self.innodb_log_file_size,
        'unmanaged_db_mysql_bin_log': self.mysql_bin_log,
    })

    return metadata

  def _IsReadyUnmanaged(self):
    """Return true if the underlying resource is ready.

    Returns:
      True if MySQL was installed successfully, False if not.

    Raises:
      Exception: If this method is called when the database is a managed one.
        Shouldn't happen.
    """
    if (
        self.spec.engine_version == '5.6'
        or self.spec.engine_version.startswith('5.6.')
    ):
      mysql_name = 'mysql56'
    elif (
        self.spec.engine_version == '5.7'
        or self.spec.engine_version.startswith('5.7.')
    ):
      mysql_name = 'mysql57'
    elif (
        self.spec.engine_version == '8.0'
        or self.spec.engine_version.startswith('8.0.')
    ):
      mysql_name = 'mysql80'
    else:
      raise NotImplementedError(
          'Invalid database engine version: %s. Only 5.6 and 5.7 '
          'and 8.0 are supported.'
          % self.spec.engine_version
      )
    stdout, stderr = self.server_vm.RemoteCommand(
        'sudo service %s status' % self.server_vm.GetServiceName(mysql_name)
    )
    return stdout and not stderr

  def _ApplyDbFlags(self):
    """Apply Flags on the database."""
    if FLAGS.db_flags:
      for flag in FLAGS.db_flags:
        _, stderr, _ = self.client_vm_query_tools.IssueSqlCommand(
            'SET %s;' % flag, ignore_failure=True
        )
        if stderr:
          raise RuntimeError('Invalid MySQL flags: %s' % stderr)

  def _InstallMySQL(self):
    if (
        self.spec.engine_version == '5.6'
        or self.spec.engine_version.startswith('5.6.')
    ):
      self.mysql_name = 'mysql56'
    elif (
        self.spec.engine_version == '5.7'
        or self.spec.engine_version.startswith('5.7.')
    ):
      self.mysql_name = 'mysql57'
    elif (
        self.spec.engine_version == '8.0'
        or self.spec.engine_version.startswith('8.0.')
    ):
      self.mysql_name = 'mysql80'
    else:
      raise NotImplementedError(
          'Invalid database engine version: %s. Only 5.6 and 5.7 '
          'and 8.0 are supported.'
          % self.spec.engine_version
      )
    self.server_vm.Install(self.mysql_name)
    self.server_vm.RemoteCommand(
        'chmod 755 %s' % self.server_vm.GetScratchDir()
    )
    self.server_vm.RemoteCommand(
        'sudo service %s stop' % self.server_vm.GetServiceName(self.mysql_name)
    )

  def _SetupLinuxUnmanagedDatabase(self):
    """Installs MySQL Server on the server vm.

    https://d0.awsstatic.com/whitepapers/Database/optimizing-mysql-running-on-amazon-ec2-using-amazon-ebs.pdf
    for minimal tuning parameters.
    Raises:
      Exception: If the requested engine version is unsupported, or if this
        method is called when the database is a managed one. The latter
        shouldn't happen.
    """
    super()._SetupLinuxUnmanagedDatabase()
    self._InstallMySQL()
    self._PrepareDataDirectories(self.mysql_name)

    # Minimal MySQL tuning; see AWS whitepaper in docstring.
    innodb_buffer_pool_gb = self.innodb_buffer_pool_size
    innodb_log_file_mb = self.innodb_log_file_size
    self.server_vm.RemoteCommand(
        'echo "\n'
        f'innodb_buffer_pool_size = {innodb_buffer_pool_gb}G\n'
        'innodb_flush_method = O_DIRECT\n'
        'innodb_flush_neighbors = 0\n'
        f'innodb_log_file_size = {innodb_log_file_mb}M'
        '" | sudo tee -a %s'
        % self.server_vm.GetPathToConfig(self.mysql_name)
    )

    if self.mysql_bin_log:
      bin_log_path = self.server_vm.GetScratchDir() + '/mysql/mysql-bin.log'
      self.server_vm.RemoteCommand(
          'echo "\nserver-id  = 1\nlog_bin = %s\n" | sudo tee -a %s'
          % (bin_log_path, self.server_vm.GetPathToConfig(self.mysql_name))
      )

    # These (and max_connections after restarting) help avoid losing connection.
    self.server_vm.RemoteCommand(
        'echo "\nskip-name-resolve\n'
        'connect_timeout        = 86400\n'
        'wait_timeout        = 86400\n'
        'interactive_timeout        = 86400" | sudo tee -a %s'
        % self.server_vm.GetPathToConfig(self.mysql_name)
    )
    self.server_vm.RemoteCommand(
        'sudo sed -i "s/^bind-address/#bind-address/g" %s'
        % self.server_vm.GetPathToConfig(self.mysql_name)
    )
    self.server_vm.RemoteCommand(
        'sudo sed -i "s/^mysqlx-bind-address/#mysqlx-bind-address/g" %s'
        % self.server_vm.GetPathToConfig(self.mysql_name)
    )
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s/max_allowed_packet\t= 16M/max_allowed_packet\t= 1024M/g" %s'
        % self.server_vm.GetPathToConfig(self.mysql_name)
    )

    # Configure logging (/var/log/mysql/error.log will print upon db deletion).
    if self.mysql_name != 'mariadb':
      self.server_vm.RemoteCommand(
          'echo "\nlog_error_verbosity        = 3" | sudo tee -a %s'
          % self.server_vm.GetPathToConfig(self.mysql_name)
      )
    # Restart.
    self.server_vm.RemoteCommand(
        'sudo service %s restart'
        % self.server_vm.GetServiceName(self.mysql_name)
    )
    self.server_vm.RemoteCommand(
        'sudo cat %s' % self.server_vm.GetPathToConfig(self.mysql_name)
    )

    self.server_vm_query_tools.IssueSqlCommand(
        'SET GLOBAL max_connections=8000;', superuser=True
    )

    self.SetMYSQLClientPrivileges()

  def _PrepareDataDirectories(self, mysql_name):
    # Make the data directories in case they don't already exist.
    self.server_vm.RemoteCommand('sudo mkdir -p /scratch/mysql')
    self.server_vm.RemoteCommand('sudo mkdir -p /scratch/tmp')
    self.server_vm.RemoteCommand('sudo chown mysql:mysql /scratch/mysql')
    self.server_vm.RemoteCommand('sudo chown mysql:mysql /scratch/tmp')
    # Copy all the contents of the default data directories to the new ones.
    self.server_vm.RemoteCommand(
        'sudo rsync -avzh /var/lib/mysql/ /scratch/mysql'
    )
    self.server_vm.RemoteCommand('sudo rsync -avzh /tmp/ /scratch/tmp')
    self.server_vm.RemoteCommand('df')
    # Configure AppArmor.
    if _SETUP_APPARMOR_MYSQL.value:
      self.server_vm.RemoteCommand(
          'echo "alias /var/lib/mysql -> /scratch/mysql," | sudo tee -a '
          '/etc/apparmor.d/tunables/alias'
      )
      self.server_vm.RemoteCommand(
          'echo "alias /tmp -> /scratch/tmp," | sudo tee -a '
          '/etc/apparmor.d/tunables/alias'
      )
      self.server_vm.RemoteCommand(
          'sudo sed -i "s|# Allow data files dir access|  /scratch/mysql/ r,'
          ' /scratch/mysql/** rwk, /scratch/tmp/ r, /scratch/tmp/** rwk,'
          ' /proc/*/status r, /sys/devices/system/node/ r,'
          ' /sys/devices/system/node/node*/meminfo r,'
          ' /sys/devices/system/node/*/* r, /sys/devices/system/node/* r, #'
          ' Allow data files dir access|g" /etc/apparmor.d/usr.sbin.mysqld'
      )
      self.server_vm.RemoteCommand(
          'sudo apparmor_parser -r /etc/apparmor.d/usr.sbin.mysqld'
      )
      self.server_vm.RemoteCommand('sudo systemctl restart apparmor')
    # Finally, change the MySQL data directory.
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s|datadir\t\t= /var/lib/mysql|datadir\t\t= /scratch/mysql|g" '
        '%s'
        % self.server_vm.GetPathToConfig(mysql_name)
    )
    self.server_vm.RemoteCommand(
        'sudo sed -i "s|tmpdir\t\t= /tmp|tmpdir\t\t= /scratch/tmp|g" %s'
        % self.server_vm.GetPathToConfig(mysql_name)
    )

  def SetMYSQLClientPrivileges(self):
    client_ip = self.client_vm.internal_ip

    self.server_vm_query_tools.IssueSqlCommand(
        "CREATE USER '%s'@'%s' IDENTIFIED BY '%s';"
        % (self.spec.database_username, client_ip, self.spec.database_password),
        superuser=True,
        ignore_failure=True,
    )

    self.server_vm_query_tools.IssueSqlCommand(
        "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%s';"
        % (self.spec.database_username, client_ip),
        superuser=True,
        ignore_failure=True,
    )
    self.server_vm_query_tools.IssueSqlCommand(
        'FLUSH PRIVILEGES;', superuser=True, ignore_failure=True
    )

  def PrintUnmanagedDbStats(self):
    """Print server logs on unmanaged db."""
    self.server_vm.RemoteCommand('sudo cat /var/log/mysql/error.log')
    self.server_vm_query_tools.IssueSqlCommand(
        "SHOW GLOBAL STATUS LIKE 'Aborted_connects';", superuser=True
    )
    self.server_vm_query_tools.IssueSqlCommand(
        "SHOW GLOBAL STATUS LIKE 'Aborted_clients';", superuser=True
    )
    self.server_vm_query_tools.IssueSqlCommand(
        "SHOW GLOBAL STATUS LIKE '%version%';", superuser=True
    )

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of MYSQL."""
    return DEFAULT_MYSQL_VERSION


def _TuneForSQL(vm):
  """Set TuneD settings specific to SQL Server on RedHat."""
  tune_settings = (
      '# A TuneD configuration for SQL Server on Linux \n'
      '[main] \n'
      'summary=Optimize for Microsoft SQL Server \n'
      'include=throughput-performance \n\n'
      '[cpu] \n'
      'force_latency=5\n\n'
      '[sysctl]\n'
      'vm.swappiness = 1\n'
      'vm.dirty_background_ratio = 3\n'
      'vm.dirty_ratio = 80\n'
      'vm.dirty_expire_centisecs = 500\n'
      'vm.dirty_writeback_centisecs = 100\n'
      'vm.transparent_hugepages=always\n'
      'vm.max_map_count=1600000\n'
      'net.core.rmem_default = 262144\n'
      'net.core.rmem_max = 4194304\n'
      'net.core.wmem_default = 262144\n'
      'net.core.wmem_max = 1048576\n'
      'kernel.numa_balancing=0'
  )
  vm.RemoteCommand('sudo mkdir -p /usr/lib/tuned/mssql')
  vm.RemoteCommand(
      'echo "{}" | sudo tee /usr/lib/tuned/mssql/tuned.conf'.format(
          tune_settings
      )
  )

  vm.RemoteCommand('sudo chmod +x /usr/lib/tuned/mssql/tuned.conf')
  vm.RemoteCommand('sudo tuned-adm profile mssql')
  vm.RemoteCommand('sudo tuned-adm list')


class MariaDbIAASRelationalDB(MysqlIAASRelationalDb):
  """Object representing MariaDB."""

  ENGINE = sql_engine_utils.MARIADB

  def _InstallMySQL(self):
    """MariaDB is a variant of MySQL."""
    self.server_vm.Install('mariadb')
    self.mysql_name = 'mariadb'
    self.innodb_buffer_pool_size = FLAGS.innodb_buffer_pool_size
    self.mysql_bin_log = FLAGS.mysql_bin_log
    self.innodb_log_file_size = FLAGS.innodb_log_file_size

  def _SetupLinuxUnmanagedDatabase(self):
    """Installs MariaDB on the server vm.

    https://mariadb.com/kb/en/getting-installing-and-upgrading-mariadb/
    Raises:
      Exception: If the requested engine version is unsupported, or if this
        method is called when the database is a managed one. The latter
        shouldn't happen.
    """

    super()._SetupLinuxUnmanagedDatabase()
    self.server_vm.Install('mariadb')
    self.server_vm.RemoteCommand('sudo service mariadb stop')
    self._PrepareDataDirectories('mariadb')
    self.server_vm.RemoteCommand('sudo service mariadb start')


def ConfigureSQLServerLinux(vm, username: str, password: str):
  """Update the username and password on a SQL Server."""
  vm.RemoteCommand(
      f'/opt/mssql-tools/bin/sqlcmd -Q "ALTER LOGIN {username} ENABLE;"'
  )
  vm.RemoteCommand(
      '/opt/mssql-tools/bin/sqlcmd -Q '
      f'"ALTER LOGIN sa WITH PASSWORD = \'{password}\' ;"'
  )
  vm.RemoteCommand('sudo systemctl restart mssql-server')
