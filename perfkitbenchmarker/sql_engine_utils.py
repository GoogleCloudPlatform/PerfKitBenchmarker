# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Utilities to support multiple engines."""
import abc

MYSQL = 'mysql'
POSTGRES = 'postgres'
AURORA_POSTGRES = 'aurora-postgresql'
AURORA_MYSQL = 'aurora-mysql'
AURORA_MYSQL56 = 'aurora'
SQLSERVER = 'sqlserver'
SQLSERVER_EXPRESS = 'sqlserver-ex'
SQLSERVER_ENTERPRISE = 'sqlserver-ee'
SQLSERVER_STANDARD = 'sqlserver-se'

ALL_ENGINES = [
    MYSQL,
    POSTGRES,
    AURORA_POSTGRES,
    AURORA_MYSQL,
    AURORA_MYSQL56,
    SQLSERVER,
    SQLSERVER_EXPRESS,
    SQLSERVER_ENTERPRISE,
    SQLSERVER_STANDARD
]

ENGINE_TYPES = [
    MYSQL,
    POSTGRES,
    SQLSERVER
]

AWS_SQLSERVER_ENGINES = ['sqlserver-ee', 'sqlserver-se',
                         'sqlserver-ex', 'sqlserver-web']

AWS_AURORA_POSTGRES_ENGINE = 'aurora-postgresql'
AWS_AURORA_MYSQL_ENGINE = 'aurora-mysql'


# Query Related tools
class DbConnectionProperties():
  """Data class to store attrubutes needed for connecting to a database."""

  def __init__(self, engine, engine_version, endpoint, database_username,
               database_password):
    self.engine = engine
    self.engine_version = engine_version
    self.endpoint = endpoint
    self.database_username = database_username
    self.database_password = database_password


class ISQLQueryTools(metaclass=abc.ABCMeta):
  """Interface for SQL related query.

    Attributes:
    vm: VM to issue command with.
    connection_properties: Propreties of the database.
  """
  ENGINE_TYPE = None

  def __init__(self, vm, connection_properties):
    """Initialize ISQLQuery class."""
    self.vm = vm
    self.connection_properties = connection_properties

  def IssueSqlCommand(self, command: str, database_name='', superuser=False,
                      **kwargs):
    """Issue Sql Command."""
    command = self.MakeSqlCommand(command, database_name=database_name)
    if superuser:
      command = 'sudo ' + command

    return self.vm.RemoteCommand(command, **kwargs)

  @abc.abstractmethod
  def InstallPackages(self) -> None:
    """Installs packages required for making queries."""
    pass

  @abc.abstractmethod
  def GetConnectionString(self, **kwargs):
    """Get connection string."""
    pass

  @abc.abstractmethod
  def MakeSqlCommand(self, command: str, **kwargs):
    """Make a sql command."""
    pass


class PostgresCliQueryTools(ISQLQueryTools):
  """SQL Query class to issue postgres related query."""
  ENGINE_TYPE = POSTGRES

  def InstallPackages(self):
    """Installs packages required for making queries."""
    self.vm.Install('postgres_client')

  def IssueSqlCommand(self, command: str, database_name='postgres', **kwargs):
    """Issue Sql Command."""
    return self.vm.RemoteCommand(
        self.MakeSqlCommand(command, database_name=database_name), **kwargs)

  def MakeSqlCommand(self, command: str, database_name='postgres'):
    return 'psql %s -c "%s"' % (self.GetConnectionString(database_name),
                                command)

  def GetConnectionString(self, database_name='postgres'):
    return '\'host={0} user={1} password={2} dbname={3}\''.format(
        self.connection_properties.endpoint,
        self.connection_properties.database_username,
        self.connection_properties.database_password, database_name)


class MysqlCliQueryTools(ISQLQueryTools):
  """SQL Query class to issue Mysql related query."""
  ENGINE_TYPE = MYSQL

  def InstallPackages(self):
    """Installs packages required for making queries."""
    if (self.connection_properties.engine_version == '5.6' or
        self.connection_properties.engine_version.startswith('5.6.')):
      mysql_name = 'mysqlclient56'
    elif (self.connection_properties.engine_version == '5.7' or
          self.connection_properties.engine_version.startswith('5.7') or
          self.connection_properties.engine_version == '8.0' or
          self.connection_properties.engine_version.startswith('8.0')):
      mysql_name = 'mysqlclient'
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6, 5.7 '
                      'and 8.0 are supported.' %
                      self.connection_properties.engine_version)
    self.vm.Install(mysql_name)

  def MakeSqlCommand(self, command, database_name=''):
    mysql_command = 'mysql %s ' % (self.GetConnectionString())
    if database_name:
      mysql_command += database_name + ' '

    return mysql_command + '-e "%s"' % command

  def GetConnectionString(self):
    return '-h {0} -P 3306 -u {1} -p{2}'.format(
        self.connection_properties.endpoint,
        self.connection_properties.database_username,
        self.connection_properties.database_password)

  def GetSysbenchConnectionString(self):
    return ('--mysql-host={0} --mysql-user={1} --mysql-password="{2}" ').format(
        self.connection_properties.endpoint,
        self.connection_properties.database_username,
        self.connection_properties.database_password)


class SqlServerCliQueryTools(ISQLQueryTools):
  """SQL Query class to issue SQL server related query."""
  ENGINE_TYPE = SQLSERVER

  def InstallPackages(self):
    """Installs packages required for making queries."""
    self.vm.Install('mssql_tools')

  def MakeSqlCommand(self, command):
    return '/opt/mssql-tools/bin/sqlcmd -S %s -U %s -P %s -Q "%s"' % (
        self.connection_properties.endpoint,
        self.connection_properties.database_username,
        self.connection_properties.database_password, command)

  def GetConnectionString(self, database_name=''):
    raise NotImplementedError('Connection string currently not supported')


# Helper functions for this module
def GetDbEngineType(db_engine: str) -> str:
  """Converts the engine type from db_engine.

  The same engine type can have multiple names because of differences
  in cloud provider or versions.

  Args:
    db_engine: db_engine defined in the spec

  Returns:
    Engine type in string.
  """

  # AWS uses sqlserver-se and sqlserver-ex as db_egine for sql server
  if db_engine in AWS_SQLSERVER_ENGINES:
    return SQLSERVER
  elif db_engine == AWS_AURORA_POSTGRES_ENGINE:
    return POSTGRES
  elif db_engine == AWS_AURORA_MYSQL_ENGINE or db_engine == AURORA_MYSQL56:
    return MYSQL

  if db_engine not in ENGINE_TYPES:
    raise TypeError('Unsupported engine type', db_engine)
  return db_engine


def GetQueryToolsByEngine(vm, connection_properties):
  engine_type = GetDbEngineType(connection_properties.engine)
  if engine_type == MYSQL:
    return MysqlCliQueryTools(vm, connection_properties)
  elif engine_type == POSTGRES:
    return PostgresCliQueryTools(vm, connection_properties)
  elif engine_type == SQLSERVER:
    return SqlServerCliQueryTools(vm, connection_properties)
  raise ValueError('Engine not supported')

