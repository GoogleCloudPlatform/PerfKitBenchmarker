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
import logging
import timeit
from typing import Dict, List, Any, Tuple, Union, Optional

from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine

SECOND = 'seconds'

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

DEFAULT_COMMAND = 'default'


# Query Related tools
class DbConnectionProperties():
  """Data class to store attrubutes needed for connecting to a database."""

  def __init__(self, engine, engine_version, endpoint, port, database_username,
               database_password):
    self.engine = engine
    self.engine_version = engine_version
    self.endpoint = endpoint
    self.port = port
    self.database_username = database_username
    self.database_password = database_password


class ISQLQueryTools(metaclass=abc.ABCMeta):
  """Interface for SQL related query.

    Attributes:
    vm: VM to issue command with.
    connection_properties: Connection properties of the database.
  """
  ENGINE_TYPE = None

  def __init__(self, vm: virtual_machine.VirtualMachine,
               connection_properties: DbConnectionProperties):
    """Initialize ISQLQuery class."""
    self.vm = vm
    self.connection_properties = connection_properties

  def TimeQuery(self,
                database_name: str,
                query: str,
                is_explain: bool = False,
                suppress_stdout: bool = False) -> Tuple[Any, Any, str]:
    """Time a query.."""
    if is_explain:
      query = self.GetExplainPrefix() + query

    start = timeit.default_timer()
    stdout_, error_ = self.IssueSqlCommand(
        query,
        database_name=database_name,
        suppress_stdout=suppress_stdout,
        timeout=60*30)
    end = timeit.default_timer()
    run_time = str(end - start)
    if error_:
      logging.info('Quries finished with error %s', error_)
      run_time = '-1'

    return stdout_, error_, run_time

  def SamplesFromQueriesWithExplain(
      self, database_name: str, queries: Dict[str, str],
      metadata: Dict[str, Any]) -> List[sample.Sample]:
    """Helper function to run quries."""
    results = []
    for query in queries:
      execution_plan, _, run_time = self.TimeQuery(
          database_name,
          queries[query],
          is_explain=True)

      logging.info('Execution Plan for Query %s: %s', query, execution_plan)
      result = sample.Sample('Query %s' % query, run_time, SECOND, metadata)
      results.append(result)

    return results

  def SamplesFromQueriesAfterRunningExplain(
      self, database_name: str, queries: Dict[str, str],
      metadata: Dict[str, Any]) -> List[sample.Sample]:
    """Run the query with explain and run the qeury once more for timing."""
    results = []
    for query in queries:
      execution_plan, _, _ = self.TimeQuery(
          database_name, queries[query], is_explain=True)

      logging.info('Execution Plan for Query %s: %s', query, execution_plan)

      _, _, run_time = self.TimeQuery(
          database_name, queries[query], is_explain=False, suppress_stdout=True)

      result = sample.Sample('Query %s' % query, run_time, SECOND, metadata)
      results.append(result)

    return results

  def IssueSqlCommand(self,
                      command: Union[str, Dict[str, str]],
                      database_name: str = '',
                      superuser: bool = False,
                      session_variables: str = '',
                      timeout: Optional[int] = None,
                      ignore_failure: bool = False,
                      suppress_warning: bool = False,
                      suppress_stdout: bool = False):
    """Issue Sql Command."""
    command_string = None
    # Get the command to issue base on type
    if isinstance(command, dict):
      if self.ENGINE_TYPE in command:
        command_string = command[self.ENGINE_TYPE]
      else:
        command_string = command[DEFAULT_COMMAND]
    elif isinstance(command, str):
      command_string = command

    command_string = self.MakeSqlCommand(
        command_string,
        database_name=database_name,
        session_variables=session_variables)

    if superuser:
      command_string = 'sudo ' + command_string

    if suppress_stdout:
      command_string = command_string + ' >/dev/null 2>&1'

    return self.vm.RemoteCommand(
        command_string, timeout=timeout, ignore_failure=ignore_failure,
        suppress_warning=suppress_warning)

  @abc.abstractmethod
  def InstallPackages(self) -> None:
    """Installs packages required for making queries."""
    pass

  @abc.abstractmethod
  def GetConnectionString(self, **kwargs):
    """Get connection string."""
    pass

  @abc.abstractmethod
  def MakeSqlCommand(self,
                     command: str,
                     database_name: str = '',
                     session_variables: str = ''):
    """Make a sql command."""
    pass

  def GetExplainPrefix(self) -> str:
    """Returns the prefix for explain query."""
    return 'EXPLIAN '

  def RunSqlScript(self,
                   file_path: str,
                   database_name: str = '') -> Tuple[str, str]:
    """Run a sql command through a file.

    The file could have multiple commands. RunSqlScript runs the sql file
    from the file_path using the database_name. Failure in one command might
    cause failure in subsequent commands.

    Args:
      file_path: The local path from the machine.
      database_name: Name of the database. Uses the master database
        or the default database if nothing is supplied.

    Returns:
      A tuple of standard output and standard error.

    """
    raise NotImplementedError('Running from a file is not currently supported.')


class PostgresCliQueryTools(ISQLQueryTools):
  """SQL Query class to issue postgres related query."""
  ENGINE_TYPE = POSTGRES

  # The default database in postgres
  DEFAULT_DATABASE = POSTGRES

  def InstallPackages(self):
    """Installs packages required for making queries."""
    self.vm.Install('postgres_client')

  def MakeSqlCommand(self,
                     command: str,
                     database_name: str = '',
                     session_variables: str = ''):
    """Make Sql Command."""
    if not database_name:
      database_name = self.DEFAULT_DATABASE
    sql_command = 'psql %s ' % self.GetConnectionString(database_name)
    if session_variables:
      for session_variable in session_variables:
        sql_command += '-c "%s" ' % session_variable

    sql_command += '-c "%s"' % command
    return sql_command

  def GetConnectionString(self, database_name=''):
    if not database_name:
      database_name = self.DEFAULT_DATABASE
    return '\'host={0} user={1} password={2} dbname={3}\''.format(
        self.connection_properties.endpoint,
        self.connection_properties.database_username,
        self.connection_properties.database_password, database_name)

  def GetSysbenchConnectionString(self):
    return ('--pgsql-host={0} --pgsql-user={1} --pgsql-password="{2}" '
            '--pgsql-port=5432').format(
                self.connection_properties.endpoint,
                self.connection_properties.database_username,
                self.connection_properties.database_password)

  def GetExplainPrefix(self) -> str:
    """Adding hints to increase the verboseness of the explain."""
    return 'EXPLAIN (ANALYZE, BUFFERS, TIMING, SUMMARY, VERBOSE) '


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

  def MakeSqlCommand(self,
                     command: str,
                     database_name: str = '',
                     session_variables: str = ''):
    """See base class."""
    if session_variables:
      raise NotImplementedError(
          'Session variables is currently not supported in mysql')
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

  def MakeSqlCommand(self,
                     command: str,
                     database_name: str = '',
                     session_variables: str = ''):
    """See base class."""
    if session_variables:
      raise NotImplementedError(
          'Session variables is currently not supported in mysql')
    sqlserver_command = '/opt/mssql-tools/bin/sqlcmd -S %s -U %s -P %s ' % (
        self.connection_properties.endpoint,
        self.connection_properties.database_username,
        self.connection_properties.database_password)
    if database_name:
      sqlserver_command += '-d %s ' % database_name

    sqlserver_command = sqlserver_command + '-Q "%s"' % command
    return sqlserver_command

  def GetConnectionString(self, database_name=''):
    raise NotImplementedError('Connection string currently not supported')

  def RunSqlScript(self,
                   file_path: str,
                   database_name: str = '') -> Tuple[str, str]:
    """Runs Sql script from sqlcmd.

    This method execute command in a sql file using sqlcmd with the -i option
    enabled.
    Args:
      file_path: The local path from the machine.
      database_name: Name of the database.

    Returns:
       A tuple of stdout and stderr from running the command.
    """
    sqlserver_command = (
        '/opt/mssql-tools/bin/sqlcmd -S '
        '%s -U %s -P %s ' %
        (self.connection_properties.endpoint,
         self.connection_properties.database_username,
         self.connection_properties.database_password))
    if database_name:
      sqlserver_command += '-d %s ' % database_name

    sqlserver_command += ' -i ' +  file_path
    return self.vm.RemoteCommand(sqlserver_command)


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
