# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS's Athena EDW service."""

import datetime
import logging

from perfkitbenchmarker import data

from perfkitbenchmarker import edw_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

AWS_ATHENA_CMD_PREFIX = ['aws', 'athena']
AWS_ATHENA_CMD_POSTFIX = ['--output', 'json']
# TODO(user): Derive the full table set from the TPC suite.
TPC_H_TABLES = [
    'customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region',
    'supplier'
]
TPC_DS_TABLES = [
    'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'dbgen_version', 'household_demographics', 'income_band', 'inventory',
    'item', 'promotion', 'reason', 'result_table', 'result_table1', 'ship_mode',
    'store', 'store_returns', 'store_sales', 'time_dim', 'warehouse',
    'web_page', 'web_returns', 'web_sales', 'web_site'
]

FLAGS = flags.FLAGS


class AthenaQueryError(RuntimeError):
  pass


def ReadScript(script_uri):
  """Method to read a sql script based on its local path.

  Arguments:
    script_uri: Local URI of file containing SQL query.

  Returns:
    Query String contents of the URI location.

  Raises:
    IOError: If the script cannot be read.
  """
  with open(script_uri) as fp:
    return fp.read()


def PrepareQueryString(query_string_template, substitutions):
  """Method to read a template Athena script and substitute placeholders.

  Args:
    query_string_template: Template version of the Athena query.
    substitutions: A dictionary of string placeholder keys and corresponding
      string values.

  Returns:
     Materialized Athena query as a string.
  """
  for key, value in substitutions.items():
    query_string = query_string_template.replace(key, value)
  return query_string


def RunScriptCommand(script_command):
  """Method to execute an AWS Athena cli command.

  Args:
    script_command: Fully compiled AWS Athena cli command.

  Returns:
    String stdout result of executing the query.
    Script Command execution duration in seconds (rounded).

  Raises:
    AthenaQueryError: If the return code does not indicate success.
  """
  start_time = datetime.datetime.now()
  stdout, _, retcode = vm_util.IssueCommand(
      script_command, raise_on_failure=False)
  if retcode:
    raise AthenaQueryError
  end_time = datetime.datetime.now()
  return stdout, int((end_time - start_time).total_seconds())


class Athena(edw_service.EdwService):
  """Object representing a Athena data warehouse."""

  CLOUD = providers.AWS
  SERVICE_TYPE = 'athena'

  def __init__(self, edw_service_spec):
    super(Athena, self).__init__(edw_service_spec)
    self.output_location = FLAGS.athena_output_location
    self.region = util.GetRegionFromZone(FLAGS.zones[0])
    if FLAGS.provision_athena:
      self.db = '_'.join(
          [FLAGS.edw_tpc_dsb_type,
           str(FLAGS.edw_tpc_dataset_size_in_GB)])
      self.data_bucket = 'pkb' + self.db.replace('_', '')
      self.tables = (
          TPC_H_TABLES if FLAGS.edw_tpc_dsb_type == 'tpc_h' else TPC_DS_TABLES)
      self.athena_db_create_time = 0
      self.athena_table_create_time = 0

  def BuildAthenaCommand(self, query_string, database=None):
    """Method to compile a AWS Athena cli command.

    Arguments:
      query_string: A string with the query that needs to be executed on Athena.
      database: The Athena database against which the query should be executed.

    Returns:
      Fully compiled AWS Athena cli command.
    """
    cmd = []
    cmd.extend(AWS_ATHENA_CMD_PREFIX)
    cmd.extend([
        '--region', self.region,
        'start-query-execution',
        '--query-string', query_string
    ])
    if database:
      cmd.extend(['--query-execution-context', ('Database=%s' % database)])
    cmd.extend([
        '--result-configuration', ('OutputLocation=%s' % self.output_location)
    ])
    cmd.extend(AWS_ATHENA_CMD_POSTFIX)
    return cmd

  def _Create(self):
    """Create a Athena data warehouse."""

    def _EmptyDatabase():
      """Remove tables, if they exist, so they can be refreshed.

      If the database and/or tables don't already exist, the drop commands
      will simply fail, which won't raise errors.
      """
      drop_script_path = data.ResourcePath('edw/athena/%s/ddl/s3_drop.sql' %
                                           FLAGS.edw_tpc_dsb_type)
      drop_script_contents = ReadScript(drop_script_path)
      # Drop all tables so the database can be dropped.
      for table in self.tables:
        # Remove the folder backing each parquet table so they can be refreshed.
        vm_util.IssueCommand([
            'aws', 's3', 'rm',
            's3://%s/%s_parquet' % (self.data_bucket, table), '--recursive'
        ], raise_on_failure=False)
        # The parquet tables don't have the type suffix so that the queries can
        # run as written without having to change the table names.
        for suffix in ['_csv', '']:
          script_contents = PrepareQueryString(drop_script_contents,
                                               {'{table}': table + suffix})
          script_command = self.BuildAthenaCommand(
              script_contents, database=self.db)
          RunScriptCommand(script_command)

      drop_database_query_string = PrepareQueryString(
          'drop database database_name', {'database_name': self.db})
      script_command = self.BuildAthenaCommand(drop_database_query_string)
      RunScriptCommand(script_command)

    def _CreateDatabase():
      create_database_query_string = PrepareQueryString(
          'create database database_name', {'database_name': self.db})
      script_command = self.BuildAthenaCommand(create_database_query_string)
      return RunScriptCommand(script_command)

    def _CreateTable(table_create_sql_template):
      template_script_path = data.ResourcePath(table_create_sql_template)
      template_script_contents = ReadScript(template_script_path)
      script_contents = PrepareQueryString(template_script_contents,
                                           {'{bucket}': self.data_bucket})
      script_command = self.BuildAthenaCommand(
          script_contents, database=self.db)
      return RunScriptCommand(script_command)

    def _CreateAllTables():
      """Create all TPC benchmarking tables."""
      cumulative_table_create_time = 0
      for table in self.tables:
        for suffix in ['_csv', '_parquet']:
          script = 'edw/athena/%s/ddl/s3_%s.sql' % (FLAGS.edw_tpc_dsb_type,
                                                    table + suffix)
          _, table_create_time = _CreateTable(script)
          cumulative_table_create_time += table_create_time
      return cumulative_table_create_time

    _EmptyDatabase()
    _, self.athena_db_create_time = _CreateDatabase()
    self.athena_table_create_time = _CreateAllTables()

  def _Exists(self):
    """Method to validate the existence of a Athena data warehouse.

    Returns:
      Boolean value indicating the existence of a Athena data warehouse.
    """
    raise NotImplementedError

  def _Delete(self):
    """Delete a Athena data warehouse."""
    if not FLAGS.teardown_athena:
      logging.info('The current resource is requested to be long living.')
      return
    raise NotImplementedError

  def GetMetadata(self):
    """Return a dictionary of the metadata for the Athena data warehouse."""
    basic_data = super(Athena, self).GetMetadata()
    return basic_data

  def RunCommandHelper(self):
    """Athena data warehouse specific run script command components."""
    raise NotImplementedError

  def InstallAndAuthenticateRunner(self, vm):
    """Method to perform installation and authentication of Athena query runner.

    Athena APIs are included in the default AWS cli
    https://docs.aws.amazon.com/cli/latest/reference/athena/index.html.

    Args:
      vm: Client vm on which the query will be run.
    """
    for pkg in ('aws_credentials', 'awscli'):
      vm.Install(pkg)

  def PrepareClientVm(self, vm):
    """Prepare phase to install the runtime environment on the client vm.

    Args:
      vm: Client vm on which the script will be run.
    """
    super(Athena, self).PrepareClientVm(vm)
    self.InstallAndAuthenticateRunner(vm)

  @classmethod
  def RunScriptOnClientVm(cls, vm, database, script):
    """A function to execute a script against AWS Athena Database."""
    stdout, _ = vm.RemoteCommand('python runner.py --database=%s --script=%s' %
                                 (database, script))
    return stdout
