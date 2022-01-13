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

import copy
import datetime
import json
import logging
import re
from typing import Dict, Text, Tuple

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util

LATEST_CLIENT_JAR = 'athena-java-client-2.1.jar'

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
    'item', 'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
    'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
    'web_sales', 'web_site'
]

FLAGS = flags.FLAGS


class AthenaQueryError(RuntimeError):
  pass


def GetAthenaClientInterface(database: str, output_bucket: str,
                             region: str) -> edw_service.EdwClientInterface:
  """Builds and Returns the requested Athena client Interface.

  Args:
    database: Name of the Athena database to execute queries against.
    output_bucket: String name of the S3 bucket to store query output.
    region: String aws region in which the database exists and client operations
      are performed.

  Returns:
    A concrete Client Interface object (subclass of EdwClientInterface)

  Raises:
    RuntimeError: if an unsupported athena_client_interface is requested
  """
  if FLAGS.athena_client_interface == 'JAVA':
    return JavaClientInterface(database, output_bucket, region)
  raise RuntimeError('Unknown Athena Client Interface requested.' +
                     FLAGS.athena_client_interface)


class GenericClientInterface(edw_service.EdwClientInterface):
  """Generic Client Interface class for Athena.

  Attributes:
    database: String name of the Athena database to execute queries against.
    output_bucket: String name of the S3 bucket to store query output.
    region: String aws region in which the database exists and client operations
      are performed.
  """

  def __init__(self, database: str, output_bucket: str, region: str):
    super(GenericClientInterface, self).__init__()
    self.database = database
    self.output_bucket = 's3://%s' % output_bucket
    self.region = region

  def GetMetadata(self) -> Dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    client_workgroup = FLAGS.athena_workgroup or 'dynamic'
    return {
        'client': f'{FLAGS.athena_client_interface}_{client_workgroup}',
        'client_region': self.region
    }


class JavaClientInterface(GenericClientInterface):
  """Java Client Interface class for Athena.
  """

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs the Java Execution Environment and a uber jar with
    a) Athena Java client libraries,
    b) An application to execute a query and gather execution details, and
    collect CW metrics
    c) their dependencies.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('openjdk')
    # Push the executable jar to the working directory on client vm
    self.client_vm.InstallPreprovisionedPackageData(
        package_name, [LATEST_CLIENT_JAR], '')

  def ExecuteQuery(self, query_name: Text) -> Tuple[float, Dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute

    Returns:
      A tuple of (execution_time, run_metadata)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      run_metadata: A dictionary of query execution attributes eg. script name
    """
    query_command = (f'java -cp {LATEST_CLIENT_JAR} '
                     'com.google.cloud.performance.edw.Single '
                     f'--region {self.region} '
                     f'--database {self.database} '
                     f'--output_location {self.output_bucket} '
                     f'--query_file {query_name} '
                     f'--query_timeout_secs {FLAGS.athena_query_timeout} '
                     f'--collect_metrics {FLAGS.athena_metrics_collection}')

    if not FLAGS.athena_metrics_collection:
      # execute the query in requested persistent workgroup
      query_command = f'{query_command} --workgroup {FLAGS.athena_workgroup} '
      query_command = f'{query_command} --delete_workgroup False'
    else:
      # the dynamic workgroup may have to live beyond the benchmark
      query_command = (f'{query_command} '
                       f'--delete_workgroup {FLAGS.athena_workgroup_delete}')

    stdout, _ = self.client_vm.RemoteCommand(query_command)
    details = copy.copy(self.GetMetadata())  # Copy the base metadata
    details.update(json.loads(stdout)['details'])
    details['query_start'] = json.loads(stdout)['query_start']
    details['query_end'] = json.loads(stdout)['query_end']
    performance = json.loads(stdout)['query_wall_time_in_secs']
    return performance, details


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
    self.region = util.GetRegionFromZone(FLAGS.zones[0])
    self.output_bucket = '-'.join(
        [FLAGS.athena_output_location_prefix, self.region, FLAGS.run_uri])
    self.client_interface = GetAthenaClientInterface(self.cluster_identifier,
                                                     self.output_bucket,
                                                     self.region)
    self.s3_service = s3.S3Service()
    self.s3_service.PrepareService(self.region)
    self.s3_service.MakeBucket(self.output_bucket)
    if FLAGS.provision_athena:
      self.data_bucket = 'pkb' + self.cluster_identifier.replace('_', '')
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
        '--result-configuration',
        ('OutputLocation=s3://%s' % self.output_bucket)
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
      drop_script_path = data.ResourcePath('edw/athena/%s/ddl/drop.sql' %
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
              script_contents, database=self.cluster_identifier)
          RunScriptCommand(script_command)

      drop_database_query_string = PrepareQueryString(
          'drop database database_name',
          {'database_name': self.cluster_identifier})
      script_command = self.BuildAthenaCommand(drop_database_query_string)
      RunScriptCommand(script_command)

    def _CreateDatabase():
      create_database_query_string = PrepareQueryString(
          'create database database_name',
          {'database_name': self.cluster_identifier})
      script_command = self.BuildAthenaCommand(create_database_query_string)
      return RunScriptCommand(script_command)

    def _CreateTable(table_create_sql_template):
      template_script_path = data.ResourcePath(table_create_sql_template)
      template_script_contents = ReadScript(template_script_path)
      script_contents = PrepareQueryString(template_script_contents,
                                           {'{bucket}': self.data_bucket})
      script_command = self.BuildAthenaCommand(
          script_contents, database=self.cluster_identifier)
      return RunScriptCommand(script_command)

    def _CreateAllTables():
      """Create all TPC benchmarking tables."""
      cumulative_table_create_time = 0
      for table in self.tables:
        for suffix in ['_csv', '_parquet']:
          script = 'edw/athena/%s/ddl/%s.sql' % (FLAGS.edw_tpc_dsb_type,
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

  def Cleanup(self):
    # Direct cleanup is used instead of _DeleteDependencies because the Athena
    # warehouse resource isn't created/deleted each time.
    self.s3_service.DeleteBucket(self.output_bucket)

  def GetDataDetails(self) -> Dict[str, str]:
    """Returns a dictionary with underlying data details.

    cluster_identifier = <dataset_id>
    Data details are extracted from the dataset_id that follows the format:
    <dataset>_<format>_<compression>_<partitioning>
    eg.
    tpch100_parquet_uncompressed_unpartitoned

    Returns:
      A dictionary set to underlying data's details (format, etc.)
    """
    data_details = {}
    # If the information isn't in the cluster identifier, skip collecting it.
    if '_' not in self.cluster_identifier:
      return data_details
    parsed_id = re.split(r'_', self.cluster_identifier)
    data_details['format'] = parsed_id[1]
    data_details['compression'] = parsed_id[2]
    data_details['partitioning'] = parsed_id[3]
    return data_details

  def GetMetadata(self):
    """Return a dictionary of the metadata for the Athena data warehouse."""
    basic_data = super(Athena, self).GetMetadata()
    basic_data.update({'database': self.cluster_identifier})
    basic_data.update(self.GetDataDetails())
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data
