# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing functionality for Snowflake EDW service resources.

Contains common functionality for Snowflake resources. Use a cloud-specific
Snowflake resource (snowflake_aws, snowflake_azure, etc.) when actually running
benchmarks.
"""

import copy
import json
import logging
import os
from typing import Any, Union, override
from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

JdbcClientDict = {
    provider_info.AWS: 'snowflake-jdbc-client-2.14-enterprise.jar',
    provider_info.AZURE: 'snowflake-jdbc-client-azure-external-2.0.jar',
}
# The python client is developed internally as of now. This requires a client
# library which should call into the DB. Contact p3rf-team [at] google [dot] com
# if you would like to run this benchmark to get the file.
SNOWFLAKE_PYTHON_CLIENT_FILE = 'sf_python_driver.py'
SNOWFLAKE_PYTHON_CLIENT_DIR = 'edw/snowflake/clients/python'


class SnowflakeClientInterface(edw_service.EdwClientInterface):
  """Base client interface for Snowflake."""

  warehouse: str
  database: str
  schema: str

  def __init__(self, warehouse: str, database: str, schema: str):
    super().__init__()
    self.warehouse = warehouse
    self.database = database
    self.schema = schema

  def GetMetadata(self) -> dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    return {'client': FLAGS.snowflake_client_interface}


class JdbcClientInterface(SnowflakeClientInterface):
  """Jdbc Client Interface class for Snowflake.

  Attributes:
    jdbc_client: String filename of the JDBC client associated with the
      Snowflake backend being tested (AWS/Azure/etc.)
  """

  def __init__(
      self, warehouse: str, database: str, schema: str, jdbc_client: str
  ):
    super().__init__(warehouse, database, schema)
    self.jdbc_client = jdbc_client

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs a java client application that uses the JDBC driver for connecting
     to a database server.
    https://docs.snowflake.com/en/user-guide/jdbc.html

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('openjdk')

    # Push the executable jar to the working directory on client vm
    if FLAGS.snowflake_jdbc_client_jar:
      self.client_vm.PushFile(FLAGS.snowflake_jdbc_client_jar)
    else:
      self.client_vm.InstallPreprovisionedPackageData(
          package_name, [self.jdbc_client], ''
      )

    # Push the private key to the working directory on client vm
    if FLAGS.snowflake_jdbc_key_file:
      self.client_vm.PushFile(FLAGS.snowflake_jdbc_key_file)
    else:
      self.client_vm.InstallPreprovisionedPackageData(
          package_name, ['snowflake_keyfile.p8'], ''
      )

  def ExecuteQuery(
      self, query_name: str, print_results: bool = True
  ) -> tuple[float, dict[str, Any]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute
      print_results: Whether to include query results in execution details.

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = (
        f'java -cp {self.jdbc_client} '
        'com.google.cloud.performance.edw.Single '
        f'--warehouse {self.warehouse} '
        f'--database {self.database} '
        f'--schema {self.schema} '
        f'--query_file {query_name} '
        '--key_file snowflake_keyfile.p8'
    )
    if print_results:
      query_command += ' --print_results true'
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    details = copy.copy(self.GetMetadata())  # Copy the base metadata
    details.update(json.loads(stdout)['details'])
    result = (json.loads(stdout)['query_wall_time_in_secs'], details)
    return result

  def ExecuteThroughput(
      self,
      concurrency_streams: list[list[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
    """Executes a throughput test and returns performance details.

    Args:
      concurrency_streams: List of streams to execute simultaneously, each of
        which is a list of string names of queries.
      labels: Not supported by this implementation.

    Returns:
      A serialized dictionary of execution details.
    """
    del labels  # Not currently supported by this implementation.
    query_command = (
        f'java -cp {self.jdbc_client} '
        'com.google.cloud.performance.edw.Throughput '
        f'--warehouse {self.warehouse} '
        f'--database {self.database} '
        f'--schema {self.schema} '
        '--key_file snowflake_keyfile.p8 '
        '--query_streams '
        f'{" ".join([",".join(stream) for stream in concurrency_streams])}'
    )
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    return stdout


class PythonClientInterface(SnowflakeClientInterface):
  """Python Client Interface class for Snowflake.

  Attributes:
    user: The user to connect to snowflake with.
    account: The snowflake account.
  """

  def __init__(
      self,
      warehouse: str,
      database: str,
      schema: str,
      user: str,
      account: str,
  ):
    super().__init__(warehouse, database, schema)
    self.user = user
    self.account = account

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query."""

    # Push the private key to the working directory on client vm
    if FLAGS.snowflake_jdbc_key_file:
      self.client_vm.PushFile(FLAGS.snowflake_jdbc_key_file)
    else:
      self.client_vm.InstallPreprovisionedPackageData(
          package_name, ['snowflake_keyfile.p8'], ''
      )

    # Install dependencies for driver
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand(
        'sudo apt-get -qq update && DEBIAN_FRONTEND=noninteractive sudo apt-get'
        ' -qq install python3.12-venv'
    )
    self.client_vm.RemoteCommand('python3 -m venv .venv')
    self.client_vm.RemoteCommand(
        'source .venv/bin/activate && pip install snowflake-connector-python'
        ' pyarrow absl-py pandas'
    )

    # Push driver script to client vm
    self.client_vm.PushDataFile(
        os.path.join(SNOWFLAKE_PYTHON_CLIENT_DIR, SNOWFLAKE_PYTHON_CLIENT_FILE)
    )
    self.client_vm.PushDataFile(
        os.path.join(
            edw_service.EDW_PYTHON_DRIVER_LIB_DIR,
            edw_service.EDW_PYTHON_DRIVER_LIB_FILE,
        )
    )

  def _RunPythonClientCommand(
      self, command: str, additional_args: list[str]
  ) -> str:
    """Runs a command on the python client.

    Args:
      command: The command to run (e.g. 'single', 'throughput').
      additional_args: A list of additional arguments for the command.

    Returns:
      The stdout from the command.
    """
    cmd_parts = [
        f'.venv/bin/python {SNOWFLAKE_PYTHON_CLIENT_FILE}',
        command,
        f'--warehouse {self.warehouse}',
        f'--database {self.database}',
        f'--schema {self.schema}',
        f'--user {self.user}',
        f'--account {self.account}',
        '--credentials_file snowflake_keyfile.p8',
    ]
    cmd_parts.extend(additional_args)
    cmd = ' '.join(cmd_parts)
    stdout, _ = self.client_vm.RobustRemoteCommand(cmd)
    return stdout

  def ExecuteQuery(
      self, query_name: str, print_results: bool = False
  ) -> tuple[float, dict[str, Any]]:
    """Executes a query and returns performance details."""
    args = [f'--query_file {query_name}']
    if print_results:
      args.append('--print_results')
    stdout = self._RunPythonClientCommand('single', args)
    details = copy.copy(self.GetMetadata())
    details.update(json.loads(stdout)['details'])
    return json.loads(stdout)['query_wall_time_in_secs'], details

  def ExecuteThroughput(
      self,
      concurrency_streams: list[list[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
    """Executes queries simultaneously on client and return performance details."""
    del labels  # Currently not supported by this implementation.
    args = [f"--query_streams='{json.dumps(concurrency_streams)}'"]
    return self._RunPythonClientCommand('throughput', args)

  def RunQueryWithResults(self, query_name: str) -> str:
    """Executes a query and returns performance details and query output."""
    args = [f'--query_file {query_name}', '--print_results']
    return self._RunPythonClientCommand('single', args)


def GetSnowflakeClientInterface(
    warehouse: str,
    database: str,
    schema: str,
    cloud: str,
    user: str | None,
    account: str | None,
) -> SnowflakeClientInterface:
  """Builds and Returns the requested Snowflake client Interface.

  Args:
    warehouse: String name of the Snowflake virtual warehouse to use during the
      benchmark
    database: String name of the Snowflake database to use during the  benchmark
    schema: String name of the Snowflake schema to use during the  benchmark
    cloud: String ID of the cloud service the client interface is for
    user: The user to connect to snowflake with.
    account: The snowflake account.

  Returns:
    A concrete Client Interface object (subclass of EdwClientInterface)

  Raises:
    RuntimeError: if an unsupported snowflake_client_interface is requested
  """
  if FLAGS.snowflake_client_interface == 'JDBC':
    return JdbcClientInterface(
        warehouse, database, schema, JdbcClientDict[cloud]
    )
  elif FLAGS.snowflake_client_interface == 'PYTHON':
    assert user and account
    return PythonClientInterface(
        warehouse,
        database,
        schema,
        user,
        account,
    )
  raise RuntimeError('Unknown Snowflake Client Interface requested.')


class Snowflake(edw_service.EdwService):
  """Object representing a Snowflake Data Warehouse Instance."""

  SEARCH_QUERY_TEMPLATE_LOCATION = 'edw/snowflake_aws/search_index'

  CREATE_INDEX_QUERY_TEMPLATE = (
      f'{SEARCH_QUERY_TEMPLATE_LOCATION}/create_index_query.sql.j2'
  )
  DELETE_INDEX_QUERY_TEMPLATE = (
      f'{SEARCH_QUERY_TEMPLATE_LOCATION}/delete_index_query.sql.j2'
  )
  GET_INDEX_STATUS_QUERY_TEMPLATE = (
      f'{SEARCH_QUERY_TEMPLATE_LOCATION}/index_status.sql.j2'
  )
  INITIALIZE_SEARCH_TABLE_QUERY_TEMPLATE = (
      f'{SEARCH_QUERY_TEMPLATE_LOCATION}/table_init.sql.j2'
  )
  LOAD_SEARCH_DATA_QUERY_TEMPLATE = (
      f'{SEARCH_QUERY_TEMPLATE_LOCATION}/ingestion_query.sql.j2'
  )
  INDEX_SEARCH_QUERY_TEMPLATE = (
      f'{SEARCH_QUERY_TEMPLATE_LOCATION}/search_query.sql.j2'
  )
  GET_ROW_COUNT_QUERY_TEMPLATE = (
      f'{SEARCH_QUERY_TEMPLATE_LOCATION}/get_row_count.sql.j2'
  )
  TIME_BOUND_QUERY_HISTORY_TEMPLATE = (
      'edw/snowflake_aws/metadata/time_bound_query_history.sql.j2'
  )
  INDIVIDUAL_QUERY_PLAN_TEMPLATE = (
      'edw/snowflake_aws/metadata/individual_query_plan.sql.j2'
  )
  INDIVIDUAL_QUERY_STATS_TEMPLATE = (
      'edw/snowflake_aws/metadata/individual_query_stats.sql.j2'
  )

  CLOUD: str = None
  SERVICE_TYPE = None

  def __init__(self, edw_service_spec):
    super().__init__(edw_service_spec)
    self.warehouse = FLAGS.snowflake_warehouse
    self.database = FLAGS.snowflake_database
    self.schema = FLAGS.snowflake_schema
    self.user = FLAGS.snowflake_user
    self.account = FLAGS.snowflake_account
    self.client_interface = GetSnowflakeClientInterface(
        self.warehouse,
        self.database,
        self.schema,
        self.CLOUD,
        self.user,
        self.account,
    )

  def IsUserManaged(self, edw_service_spec):
    # TODO(saksena): Remove the assertion after implementing provisioning of
    # virtual warehouses.
    return True

  def _Create(self):
    """Create a Snowflake cluster."""
    raise NotImplementedError

  def _Exists(self):
    """Method to validate the existence of a Snowflake cluster.

    Returns:
      Boolean value indicating the existence of a cluster.
    """
    return True

  def _Delete(self):
    """Delete a Snowflake cluster."""
    raise NotImplementedError

  def RemoveDataset(self, dataset: Union[str, None] = None):
    """Removes a dataset.

    Args:
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
    """
    if dataset is None:
      dataset = f'pkb_{FLAGS.run_uri}_snowflake_autoname'
    query = f'DROP SCHEMA {dataset}'
    self._RunConfigurationStatement('drop_schema', query)

  def CreateDataset(
      self,
      dataset: Union[str, None] = None,
      description: Union[str, None] = None,
  ):
    """Create a new dataset.

    For Snowflake, we define a 'dataset' to be a 'schema', Snowflake's second
    level division of data, below 'database'. This is analogous to the
    GCP project/dataset hierarchy in BigQuery.

    Args:
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
      description: Optional description of the dataset.
    """
    if dataset is None:
      dataset = f'pkb_{FLAGS.run_uri}_snowflake_autoname'
    query = f'CREATE SCHEMA {dataset}'
    self._RunConfigurationStatement('create_schema', query)

  def LoadDataset(self, source_bucket, tables, dataset=None):
    """Load all tables in a dataset to a database from object storage.

    Args:
      source_bucket: Name of the bucket to load the data from. Should already
        exist. Each table must have its own subfolder in the bucket named after
        the table, containing one or more csv files that make up the table data.
      tables: List of table names to load.
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
    """
    raise NotImplementedError

  def CopyTable(self, copy_table_name: str, to_dataset: str) -> None:
    """Copy a table from the active dataset to the specified dataset.

    Copies a table between datasets, from the active (current) dataset to
    another named dataset in the same project.

    Args:
      copy_table_name: Name of the table to copy from the loaded dataset to the
        copy dataset.
      to_dataset: Name of the dataset to copy the table into.
    """
    database = self.client_interface.database
    from_dataset = self.client_interface.schema
    self._CreateLikeTable(copy_table_name, to_dataset)
    self._CopyData(database, copy_table_name, to_dataset, from_dataset)

  def _CreateLikeTable(self, table_name: str, dataset: str) -> None:
    """Create a table in the specified dataset with the same name and schema.

    Args:
      table_name: Name of the table to create a schema-copy of in the target
        dataset. Must be an extant table in the active dataset.
      dataset: The dataset to create a table in.
    """
    database = self.client_interface.database
    from_dataset = self.client_interface.schema
    statement = (
        'CREATE TABLE'
        f' "{database}"."{dataset}".{table_name} LIKE'
        f' "{database}"."{from_dataset}".{table_name}'
    )

    logging.info(
        'Creating Snowflake table %s in target schema %s ...',
        table_name,
        dataset,
    )

    self._RunConfigurationStatement('create_table', statement)

  def _CopyData(
      self, database, copy_table_name: str, to_dataset: str, from_dataset: str
  ) -> None:
    """Copy data from a table in dataset to same name table in other dataset.

    Args:
      database: The database that contains the to and from datasets.
      copy_table_name: Name of the table to copy from the loaded dataset to the
        copy dataset.
      to_dataset: Name of the dataset to copy the table into.
      from_dataset: Name of the dataset to copy the table from.
    """

    copy_data_statement = (
        'INSERT INTO'
        f' "{database}"."{to_dataset}".{copy_table_name} '
        'SELECT * FROM'
        f' "{database}"."{from_dataset}".{copy_table_name}'
    )

    logging.info('Copying data from %s to %s ...', from_dataset, to_dataset)
    self._RunConfigurationStatement('copy_data', copy_data_statement)

  def _RunConfigurationStatement(self, stmt_name: str, query_text: str) -> None:
    """Run the provided statement on the Snowflake instance, and return results.

    Does not time the specified statement.

    Args:
      stmt_name: A name for the provided statement
      query_text: The query text of the statement that should be run.

    Returns:
    Results returned in response to running the provided statement.
    """
    vm_util.CreateRemoteFile(
        self.client_interface.client_vm, query_text, stmt_name
    )
    logging.info(
        'Now running %s statement: %s', stmt_name, query_text, stacklevel=2
    )
    self.client_interface.ExecuteQuery(stmt_name)

  def OpenDataset(self, dataset: str):
    """Switch from the currently active dataset to the one specified.

    Switches the dataset that will be accessed by queries sent through the
    client interface that this EDW service provides.

    For Snowflake, 'Dataset' is taken to mean what Snowflake calls 'Schemas',
    which are a level of organization down from 'Databases'.

    Args:
      dataset: Name of the dataset to make the active dataset.
    """
    self.client_interface.schema = dataset

  def GetMetadata(self):
    """Return a metadata dictionary of the benchmarked Snowflake cluster."""
    basic_data = super().GetMetadata()
    basic_data['warehouse'] = self.warehouse
    basic_data['database'] = self.database
    basic_data['schema'] = self.schema
    if self.user:
      basic_data['user'] = self.user
    if self.account:
      basic_data['account'] = self.account
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data

  def CreateSearchIndex(
      self, table_path: str, index_name: str
  ) -> tuple[float, dict[str, Any]]:
    query_name = f'create_index_{index_name}'
    context = {
        'table_name': table_path,
        'index_name': index_name,
    }
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(self.CREATE_INDEX_QUERY_TEMPLATE),
        query_name,
        context,
        should_log_file=True,
    )
    return self.client_interface.ExecuteQuery(query_name, print_results=True)

  def DropSearchIndex(
      self, table_path: str, index_name: str
  ) -> tuple[float, dict[str, Any]]:
    query_name = 'delete_index'
    context = {
        'table_name': table_path,
        'index_name': index_name,
    }
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(self.DELETE_INDEX_QUERY_TEMPLATE),
        query_name,
        context,
        should_log_file=True,
    )
    return self.client_interface.ExecuteQuery(query_name, print_results=True)

  def GetSearchIndexCompletionPercentage(
      self, table_path: str, index_name: str
  ) -> tuple[int, dict[str, Any]]:
    query_name = 'get_index_status'
    context = {
        'table_name': table_path,
        'index_name': index_name,
    }
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(self.GET_INDEX_STATUS_QUERY_TEMPLATE),
        query_name,
        context,
        should_log_file=True,
    )
    _, meta = self.client_interface.ExecuteQuery(query_name, print_results=True)
    qres = int(meta['query_results']['COVERAGE_PERCENTAGE'][0])
    return qres, meta

  def InitializeSearchStarterTable(
      self, table_path: str, storage_path: str
  ) -> tuple[float, dict[str, Any]]:
    query_name = 'initialize_search_table'
    context = {
        'table_name': table_path,
    }
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(self.INITIALIZE_SEARCH_TABLE_QUERY_TEMPLATE),
        query_name,
        context,
        should_log_file=True,
    )
    return self.client_interface.ExecuteQuery(query_name, print_results=True)

  def InsertSearchData(
      self, table_path: str, storage_path: str
  ) -> tuple[float, dict[str, Any]]:
    query_name = 'load_search_data'
    context = {
        'table_name': table_path,
        'storage_path': storage_path,
    }
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(self.LOAD_SEARCH_DATA_QUERY_TEMPLATE),
        query_name,
        context,
        should_log_file=True,
    )
    # Snowflake returns info for each csv file processed, which pollutes logs
    # and datapoints needlessly. So print_results=False.
    return self.client_interface.ExecuteQuery(query_name, print_results=False)

  def GetTableRowCount(self, table_path: str) -> tuple[int, dict[str, Any]]:
    query_name = 'get_row_count'
    context = {
        'table_name': table_path,
    }
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(self.GET_ROW_COUNT_QUERY_TEMPLATE),
        query_name,
        context,
        should_log_file=True,
    )
    _, meta = self.client_interface.ExecuteQuery(query_name, print_results=True)
    qres = int(meta['query_results']['TOTAL_ROW_COUNT'][0])
    return qres, meta

  def TextSearchQuery(
      self, table_path: str, search_keyword: str, index_name: str
  ) -> tuple[float, dict[str, Any]]:
    query_name = 'text_search_query'
    context = {
        'table_name': table_path,
        'search_text': search_keyword,
        'index_name': index_name,
    }
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(self.INDEX_SEARCH_QUERY_TEMPLATE),
        query_name,
        context,
        should_log_file=True,
    )
    res, meta = self.client_interface.ExecuteQuery(
        query_name, print_results=True
    )
    meta['edw_search_result_rows'] = int(
        meta['query_results']['RESULT_ROWS'][0]
    )
    return res, meta

  def SetWarehouse(self, warehouse: str):
    """Switches Snowflake Warehouse."""
    assert isinstance(
        self.client_interface, (PythonClientInterface, JdbcClientInterface)
    )
    self.warehouse = warehouse
    self.client_interface.warehouse = warehouse

  def _RunMetadataQuery(
      self, query_template: str, query_name: str, context: dict[str, Any]
  ) -> dict[str, Any]:
    self.client_interface.client_vm.RenderTemplate(
        data.ResourcePath(query_template),
        query_name,
        context,
        should_log_file=True,
    )
    _, output = self.client_interface.ExecuteQuery(
        query_name, print_results=True
    )
    col_res = output['query_results']
    return col_res

  def _GetIndividualQueryMetadata(self, query_id: str) -> list[dict[str, Any]]:
    query_plan_file_name = f'individual_query_plan_{query_id}.sql'
    query_stats_file_name = f'individual_query_stats_{query_id}.sql'
    context = {
        'query_id': query_id,
    }
    query_plan_rows = self.ColsToRows(
        self._RunMetadataQuery(
            self.INDIVIDUAL_QUERY_PLAN_TEMPLATE,
            query_plan_file_name,
            context,
        )
    )
    query_stats_rows = self.ColsToRows(
        self._RunMetadataQuery(
            self.INDIVIDUAL_QUERY_STATS_TEMPLATE,
            query_stats_file_name,
            context,
        )
    )
    results = [
        {
            'metric': 'edw_sf_query_plan',
            'value': 1,
            'unit': 'metadata',
            'metadata': {
                f'sf_{key.lower()}': value
                for key, value in query_plan_rows[0].items()
            },
        },
        {
            'metric': 'edw_sf_query_stats',
            'value': 1,
            'unit': 'metadata',
            'metadata': {
                'sf_query_id': query_id,
                'sf_query_stats': json.dumps(query_stats_rows, default=str),
            },
        },
    ]
    return results

  @override
  def GetTimeBoundAuxiliaryMetrics(
      self, start_timestamp: float, end_timestamp: float
  ) -> list[dict[str, Any]]:
    """Returns the auxiliary metrics for the given run."""
    query_file_name = f'metadata_query_{start_timestamp}.sql'
    context = {
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp,
        'warehouse': self.warehouse,
    }
    col_res = self._RunMetadataQuery(
        self.TIME_BOUND_QUERY_HISTORY_TEMPLATE,
        query_file_name,
        context,
    )
    row_res = self.ColsToRows(col_res)
    history_results = []
    for row in row_res:
      history_results.append({
          'metric': 'sf_query_metadata',
          'value': 1,
          'unit': 'metadata',
          'metadata': {
              f'sf_{key.lower()}': value for key, value in row.items()
          },
      })
    for qid in col_res['QUERY_ID']:
      history_results.extend(self._GetIndividualQueryMetadata(qid))
    return history_results
