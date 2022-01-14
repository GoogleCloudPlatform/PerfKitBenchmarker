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
"""Module containing class for Snowflake EDW service resource hosted on AWS."""

import copy
import json
from typing import Dict, List, Text, Tuple
from absl import flags
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import providers


FLAGS = flags.FLAGS


def GetSnowflakeClientInterface(warehouse: str, database: str,
                                schema: str) -> edw_service.EdwClientInterface:
  """Builds and Returns the requested Snowflake client Interface.

  Args:
    warehouse: String name of the Snowflake virtual warehouse to use during the
      benchmark
    database: String name of the Snowflake database to use during the  benchmark
    schema: String name of the Snowflake schema to use during the  benchmark

  Returns:
    A concrete Client Interface object (subclass of EdwClientInterface)

  Raises:
    RuntimeError: if an unsupported snowflake_client_interface is requested
  """
  if FLAGS.snowflake_client_interface == 'JDBC':
    return JdbcClientInterface(warehouse, database, schema)
  raise RuntimeError('Unknown Snowflake Client Interface requested.')


class JdbcClientInterface(edw_service.EdwClientInterface):
  """Jdbc Client Interface class for Snowflake.

  Attributes:
    warehouse: String name of the virtual warehouse used during benchmark
    database: String name of the database to benchmark
    schema: String name of the schema to benchmark
  """

  def __init__(self, warehouse: str, database: str, schema: str):
    self.warehouse = warehouse
    self.database = database
    self.schema = schema

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
    self.client_vm.InstallPreprovisionedPackageData(
        package_name, ['snowflake-jdbc-client-2.0.jar'], '')

  def ExecuteQuery(self, query_name: Text) -> Tuple[float, Dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = ('java -cp snowflake-jdbc-client-2.0.jar '
                     'com.google.cloud.performance.edw.Single --warehouse {} '
                     '--database {} --schema {} --query_file {}').format(
                         self.warehouse, self.database, self.schema, query_name)
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    details = copy.copy(self.GetMetadata())  # Copy the base metadata
    details.update(json.loads(stdout)['details'])
    return json.loads(stdout)['query_wall_time_in_secs'], details

  def ExecuteSimultaneous(self, submission_interval: int,
                          queries: List[str]) -> str:
    """Executes queries simultaneously on client and return performance details.

    Simultaneous app expects queries as white space separated query file names.

    Args:
      submission_interval: Simultaneous query submission interval in
        milliseconds.
      queries: List of strings (names) of queries to execute.

    Returns:
      A serialized dictionary of execution details.
    """
    query_command = (
        'java -cp snowflake-jdbc-client-2.0.jar '
        'com.google.cloud.performance.edw.Simultaneous --warehouse {} '
        '--database {} --schema {} --submission_interval {} --query_files {}'
    ).format(self.warehouse, self.database, self.schema, submission_interval,
             ' '.join(queries))
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    return stdout

  def ExecuteThroughput(self, concurrency_streams: List[List[str]]) -> str:
    """Executes a throughput test and returns performance details.

    Args:
      concurrency_streams: List of streams to execute simultaneously, each of
        which is a list of string names of queries.

    Returns:
      A serialized dictionary of execution details.
    """
    query_command = ('java -cp snowflake-jdbc-client-2.0.jar '
                     'com.google.cloud.performance.edw.Throughput --warehouse'
                     ' {} --database {} --schema {} --query_streams {}').format(
                         self.warehouse, self.database, self.schema, ' '.join([
                             ','.join(stream) for stream in concurrency_streams
                         ]))
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    return stdout

  def GetMetadata(self) -> Dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    return {'client': FLAGS.snowflake_client_interface}


class Snowflake(edw_service.EdwService):
  """Object representing a Snowflake Data Warehouse Instance hosted on AWS."""
  CLOUD = providers.AWS
  SERVICE_TYPE = 'snowflake_aws'

  def __init__(self, edw_service_spec):
    super(Snowflake, self).__init__(edw_service_spec)
    self.warehouse = FLAGS.snowflake_warehouse
    self.database = FLAGS.snowflake_database
    self.schema = FLAGS.snowflake_schema
    self.client_interface = GetSnowflakeClientInterface(self.warehouse,
                                                        self.database,
                                                        self.schema)

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

  def GetMetadata(self):
    """Return a metadata dictionary of the benchmarked Snowflake cluster."""
    basic_data = super(Snowflake, self).GetMetadata()
    basic_data['warehouse'] = self.warehouse
    basic_data['database'] = self.database
    basic_data['schema'] = self.schema
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data


class Snowflakeexternal(Snowflake):
  """Class representing Snowflake External Warehouses."""

  SERVICE_TYPE = 'snowflakeexternal_aws'

  def GetMetadata(self) -> Dict[str, str]:
    """Return a dictionary of the metadata for the Snowflake External service.

    Returns:
      A dictionary set to service details.
    """
    basic_data = super(Snowflakeexternal, self).GetMetadata()
    basic_data['edw_service_type'] = Snowflakeexternal.SERVICE_TYPE
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data
