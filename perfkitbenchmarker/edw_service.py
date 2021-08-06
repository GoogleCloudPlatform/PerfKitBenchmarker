# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Resource encapsulating provisioned Data Warehouse in the cloud Services.

Classes to wrap specific backend services are in the corresponding provider
directory as a subclass of BaseEdwService.
"""
import os
from typing import Dict, List, Text, Tuple

from absl import flags
from perfkitbenchmarker import resource


flags.DEFINE_integer('edw_service_cluster_concurrency', 5,
                     'Number of queries to run concurrently on the cluster.')
flags.DEFINE_string('edw_service_cluster_snapshot', None,
                    'If set, the snapshot to restore as cluster.')
flags.DEFINE_string('edw_service_cluster_identifier', None,
                    'If set, the preprovisioned edw cluster.')
flags.DEFINE_string('edw_service_endpoint', None,
                    'If set, the preprovisioned edw cluster endpoint.')
flags.DEFINE_string('edw_service_cluster_db', None,
                    'If set, the db on cluster to use during the benchmark ('
                    'only applicable when using snapshots).')
flags.DEFINE_string('edw_service_cluster_user', None,
                    'If set, the user authorized on cluster (only applicable '
                    'when using snapshots).')
flags.DEFINE_string('edw_service_cluster_password', None,
                    'If set, the password authorized on cluster (only '
                    'applicable when using snapshots).')
flags.DEFINE_string('snowflake_snowsql_config_override_file', None,
                    'The SnowSQL configuration to use.'
                    'https://docs.snowflake.net/manuals/user-guide/snowsql-config.html#snowsql-config-file')  # pylint: disable=line-too-long
flags.DEFINE_string('snowflake_connection', None,
                    'Named Snowflake connection defined in SnowSQL config file.'
                    'https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-named-connections')  # pylint: disable=line-too-long
flags.DEFINE_integer('edw_suite_iterations', 1, 'Number of suite iterations to perform.')
# TODO(user): Revisit flags for accepting query lists.
flags.DEFINE_string('edw_simultaneous_queries',
                    None, 'CSV list of simultaneous queries to benchmark.')
flags.DEFINE_integer('edw_simultaneous_query_submission_interval', '0',
                     'Simultaneous query submission interval in milliseconds.')
flags.DEFINE_string('edw_power_queries', None,
                    'CSV list of power queries to benchmark.')
flags.DEFINE_multi_string(
    'concurrency_streams', [], 'List of all query streams to execute. Each '
    'stream should be passed in separately and the queries should be comma '
    'separated, e.g. --concurrency_streams=1,2,3 --concurrency_streams=3,2,1')
flags.DEFINE_string('snowflake_warehouse', None,
                    'A virtual warehouse, often referred to simply as a - '
                    'warehouse, is a cluster of compute in Snowflake. '
                    'https://docs.snowflake.com/en/user-guide/warehouses.html')  # pylint: disable=line-too-long
flags.DEFINE_string(
    'snowflake_database', None,
    'The hosted snowflake database to use during the benchmark.')
flags.DEFINE_string(
    'snowflake_schema', None,
    'The schema of the hosted snowflake database to use during the benchmark.')
flags.DEFINE_enum(
    'snowflake_client_interface', 'JDBC', ['JDBC'],
    'The Runtime Interface used when interacting with Snowflake.')


FLAGS = flags.FLAGS


TYPE_2_PROVIDER = dict([('athena', 'aws'), ('redshift', 'aws'),
                        ('spectrum', 'aws'), ('snowflake_aws', 'aws'),
                        ('snowflakeexternal_aws', 'aws'), ('bigquery', 'gcp'),
                        ('endor', 'gcp'), ('endorazure', 'gcp'),
                        ('bqfederated', 'gcp'),
                        ('azuresqldatawarehouse', 'azure')])
TYPE_2_MODULE = dict([
    ('athena', 'perfkitbenchmarker.providers.aws.athena'),
    ('redshift', 'perfkitbenchmarker.providers.aws.redshift'),
    ('spectrum', 'perfkitbenchmarker.providers.aws.spectrum'),
    ('snowflake_aws', 'perfkitbenchmarker.providers.aws.snowflake'),
    ('snowflakeexternal_aws', 'perfkitbenchmarker.providers.aws.snowflake'),
    ('bigquery', 'perfkitbenchmarker.providers.gcp.bigquery'),
    ('endor', 'perfkitbenchmarker.providers.gcp.bigquery'),
    ('endorazure', 'perfkitbenchmarker.providers.gcp.bigquery'),
    ('bqfederated', 'perfkitbenchmarker.providers.gcp.bigquery'),
    ('azuresqldatawarehouse', 'perfkitbenchmarker.providers.azure.'
     'azure_sql_data_warehouse')
])
DEFAULT_NUMBER_OF_NODES = 1
# The order of stages is important to the successful lifecycle completion.
EDW_SERVICE_LIFECYCLE_STAGES = ['create', 'load', 'query', 'delete']
SAMPLE_QUERY_PATH = '/tmp/sample.sql'
SAMPLE_QUERY = 'select * from INFORMATION_SCHEMA.TABLES;'


class EdwExecutionError(Exception):
  """Encapsulates errors encountered during execution of a query."""


class EdwClientInterface(object):
  """Defines the interface for EDW service clients.

  Attributes:
    client_vm: An instance of virtual_machine.BaseVirtualMachine used to
      interface with the edw service.
    whitelist_ip: The IP to whitelist.
  """

  def __init__(self):
    self.client_vm = None

    # set by derived classes
    self.whitelist_ip = None

  def SetProvisionedAttributes(self, benchmark_spec):
    """Sets any attributes that were unknown during initialization."""
    self.client_vm = benchmark_spec.vms[0]
    self.client_vm.RemoteCommand('echo "\nMaxSessions 100" | '
                                 'sudo tee -a /etc/ssh/sshd_config')

  def Prepare(self, package_name: Text) -> None:
    """Prepares the client vm to execute query.

    The default implementation raises an Error, to ensure client specific
    installation and authentication of runner utilities.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    raise NotImplementedError

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
    raise NotImplementedError

  def ExecuteSimultaneous(self, submission_interval: int,
                          queries: List[str]) -> str:
    """Executes queries simultaneously on client and return performance details.

    Simultaneous app expects queries as white space separated query file names.
    Response format:
      {"simultaneous_end":1601145943197,"simultaneous_start":1601145940113,
      "stream_performance_array":[{"query_wall_time_in_secs":2.079,
      "query_end":1601145942208,"job_id":"914682d9-4f64-4323-bad2-554267cbbd8d",
      "query":"1","query_start":1601145940129},{"query_wall_time_in_secs":2.572,
      "query_end":1601145943192,"job_id":"efbf93a1-614c-4645-a268-e3801ae994f1",
      "query":"2","query_start":1601145940620}],
      "simultaneous_wall_time_in_secs":3.084}

    Args:
      submission_interval: Simultaneous query submission interval in milliseconds.
      queries: List of string names of the queries to execute simultaneously.

    Returns:
      performance_details: A serialized dictionary of execution details.
    """
    raise NotImplementedError

  def ExecuteThroughput(self, concurrency_streams: List[List[str]]) -> str:
    """Executes a throughput test and returns performance details.

    Response format:
      {"throughput_start":1601666911596,"throughput_end":1601666916139,
        "throughput_wall_time_in_secs":4.543,
        "all_streams_performance_array":[
          {"stream_start":1601666911597,"stream_end":1601666916139,
            "stream_wall_time_in_secs":4.542,
            "stream_performance_array":[
              {"query_wall_time_in_secs":2.238,"query_end":1601666913849,
                "query":"1","query_start":1601666911611,
                "details":{"job_id":"438170b0-b0cb-4185-b733-94dd05b46b05"}},
              {"query_wall_time_in_secs":2.285,"query_end":1601666916139,
                "query":"2","query_start":1601666913854,
                "details":{"job_id":"371902c7-5964-46f6-9f90-1dd00137d0c8"}}
              ]},
          {"stream_start":1601666911597,"stream_end":1601666916018,
            "stream_wall_time_in_secs":4.421,
            "stream_performance_array":[
              {"query_wall_time_in_secs":2.552,"query_end":1601666914163,
                "query":"2","query_start":1601666911611,
                "details":{"job_id":"5dcba418-d1a2-4a73-be70-acc20c1f03e6"}},
              {"query_wall_time_in_secs":1.855,"query_end":1601666916018,
                "query":"1","query_start":1601666914163,
                "details":{"job_id":"568c4526-ae26-4e9d-842c-03459c3a216d"}}
            ]}
        ]}

    Args:
      concurrency_streams: List of streams to execute simultaneously, each of
        which is a list of string names of queries.

    Returns:
      A serialized dictionary of execution details.
    """
    raise NotImplementedError

  def WarmUpQuery(self):
    """Executes a service-agnostic query that can detect cold start issues."""
    with open(SAMPLE_QUERY_PATH, 'w+') as f:
      f.write(SAMPLE_QUERY)
    self.client_vm.PushFile(SAMPLE_QUERY_PATH)
    query_name = os.path.basename(SAMPLE_QUERY_PATH)
    self.ExecuteQuery(query_name)

  def GetMetadata(self) -> Dict[str, str]:
    """Returns the client interface metadata."""
    raise NotImplementedError


class EdwService(resource.BaseResource):
  """Object representing a EDW Service."""

  def __init__(self, edw_service_spec):
    """Initialize the edw service object.

    Args:
      edw_service_spec: spec of the edw service.
    """
    # Hand over the actual creation to the resource module, which assumes the
    # resource is pkb managed by default
    is_user_managed = self.IsUserManaged(edw_service_spec)
    # edw_service attribute
    self.cluster_identifier = self.GetClusterIdentifier(edw_service_spec)
    super(EdwService, self).__init__(user_managed=is_user_managed)

    # Provision related attributes
    if edw_service_spec.snapshot:
      self.snapshot = edw_service_spec.snapshot
    else:
      self.snapshot = None

    # Cluster related attributes
    self.concurrency = edw_service_spec.concurrency
    self.node_type = edw_service_spec.node_type

    if edw_service_spec.node_count:
      self.node_count = edw_service_spec.node_count
    else:
      self.node_count = DEFAULT_NUMBER_OF_NODES

    # Interaction related attributes
    if edw_service_spec.endpoint:
      self.endpoint = edw_service_spec.endpoint
    else:
      self.endpoint = ''
    self.db = edw_service_spec.db
    self.user = edw_service_spec.user
    self.password = edw_service_spec.password
    # resource config attribute
    self.spec = edw_service_spec
    # resource workflow management
    self.supports_wait_on_delete = True
    self.client_interface = None

  def GetClientInterface(self) -> EdwClientInterface:
    """Gets the active Client Interface."""
    return self.client_interface

  def IsUserManaged(self, edw_service_spec):
    """Indicates if the edw service instance is user managed.

    Args:
      edw_service_spec: spec of the edw service.

    Returns:
      A boolean, set to True if the edw service instance is user managed, False
       otherwise.
    """
    return edw_service_spec.cluster_identifier is not None

  def GetClusterIdentifier(self, edw_service_spec):
    """Returns a string name of the Cluster Identifier.

    Args:
      edw_service_spec: spec of the edw service.

    Returns:
      A string, set to the name of the cluster identifier.
    """
    if self.IsUserManaged(edw_service_spec):
      return edw_service_spec.cluster_identifier
    else:
      return 'pkb-' + FLAGS.run_uri

  def GetMetadata(self):
    """Return a dictionary of the metadata for this edw service."""
    basic_data = {'edw_service_type': self.spec.type,
                  'edw_cluster_identifier': self.cluster_identifier,
                  'edw_cluster_node_type': self.node_type,
                  'edw_cluster_node_count': self.node_count}
    return basic_data

  def GenerateLifecycleStageScriptName(self, lifecycle_stage):
    """Computes the default name for script implementing an edw lifecycle stage.

    Args:
      lifecycle_stage: Stage for which the corresponding sql script is desired.

    Returns:
      script name for implementing the argument lifecycle_stage.
    """
    return os.path.basename(
        os.path.normpath('database_%s.sql' % lifecycle_stage))

  def Cleanup(self):
    """Cleans up any temporary resources created for the service."""
    pass

  def GetDatasetLastUpdatedTime(self, dataset=None):
    """Get the formatted last modified timestamp of the dataset."""
    raise NotImplementedError

  def ExtractDataset(self,
                     dest_bucket,
                     dataset=None,
                     tables=None,
                     dest_format='CSV'):
    """Extract all tables in a dataset to object storage.

    Args:
      dest_bucket: Name of the bucket to extract the data to. Should already
        exist.
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
      tables: Optional list of table names to extract. If none, all tables in
        the dataset will be extracted.
      dest_format: Format to extract data in.
    """
    raise NotImplementedError

  def RemoveDataset(self, dataset=None):
    """Removes a dataset.

    Args:
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
    """
    raise NotImplementedError

  def CreateDataset(self, dataset=None, description=None):
    """Creates a new dataset.

    Args:
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
      description: Optional description of the dataset.
    """
    raise NotImplementedError

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

  def RequiresWarmUpSuite(self) -> bool:
    """Verifies if the edw_service requires a warm up suite execution.

    Currently enabled for all service types, for parity.

    Returns:
      A boolean value (True) if the warm suite is recommended.
    """
    return True
