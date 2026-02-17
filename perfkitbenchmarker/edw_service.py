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

import datetime
import os
from typing import Any, Dict, List

from absl import flags
from absl import logging
from perfkitbenchmarker import resource
from perfkitbenchmarker.resources.container_service import kubernetes_cluster

flags.DEFINE_integer(
    'edw_service_cluster_concurrency',
    5,
    'Number of queries to run concurrently on the cluster.',
)
flags.DEFINE_string(
    'edw_service_cluster_snapshot',
    None,
    'If set, the snapshot to restore as cluster.',
)
flags.DEFINE_string(
    'edw_service_cluster_identifier',
    None,
    'If set, the preprovisioned edw cluster.',
)
flags.DEFINE_string(
    'edw_service_endpoint',
    None,
    'If set, the preprovisioned edw cluster endpoint.',
)
flags.DEFINE_string(
    'edw_service_cluster_db',
    None,
    'If set, the db on cluster to use during the benchmark ('
    'only applicable when using snapshots).',
)
flags.DEFINE_string(
    'edw_service_cluster_user',
    None,
    'If set, the user authorized on cluster (only applicable '
    'when using snapshots).',
)
flags.DEFINE_string(
    'edw_service_cluster_password',
    None,
    'If set, the password authorized on cluster (only '
    'applicable when using snapshots).',
)
flags.DEFINE_string(
    'snowflake_snowsql_config_override_file',
    None,
    'The SnowSQL configuration to use.'
    'https://docs.snowflake.net/manuals/user-guide/snowsql-config.html#snowsql-config-file',
)  # pylint: disable=line-too-long
flags.DEFINE_string(
    'snowflake_connection',
    None,
    'Named Snowflake connection defined in SnowSQL config file.'
    'https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-named-connections',
)  # pylint: disable=line-too-long
TRINO_CATALOG = flags.DEFINE_string(
    'trino_catalog', 'dpms', 'Catalog for Trino.'
)
TRINO_SCHEMA = flags.DEFINE_string(
    'trino_schema', 'imt_tpcds_10t', 'Trino schema to use.'
)
TRINO_MEMORY = flags.DEFINE_integer(
    'trino_worker_memory',
    200,
    'Amount of memory in GiB used by each Trino worker.',
)
flags.DEFINE_integer(
    'edw_suite_iterations', 1, 'Number of suite iterations to perform.'
)
flags.DEFINE_integer(
    'edw_suite_warmup_iterations',
    None,
    'Number of suite warmup iterations to perform.',
)
# TODO(user): Revisit flags for accepting query lists.
flags.DEFINE_string(
    'edw_simultaneous_queries',
    None,
    'CSV list of simultaneous queries to benchmark.',
)
flags.DEFINE_integer(
    'edw_simultaneous_query_submission_interval',
    '0',
    'Simultaneous query submission interval in milliseconds.',
)
flags.DEFINE_string(
    'edw_power_queries', None, 'CSV list of power queries to benchmark.'
)
flags.DEFINE_multi_string(
    'concurrency_streams',
    [],
    'List of all query streams to execute. Each '
    'stream should be passed in separately and the queries should be comma '
    'separated, e.g. --concurrency_streams=1,2,3 --concurrency_streams=3,2,1',
)
flags.DEFINE_boolean(
    'edw_warmup_tolerate_failure',
    False,
    'If set, the benchmark will tolerate query failures during the warmup '
    'phase. Mostly useful for testing.',
)
flags.DEFINE_string(
    'snowflake_warehouse',
    None,
    'A virtual warehouse, often referred to simply as a - '
    'warehouse, is a cluster of compute in Snowflake. '
    'https://docs.snowflake.com/en/user-guide/warehouses.html',
)  # pylint: disable=line-too-long
flags.DEFINE_string(
    'snowflake_database',
    None,
    'The hosted snowflake database to use during the benchmark.',
)
flags.DEFINE_string(
    'snowflake_schema',
    None,
    'The schema of the hosted snowflake database to use during the benchmark.',
)
flags.DEFINE_string(
    'snowflake_account',
    None,
    'The Snowflake account to use during the benchmark. Must follow the format'
    ' "<org_id>-<account_name>".',
)
flags.DEFINE_string(
    'snowflake_user',
    None,
    'The Snowflake user to use during the benchmark.',
)
flags.DEFINE_enum(
    'snowflake_client_interface',
    'JDBC',
    ['JDBC', 'PYTHON'],
    'The Runtime Interface used when interacting with Snowflake.',
)
flags.DEFINE_string(
    'snowflake_jdbc_client_jar',
    None,
    'Local location of the snowflake_jdbc_client_jar.',
)
flags.DEFINE_string(
    'snowflake_jdbc_key_file',
    None,
    'Local location of the Private key file for Snowflake authentication. '
    'Must be named snowflake_keyfile.p8',
)
flags.DEFINE_boolean(
    'edw_get_service_auxiliary_metrics',
    'False',
    'If set, the benchmark will collect service-specific metrics from the'
    ' remote service after the benchmark has completed. Additional delay may be'
    ' incurred due to the need to wait for metadata propogation.',
)
flags.DEFINE_enum(
    'edw_bq_feature_config',
    'default',
    ['default', 'job_optional'],
    'Selects from among various BigQuery feature configurations. '
    'Currently supported: default (no special features), job_optional '
    '(enables job_creation_optional query preview feature). '
    'Only supported for Python client.',
)

# MARK: index flags
# Flags for EDW search index benchmarks.
EDW_SEARCH_TABLE_NAME = flags.DEFINE_string(
    'edw_search_table_name',
    None,
    'Table name to use for edw search index benchmarks.',
)
EDW_SEARCH_INIT_DATA_LOCATION = flags.DEFINE_string(
    'edw_search_init_data_location',
    None,
    'Cloud directory of bucket to source initialization data '
    'for EDW search benchmarks.',
)
EDW_SEARCH_DATA_LOCATION = flags.DEFINE_string(
    'edw_search_data_location',
    None,
    'Cloud directory of bucket to source ongoing load data '
    'for EDW search benchmarks (without rare token).',
)
EDW_SEARCH_INDEX_NAME = flags.DEFINE_string(
    'edw_search_index_name',
    None,
    'Name of index for edw search index benchmarks',
)


FLAGS = flags.FLAGS

EDW_PYTHON_DRIVER_LIB_FILE = 'edw_python_driver_lib.py'
EDW_PYTHON_DRIVER_LIB_DIR = 'edw/common/clients/python'

TYPE_2_PROVIDER = dict([
    ('athena', 'aws'),
    ('redshift', 'aws'),
    ('spectrum', 'aws'),
    ('trino', 'gcp'),
    ('snowflake_aws', 'aws'),
    ('snowflake_azure', 'azure'),
    ('snowflakeexternal_aws', 'aws'),
    ('snowflakeicebergexternal_aws', 'aws'),
    ('snowflakeicebergmanaged_aws', 'aws'),
    ('snowflakeexternal_azure', 'azure'),
    ('bigquery', 'gcp'),
    ('endor', 'gcp'),
    ('endorazure', 'gcp'),
    ('bqfederated', 'gcp'),
    ('azuresqldatawarehouse', 'azure'),
])
TYPE_2_MODULE = dict([
    ('athena', 'perfkitbenchmarker.providers.aws.athena'),
    ('redshift', 'perfkitbenchmarker.providers.aws.redshift'),
    ('spectrum', 'perfkitbenchmarker.providers.aws.spectrum'),
    ('trino', 'perfkitbenchmarker.providers.gcp.trino'),
    ('snowflake_aws', 'perfkitbenchmarker.providers.aws.snowflake_aws'),
    (
        'snowflake_azure',
        'perfkitbenchmarker.providers.azure.snowflake_azure',
    ),
    (
        'snowflakeexternal_aws',
        'perfkitbenchmarker.providers.aws.snowflake_aws',
    ),
    (
        'snowflakeicebergexternal_aws',
        'perfkitbenchmarker.providers.aws.snowflake_aws',
    ),
    (
        'snowflakeicebergmanaged_aws',
        'perfkitbenchmarker.providers.aws.snowflake_aws',
    ),
    (
        'snowflakeexternal_azure',
        'perfkitbenchmarker.providers.azure.snowflake_azure',
    ),
    ('bigquery', 'perfkitbenchmarker.providers.gcp.bigquery'),
    ('endor', 'perfkitbenchmarker.providers.gcp.bigquery'),
    ('endorazure', 'perfkitbenchmarker.providers.gcp.bigquery'),
    ('bqfederated', 'perfkitbenchmarker.providers.gcp.bigquery'),
    (
        'azuresqldatawarehouse',
        'perfkitbenchmarker.providers.azure.azure_sql_data_warehouse',
    ),
])
DEFAULT_NUMBER_OF_NODES = 1
# The order of stages is important to the successful lifecycle completion.
EDW_SERVICE_LIFECYCLE_STAGES = ['create', 'load', 'query', 'delete']
SAMPLE_QUERY_PATH = '/tmp/sample.sql'
SAMPLE_QUERY = 'select * from INFORMATION_SCHEMA.TABLES;'


class EdwExecutionError(Exception):
  """Encapsulates errors encountered during execution of a query."""


class EdwClientInterface:
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
    self.client_vm.RemoteCommand(
        'echo "\nMaxSessions 100" | sudo tee -a /etc/ssh/sshd_config'
    )

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    The default implementation raises an Error, to ensure client specific
    installation and authentication of runner utilities.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    raise NotImplementedError

  def ExecuteQuery(
      self, query_name: str, print_results: bool = False
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
    raise NotImplementedError

  def ExecuteSimultaneous(
      self, submission_interval: int, queries: List[str]
  ) -> str:
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
      submission_interval: Simultaneous query submission interval in
        milliseconds.
      queries: List of string names of the queries to execute simultaneously.

    Returns:
      performance_details: A serialized dictionary of execution details.
    """
    raise NotImplementedError

  def ExecuteThroughput(
      self,
      concurrency_streams: List[List[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
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
      labels: A dictionary of labels to apply to the query for service-side
        tracking and analysis. Must include the mandatory label
        'minimal_run_key', which should be unique per individual iteration.

    Returns:
      A serialized dictionary of execution details.
    """
    raise NotImplementedError

  def WarmUpQuery(self):
    """Executes a service-agnostic query that can detect cold start issues."""
    if self.client_vm is None:
      raise ValueError('SetProvisionedAttributes() not called')
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

  SERVICE_TYPE = 'abstract'
  QUERY_SET = 'abstract'

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
    super().__init__(user_managed=is_user_managed)

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
    self.client_interface: EdwClientInterface
    self.container_cluster: kubernetes_cluster.KubernetesCluster | None = None

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
    basic_data = {
        'edw_service_type': self.spec.type,
        'edw_cluster_identifier': self.cluster_identifier,
        'edw_cluster_node_type': self.node_type,
        'edw_cluster_node_count': self.node_count,
        'edw_query_set': self.QUERY_SET,
    }
    return basic_data

  def GenerateLifecycleStageScriptName(self, lifecycle_stage):
    """Computes the default name for script implementing an edw lifecycle stage.

    Args:
      lifecycle_stage: Stage for which the corresponding sql script is desired.

    Returns:
      script name for implementing the argument lifecycle_stage.
    """
    return os.path.basename(
        os.path.normpath('database_%s.sql' % lifecycle_stage)
    )

  def Cleanup(self):
    """Cleans up any temporary resources created for the service."""
    pass

  def GetDatasetLastUpdatedTime(self, dataset=None):
    """Get the formatted last modified timestamp of the dataset."""
    raise NotImplementedError

  def SetDestinationTable(self, dataset: str):
    pass

  def SetContainerCluster(
      self, container_cluster: kubernetes_cluster.KubernetesCluster
  ):
    """Sets the container cluster if one is applicable."""
    self.container_cluster = container_cluster

  def ExtractDataset(
      self, dest_bucket, dataset=None, tables=None, dest_format='CSV'
  ):
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

  def OpenDataset(self, dataset: str):
    """Switch from the currently active dataset to the one specified.

    Switches the dataset that will be accessed by queries sent through the
    client interface that this EDW service provides.

    Args:
      dataset: Name of the dataset to make the active dataset.
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
    raise NotImplementedError

  def RequiresWarmUpSuite(self) -> bool:
    """Verifies if the edw_service requires a warm up suite execution.

    Currently enabled for all service types, for parity.

    Returns:
      A boolean value (True) if the warm suite is recommended.
    """
    return True

  def GetIterationAuxiliaryMetrics(self, iter_run_key: str) -> Dict[str, Any]:
    """Returns service-specific metrics derived from server-side metadata.

      Must be run after the benchmark has completed.

    Args:
      iter_run_key: The unique identifier of the run and iteration to fetch
        metrics for.

    Returns:
      A dictionary of the following format:
        { 'metric_1': { 'value': 1, 'unit': 'imperial femtoseconds'},
          'metric_2': { 'value': 2, 'unit': 'metric dollars'}
        ...}
    """
    logging.info(
        'Per-iteration auxiliary metrics are not supported for this service.'
    )
    del iter_run_key
    return {}

  def GetTimeBoundAuxiliaryMetrics(
      self, start_timestamp: float, end_timestamp: float
  ) -> List[Dict[str, Any]]:
    """Returns service-specific metrics from a set time range.

    Whenever possible, the service should return metrics only from the compute
    cluster used for the current benchmark run.

    Args:
      start_timestamp: Start of the time range to retrieve metrics for.
      end_timestamp: End of the time range to retrieve metrics for.

    Returns:
      A list of the following format:
        [{'metric_1': 'value': 1, 'unit': 'imperial nanoseconds', metadata: {}},
         {'metric_2': 'value': 2, 'unit': 'metric dollars', metadata: {}}
        ...]
    """
    logging.info(
        'Time-bound auxiliary metrics are not supported for this service.'
    )
    del start_timestamp, end_timestamp
    return []

  def CreateSearchIndex(
      self, table_path: str, index_name: str
  ) -> tuple[float, dict[str, Any]]:
    """Create a search index on a table.

    Create a text search index across all supported columns in a table.
    Search indexes generally build asynchronously. Expect this to return
    immediately, and then use GetSearchIndexStatus to monitor indexing
    progression.

    The `index_name` parameter should be provided, but some services may
    discard it if they do not support named indexes. In this case, an unnamed
    index will be created on the given table.

    Metadata returned by this method is arbitrary, and is for the purpose of
    inclusion in benchmark result metadata. The presence or absence of specific
    values should not be relied on. Example returned metadata:
    {'query_results_loaded':True,'results_load_time':0.873234,'rows_returned':1}

    Args:
      table_path: The name of the table to create the index on.
      index_name: The name for the new search index.

    Returns:
      A tuple of execution time in seconds and a dictionary of metadata.
    """
    raise NotImplementedError

  def DropSearchIndex(
      self, table_path: str, index_name: str
  ) -> tuple[float, dict[str, Any]]:
    """Deletes a search index from a table.

    The `index_name` parameter should be provided, but some services may
    discard it if they do not support named indexes. In this case, all indexes
    will be deleted from the given table.

    Metadata returned by this method is arbitrary, and is for the purpose of
    inclusion in benchmark result metadata. The presence or absence of specific
    values should not be relied on. Example returned metadata:
    {'query_results_loaded':True,'results_load_time':0.072512,'rows_returned':1}

    Args:
      table_path: The name of the table to delete the index from.
      index_name: The name of the search index to delete.

    Returns:
      A tuple of execution time in seconds and a dictionary of metadata.
    """
    raise NotImplementedError

  def GetSearchIndexCompletionPercentage(
      self, table_path: str, index_name: str
  ) -> tuple[int, dict[str, Any]]:
    """Gets the status of a search index.

    Returns the completion status of a search index on a table, expressed as an
    integer percentage.

    Metadata returned by this method is arbitrary, and is for the purpose of
    inclusion in benchmark result metadata. The presence or absence of specific
    values should not be relied on.

    Args:
      table_path: The name of the table to get index status from.
      index_name: The name of the search index.

    Returns:
      A tuple containing the index coverage percentage (int) and a dictionary
      of metadata.
    """
    raise NotImplementedError

  def InitializeSearchStarterTable(
      self, table_path: str, storage_path: str
  ) -> tuple[float, dict[str, Any]]:
    """Initializes a table for search index benchmarks.

    Creates a table using the standard search index benchmark init template
    for a given service.

    Metadata returned by this method is arbitrary, and is for the purpose of
    inclusion in benchmark result metadata. The presence or absence of specific
    values should not be relied on.

    Args:
      table_path: The full path or name of the table to initialize.
      storage_path: The path to the data source for initialization.

    Returns:
      A tuple of execution time in seconds and a dictionary of metadata.
    """
    raise NotImplementedError

  def InsertSearchData(
      self, table_path: str, storage_path: str
  ) -> tuple[float, dict[str, Any]]:
    """Inserts data into a table for search index benchmarks.

    Metadata returned by this method is arbitrary, and is for the purpose of
    inclusion in benchmark result metadata. The presence or absence of specific
    values should not be relied on.

    Args:
      table_path: The full path or name of the table to insert data into.
      storage_path: The path to the data source to load.

    Returns:
      A tuple of execution time in seconds and a dictionary of metadata.
    """
    raise NotImplementedError

  def GetTableRowCount(self, table_path: str) -> tuple[int, dict[str, Any]]:
    """Gets the total number of rows in a table.

    Metadata returned by this method is arbitrary, and is for the purpose of
    inclusion in benchmark result metadata. The presence or absence of specific
    values should not be relied on.

    Args:
      table_path: The full path or name of the table.

    Returns:
      A tuple containing the total row count (int) and a dictionary of metadata.
    """
    raise NotImplementedError

  def TextSearchQuery(
      self,
      table_path: str,
      search_keyword: str,
      order_by: str | None = None,
      limit: int | None = None,
      date_between: tuple[datetime.date, datetime.date] | None = None,
  ) -> tuple[float, dict[str, Any]]:
    """Executes a text search query against a table.

    Typically used on tables with a search index.

    The metadata returned by implementations of this method should include the
    following k-v pairs:

    {edw_search_index_coverage_percentage: int|None,
     edw_search_result_rows: int|None,
     edw_search_total_row_count: int|None}

    Other arbitrary values may also be returned by the service, and callers
    should not rely on the presence or absence of specific values beyond
    those specified above.

    Args:
      table_path: The full path or name of the table to query.
      search_keyword: The text to search for within the indexed columns.
      order_by: The column to order the results by.
      limit: The maximum number of rows to return.
      date_between: A tuple of two dates to filter the results by.

    Returns:
      A tuple of execution time in seconds and a dictionary of metadata.
    """
    raise NotImplementedError

  def InjectTokenIntoTable(
      self, table_path: str, token: str, token_count: int
  ) -> tuple[float, dict[str, Any]]:
    """Updates N rows to append a token to a well-known string column.

    Metadata returned by this method is arbitrary, and is for the purpose of
    inclusion in benchmark result metadata. The presence or absence of specific
    values should not be relied on.

    Args:
      table_path: The full path or name of the table to insert data into.
      token: The token to append to the column.
      token_count: The number of rows to update.

    Returns:
      A tuple of execution time in seconds and a dictionary of metadata.
    """
    raise NotImplementedError

  @staticmethod
  def ColsToRows(col_res: dict[str, list[Any]]) -> list[dict[str, Any]]:
    """Converts a dictionary of columns to a list of rows.

    Args:
      col_res: A dictionary of columns to convert to a list of rows.

    Returns:
      A list of dictionaries, where each dictionary represents a row.

      e.g. {'col1': [1, 2, 3], 'col2': [4, 5, 6]} -> [{'col1': 1, 'col2': 4},
        {'col1': 2, 'col2': 5}, {'col1': 3, 'col2': 6}].
    """
    return [dict(zip(col_res.keys(), row)) for row in zip(*col_res.values())]
