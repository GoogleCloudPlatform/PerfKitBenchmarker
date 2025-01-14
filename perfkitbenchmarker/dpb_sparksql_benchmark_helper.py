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
"""Re-usable helpers for dpb_sparksql_benchmark."""

import json
import os
import re
import typing
from typing import Any

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker import vm_util

BENCHMARK_NAMES = {'tpcds_2_4': 'TPC-DS', 'tpch': 'TPC-H'}


flags.DEFINE_string(
    'dpb_sparksql_data',
    None,
    'The HCFS based dataset to run Spark SQL query against',
)
flags.DEFINE_bool(
    'dpb_sparksql_create_hive_tables',
    False,
    'Whether to load dpb_sparksql_data into external hive tables or not.',
)
flags.DEFINE_bool(
    'dpb_sparksql_simultaneous',
    False,
    'Run all queries simultaneously instead of one by one. '
    'Depending on the service type and cluster shape, it might '
    'fail if too many queries are to be run. This flag and '
    '--dpb_sparksql_streams are mutually exclusive.',
)
flags.DEFINE_string(
    'dpb_sparksql_database',
    None,
    'Name of preprovisioned Hive database to look for data in '
    '(https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html).',
)
flags.DEFINE_string(
    'dpb_sparksql_data_format',
    None,
    "Format of data to load. Assumed to be 'parquet' for HCFS "
    "and 'bigquery' for bigquery if unspecified.",
)
flags.DEFINE_string(
    'dpb_sparksql_data_compression',
    None,
    'Compression format of the data to load. Since for most formats available '
    'in Spark this may only be specified when writing data, for most cases '
    'this option is a no-op and only serves the purpose of tagging the results '
    'reported by PKB with the appropriate compression format. One notable '
    'exception though is when the dpb_sparksql_copy_to_hdfs flag is passed. In '
    'that case the compression data passed will be used to write data into '
    'HDFS.',
)
flags.DEFINE_string(
    'dpb_sparksql_csv_delimiter', ',', 'CSV delimiter to load the CSV file.'
)
flags.DEFINE_enum(
    'dpb_sparksql_query',
    'tpcds_2_4',
    BENCHMARK_NAMES.keys(),
    'A list of query to run on dpb_sparksql_data',
)
flags.DEFINE_list(
    'dpb_sparksql_order',
    [],
    'The names (numbers) of the queries to run in order in a POWER run or all '
    'at the same time in a SIMULTANEOUS run. For DPB SparkSQL benchmarks '
    'either this flag or --dpb_sparksql_streams must be set.',
)
_STREAMS = flags.DEFINE_multi_string(
    'dpb_sparksql_streams',
    [],
    'List of all query streams to execute for a TPC-DS/H THROUGHPUT run. Each '
    'stream should be passed in separately and the queries should be comma '
    'separated, e.g. --dpb_sparksql_streams=1,2,3 '
    '--dpb_sparksql_streams=3,2,1. For DPB SparkSQL benchmarks either this '
    'flag or --dpb_sparksql_order must be set.',
)
flags.DEFINE_bool(
    'dpb_sparksql_copy_to_hdfs',
    False,
    'Instead of reading the data directly, copy into HDFS and read from there.',
)
flags.DEFINE_enum(
    'dpb_sparksql_table_cache',
    None,
    ['eager', 'lazy'],
    'Optionally tell Spark to cache all tables to memory and spilling to disk. '
    'Eager cache will prefetch all tables in lexicographic order. '
    'Lazy will cache tables as they are read. This might have some '
    'counter-intuitive results and Spark reads more data than necessary to '
    "populate it's cache.",
)
_QUERIES_URL = flags.DEFINE_string(
    'dpb_sparksql_queries_url',
    None,
    'Object Storage (e.g. GCS or S3) directory URL where the benchmark query '
    'files are contained. Their name must be their query number alone (e.g: '
    '"1" or "14a") without any prefix or extension. If omitted, queries will '
    'be fetched from databricks/spark-sql-perf Github repo and use the '
    '--dpb_sparksql_query flag to decide whether to get TPC-DS or TPC-H '
    'queries.',
)
DUMP_SPARK_CONF = flags.DEFINE_bool(
    'dpb_sparksql_dump_spark_conf',
    False,
    "Dump job's spark configuration properties to the DPB service's bucket. "
    'For debugging purposes.',
)

FLAGS = flags.FLAGS

SCRIPT_DIR = 'spark_sql_test_scripts'
SPARK_SQL_DISTCP_SCRIPT = os.path.join(SCRIPT_DIR, 'spark_sql_distcp.py')
# Creates spark table using pyspark by loading the parquet data.
# Args:
# argv[1]: string, The table name in the dataset that this script will create.
# argv[2]: string, The data path of the table.
SPARK_TABLE_SCRIPT = os.path.join(SCRIPT_DIR, 'spark_table.py')
SPARK_SQL_RUNNER_SCRIPT = os.path.join(SCRIPT_DIR, 'spark_sql_runner.py')
SPARK_SQL_PERF_GIT = 'https://github.com/databricks/spark-sql-perf.git'
SPARK_SQL_PERF_GIT_COMMIT = '6b2bf9f9ad6f6c2f620062fda78cded203f619c8'


def GetStreams() -> list[list[str]]:
  """Gets a list of query number streams to run. Respects order in flags."""
  if _STREAMS.value:
    return [stream.split(',') for stream in _STREAMS.value]
  return [FLAGS.dpb_sparksql_order]


def GetQueryIdsToStage() -> list[str]:
  """Gets all the query IDs to stage according to the flags passed."""
  if _STREAMS.value:
    queries = set()
    for stream in GetStreams():
      queries.update(stream)
    return sorted(queries)
  return FLAGS.dpb_sparksql_order


def StageMetadata(
    json_metadata: Any,
    storage_service: object_storage_service.ObjectStorageService,
    staged_file: str,
):
  """Write JSON metadata to object storage."""
  # Write computed metadata to object storage.
  temp_run_dir = temp_dir.GetRunDirPath()
  local_file = os.path.join(temp_run_dir, os.path.basename(staged_file))
  with open(local_file, 'w') as f:
    json.dump(json_metadata, f)
  storage_service.Copy(local_file, staged_file)


def GetQueryId(filename: str) -> str | None:
  """Extract query id from file name."""
  match = re.match(r'(.*/)?q?([0-9]+[ab]?)(\.sql)?$', filename)
  if match:
    return match.group(2)


def Prepare(benchmark_spec):
  """Copies scripts and all the queries to cloud."""
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service
  benchmark_spec.query_dir = LoadAndStageQueries(
      storage_service, cluster.base_dir
  )
  benchmark_spec.query_streams = GetStreams()

  scripts_to_upload = [
      SPARK_SQL_DISTCP_SCRIPT,
      SPARK_TABLE_SCRIPT,
      SPARK_SQL_RUNNER_SCRIPT,
  ] + cluster.GetServiceWrapperScriptsToUpload()
  for script in scripts_to_upload:
    src_url = data.ResourcePath(script)
    storage_service.CopyToBucket(src_url, cluster.bucket, script)

  benchmark_spec.table_subdirs = []
  benchmark_spec.data_dir = None
  if FLAGS.dpb_sparksql_data:
    # Replace s3a scheme (used for S3 Express in Spark) with s3
    table_dir = re.sub(r'^s3a://', 's3://', FLAGS.dpb_sparksql_data)
    table_dir = table_dir.rstrip('/') + '/'
    stdout = storage_service.List(table_dir)
    for line in stdout.split('\n'):
      # GCS will sometimes list the directory itself.
      if line and line != table_dir:
        benchmark_spec.table_subdirs.append(
            re.split(' |/', line.rstrip('/')).pop()
        )

    benchmark_spec.data_dir = FLAGS.dpb_sparksql_data


def LoadAndStageQueries(
    storage_service: object_storage_service.ObjectStorageService, base_dir: str
) -> str:
  """Loads queries stages them in object storage if needed.

  Queries are selected using --dpb_sparksql_query and --dpb_sparksql_order.

  Args:
    storage_service: object_storage_service to stage queries into.
    base_dir: object storage directory to stage queries into.

  Returns:
    The object storage path where the queries are staged into.

  Raises:
    PrepareException if a requested query is not found.
  """

  if _QUERIES_URL.value:
    _GetQueryFilesFromUrl(storage_service, _QUERIES_URL.value)
    # casting it, so it doesn't complain about being str | None
    return typing.cast(str, _QUERIES_URL.value)
  _StageQueriesFromRepo(storage_service, base_dir)
  return base_dir


def _GetQueryFilesFromUrl(
    storage_service: object_storage_service.ObjectStorageService,
    queries_url: str,
) -> None:
  """Checks if relevant query files from queries_url exist.

  Args:
    storage_service: object_storage_service to list query files.
    queries_url: Object Storage directory URL where the benchmark queries are
      contained.
  """
  query_paths = {q: os.path.join(queries_url, q) for q in GetQueryIdsToStage()}
  queries_missing = set()
  for q in query_paths:
    try:
      storage_service.List(query_paths[q])
    except errors.VmUtil.IssueCommandError:  # Handling query not found
      queries_missing.add(q)
  if queries_missing:
    raise errors.Benchmarks.PrepareException(
        'Could not find queries {}'.format(', '.join(sorted(queries_missing)))
    )


def _StageQueriesFromRepo(
    storage_service: object_storage_service.ObjectStorageService, base_dir: str
) -> None:
  """Copies queries from default Github repo to object storage.

  Args:
    storage_service: object_storage_service to stage queries into.
    base_dir: object storage directory to stage queries into.
  """
  temp_run_dir = temp_dir.GetRunDirPath()
  spark_sql_perf_dir = os.path.join(temp_run_dir, 'spark_sql_perf_dir')

  # Clone repo
  vm_util.IssueCommand(['git', 'clone', SPARK_SQL_PERF_GIT, spark_sql_perf_dir])
  vm_util.IssueCommand(
      ['git', 'checkout', SPARK_SQL_PERF_GIT_COMMIT], cwd=spark_sql_perf_dir
  )
  query_dir = os.path.join(
      spark_sql_perf_dir, 'src', 'main', 'resources', FLAGS.dpb_sparksql_query
  )

  # Search repo for queries
  query_file = {}  # map query -> staged file
  queries_to_stage = GetQueryIdsToStage()
  for dir_name, _, files in os.walk(query_dir):
    for filename in files:
      query_id = GetQueryId(filename)
      if query_id:
        # only upload specified queries
        if query_id in queries_to_stage:
          src_file = os.path.join(dir_name, filename)
          staged_filename = query_id
          staged_file = '{}/{}'.format(base_dir, staged_filename)
          storage_service.Copy(src_file, staged_file)
          query_file[query_id] = staged_file

  # Validate all requested queries are present.
  missing_queries = set(queries_to_stage) - set(query_file.keys())
  if missing_queries:
    raise errors.Benchmarks.PrepareException(
        'Could not find queries {}'.format(missing_queries)
    )
