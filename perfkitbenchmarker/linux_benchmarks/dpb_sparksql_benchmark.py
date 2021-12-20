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
"""Executes a series of queries using Apache Spark SQL and records latencies.

Queries:
This benchmark uses TPC-DS and TPC-H queries from
https://github.com/databricks/spark-sql-perf, because spark SQL doesn't support
all the queries that using dialect netezza.

Data:
The Data (TPCDS or TPCH) needs be generated first by user and loaded into object
storage.
TPCDS and TPCH tools.
TPCDS: https://github.com/databricks/tpcds-kit
TPCH: https://github.com/databricks/tpch-dbgen

Spark SQL can either read from Hive tables where the data is stored in a series
of files on a Hadoop Compatible File System (HCFS) or it can read from temporary
views registered from Spark SQL data sources:
https://spark.apache.org/docs/latest/sql-data-sources.html.

Spark SQL queries are run using custom pyspark runner.

This benchmark can create and replace external Hive tables using
`--dpb_sparksql_create_hive_tables=true` Alternatively you could pre-provision
a Hive metastore with data before running the benchmark.

If you do not want to use a Hive Metastore, the custom pyspark runner can
register data as temporary views during job submission. This supports the
entire Spark datasource API and is the default.

One data soruce of note is Google BigQuery using
https://github.com/GoogleCloudPlatform/spark-bigquery-connector.
"""

import json
import logging
import os
import re
import time
from typing import List, Optional

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'dpb_sparksql_benchmark'

BENCHMARK_CONFIG = """
dpb_sparksql_benchmark:
  description: Run Spark SQL on dataproc and emr
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
        AWS:
          machine_type: m5.xlarge
      disk_spec:
        GCP:
          disk_size: 1000
          disk_type: pd-standard
          # Only used by unmanaged
          mount_point: /scratch
        AWS:
          disk_size: 1000
          disk_type: gp2
          # Only used by unmanaged
          mount_point: /scratch
        Azure:
          disk_size: 1000
          disk_type: Standard_LRS
          # Only used by unmanaged
          mount_point: /scratch
    worker_count: 2
"""

BENCHMARK_NAMES = {
    'tpcds_2_4': 'TPC-DS',
    'tpch': 'TPC-H'
}


flags.DEFINE_string(
    'dpb_sparksql_data', None,
    'The HCFS based dataset to run Spark SQL query '
    'against')
flags.DEFINE_bool('dpb_sparksql_create_hive_tables', False,
                  'Whether to load dpb_sparksql_data into external hive tables '
                  'or not.')
flags.DEFINE_bool('dpb_sparksql_simultaneous', False,
                  'Run all queries simultaneously instead of one by one. '
                  'Depending on the service type and cluster shape, it might '
                  'fail if too many queries are to be run.')
flags.DEFINE_string(
    'dpb_sparksql_database', None,
    'Name of preprovisioned Hive database to look for data in '
    '(https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html).')
flags.DEFINE_string(
    'dpb_sparksql_data_format', None,
    "Format of data to load. Assumed to be 'parquet' for HCFS "
    "and 'bigquery' for bigquery if unspecified.")
flags.DEFINE_enum('dpb_sparksql_query', 'tpcds_2_4', BENCHMARK_NAMES.keys(),
                  'A list of query to run on dpb_sparksql_data')
flags.DEFINE_list(
    'dpb_sparksql_order', [],
    'The names (numbers) of the queries to run in order. '
    'Required.')
flags.DEFINE_bool(
    'dpb_sparksql_copy_to_hdfs', False,
    'Instead of reading the data directly, copy into HDFS and read from there.')
flags.DEFINE_enum(
    'dpb_sparksql_table_cache', None, ['eager', 'lazy'],
    'Optionally tell Spark to cache all tables to memory and spilling to disk. '
    'Eager cache will prefetch all tables in lexicographic order. '
    'Lazy will cache tables as they are read. This might have some '
    'counter-intuitive results and Spark reads more data than necessary to '
    "populate it's cache.")
flags.DEFINE_string(
    'spark_bigquery_connector',
    None,
    'The Spark BigQuery Connector jar to pass to the Spark Job')
flags.DEFINE_list(
    'bigquery_tables', [],
    'A list of BigQuery tables to load as Temporary Spark SQL views instead '
    'of reading from external Hive tables.'
)
flags.DEFINE_string(
    'bigquery_record_format', None,
    'The record format to use when connecting to BigQuery storage. See: '
    'https://github.com/GoogleCloudDataproc/spark-bigquery-connector#properties'
)

FLAGS = flags.FLAGS

# Creates spark table using pyspark by loading the parquet data.
# Args:
# argv[1]: string, The table name in the dataset that this script will create.
# argv[2]: string, The data path of the table.
SCRIPT_DIR = 'spark_sql_test_scripts'
SPARK_SQL_DISTCP_SCRIPT = os.path.join(SCRIPT_DIR, 'spark_sql_distcp.py')
SPARK_TABLE_SCRIPT = os.path.join(SCRIPT_DIR, 'spark_table.py')
SPARK_SQL_RUNNER_SCRIPT = os.path.join(SCRIPT_DIR, 'spark_sql_runner.py')
SPARK_SQL_PERF_GIT = 'https://github.com/databricks/spark-sql-perf.git'
SPARK_SQL_PERF_GIT_COMMIT = '6b2bf9f9ad6f6c2f620062fda78cded203f619c8'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Config needed to run the Spark SQL.

  Raises:
    Config.InvalidValue: On encountering invalid configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if not FLAGS.dpb_sparksql_data and FLAGS.dpb_sparksql_create_hive_tables:
    raise errors.Config.InvalidValue(
        'You must pass dpb_sparksql_data with dpb_sparksql_create_hive_tables')
  if FLAGS.dpb_sparksql_database and FLAGS.dpb_sparksql_create_hive_tables:
    raise errors.Config.InvalidValue(
        'You cannot create hive tables in a custom database.')
  if FLAGS.bigquery_tables and not FLAGS.spark_bigquery_connector:
    # Remove if Dataproc ever bundles BigQuery connector
    raise errors.Config.InvalidValue(
        'You must provide the BigQuery connector using '
        '--spark_bigquery_connector.')
  if not (FLAGS.dpb_sparksql_data or FLAGS.bigquery_tables or
          FLAGS.dpb_sparksql_database):
    # In the case of a static dpb_service, data could pre-exist
    logging.warning(
        'You did not specify --dpb_sparksql_data, --bigquery_tables, '
        'or dpb_sparksql_database. You will probably not have data to query!')
  if not FLAGS.dpb_sparksql_order:
    raise errors.Config.InvalidValue(
        'You must specify the queries to run with --dpb_sparksql_order')


def Prepare(benchmark_spec):
  """Installs and sets up dataset on the Spark clusters.

  Copies scripts and all the queries to cloud.
  Creates external Hive tables for data (unless BigQuery is being used).

  Args:
    benchmark_spec: The benchmark specification
  """
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service
  benchmark_spec.staged_queries = _LoadAndStageQueries(
      storage_service, cluster.base_dir)

  for script in [
      SPARK_SQL_DISTCP_SCRIPT,
      SPARK_TABLE_SCRIPT,
      SPARK_SQL_RUNNER_SCRIPT]:
    src_url = data.ResourcePath(script)
    storage_service.CopyToBucket(src_url, cluster.bucket, script)

  benchmark_spec.table_subdirs = []
  if FLAGS.dpb_sparksql_data:
    table_dir = FLAGS.dpb_sparksql_data.rstrip('/') + '/'
    stdout = storage_service.List(table_dir)
    for line in stdout.split('\n'):
      # GCS will sometimes list the directory itself.
      if line and line != table_dir:
        benchmark_spec.table_subdirs.append(
            re.split(' |/', line.rstrip('/')).pop())

    benchmark_spec.data_dir = FLAGS.dpb_sparksql_data
    if FLAGS.dpb_sparksql_copy_to_hdfs:
      job_arguments = []
      copy_dirs = {
          'source': benchmark_spec.data_dir,
          'destination': 'hdfs:/tmp/spark_sql/',
      }
      for flag, data_dir in copy_dirs.items():
        staged_file = os.path.join(cluster.base_dir, flag + '-metadata.json')
        metadata = _GetDistCpMetadata(data_dir, benchmark_spec.table_subdirs)
        _StageMetadata(metadata, storage_service, staged_file)
        job_arguments += ['--{}-metadata'.format(flag), staged_file]
      try:
        result = cluster.SubmitJob(
            pyspark_file='/'.join([cluster.base_dir, SPARK_SQL_DISTCP_SCRIPT]),
            job_type=BaseDpbService.PYSPARK_JOB_TYPE,
            job_arguments=job_arguments)
        logging.info(result)
        # Tell the benchmark to read from HDFS instead.
        benchmark_spec.data_dir = copy_dirs['destination']
      except dpb_service.JobSubmissionError as e:
        raise errors.Benchmarks.PrepareException(
            'Copying tables into HDFS failed') from e

  # Create external Hive tables
  if FLAGS.dpb_sparksql_create_hive_tables:
    try:
      result = cluster.SubmitJob(
          pyspark_file='/'.join([cluster.base_dir, SPARK_TABLE_SCRIPT]),
          job_type=BaseDpbService.PYSPARK_JOB_TYPE,
          job_arguments=[
              benchmark_spec.data_dir, ','.join(benchmark_spec.table_subdirs)
          ])
      logging.info(result)
    except dpb_service.JobSubmissionError as e:
      raise errors.Benchmarks.PrepareException(
          'Creating tables from {}/* failed'.format(
              benchmark_spec.data_dir)) from e


def Run(benchmark_spec):
  """Runs a sequence of Spark SQL Query.

  Args:
    benchmark_spec: Spec needed to run the Spark SQL.

  Returns:
    A list of samples, comprised of the detailed run times of individual query.

  Raises:
    Benchmarks.RunError if no query succeeds.
  """
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service
  metadata = benchmark_spec.dpb_service.GetMetadata()

  metadata['benchmark'] = BENCHMARK_NAMES[FLAGS.dpb_sparksql_query]

  # Run PySpark Spark SQL Runner
  report_dir = '/'.join([cluster.base_dir, f'report-{int(time.time()*1000)}'])
  args = [
      '--sql-scripts',
      ','.join(benchmark_spec.staged_queries),
      '--report-dir',
      report_dir,
  ]
  if FLAGS.dpb_sparksql_database:
    args += ['--database', FLAGS.dpb_sparksql_database]
  table_metadata = _GetTableMetadata(benchmark_spec)
  if table_metadata:
    table_metadata_file = '/'.join([cluster.base_dir, 'metadata.json'])
    _StageMetadata(table_metadata, storage_service, table_metadata_file)
    args += ['--table-metadata', table_metadata_file]
  else:
    # If we don't pass in tables, we must be reading from hive.
    # Note you can even read from Hive without --create_hive_tables if they
    # were precreated.
    args += ['--enable-hive', 'True']
  if FLAGS.dpb_sparksql_table_cache:
    args += ['--table-cache', FLAGS.dpb_sparksql_table_cache]
  if FLAGS.dpb_sparksql_simultaneous:
    args += ['--simultaneous', 'True']
  jars = []
  if FLAGS.spark_bigquery_connector:
    jars.append(FLAGS.spark_bigquery_connector)
  job_result = cluster.SubmitJob(
      pyspark_file='/'.join([cluster.base_dir, SPARK_SQL_RUNNER_SCRIPT]),
      job_arguments=args,
      job_jars=jars,
      job_type=dpb_service.BaseDpbService.PYSPARK_JOB_TYPE)

  # Spark can only write data to directories not files. So do a recursive copy
  # of that directory and then search it for the single JSON file with the
  # results.
  temp_run_dir = temp_dir.GetRunDirPath()
  storage_service.Copy(report_dir, temp_run_dir, recursive=True)
  report_file = None
  for dir_name, _, files in os.walk(
      os.path.join(temp_run_dir, os.path.basename(report_dir))):
    for filename in files:
      if filename.endswith('.json'):
        report_file = os.path.join(dir_name, filename)
        logging.info(report_file)
  if not report_file:
    raise errors.Benchmarks.RunError('Job report not found.')

  results = []
  run_times = {}
  passing_queries = set()
  with open(report_file, 'r') as file:
    for line in file:
      result = json.loads(line)
      logging.info('Timing: %s', result)
      query_id = _GetQueryId(result['script'])
      assert query_id
      passing_queries.add(query_id)
      metadata_copy = metadata.copy()
      metadata_copy['query'] = query_id
      results.append(
          sample.Sample('sparksql_run_time', result['duration'], 'seconds',
                        metadata_copy))
      run_times[query_id] = result['duration']

  metadata['failing_queries'] = ','.join(
      sorted(set(FLAGS.dpb_sparksql_order) - passing_queries))

  results.append(
      sample.Sample('sparksql_total_wall_time', job_result.wall_time, 'seconds',
                    metadata))
  results.append(
      sample.Sample('sparksql_geomean_run_time',
                    sample.GeoMean(run_times.values()), 'seconds', metadata))
  cluster_create_time = cluster.GetClusterCreateTime()
  if cluster_create_time is not None:
    results.append(
        sample.Sample('dpb_cluster_create_time', cluster_create_time, 'seconds',
                      metadata))
  return results


def _GetTableMetadata(benchmark_spec):
  """Compute map of table metadata for spark_sql_runner --table_metadata."""
  metadata = {}
  if not FLAGS.dpb_sparksql_create_hive_tables:
    for subdir in benchmark_spec.table_subdirs or []:
      # Subdir is table name
      metadata[subdir] = (FLAGS.dpb_sparksql_data_format or 'parquet', {
          'path': os.path.join(benchmark_spec.data_dir, subdir)
      })
  for table in FLAGS.bigquery_tables:
    name = table.split('.')[-1]
    bq_options = {'table': table}
    if FLAGS.bigquery_record_format:
      bq_options['readDataFormat'] = FLAGS.bigquery_record_format
    metadata[name] = (FLAGS.dpb_sparksql_data_format or 'bigquery', bq_options)
  return metadata


def _GetDistCpMetadata(base_dir: str, subdirs: List[str]):
  """Compute list of table metadata for spark_sql_distcp metadata flags."""
  metadata = []
  for subdir in subdirs or []:
    metadata += [(FLAGS.dpb_sparksql_data_format or 'parquet', {
        'path': '/'.join([base_dir, subdir])
    })]
  return metadata


def _StageMetadata(json_metadata, storage_service, staged_file: str):
  """Write JSON metadata to object storage."""
  # Write computed metadata to object storage.
  temp_run_dir = temp_dir.GetRunDirPath()
  local_file = os.path.join(temp_run_dir, os.path.basename(staged_file))
  with open(local_file, 'w') as f:
    json.dump(json_metadata, f)
  storage_service.Copy(local_file, staged_file)


def _GetQueryId(filename: str) -> Optional[str]:
  """Extract query id from file name."""
  match = re.match(r'(.*/)?q?([0-9]+[ab]?)\.sql$', filename)
  if match:
    return match.group(2)


def _LoadAndStageQueries(storage_service, base_dir: str) -> List[str]:
  """Loads queries from Github and stages them in object storage.

  Queries are selected using --dpb_sparksql_query and --dpb_sparksql_order.

  Args:
    storage_service: object_strorage_service to stage queries into.
    base_dir: object storage directory to stage queries into.

  Returns:
    The paths to the stage queries.

  Raises:
    PrepareException if a requested query is not found.
  """
  temp_run_dir = temp_dir.GetRunDirPath()
  spark_sql_perf_dir = os.path.join(temp_run_dir, 'spark_sql_perf_dir')

  # Clone repo
  vm_util.IssueCommand(['git', 'clone', SPARK_SQL_PERF_GIT, spark_sql_perf_dir])
  vm_util.IssueCommand(['git', 'checkout', SPARK_SQL_PERF_GIT_COMMIT],
                       cwd=spark_sql_perf_dir)
  query_dir = os.path.join(spark_sql_perf_dir, 'src', 'main', 'resources',
                           FLAGS.dpb_sparksql_query)

  # Search repo for queries
  query_file = {}  # map query -> staged file
  for dir_name, _, files in os.walk(query_dir):
    for filename in files:
      query_id = _GetQueryId(filename)
      if query_id:
        # only upload specified queries
        if query_id in FLAGS.dpb_sparksql_order:
          src_file = os.path.join(dir_name, filename)
          staged_file = '{}/{}'.format(base_dir, filename)
          storage_service.Copy(src_file, staged_file)
          query_file[query_id] = staged_file

  # Validate all requested queries are present.
  missing_queries = set(FLAGS.dpb_sparksql_order) - set(query_file.keys())
  if missing_queries:
    raise errors.Benchmarks.PrepareException(
        'Could not find queries {}'.format(missing_queries))

  # Return staged queries in proper order
  return [query_file[query] for query in FLAGS.dpb_sparksql_order]


def Cleanup(benchmark_spec):
  """Cleans up the Benchmark."""
  del benchmark_spec  # unused
