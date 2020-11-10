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

Spark SQL queries can either be run using the spark-sql CLI or a custom pyspark
runner.

The spark-sql CLI only supports Hive data. This benchmark can create and
replace external Hive tables using `--dpb_sparksql_create_hive_tables=true`
Alternatively you could pre-provision a Hive metastore with data before running
the benchmark.

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
from absl import flags
import numpy as np
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
        AWS:
          disk_size: 1000
          disk_type: gp2
    worker_count: 2
"""

BENCHMARK_NAMES = {
    'tpcds_2_4': 'TPC-DS',
    'tpch': 'TPC-H'
}

SPARK_SQL = dpb_service.BaseDpbService.SPARKSQL_JOB_TYPE
PYSPARK = dpb_service.BaseDpbService.PYSPARK_JOB_TYPE

flags.DEFINE_string(
    'dpb_sparksql_data', None,
    'The HCFS based dataset to run Spark SQL query '
    'against')
flags.DEFINE_enum(
    'dpb_sparksql_job_type', PYSPARK, [PYSPARK, SPARK_SQL],
    'How to trigger the query. Either with the spark-sql CLI '
    'or with a PySpark harness inside PKB.')
flags.DEFINE_bool('dpb_sparksql_create_hive_tables', False,
                  'Whether to load dpb_sparksql_data into external hive tables '
                  'or not.')
flags.DEFINE_string(
    'dpb_sparksql_data_format', None,
    "Format of data to load. Assumed to be 'parquet' for HCFS "
    "and 'bigquery' for bigquery if unspecified.")
flags.DEFINE_enum('dpb_sparksql_query', 'tpcds_2_4', BENCHMARK_NAMES.keys(),
                  'A list of query to run on dpb_sparksql_data')
flags.DEFINE_list('dpb_sparksql_order', [],
                  'The names (numbers) of the queries to run in order. '
                  'If omitted all queries are run in lexographic order.')
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

SUPPORTED_DPB_BACKENDS = [dpb_service.DATAPROC, dpb_service.EMR]
JOB_CATEGORY = BaseDpbService.SPARK_JOB_TYPE
JOB_TYPE = 'spark-sql'
# Creates spark table using pyspark by loading the parquet data.
# Args:
# argv[1]: string, The table name in the dataset that this script will create.
# argv[2]: string, The data path of the table.
SPARK_TABLE_SCRIPT = 'spark_table.py'
SPARK_SQL_RUNNER_SCRIPT = 'spark_sql_runner.py'
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
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        'Invalid backend {} for Spark SQL. Not in: {}'.format(
            dpb_service_type, SUPPORTED_DPB_BACKENDS))

  if not FLAGS.dpb_sparksql_data and FLAGS.dpb_sparksql_create_hive_tables:
    raise errors.Config.InvalidValue(
        'You must pass dpb_sparksql_data with dpb_sparksql_create_hive_tables')
  if FLAGS.dpb_sparksql_job_type == SPARK_SQL:
    if FLAGS.bigquery_tables:
      raise errors.Config.InvalidValue(
          'spark-sql job type does not support temporary bigquery views.')
    if FLAGS.dpb_sparksql_data and not FLAGS.dpb_sparksql_create_hive_tables:
      raise errors.Config.InvalidValue(
          'spark-sql job type cannot query HCFS data without '
          '--dpb_sparksql_create_hive_tables.')
  if FLAGS.bigquery_tables and not FLAGS.spark_bigquery_connector:
    # Remove if Dataproc ever bundles BigQuery connector
    raise errors.Config.InvalidValue(
        'You must provide the BigQuery connector using '
        '--spark_bigquery_connector.')
  if not (FLAGS.dpb_sparksql_data or FLAGS.bigquery_tables):
    # In the case of a static dpb_service, data could pre-exist
    logging.warning(
        'You did not specify --dpb_sparksql_data or --bigquery_tables. '
        'You will probably not have data to query!')


def Prepare(benchmark_spec):
  """Installs and sets up dataset on the Spark clusters.

  Copies scripts and all the queries to cloud.
  Creates external Hive tables for data (unless BigQuery is being used).

  Args:
    benchmark_spec: The benchmark specification
  """
  dpb_service_instance = benchmark_spec.dpb_service
  # buckets must start with a letter
  bucket = 'pkb-' + benchmark_spec.uuid.split('-')[0]
  storage_service = dpb_service_instance.storage_service
  storage_service.MakeBucket(bucket)
  benchmark_spec.base_dir = dpb_service_instance.PERSISTENT_FS_PREFIX + bucket

  temp_run_dir = temp_dir.GetRunDirPath()
  spark_sql_perf_dir = os.path.join(temp_run_dir, 'spark_sql_perf_dir')
  vm_util.IssueCommand(['git', 'clone', SPARK_SQL_PERF_GIT, spark_sql_perf_dir])
  vm_util.IssueCommand(['git', 'checkout', SPARK_SQL_PERF_GIT_COMMIT],
                       cwd=spark_sql_perf_dir)
  benchmark_spec.queries = []
  query_dir = os.path.join(spark_sql_perf_dir, 'src', 'main', 'resources',
                           FLAGS.dpb_sparksql_query)
  for dir_name, _, files in os.walk(query_dir):
    for filename in files:
      match = re.match(r'q?([0-9]+)a?.sql', filename)
      if match:
        query_id = match.group(1)
        # if order is specified only upload those queries
        if not FLAGS.dpb_sparksql_order or query_id in FLAGS.dpb_sparksql_order:
          benchmark_spec.queries.append(query_id)
          query = '{}.sql'.format(query_id)
          src_url = os.path.join(dir_name, filename)
          storage_service.CopyToBucket(src_url, bucket, query)
  if not benchmark_spec.queries:
    raise errors.Benchmarks.PrepareException('No queries were staged')

  for script in [SPARK_TABLE_SCRIPT, SPARK_SQL_RUNNER_SCRIPT]:
    src_url = data.ResourcePath(script)
    storage_service.CopyToBucket(src_url, bucket, script)

  benchmark_spec.table_subdirs = []
  if FLAGS.dpb_sparksql_data:
    table_dir = FLAGS.dpb_sparksql_data.rstrip('/') + '/'
    stdout = storage_service.List(table_dir)
    for line in stdout.split('\n'):
      # GCS will sometimes list the directory itself.
      if line and line != table_dir:
        benchmark_spec.table_subdirs.append(
            re.split(' |/', line.rstrip('/')).pop())

  # Create external Hive tables
  if FLAGS.dpb_sparksql_create_hive_tables:
    try:
      result = dpb_service_instance.SubmitJob(
          pyspark_file=os.path.join(benchmark_spec.base_dir,
                                    SPARK_TABLE_SCRIPT),
          job_type=BaseDpbService.PYSPARK_JOB_TYPE,
          job_arguments=[
              FLAGS.dpb_sparksql_data, ','.join(benchmark_spec.table_subdirs)
          ])
      logging.info(result)
    except dpb_service.JobSubmissionError as e:
      raise errors.Benchmarks.PrepareException(
          'Creating tables from {}/* failed'.format(
              FLAGS.dpb_sparksql_data)) from e


def Run(benchmark_spec):
  """Runs a sequence of Spark SQL Query.

  Args:
    benchmark_spec: Spec needed to run the Spark SQL.

  Returns:
    A list of samples, comprised of the detailed run times of individual query.

  Raises:
    Benchmarks.RunError if no query succeeds.
  """
  dpb_service_instance = benchmark_spec.dpb_service
  metadata = benchmark_spec.dpb_service.GetMetadata()

  metadata['benchmark'] = BENCHMARK_NAMES[FLAGS.dpb_sparksql_query]

  results = []
  failing_queries = []
  run_times = {}
  wall_times = {}
  for query in benchmark_spec.queries:
    try:
      result = _RunSparkSqlJob(
          dpb_service_instance,
          os.path.join(benchmark_spec.base_dir, query + '.sql'),
          os.path.join(benchmark_spec.base_dir, SPARK_SQL_RUNNER_SCRIPT),
          benchmark_spec.table_subdirs)
      logging.info(result)
      metadata_copy = metadata.copy()
      metadata_copy['query'] = query
      results.append(
          sample.Sample('sparksql_wall_time', result.wall_time, 'seconds',
                        metadata_copy))
      results.append(
          sample.Sample('sparksql_run_time', result.run_time, 'seconds',
                        metadata_copy))
      wall_times[query] = result.wall_time
      run_times[query] = result.run_time
    except dpb_service.JobSubmissionError:
      failing_queries.append(query)

  metadata['failing_queries'] = ','.join(sorted(failing_queries))

  if results:
    results.append(
        sample.Sample('sparksql_total_wall_time',
                      np.fromiter(wall_times.values(), dtype='float').sum(),
                      'seconds', metadata))
    results.append(
        sample.Sample('sparksql_total_run_time',
                      np.fromiter(run_times.values(), dtype='float').sum(),
                      'seconds', metadata))
    results.append(
        sample.Sample('sparksql_geomean_wall_time',
                      sample.GeoMean(wall_times.values()), 'seconds', metadata))
    results.append(
        sample.Sample('sparksql_geomean_run_time',
                      sample.GeoMean(run_times.values()), 'seconds', metadata))
  else:
    raise errors.Benchmarks.RunError('No queries succeeded.')

  return results


def _RunSparkSqlJob(dpb_service_instance,
                    staged_sql_file,
                    staged_sql_runner_file=None,
                    table_subdirs=None):
  """Run a Spark SQL script either with the spark-sql command or spark_sql_runner.py."""
  if FLAGS.dpb_sparksql_job_type == SPARK_SQL:
    return dpb_service_instance.SubmitJob(
        query_file=staged_sql_file, job_type=SPARK_SQL)
  if FLAGS.dpb_sparksql_job_type == PYSPARK:
    assert staged_sql_runner_file
    args = [
        os.path.basename(staged_sql_file), '--table_metadata',
        _GetTableMetadataJson(table_subdirs)
    ]
    jars = []
    if FLAGS.spark_bigquery_connector:
      jars.append(FLAGS.spark_bigquery_connector)
    return dpb_service_instance.SubmitJob(
        pyspark_file=staged_sql_runner_file,
        job_arguments=args,
        job_files=[staged_sql_file],
        job_jars=jars,
        job_type=PYSPARK)
  raise errors.Config.UnrecognizedOption('Unsupported job type ' +
                                         FLAGS.dpb_sparksql_job_type)


def _GetTableMetadataJson(table_subdirs=None):
  """Compute a JSON map of table metadata for spark_sql_runner --table_metadata."""
  metadata = {}
  if not FLAGS.dpb_sparksql_create_hive_tables:
    for subdir in table_subdirs or []:
      # Subdir is table name
      metadata[subdir] = (FLAGS.dpb_sparksql_data_format or 'parquet', {
          'path': os.path.join(FLAGS.dpb_sparksql_data, subdir)
      })
  for table in FLAGS.bigquery_tables:
    name = table.split('.')[-1]
    bq_options = {'table': table}
    if FLAGS.bigquery_record_format:
      bq_options['readDataFormat'] = FLAGS.bigquery_record_format
    metadata[name] = (FLAGS.dpb_sparksql_data_format or 'bigquery', bq_options)
  return json.dumps(metadata)


def Cleanup(_):
  """Cleans up the Spark SQL."""
  pass
