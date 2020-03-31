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
"""Executes query on Spark SQL and records the latency.

The Data (TPCDS or TPCH) needs be generated first by user.
TPCDS and TPCH tools.
TPCDS: https://github.com/databricks/tpcds-kit
TPCH: https://github.com/databricks/tpch-dbgen

This benchmark uses queries from https://github.com/databricks/spark-sql-perf.
Because spark SQL doesn't support all the queries that using dialect netezza.

It can optionally read the data from Google BigQuery using
https://github.com/GoogleCloudPlatform/spark-bigquery-connector.
"""

import logging
import os
import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
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
flags.DEFINE_string('dpb_sparksql_data', None,
                    'The dataset to run Spark SQL query')
flags.DEFINE_enum('dpb_sparksql_query', 'tpcds_2_4', ['tpcds_2_4', 'tpch'],
                  'A list of query to run on dpb_sparksql_data')
flags.DEFINE_list('dpb_sparksql_order', [],
                  'The names (numbers) of the queries to run in order. '
                  'If omitted all queries are run in lexographic order.')
flags.DEFINE_string(
    'spark_bigquery_connector',
    'gs://spark-lib/bigquery/spark-bigquery-latest.jar',
    'The Spark BigQuery Connector jar to pass to the Spark Job')
flags.DEFINE_list(
    'bigquery_tables', [],
    'A list of BigQuery tables to load as Temporary Spark SQL views instead '
    'of reading from external Hive tables.'
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
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        'Invalid backend {} for Spark SQL. Not in: {}'.format(
            dpb_service_type, SUPPORTED_DPB_BACKENDS))

  if not (FLAGS.dpb_sparksql_data or FLAGS.bigquery_tables):
    # In the case of a static dpb_service, data could pre-exist
    logging.warning(
        'You did not specify --dpb_sparksql_data or --bigquery_tables. '
        'You will probably not have data to query!')
  if FLAGS.dpb_sparksql_data and FLAGS.bigquery_tables:
    raise errors.Config.InvalidValue(
        'You cannot specify both --dpb_sparksql_data and --bigquery_tables')


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
  query_dir = os.path.join(spark_sql_perf_dir, 'src', 'main', 'resources',
                           FLAGS.dpb_sparksql_query)
  for dir_name, _, files in os.walk(query_dir):
    for filename in files:
      match = re.match(r'q?([0-9]+)a?.sql', filename)
      if match:
        query_id = match.group(1)
        # if order is specified only upload those queries
        if not FLAGS.dpb_sparksql_order or query_id in FLAGS.dpb_sparksql_order:
          query = '{}.sql'.format(query_id)
          src_url = os.path.join(dir_name, filename)
          storage_service.CopyToBucket(src_url, bucket, query)
  for script in [SPARK_TABLE_SCRIPT, SPARK_SQL_RUNNER_SCRIPT]:
    src_url = data.ResourcePath(script)
    storage_service.CopyToBucket(src_url, bucket, script)

  # Create external Hive tables if not reading the data from BigQuery
  if FLAGS.dpb_sparksql_data:
    stdout = storage_service.List(FLAGS.dpb_sparksql_data)

    for table_dir in stdout.split('\n'):
      # The directory name is the table name.
      if not table_dir:
        continue
      table = re.split(' |/', table_dir.rstrip('/')).pop()
      stats = dpb_service_instance.SubmitJob(
          pyspark_file=os.path.join(benchmark_spec.base_dir,
                                    SPARK_TABLE_SCRIPT),
          job_type=BaseDpbService.PYSPARK_JOB_TYPE,
          job_arguments=[FLAGS.dpb_sparksql_data, table])
      logging.info(stats)
      if not stats['success']:
        logging.warning('Creates table %s from %s failed', table, table_dir)


def Run(benchmark_spec):
  """Runs Spark SQL.

  Args:
    benchmark_spec: Spec needed to run the Spark SQL.

  Returns:
    A list of samples, comprised of the detailed run times of individual query.
  """
  dpb_service_instance = benchmark_spec.dpb_service
  metadata = benchmark_spec.dpb_service.GetMetadata()

  results = []
  total_wall_time = 0
  total_run_time = 0
  unit = 'seconds'
  all_succeeded = True
  for query_number in FLAGS.dpb_sparksql_order:
    query = '{}.sql'.format(query_number)
    stats = _RunSparkSqlJob(
        dpb_service_instance, os.path.join(benchmark_spec.base_dir, query),
        os.path.join(benchmark_spec.base_dir, SPARK_SQL_RUNNER_SCRIPT))
    logging.info(stats)
    metadata_copy = metadata.copy()
    metadata_copy['query'] = query
    if stats[dpb_service.SUCCESS]:
      run_time = stats[dpb_service.RUNTIME]
      wall_time = run_time + stats[dpb_service.WAITING]
      results.append(
          sample.Sample('sparksql_wall_time', wall_time, unit, metadata_copy))
      results.append(
          sample.Sample('sparksql_run_time', run_time, unit, metadata_copy))
      total_wall_time += wall_time
      total_run_time += run_time
    else:
      all_succeeded = False

  if all_succeeded:
    results.append(
        sample.Sample('sparksql_total_wall_time', total_wall_time, unit,
                      metadata))
    results.append(
        sample.Sample('sparksql_total_run_time', total_run_time, unit,
                      metadata))
  return results


def _RunSparkSqlJob(dpb_service_instance,
                    staged_sql_file,
                    staged_sql_runner_file=None):
  """Run a Spark SQL script either with the spark-sql command or spark_sql_runner.py."""
  if staged_sql_runner_file and FLAGS.bigquery_tables:
    args = [
        os.path.basename(staged_sql_file), '--bigquery_tables',
        ','.join(FLAGS.bigquery_tables)
    ]
    return dpb_service_instance.SubmitJob(
        pyspark_file=staged_sql_runner_file,
        job_arguments=args,
        job_files=[staged_sql_file],
        job_jars=[FLAGS.spark_bigquery_connector],
        job_type=BaseDpbService.PYSPARK_JOB_TYPE)
  return dpb_service_instance.SubmitJob(
      query_file=staged_sql_file, job_type=BaseDpbService.SPARKSQL_JOB_TYPE)


def Cleanup(_):
  """Cleans up the Spark SQL."""
  pass
