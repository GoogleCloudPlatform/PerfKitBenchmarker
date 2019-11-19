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
"""

import logging
import os
import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import object_storage_service
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
          machine_type: n1-standard-1
        AWS:
          machine_type: m5.xlarge
    worker_count: 2
"""
flags.DEFINE_string('dpb_sparksql_data', None,
                    'The dataset to run Spark SQL query')
flags.DEFINE_enum('dpb_sparksql_query', 'tpcds_2_4', ['tpcds_2_4', 'tpch'],
                  'A list of query to run on dpb_sparksql_data')
flags.DEFINE_list('dpb_sparksql_order', [],
                  'The order of query templates in each query stream.')

FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = [dpb_service.DATAPROC, dpb_service.EMR]
JOB_CATEGORY = BaseDpbService.SPARK_JOB_TYPE
JOB_TYPE = 'spark-sql'
# Creates spark table using pyspark by loading the parquet data.
# Args:
# argv[1]: string, The table name in the dataset that this script will create.
# argv[2]: string, The data path of the table.
SPARK_TABLE_SCRIPT = 'spark_table.py'
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


def Prepare(benchmark_spec):
  """Installs and sets up dataset on the Spark clusters.

  Copies SPARK_TABLE_SCRIPT and all the queries to cloud.
  Creates sparktable using SPARK_TABLE_SCRIPT.

  Args:
    benchmark_spec: The benchmark specification
  """
  dpb_service_instance = benchmark_spec.dpb_service
  run_uri = benchmark_spec.uuid.split('-')[0]
  dpb_service_instance.CreateBucket(run_uri)

  temp_run_dir = temp_dir.GetRunDirPath()
  spark_sql_perf_dir = os.path.join(temp_run_dir, 'spark_sql_perf_dir')
  vm_util.IssueCommand(['git', 'clone', SPARK_SQL_PERF_GIT, spark_sql_perf_dir])
  vm_util.IssueCommand(['git', 'checkout', SPARK_SQL_PERF_GIT_COMMIT],
                       cwd=spark_sql_perf_dir)
  query_dir = os.path.join(spark_sql_perf_dir, 'src', 'main', 'resources',
                           FLAGS.dpb_sparksql_query)

  storage_service = object_storage_service.GetObjectStorageClass(FLAGS.cloud)()
  dst_url = '{prefix}{uri}'.format(
      prefix=dpb_service_instance.PERSISTENT_FS_PREFIX, uri=run_uri)
  for dir_name, _, files in os.walk(query_dir):
    for filename in files:
      match = re.match(r'q?([0-9]+)a?.sql', filename)
      if match:
        query_id = match.group(1)
        query = '{}.sql'.format(query_id)
        src_url = os.path.join(dir_name, filename)
        storage_service.Copy(src_url, os.path.join(dst_url, query))

  src_url = data.ResourcePath(SPARK_TABLE_SCRIPT)
  storage_service.Copy(src_url, dst_url)
  benchmark_spec.base_dir = dst_url

  stdout = storage_service.List(FLAGS.dpb_sparksql_data)

  for table_dir in stdout.split('\n'):
    # The directory name is the table name.
    if not table_dir:
      continue
    table = re.split(' |/', table_dir.rstrip('/')).pop()
    stats = dpb_service_instance.SubmitJob(
        pyspark_file=os.path.join(dst_url, SPARK_TABLE_SCRIPT),
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
  for query_number in FLAGS.dpb_sparksql_order:
    query = '{}.sql'.format(query_number)
    stats = dpb_service_instance.SubmitJob(
        None,
        None,
        query_file=os.path.join(benchmark_spec.base_dir, query),
        job_type=BaseDpbService.SPARKSQL_JOB_TYPE)
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
  results.append(sample.Sample(
      'sparksql_total_wall_time', total_wall_time, unit, metadata))
  results.append(sample.Sample(
      'sparksql_total_run_time', total_run_time, unit, metadata))
  return results


def Cleanup(_):
  """Cleans up the Spark SQL."""
  pass
