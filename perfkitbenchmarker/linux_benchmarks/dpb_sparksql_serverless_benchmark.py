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
"""Variant of dpb_sparksql_benchmark for serverless Spark services.

The main difference between this and dpb_sparksql_benchmark is the query time
measurement methodology. Here we submit one spark job per query and report the
whole job script execution wall time as measured by PKB, including the time the
job is pending and provisioning the underlying infrastructure to run the jobs.
This makes sense as job pending time in serverless services is not negligible.
By contrast, dpb_sparksql_benchmark submits only one job for all the queries and
reports the query times as measured inside the script, not considering the
provisioning time.

Otherwise, refer to dpb_sparksql_benchmark for more information.

NOTE: The dpb_sparksql_create_hive_tables option is not supported in this
benchmark, nor using the BigQuery connector in Dataproc.
"""

import logging
import time

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import dpb_sparksql_benchmark_helper
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'dpb_sparksql_serverless_benchmark'

BENCHMARK_CONFIG = """
dpb_sparksql_serverless_benchmark:
  description: Run Spark SQL on serverless Spark PaaS.
  dpb_service:
    service_type: dataproc_serverless
    worker_group:
      vm_spec:
        GCP: {}
        AWS:
          machine_type: G.2X
      disk_spec:
        GCP:
          disk_size: 100
        AWS:
          disk_size: 100
    worker_count: 2
"""

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Config needed to run the Spark SQL.

  Raises:
    Config.InvalidValue: On encountering invalid configuration.
  """
  del benchmark_config  # unused
  if FLAGS.dpb_sparksql_create_hive_tables:
    raise errors.Config.InvalidValue(
        'dpb_sparksql_create_hive_tables flag is not supported in SparkSQL '
        'Serverless benchmarking yet.')
  if FLAGS.dpb_sparksql_database:
    raise errors.Config.InvalidValue(
        'dpb_sparksql_database flag is not supported in SparkSQL serverless '
        'benchmarking yet.'
    )
  if FLAGS.bigquery_tables or FLAGS.spark_bigquery_connector:
    raise errors.Config.InvalidValue('BigQuery is not supported in SparkSQL '
                                     'Serverless benchmark.')
  if not FLAGS.dpb_sparksql_data:
    logging.warning(
        'You did not specify --dpb_sparksql_data. You will probably not have '
        'data to query!')
  if FLAGS.dpb_sparksql_simultaneous:
    raise errors.Config.InvalidValue(
        '--dpb_sparksql_simultaneous not supported in SparkSQL Serverless '
        'benchmark.')
  # TODO(odiego): DRY out
  if not FLAGS.dpb_sparksql_order:
    raise errors.Config.InvalidValue(
        'You must specify the queries to run with --dpb_sparksql_order.')


def Prepare(benchmark_spec):
  """Copies scripts and all the queries to cloud.

  Args:
    benchmark_spec: The benchmark specification
  """
  dpb_sparksql_benchmark_helper.Prepare(benchmark_spec)


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

  metadata['benchmark'] = dpb_sparksql_benchmark_helper.BENCHMARK_NAMES[
      FLAGS.dpb_sparksql_query]
  if FLAGS.dpb_sparksql_data_format:
    metadata['data_format'] = FLAGS.dpb_sparksql_data_format
  if FLAGS.dpb_sparksql_data_compression:
    metadata['data_compression'] = FLAGS.dpb_sparksql_data_compression

  # Run PySpark Spark SQL Runner
  report_dir = '/'.join([
      cluster.base_dir, f'report-{int(time.time()*1000)}'])

  # Run PySpark Spark SQL Runner
  report_dir = '/'.join([cluster.base_dir, f'report-{int(time.time()*1000)}'])
  run_times = {}
  for query in benchmark_spec.staged_queries:
    # TODO(odiego): DRY out the SubmitJob logic (too similar to
    # dpb_sparksql_benchmark).
    args = ['--sql-scripts', query, '--report-dir', report_dir]
    table_metadata = dpb_sparksql_benchmark_helper.GetTableMetadata(
        benchmark_spec)
    table_metadata_file = '/'.join([cluster.base_dir, 'metadata.json'])
    dpb_sparksql_benchmark_helper.StageMetadata(
        table_metadata, storage_service, table_metadata_file)
    args += ['--table-metadata', table_metadata_file]
    args += ['--fail-on-query-execution-errors', 'True']
    if FLAGS.dpb_sparksql_table_cache:
      args += ['--table-cache', FLAGS.dpb_sparksql_table_cache]
    query_failed = False
    start_time = time.time()
    try:
      cluster.SubmitJob(
          pyspark_file='/'.join(
              [cluster.base_dir,
               dpb_sparksql_benchmark_helper.SPARK_SQL_RUNNER_SCRIPT]),
          job_arguments=args,
          job_jars=[],
          job_type=dpb_service.BaseDpbService.PYSPARK_JOB_TYPE)
    except dpb_service.JobSubmissionError:
      logging.warning(
          'Job submission for query %s failed.',
          dpb_sparksql_benchmark_helper.GetQueryId(query))
      query_failed = True
    end_time = time.time()
    if not query_failed:
      run_times[dpb_sparksql_benchmark_helper.GetQueryId(query)] = (
          end_time - start_time)

  metadata['failing_queries'] = ','.join(
      sorted(set(FLAGS.dpb_sparksql_order) - set(run_times.keys())))
  samples = []
  for query_id, run_time in run_times.items():
    metadata_copy = metadata.copy()
    metadata_copy['query'] = query_id
    samples.append(
        sample.Sample(
            'sparksql_total_run_time', run_time, 'seconds', metadata_copy))
  samples.append(
      sample.Sample('sparksql_geomean_run_time',
                    sample.GeoMean(run_times.values()), 'seconds', metadata))
  return samples


def Cleanup(benchmark_spec):
  """Cleans up the Benchmark."""
  del benchmark_spec  # unused
