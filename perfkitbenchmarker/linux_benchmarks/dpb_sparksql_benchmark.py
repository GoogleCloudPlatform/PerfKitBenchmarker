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

from collections.abc import MutableMapping
import json
import logging
import os
import time
from typing import List

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import dpb_sparksql_benchmark_helper
from perfkitbenchmarker import errors
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir

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
  if bool(FLAGS.dpb_sparksql_order) == bool(FLAGS.dpb_sparksql_streams):
    raise errors.Config.InvalidValue(
        'You must specify the queries to run with either --dpb_sparksql_order '
        'or --dpb_sparksql_streams (but not both).')
  if FLAGS.dpb_sparksql_simultaneous and FLAGS.dpb_sparksql_streams:
    raise errors.Config.InvalidValue(
        '--dpb_sparksql_simultaneous is not compatible with '
        '--dpb_sparksql_streams.')


def Prepare(benchmark_spec):
  """Installs and sets up dataset on the Spark clusters.

  Copies scripts and all the queries to cloud.
  Creates external Hive tables for data (unless BigQuery is being used).

  Args:
    benchmark_spec: The benchmark specification
  """
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service
  dpb_sparksql_benchmark_helper.Prepare(benchmark_spec)

  # Copy to HDFS
  if FLAGS.dpb_sparksql_data and FLAGS.dpb_sparksql_copy_to_hdfs:
    job_arguments = []
    copy_dirs = {
        'source': benchmark_spec.data_dir,
        'destination': 'hdfs:/tmp/spark_sql/',
    }
    for flag, data_dir in copy_dirs.items():
      staged_file = os.path.join(cluster.base_dir, flag + '-metadata.json')
      extra = {}
      if flag == 'destination' and FLAGS.dpb_sparksql_data_compression:
        extra['compression'] = FLAGS.dpb_sparksql_data_compression
      metadata = _GetDistCpMetadata(
          data_dir, benchmark_spec.table_subdirs, extra_metadata=extra)
      dpb_sparksql_benchmark_helper.StageMetadata(
          metadata, storage_service, staged_file)
      job_arguments += ['--{}-metadata'.format(flag), staged_file]
    try:
      result = cluster.SubmitJob(
          pyspark_file='/'.join([
              cluster.base_dir,
              dpb_sparksql_benchmark_helper.SPARK_SQL_DISTCP_SCRIPT]),
          job_type=dpb_service.BaseDpbService.PYSPARK_JOB_TYPE,
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
          pyspark_file='/'.join([
              cluster.base_dir,
              dpb_sparksql_benchmark_helper.SPARK_TABLE_SCRIPT]),
          job_type=dpb_service.BaseDpbService.PYSPARK_JOB_TYPE,
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
  metadata = _GetSampleMetadata(benchmark_spec)

  # Run PySpark Spark SQL Runner
  report_dir, job_result = _RunQueries(benchmark_spec)

  results = _GetQuerySamples(storage_service, report_dir, metadata)
  results += _GetGlobalSamples(results, cluster, job_result, metadata)
  return results


def _GetSampleMetadata(benchmark_spec):
  """Gets metadata dict to be attached to exported benchmark samples/metrics."""
  metadata = benchmark_spec.dpb_service.GetMetadata()
  metadata['benchmark'] = dpb_sparksql_benchmark_helper.BENCHMARK_NAMES[
      FLAGS.dpb_sparksql_query]
  if FLAGS.bigquery_record_format:
    # This takes higher priority since for BQ dpb_sparksql_data_format actually
    # holds a fully qualified Java class/package name.
    metadata['data_format'] = FLAGS.bigquery_record_format
  elif FLAGS.dpb_sparksql_data_format:
    metadata['data_format'] = FLAGS.dpb_sparksql_data_format
  if FLAGS.dpb_sparksql_data_compression:
    metadata['data_compression'] = FLAGS.dpb_sparksql_data_compression

  if FLAGS.dpb_sparksql_simultaneous:
    metadata['run_type'] = 'SIMULTANEOUS'
  elif FLAGS.dpb_sparksql_streams:
    metadata['run_type'] = 'THROUGHPUT'
  else:
    metadata['run_type'] = 'POWER'
  return metadata


def _RunQueries(benchmark_spec) -> tuple[str, dpb_service.JobResult]:
  """Runs queries. Returns storage path with metrics and JobResult object."""
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service
  report_dir = '/'.join([cluster.base_dir, f'report-{int(time.time()*1000)}'])
  args = ['--sql-scripts-dir', benchmark_spec.query_dir]
  if FLAGS.dpb_sparksql_simultaneous:
    # Assertion true bc of --dpb_sparksql_simultaneous and
    # --dpb_sparksql_streams being mutually exclusive.
    assert len(benchmark_spec.query_streams) == 1
    for query in benchmark_spec.query_streams[0]:
      args += ['--sql-scripts', query]
  else:
    for stream in benchmark_spec.query_streams:
      args += ['--sql-scripts', ','.join(stream)]
  args += ['--report-dir', report_dir]
  if FLAGS.dpb_sparksql_database:
    args += ['--database', FLAGS.dpb_sparksql_database]
  table_metadata = _GetTableMetadata(benchmark_spec)
  if table_metadata:
    table_metadata_file = '/'.join([cluster.base_dir, 'metadata.json'])
    dpb_sparksql_benchmark_helper.StageMetadata(
        table_metadata, storage_service, table_metadata_file)
    args += ['--table-metadata', table_metadata_file]
  else:
    # If we don't pass in tables, we must be reading from hive.
    # Note you can even read from Hive without --create_hive_tables if they
    # were precreated.
    args += ['--enable-hive', 'True']
  if FLAGS.dpb_sparksql_table_cache:
    args += ['--table-cache', FLAGS.dpb_sparksql_table_cache]
  if dpb_sparksql_benchmark_helper.DUMP_SPARK_CONF.value:
    args += ['--dump-spark-conf', os.path.join(cluster.base_dir, 'spark-conf')]
  jars = []
  if FLAGS.spark_bigquery_connector:
    jars.append(FLAGS.spark_bigquery_connector)
  job_result = cluster.SubmitJob(
      pyspark_file='/'.join([
          cluster.base_dir,
          dpb_sparksql_benchmark_helper.SPARK_SQL_RUNNER_SCRIPT]),
      job_arguments=args,
      job_jars=jars,
      job_type=dpb_service.BaseDpbService.PYSPARK_JOB_TYPE)
  return report_dir, job_result


def _GetQuerySamples(
    storage_service: object_storage_service.ObjectStorageService,
    report_dir: str,
    base_metadata: MutableMapping[str, str]
) -> list[sample.Sample]:
  """Get Sample objects from metrics storage path."""
  # Spark can only write data to directories not files. So do a recursive copy
  # of that directory and then search it for the collection of JSON files with
  # the results.
  temp_run_dir = temp_dir.GetRunDirPath()
  storage_service.Copy(report_dir, temp_run_dir, recursive=True)
  report_files = []
  for dir_name, _, files in os.walk(
      os.path.join(temp_run_dir, os.path.basename(report_dir))):
    for filename in files:
      if filename.endswith('.json'):
        report_file = os.path.join(dir_name, filename)
        report_files.append(report_file)
        logging.info("Found report file '%s'.", report_file)
  if not report_files:
    raise errors.Benchmarks.RunError('Job report not found.')

  samples = []
  for report_file in report_files:
    with open(report_file, 'r') as file:
      for line in file:
        result = json.loads(line)
        logging.info('Timing: %s', result)
        query_id = dpb_sparksql_benchmark_helper.GetQueryId(result['script'])
        assert query_id
        metadata_copy = base_metadata.copy()
        metadata_copy['query'] = query_id
        if FLAGS.dpb_sparksql_streams:
          metadata_copy['stream'] = result['stream']
        samples.append(
            sample.Sample('sparksql_run_time', result['duration'], 'seconds',
                          metadata_copy))
  return samples


def _GetGlobalSamples(
    query_samples: list[sample.Sample],
    cluster: dpb_service.BaseDpbService,
    job_result: dpb_service.JobResult,
    metadata: MutableMapping[str, str]) -> list[sample.Sample]:
  """Gets samples that summarize the whole benchmark run."""

  run_times = {}
  passing_queries = {}
  for s in query_samples:
    query_id = s.metadata['query']
    passing_queries.setdefault(s.metadata.get('stream', 0), set()).add(query_id)
    run_times[query_id] = s.value

  samples = []
  if FLAGS.dpb_sparksql_streams:
    for i, stream in enumerate(dpb_sparksql_benchmark_helper.GetStreams()):
      metadata[f'failing_queries_stream_{i}'] = ','.join(
          sorted(set(stream) - passing_queries.get(i, set())))
  else:
    all_passing_queries = set()
    for stream_passing_queries in passing_queries.values():
      all_passing_queries.update(stream_passing_queries)
    metadata['failing_queries'] = ','.join(
        sorted(set(FLAGS.dpb_sparksql_order) - all_passing_queries))

  # TODO(user): Compute aggregated time for each query across streams and
  # iterations.
  samples.append(
      sample.Sample('sparksql_total_wall_time', job_result.wall_time, 'seconds',
                    metadata))
  samples.append(
      sample.Sample('sparksql_geomean_run_time',
                    sample.GeoMean(run_times.values()), 'seconds', metadata))
  cluster_create_time = cluster.GetClusterCreateTime()
  if cluster_create_time is not None:
    samples.append(
        sample.Sample('dpb_cluster_create_time', cluster_create_time, 'seconds',
                      metadata))
  samples.append(sample.Sample('dpb_sparksql_job_pending',
                               job_result.pending_time, 'seconds', metadata))
  if FLAGS.dpb_export_job_stats:
    run_cost = cluster.CalculateCost()
    if run_cost is not None:
      samples.append(
          sample.Sample('sparksql_run_cost', run_cost, '$', metadata))
  return samples


def _GetDistCpMetadata(base_dir: str, subdirs: List[str], extra_metadata=None):
  """Compute list of table metadata for spark_sql_distcp metadata flags."""
  metadata = []
  if not extra_metadata:
    extra_metadata = {}
  for subdir in subdirs or []:
    metadata += [(FLAGS.dpb_sparksql_data_format or 'parquet', {
        'path': '/'.join([base_dir, subdir]),
        **extra_metadata
    })]
  return metadata


def _GetTableMetadata(benchmark_spec):
  metadata = dpb_sparksql_benchmark_helper.GetTableMetadata(benchmark_spec)
  for table in FLAGS.bigquery_tables:
    name = table.split('.')[-1]
    bq_options = {'table': table}
    if FLAGS.bigquery_record_format:
      bq_options['readDataFormat'] = FLAGS.bigquery_record_format
    metadata[name] = (FLAGS.dpb_sparksql_data_format or 'bigquery', bq_options)
  return metadata


def Cleanup(benchmark_spec):
  """Cleans up the Benchmark."""
  del benchmark_spec  # unused
