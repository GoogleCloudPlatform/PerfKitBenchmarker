"""Runs a Spark SQL query with preloaded temp views.

Views can be BigQuery tables or HCFS directories containing Parquet.
This is useful for Storage formats not expressible as External Hive Tables.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from concurrent import futures
import json
import logging
import os
import time

import py4j
from pyspark import sql


_results_logger = logging.getLogger('spark_sql_runner_results')
_results_logger.propagate = False
_results_logger.setLevel(logging.INFO)
_results_logger_handler = logging.StreamHandler()
_results_logger_handler.setFormatter(logging.Formatter())
_results_logger.addHandler(_results_logger_handler)


# This snippet will be replaced with an actual dict[str, str] from query_id to
# SQL string before uploading this file. Not using a Jinja template, since we
# also want to load this as a proper python file for unit testing.
# spark_sql_queries:start
QUERIES = {}
# spark_sql_queries:end


def parse_args(args=None):
  """Parse argv."""

  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      '--sql-queries',
      action='append',
      type=lambda csv: csv.split(','),
      required=True,
      help=(
          'Comma-separated list of SQL files to run. If you pass this argument'
          ' many times then it will run each SQL query list/stream in'
          ' parallel.'
      ),
  )
  data_group = parser.add_mutually_exclusive_group()
  data_group.add_argument(
      '--database', help='Hive database to look for data in.'
  )
  data_group.add_argument(
      '--table-base-dir',
      help=(
          'Base HCFS path containing the table data to be registered into Spark'
          ' temporary view.'
      ),
  )
  data_group.add_argument(
      '--bigquery-dataset',
      help=(
          'BQ Dataset containing the tables passed in --table-names to be'
          ' registered into Spark temporary view.'
      ),
  )
  parser.add_argument(
      '--table-names',
      nargs='+',
      help='Names of the tables to be registered into Spark temporary view.',
  )
  parser.add_argument(
      '--table-format',
      help=(
          'Format of data to be registered into Spark temporary view as passed'
          ' to `spark.read.format()`. Assumed to be "parquet", or "bigquery" if'
          ' a BQ dataset is also specified.'
      ),
  )
  parser.add_argument(
      '--bigquery-read-data-format',
      help=(
          'The record format to use when connecting to BigQuery storage. See:'
          ' https://github.com/GoogleCloudDataproc/spark-bigquery-connector#properties'
      ),
  )
  parser.add_argument(
      '--csv-delimiter', help='CSV delimiter to load CSV files', default=','
  )
  parser.add_argument(
      '--enable-hive',
      type=bool,
      default=False,
      help='Whether to try to read data from Hive.',
  )
  parser.add_argument(
      '--table-cache',
      choices=['eager', 'lazy'],
      help='Whether to cache the tables in memory spilling to local-disk.',
  )
  results_group = parser.add_mutually_exclusive_group(required=True)
  results_group.add_argument(
      '--log-results',
      type=bool,
      default=False,
      help=(
          'Log query timings to stdout/stderr instead of writing them to some'
          ' object storage location. Reduces runner latency (and hence its'
          ' total wall time), but it is not supported by all DPB services.'
      ),
  )
  results_group.add_argument(
      '--report-dir',
      help='Directory to write out query timings to.',
  )
  parser.add_argument(
      '--fail-on-query-execution-errors',
      type=bool,
      default=False,
      help=(
          'Fail the whole script on an error while executing the queries, '
          'instead of continuing and not reporting that query run time (the '
          'default).'
      ),
  )
  parser.add_argument(
      '--dump-spark-conf',
      help=(
          'Directory to dump the spark conf props for this job. For debugging '
          'purposes.'
      ),
  )
  parser.add_argument(
      '--catalog',
      help='Spark catalog to look for data in.',
  )
  if args is None:
    return parser.parse_args()
  return parser.parse_args(args)


def get_results_logger(spark_session):
  """Gets results logger.

  Injected into main fn to be replaceable by wrappers.

  Args:
    spark_session: Spark Session object.

  Returns:
    A python logger object.
  """
  del spark_session
  return _results_logger


def main(args, results_logger_getter=get_results_logger):
  builder = sql.SparkSession.builder.appName('Spark SQL Query')
  if args.enable_hive:
    builder = builder.enableHiveSupport()
  query_streams = args.sql_queries
  if len(query_streams) > 1:
    # this guarantees all query streams will use more or less the same resources
    builder = builder.config('spark.scheduler.mode', 'FAIR')
  spark = builder.getOrCreate()
  # Queries supplied to this script by default are not ANSI-compliant, so we are
  # disabling ANSI SQL explictly, since new Spark versions default to it.
  spark.conf.set('spark.sql.ansi.enabled', 'false')
  if args.catalog:
    spark.catalog.setCurrentCatalog(args.catalog)
  if args.database:
    spark.catalog.setCurrentDatabase(args.database)
  for name, (fmt, options) in get_table_metadata(args).items():
    logging.info('Loading %s', name)
    spark.read.format(fmt).options(**options).load().createTempView(name)
  if args.table_cache:
    # This captures both tables in args.database and views from table_metadata
    for table in spark.catalog.listTables():
      spark.sql(
          'CACHE {lazy} TABLE {name}'.format(
              lazy='LAZY' if args.table_cache == 'lazy' else '', name=table.name
          )
      )
  if args.dump_spark_conf:
    logging.info(
        'Dumping the spark conf properties to %s', args.dump_spark_conf
    )
    props = [
        sql.Row(key=key, val=val) for key, val in spark.conf.getAll().items()
    ]
    spark.createDataFrame(props).coalesce(1).write.mode('append').json(
        args.dump_spark_conf
    )

  threads = len(query_streams)
  executor = futures.ThreadPoolExecutor(max_workers=threads)
  result_futures = [
      executor.submit(
          run_sql_query, spark, stream, i, args.fail_on_query_execution_errors
      )
      for i, stream in enumerate(query_streams)
  ]
  futures.wait(result_futures)
  results = []
  for f in result_futures:
    results += f.result()

  if args.log_results:
    dumped_results = '\n'.join([
        '----@spark_sql_runner:results_start@----',
        json.dumps(results),
        '----@spark_sql_runner:results_end@----',
    ])
    results_logger = results_logger_getter(spark)
    results_logger.info(dumped_results)
  else:
    logging.info('Writing results to %s', args.report_dir)
    results_as_rows = [
        sql.Row(
            stream=r['stream'], query_id=r['query_id'], duration=r['duration']
        )
        for r in results
    ]
    spark.createDataFrame(results_as_rows).coalesce(1).write.mode(
        'append'
    ).json(args.report_dir)


def get_table_metadata(args):
  """Gets table metadata to create temporary views according to args passed."""
  metadata = {}
  if args.table_base_dir:
    for table_name in args.table_names:
      option_params = {'path': os.path.join(args.table_base_dir, table_name)}
      if args.table_format == 'csv':
        option_params['header'] = 'true'
        option_params['delimiter'] = args.csv_delimiter
      metadata[table_name] = (args.table_format or 'parquet', option_params)
  elif args.bigquery_dataset:
    for table_name in args.table_names:
      bq_options = {'table': '.'.join([args.bigquery_dataset, table_name])}
      if args.bigquery_read_data_format:
        bq_options['readDataFormat'] = args.bigquery_read_data_format
      metadata[table_name] = (args.table_format or 'bigquery', bq_options)
  return metadata


def run_sql_query(
    spark_session, query_stream, stream_id, raise_query_execution_errors
):
  """Runs a SQL query stream, returns list[dict] with durations."""

  results = []
  for query_id in query_stream:
    query = QUERIES[query_id]

    try:
      logging.info('Running query %s', query_id)
      start = time.time()
      df = spark_session.sql(query)
      df.collect()
      duration = time.time() - start
      results.append(
          {'stream': stream_id, 'query_id': query_id, 'duration': duration}
      )
    # These correspond to errors in low level Spark Excecution.
    # Let ParseException and AnalysisException fail the job.
    except (
        sql.utils.QueryExecutionException,
        py4j.protocol.Py4JJavaError,
    ) as e:
      logging.error('Query %s failed', query_id, exc_info=e)
      if raise_query_execution_errors:
        raise
  return results


if __name__ == '__main__':
  main(parse_args())
