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


def parse_args(args=None):
  """Parse argv."""

  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      '--sql-scripts',
      action='append',
      type=lambda csv: csv.split(','),
      required=True,
      help='Comma-separated list of SQL files to run located inside the object '
           'storage directory specified with --sql-scripts-dir. If you pass '
           'argument many times then it will run each SQL script list/stream '
           'in parallel.',
  )
  parser.add_argument(
      '--sql-scripts-dir',
      required=True,
      help='Object storage path where the SQL queries are located.',
  )
  parser.add_argument('--database', help='Hive database to look for data in.')
  parser.add_argument(
      '--table-metadata',
      metavar='METADATA_FILE',
      help="""\
HCFS file containing JSON Object mapping table names to arrays of length 2.
The arrays contain the format of the data and the options to pass to the
dataframe reader. e.g.:
{

  "my_bq_table": ["bigquery", {"table": "bigquery_public_data:dataset.table"}],
  "my_parquet_table": ["parquet", {"path": "gs://some/directory"}]
}""")
  parser.add_argument(
      '--enable-hive',
      type=bool,
      default=False,
      help='Whether to try to read data from Hive.')
  parser.add_argument(
      '--table-cache',
      choices=['eager', 'lazy'],
      help='Whether to cache the tables in memory spilling to local-disk.')
  parser.add_argument(
      '--report-dir',
      required=True,
      help='Directory to write out query timings to.')
  parser.add_argument(
      '--fail-on-query-execution-errors',
      type=bool,
      default=False,
      help='Fail the whole script on an error while executing the queries, '
           'instead of continuing and not reporting that query run time (the '
           'default).'
  )
  if args is None:
    return parser.parse_args()
  return parser.parse_args(args)


def load_file(spark, object_path):
  """Load an HCFS file into a string."""
  return '\n'.join(spark.sparkContext.textFile(object_path).collect())


def main(args):
  builder = sql.SparkSession.builder.appName('Spark SQL Query')
  if args.enable_hive:
    builder = builder.enableHiveSupport()
  script_streams = get_script_streams(args)
  if len(script_streams) > 1:
    # this guarantees all query streams will use more or less the same resources
    builder = builder.config('spark.scheduler.mode', 'FAIR')
  spark = builder.getOrCreate()
  if args.database:
    spark.catalog.setCurrentDatabase(args.database)
  table_metadata = []
  if args.table_metadata:
    table_metadata = json.loads(load_file(spark, args.table_metadata)).items()
  for name, (fmt, options) in table_metadata:
    logging.info('Loading %s', name)
    spark.read.format(fmt).options(**options).load().createTempView(name)
  if args.table_cache:
    # This captures both tables in args.database and views from table_metadata
    for table in spark.catalog.listTables():
      spark.sql('CACHE {lazy} TABLE {name}'.format(
          lazy='LAZY' if args.table_cache == 'lazy' else '',
          name=table.name))

  results = []

  threads = len(script_streams)
  executor = futures.ThreadPoolExecutor(max_workers=threads)
  result_futures = [
      executor.submit(
          run_sql_script, spark, stream, i, args.fail_on_query_execution_errors)
      for i, stream in enumerate(script_streams)
  ]
  futures.wait(result_futures)
  results = []
  for f in result_futures:
    results += f.result()
  logging.info('Writing results to %s', args.report_dir)
  spark.createDataFrame(results).coalesce(1).write.mode('append').json(
      args.report_dir)


def get_script_streams(args):
  """Gets the script streams to run.

  Args:
    args: Argument object as returned by ArgumentParser.parse_args().

  Returns:
    A list of list of str. Each list of str represents a sequence of full object
    storage paths to SQL files that will be executed in order.
  """
  return [
      [os.path.join(args.sql_scripts_dir, q) for q in stream]
      for stream in args.sql_scripts
  ]


def run_sql_script(
    spark_session, script_stream, stream_id, raise_query_execution_errors):
  """Runs a SQL script stream, returns list[pyspark.sql.Row] with durations."""

  results = []
  for script in script_stream:
    # Read script from object storage using rdd API
    query = load_file(spark_session, script)

    try:
      logging.info('Running %s', script)
      start = time.time()
      # spark-sql does not limit its output. Replicate that here by setting
      # limit to max Java Integer. Hopefully you limited the output in SQL or
      # you are going to have a bad time. Note this is not true of all TPC-DS or
      # TPC-H queries and they may crash with small JVMs.
      # pylint: disable=protected-access
      df = spark_session.sql(query)
      df.show(spark_session._jvm.java.lang.Integer.MAX_VALUE)
      # pylint: enable=protected-access
      duration = time.time() - start
      results.append(
          sql.Row(stream=stream_id, script=script, duration=duration))
    # These correspond to errors in low level Spark Excecution.
    # Let ParseException and AnalysisException fail the job.
    except (sql.utils.QueryExecutionException,
            py4j.protocol.Py4JJavaError) as e:
      logging.error('Script %s failed', script, exc_info=e)
      if raise_query_execution_errors:
        raise
  return results


if __name__ == '__main__':
  main(parse_args())
