# Lint as: python2, python3
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
import time

import py4j
from pyspark import sql


def parse_args():
  """Parse argv."""

  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      '--sql-scripts',
      type=lambda csv: csv.split(','),
      required=True,
      help='List of SQL scripts staged in object storage to run')
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
      '--simultaneous',
      type=bool,
      default=False,
      help='Run all queries simultaneously instead of one by one.')
  return parser.parse_args()


def load_file(spark, object_path):
  """Load an HCFS file into a string."""
  return '\n'.join(spark.sparkContext.textFile(object_path).collect())


def main(args):
  builder = sql.SparkSession.builder.appName('Spark SQL Query')
  if args.enable_hive:
    builder = builder.enableHiveSupport()
  if args.simultaneous:
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

  if args.simultaneous:
    threads = len(args.sql_scripts)
    executor = futures.ThreadPoolExecutor(max_workers=threads)
    result_futures = [
        executor.submit(run_sql_script, spark, script)
        for script in args.sql_scripts
    ]
    futures.wait(result_futures)
    results = [f.result() for f in result_futures]
  else:
    results = [run_sql_script(spark, script) for script in args.sql_scripts]
  results = [r for r in results if r is not None]
  logging.info('Writing results to %s', args.report_dir)
  spark.createDataFrame(results).coalesce(1).write.mode('overwrite').json(
      args.report_dir)


def run_sql_script(spark_session, script):
  """Runs a SQL script, returns a pyspark.sql.Row with its duration."""

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
    return sql.Row(script=script, duration=duration)
  # These correspond to errors in low level Spark Excecution.
  # Let ParseException and AnalysisException fail the job.
  except (sql.utils.QueryExecutionException,
          py4j.protocol.Py4JJavaError) as e:
    logging.error('Script %s failed', script, exc_info=e)


if __name__ == '__main__':
  main(parse_args())
