# Lint as: python2, python3
"""Runs a Spark SQL query with preloaded temp views.

Views can be BigQuery tables or HCFS directories containing Parquet.
This is useful for Storage formats not expressible as External Hive Tables.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
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
  parser.add_argument(
      '--table-metadata',
      metavar='METADATA',
      type=lambda s: json.loads(s).items(),
      default={},
      help="""\
JSON Object mappiing table names to arrays of length 2. The arrays contain  the
format of the data and the options to pass to the dataframe reader. e.g.:
{

  "my_bq_table": ["bigquery", {"table": "bigquery_public_data:dataset.table"}],
  "my_parquet_table": ["parquet", {"path": "gs://some/directory"}]
}""")
  parser.add_argument(
      '--report-dir',
      required=True,
      help='Directory to write out query timings to.')
  return parser.parse_args()


def main(args):
  spark = (sql.SparkSession.builder
           .appName('Spark SQL Query')
           .enableHiveSupport()
           .getOrCreate())
  for name, (fmt, options) in args.table_metadata:
    logging.info('Loading %s', name)
    spark.read.format(fmt).options(**options).load().createTempView(name)

  results = []
  for script in args.sql_scripts:
    # Read script from object storage using rdd API
    query = '\n'.join(spark.sparkContext.textFile(script).collect())

    try:
      logging.info('Running %s', script)
      start = time.time()
      # spark-sql does not limit its output. Replicate that here by setting
      # limit to max Java Integer. Hopefully you limited the output in SQL or
      # you are going to have a bad time. Note this is not true of all TPC-DS or
      # TPC-H queries and they may crash with small JVMs.
      # pylint: disable=protected-access
      spark.sql(query).show(spark._jvm.java.lang.Integer.MAX_VALUE)
      # pylint: enable=protected-access
      duration = time.time() - start
      results.append(sql.Row(script=script, duration=duration))
    # These correspond to errors in low level Spark Excecution.
    # Let ParseException and AnalysisException fail the job.
    except (sql.utils.QueryExecutionException,
            py4j.protocol.Py4JJavaError) as e:
      logging.error('Script %s failed', script, exc_info=e)

  logging.info('Writing results to %s', args.report_dir)
  spark.createDataFrame(results).coalesce(1).write.json(args.report_dir)


if __name__ == '__main__':
  main(parse_args())
