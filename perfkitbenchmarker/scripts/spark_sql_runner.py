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

from pyspark.sql import SparkSession


def parse_args():
  """Parse argv."""

  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      'sql_script', help='The local path to the SQL file to run')
  parser.add_argument(
      '--table_metadata',
      metavar='METADATA',
      type=lambda s: json.loads(s).items(),
      default=[],
      help="""\
JSON Object mappiing table names to arrays of length 2. The arrays contain  the
format of the data and the options to pass to the dataframe reader. e.g.:
{

  "my_bq_table": ["bigquery", {"table": "bigquery_public_data:dataset.table"}],
  "my_parquet_table": ["parquet", {"path": "gs://some/directory"}]
}""")
  return parser.parse_args()


def main(args):
  spark = (SparkSession.builder.appName('Spark SQL Query').getOrCreate())
  for name, (fmt, options) in args.table_metadata:
    logging.info('Loading %s', name)
    spark.read.format(fmt).options(**options).load().createTempView(name)

  logging.info('Running %s', args.sql_script)
  with open(args.sql_script) as f:
    sql = f.read()

  # spark-sql does not limit it's output. Replicate that here by setting limit
  # to max Java Integer. Hopefully you limited the output in SQL or you are
  # going to have a bad time.
  # pylint: disable=protected-access
  spark.sql(sql).show(spark._jvm.java.lang.Integer.MAX_VALUE)
  # pylint: enable=protected-access


if __name__ == '__main__':
  main(parse_args())
