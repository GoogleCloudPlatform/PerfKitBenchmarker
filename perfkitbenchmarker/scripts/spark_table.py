# Lint as: python2, python3
"""A PySpark driver that creates Spark tables for Spark SQL benchmark.

It takes an HCFS directory and a list of the names of the subdirectories of that
root directory. The subdirectories each hold Parquet data and are to be
converted into a table of the same name. The subdirectories are explicitly
providing because listing HCFS directories in PySpark is ugly.

sys.argv[1]: The root HCFS directory
sys.argv[2]: A comma separated list of the subdirectories/table names
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('root_dir')
  parser.add_argument('tables', type=lambda csv: csv.split(','))
  args = parser.parse_args()
  spark = (SparkSession.builder
           .appName('Setup Spark tables')
           .enableHiveSupport()
           .getOrCreate())
  for table in args.tables:
    logging.info('Creating table %s', table)
    table_dir = os.path.join(args.root_dir, table)
    # clean up previous table
    spark.sql('DROP TABLE IF EXISTS ' + table)
    # register new table
    spark.catalog.createTable(table, table_dir, source='parquet')
    try:
      # This loads the partitions under the table if table is partitioned.
      spark.sql('MSCK REPAIR TABLE ' + table)
    except AnalysisException:
      # The table was not partitioned, which was presumably expected
      pass
if __name__ == '__main__':
  main()
