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

import os
import sys
from pyspark.sql import SparkSession


def main():
  spark = (SparkSession.builder
           .appName('Setup Spark tables')
           .enableHiveSupport()
           .getOrCreate())
  root_dir = sys.argv[1]
  tables = sys.argv[2].split(',')
  for table in tables:
    table_dir = os.path.join(root_dir, table)
    # clean up previous table
    spark.sql('drop table if exists ' + table)
    # register new table
    spark.catalog.createTable(table, table_dir, source='parquet')

if __name__ == '__main__':
  main()
