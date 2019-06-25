"""Creates Spark table for Spark SQL benchmark.

sys.argv[1]: The table name in the dataset that this script will create.
sys.argv[2]: The data path of the table.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
from pyspark.sql import SparkSession


def main():
  spark = (SparkSession.builder
           .appName('Setup Spark table')
           .enableHiveSupport()
           .getOrCreate())
  table = sys.argv[2]
  table_dir = os.path.join(sys.argv[1], table)
  # clean up previous table
  spark.sql('drop table if exists ' + table)
  # register new table
  spark.catalog.createTable(table, table_dir, source='parquet')

if __name__ == '__main__':
  main()
