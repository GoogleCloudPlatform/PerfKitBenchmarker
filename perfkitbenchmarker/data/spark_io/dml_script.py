"""Spark application to create a Spark table for IO intensive benchmarking."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
from pyspark.sql import SparkSession


def main():
  spark = (SparkSession.builder
           .appName('Setup Spark table')
           .enableHiveSupport()
           .getOrCreate())
  table = 'warehouse'
  table_dir = sys.argv[1]
  # clean up previous table
  spark.sql('drop table if exists ' + table)
  # register new table
  spark.catalog.createTable(table, table_dir, source='parquet')

if __name__ == '__main__':
  main()
