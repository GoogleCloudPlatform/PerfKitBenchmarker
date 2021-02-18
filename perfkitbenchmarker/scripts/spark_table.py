"""A PySpark driver that creates Spark tables for Spark SQL benchmark.

It takes an HCFS directory and a list of the names of the subdirectories of that
root directory. The subdirectories each hold Parquet data and are to be
converted into a table of the same name. The subdirectories are explicitly
providing because listing HCFS directories in PySpark is ugly.

sys.argv[1]: The root HCFS directory
sys.argv[2]: A comma separated list of the subdirectories/table names
"""


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
    # Compute column statistics. Spark persists them in the TBL_PARAMS table of
    # the Hive Metastore. I do not believe this interoperates with Hive's own
    # statistics. See
    # https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-LogicalPlan-AnalyzeColumnCommand.html
    columns = ','.join(spark.table(table).columns)
    spark.sql(
        'ANALYZE TABLE {} COMPUTE STATISTICS FOR COLUMNS {}'.format(
            table, columns))
if __name__ == '__main__':
  main()
