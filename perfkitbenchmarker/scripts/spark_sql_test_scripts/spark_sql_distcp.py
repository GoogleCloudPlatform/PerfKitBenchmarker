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

from pyspark import sql


def parse_args():
  """Parse argv."""

  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      '--source-metadata',
      required=True,
      metavar='METADATA_FILE',
      help="""\
HCFS file containing JSON array of arrays of length 2.
The arrays contain the format of the data and the options to pass to the
dataframe reader. e.g.:
[
  ["bigquery", {"table": "bigquery_public_data:dataset.table"}],
  ["parquet", {"path": "gs://some/directory"}]
]""")
  parser.add_argument(
      '--destination-metadata',
      required=True,
      metavar='METADATA_FILE',
      help="""\
HCFS file containing JSON array of arrays of length 2.
The arrays contain the format of the data and the options to pass to the
dataframe writer. e.g.:
[
  ["bigquery", {"table": "bigquery_public_data:dataset.table"}],
  ["parquet", {"path": "gs://some/directory"}]
]""")
  return parser.parse_args()


def load_file(spark, object_path):
  """Load an HCFS file into a string."""
  return '\n'.join(spark.sparkContext.textFile(object_path).collect())


def main(args):
  spark = sql.SparkSession.builder.appName('Spark SQL DistCp').getOrCreate()
  source_metadata = json.loads(load_file(spark, args.source_metadata))
  dest_metadata = json.loads(load_file(spark, args.destination_metadata))
  for (r_fmt, r_options), (w_fmt, w_options) in zip(source_metadata,
                                                    dest_metadata):
    df = spark.read.format(r_fmt).options(**r_options).load()
    # TODO(saksena): Split into read and write times
    # If we want to use this as a benchmark instead of just as a utility, we
    # should split read and write time by measuring the duration of
    # read: df.cache().count() and write: (the next line)
    # Also report per table durations (and data size?)
    df.write.format(w_fmt).options(**w_options).save()

if __name__ == '__main__':
  main(parse_args())
