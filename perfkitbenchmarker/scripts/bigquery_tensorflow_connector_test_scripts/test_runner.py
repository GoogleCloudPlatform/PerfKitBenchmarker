# Lint as: python2, python3
"""Runs a BigQuery TensorFlow connector benchmark."""

import time

from absl import app
from absl import flags
from google.cloud import bigquery
import tensorflow as tf
from tensorflow.python.framework import dtypes
from tensorflow_io.bigquery import BigQueryClient


FLAGS = flags.FLAGS
flags.DEFINE_string("project_id", None,
                    "GCP project id benchmark is run under.")
flags.mark_flag_as_required("project_id")
flags.DEFINE_string("dataset_project_id", "bigquery-public-data",
                    "GCP project where dataset is located.")
flags.DEFINE_string("dataset_id", "baseball", "Dataset id.")
flags.DEFINE_string("table_id", "games_wide", "Table id.")
flags.DEFINE_integer("num_iterations", 1000, "Number of batches to load.")
flags.DEFINE_integer("num_warmup_iterations", 10,
                     "Number of warmup batches to load that doesn't count "
                     "towards benchmark results.")

flags.DEFINE_integer("requested_streams", 1, "Number of streams.")
flags.DEFINE_integer("batch_size", 2048, "Batch size.")
flags.DEFINE_integer("prefetch_size", None, "Prefetch size.")
flags.DEFINE_integer(
    "mini_batch_size", 100, "Mini batch size - to divide num_iterations."
)
flags.DEFINE_integer("num_columns", 120, "Number of columns to read.")
flags.DEFINE_bool(
    "sloppy",
    False,
    "If True the implementation is allowed, for the sake of expediency, "
    "to produce elements in a non-deterministic order",
)
flags.DEFINE_enum("format", "AVRO", ["AVRO", "ARROW"],
                  "Serialization format - AVRO or ARROW")


def convert_field_type(field_type):
  if field_type == "STRING":
    return dtypes.string
  if field_type == "INTEGER":
    return dtypes.int64
  if field_type == "TIMESTAMP":
    return dtypes.int64
  raise ValueError(f"unsupported field_type:{field_type}")


def get_dataset_schema(dataset_project_id, dataset_id, table_id):
  client = bigquery.Client(project=FLAGS.project_id)
  dataset_ref = client.dataset(dataset_id, project=dataset_project_id)
  table_ref = dataset_ref.table(table_id)
  table = client.get_table(table_ref)
  column_names = [field.name for field in table.schema]
  output_types = [convert_field_type(field.field_type)
                  for field in table.schema]
  return (column_names, output_types)


def get_dataset_from_bigquery(dataset_project_id, dataset_id, table_id):
  """Reads data from BigQuery and returns it as a TensorFlow dataset."""
  (selected_fields, output_types) = get_dataset_schema(
      dataset_project_id,
      dataset_id,
      table_id)

  client = BigQueryClient()

  read_session = client.read_session(
      "projects/" + FLAGS.project_id,
      dataset_project_id,
      table_id,
      dataset_id,
      selected_fields=selected_fields,
      output_types=output_types,
      requested_streams=FLAGS.requested_streams,
      data_format=BigQueryClient.DataFormat[FLAGS.format])

  streams = read_session.get_streams()
  print(
      "Requested %d streams, BigQuery returned %d streams"
      % (FLAGS.requested_streams, len(streams))
  )

  def read_rows(stream):
    dataset = read_session.read_rows(stream)
    if FLAGS.batch_size != 1:
      dataset = dataset.batch(FLAGS.batch_size)
    return dataset

  streams_count = tf.size(streams)
  streams_count64 = tf.cast(streams_count, dtype=tf.int64)
  streams_ds = tf.data.Dataset.from_tensor_slices(streams)
  dataset = streams_ds.interleave(
      read_rows,
      cycle_length=streams_count64,
      num_parallel_calls=streams_count64,
      deterministic=not FLAGS.sloppy)

  if FLAGS.prefetch_size is not None:
    dataset = dataset.prefetch(FLAGS.prefetch_size)

  return dataset.repeat()


def run_benchmark(_):
  """Runs a BigQuery TensorFlow Connector benchmark."""
  dataset = get_dataset_from_bigquery(FLAGS.dataset_project_id,
                                      FLAGS.dataset_id,
                                      FLAGS.table_id)
  num_iterations = FLAGS.num_iterations
  batch_size = FLAGS.batch_size

  itr = tf.compat.v1.data.make_one_shot_iterator(dataset)
  mini_batch = FLAGS.mini_batch_size

  print("Started benchmark warmup")

  for _ in range(FLAGS.num_warmup_iterations):
    _ = itr.get_next()

  print("Started benchmark")

  n = 0
  start = time.time()
  for _ in range(num_iterations // mini_batch):
    local_start = time.time()
    start_n = n
    for _ in range(mini_batch):
      n += batch_size
      _ = itr.get_next()

    local_end = time.time()
    print(
        "Processed %d entries in %f seconds. [%f] rows/s"
        % (
            n - start_n,
            local_end - local_start,
            (mini_batch * batch_size) / (local_end - local_start),
        )
    )

  end = time.time()
  print("Processed %d entries in %f seconds. [%f] rows/s" %
        (n, end - start, n / (end - start)))
  print("Benchmark result: [%f] rows/s" % (n / (end - start)))

# Run as:
# pylint: disable=line-too-long
# python3 test_runner.py --project_id=<your project id> --batch_size=2048 --num_iterations=100 --mini_batch_size=10 --num_columns=120 --requested_streams=20
if __name__ == "__main__":
  app.run(run_benchmark)
