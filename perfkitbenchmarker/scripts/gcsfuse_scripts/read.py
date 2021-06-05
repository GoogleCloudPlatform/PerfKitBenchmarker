"""Script that concurrently reads files to test the max throughput.

Example usages:

(1) Specify `--mountpoint` to read from gcsfuse.
> gsutil ls gs://gcsfuse-benchmark/10M/ | python read.py --mountpoint=/gcs/

(2) Omit `--mountpoint` to read from GCS using tf.io.gfile; specify
`--iterations` to run it multiple times.
> gsutil ls gs://gcsfuse-benchmark/10M/ | python read.py --iterations=3
"""
import concurrent.futures
import sys
import time
from typing import Tuple

from absl import flags
from absl import logging
import tensorflow as tf

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    "iterations", 1, "Number of iterations this benchmark should repeated run.")

flags.DEFINE_integer("workers", 16,
                     "Number of workers this benchmark runs concurrently.")

flags.DEFINE_string(
    "mountpoint", None,
    "The directory where all the GCS buckets are mounted. If "
    "omitted, the benchmark reads the objects with tf.io.gfile "
    "instead.")

flags.DEFINE_bool("verbose", False, "Print the results with extra information.")


class ObjectReader:
  """Provides a function to open and read an object as a file from GCS."""

  def __init__(self, object_name, mountpoint):
    self.object_name = object_name
    self.mountpoint = mountpoint

  def OpenFile(self):
    """Opens a Gfile or a file in the file system mounted by gcsfuse.

    Returns:
      An opened file or Gfile.
    """
    if self.mountpoint:
      file_name = self.object_name.replace("gs://", self.mountpoint)
      return open(file_name, "rb")
    else:
      return tf.io.gfile.GFile(self.object_name, "rb")

  def ReadFull(self):
    """Reads the entire file and returns the bytes read.

    Returns:
      The number of bytes read from the file.
    """
    bytes_read = 0
    with self.OpenFile() as f:
      while True:
        data = f.read(2 * 1024 * 1024)
        if data:
          bytes_read += len(data)
        else:
          break
    return bytes_read


class ReadBenchmark:
  """Runs a benchmark by reading files and measure the throughput."""

  def __init__(self):
    self.iterations = FLAGS.iterations
    self.executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=FLAGS.workers)

    objects = sys.stdin.read().split("\n")
    self.readers = [ObjectReader(o, FLAGS.mountpoint) for o in objects if o]

  def Run(self) -> None:
    """Run the benchmark N times, printing all the metrics per iteration."""
    for it in range(self.iterations):
      total_mb, duration_sec = self.RunAllReaders()
      self.PrintResult(it, total_mb, duration_sec)
    self.executor.shutdown()

  def RunAllReaders(self) -> Tuple[float, float]:
    """Read all files, returning bytes read and duration.

    Returns:
      A tuple including a total number of Megabyes read, and duration taken in
      seconds.
    """
    start = time.time()
    size_list = list(self.executor.map(lambda r: r.ReadFull(), self.readers))
    total_mb = sum(size_list) * 1.0 / (1024 * 1024)
    duration_sec = time.time() - start
    return total_mb, duration_sec

  def PrintResult(self, iteration: int, total_mb: float,
                  duration_sec: float) -> None:
    """Print the benchmark result.

    Args:
      iteration: An int N indicates this result comes from N-th iteration.
      total_mb: Total amount of data being read in the iteration, in Megabytes.
      duration_sec: The seconds it took to finish this iteration.
    """
    throughput = total_mb / duration_sec
    if FLAGS.verbose:
      info = "#{}: {} MB, {:.1f} seconds, {:.1f} MB/s".format(
          iteration, total_mb, duration_sec, throughput)
      print(info)
    else:
      print(throughput)


if __name__ == "__main__":
  try:
    FLAGS(sys.argv)
  except flags.Error as e:
    logging.exception("%s\nUsage: %s ARGS\n%s", e, sys.argv[0], FLAGS)
    sys.exit(1)
  ReadBenchmark().Run()
