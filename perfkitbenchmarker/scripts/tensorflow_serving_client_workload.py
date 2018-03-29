"""Tensorflow Serving client workload.

Performs image classification requests against a Tensorflow Model Server.
Inspired by
https://github.com/tensorflow/serving/blob/master/tensorflow_serving/example/inception_client.py

This client-side load generator does the following:
  * launches a specified number of worker threads (FLAGS.num_threads).
  * each thread chooses a random image from the dataset (FLAGS.image_directory)
    and sends a prediction request to the server, notes the latency,
    and repeats with a new random image.
  * once the specified time period is up (FLAGS.runtime),
    the results are printed to stdout.

The following stats are reported:
  * number of successful prediction requests
  * number of failed requests
  * throughput (successful requests / second)
  * runtime
  * number of threads used
  * a list of all measured latencies
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import os
import random
import sys
import threading
import time

from absl import app
from absl import flags
from grpc.beta import implementations
from grpc.framework.interfaces.face.face import ExpirationError
import tensorflow as tf
from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import prediction_service_pb2

ILSVRC_VALIDATION_IMAGES = 'ILSVRC2012_img_val'
MODEL_NAME = 'inception'
RANDOM_SEED = 98103
DEFAULT_TIMEOUT = 3600  # one hour "infinite" timeout

FLAGS = flags.FLAGS
flags.DEFINE_string('server', 'localhost:9000', 'PredictionService host:port')
flags.DEFINE_string(
    'image_directory', ILSVRC_VALIDATION_IMAGES,
    'Path to a directory containing images to be classified. '
    'A random image from the directory will be chosen for '
    'every classification request.')
flags.DEFINE_integer('runtime', 60, 'Runtime in seconds.')
flags.DEFINE_integer('num_threads', 16,
                     'Number of concurrent worker threads to launch.')
flags.DEFINE_integer('rpc_timeout', DEFAULT_TIMEOUT,
                     'Number of seconds to set the rpc timeout to.')

tf.logging.set_verbosity(tf.logging.ERROR)


def get_files_in_directory_sorted(directory):
  """Returns a list of files in directory, sorted alphabetically."""
  return sorted([
      os.path.join(directory, name)
      for name in os.listdir(directory)
      if os.path.isfile(os.path.join(directory, name))
  ])


class TfServingClientWorkload(object):
  """Tensorflow Serving client workload generator.

  See module-level docstring for more details.
  """

  def __init__(self):
    self.thread_lock = threading.Lock()
    self.num_completed_requests = 0
    self.num_failed_requests = 0
    self.latencies = []
    self.file_list = get_files_in_directory_sorted(FLAGS.image_directory)
    self.num_images = len(self.file_list)

    host, port = FLAGS.server.split(':')
    channel = implementations.insecure_channel(host, int(port))
    self.stub = prediction_service_pb2.beta_create_PredictionService_stub(
        channel)

    # Fix random seed so that sequence of images sent to server is
    # deterministic.
    random.seed(RANDOM_SEED)

  def get_random_image(self):
    """Returns a random image from self.file_list."""
    random_index = random.randint(0, self.num_images - 1)
    return self.file_list[random_index]

  def classify_random_image(self):
    """Chooses a random image and sends a prediction request to the server.

    If a response is receieved before the requests timesout, its latency is
    saved, and the request is counted as successful. If the request timesout
    or otherwise errors, its latency is discarded, and it is counted as a
    failed request.
    """
    image = self.get_random_image()
    with open(image, 'rb') as f:
      data = f.read()
      request = predict_pb2.PredictRequest()
      request.model_spec.name = MODEL_NAME
      request.model_spec.signature_name = 'predict_images'
      request.inputs['images'].CopyFrom(
          tf.contrib.util.make_tensor_proto(data, shape=[1]))

      try:
        start_time = time.time()
        self.stub.Predict(request, FLAGS.rpc_timeout)
        end_time = time.time()
        with self.thread_lock:
          self.num_completed_requests += 1
          self.latencies.append(end_time - start_time)

      except ExpirationError:
        with self.thread_lock:
          self.num_failed_requests += 1

  def run_worker_thread(self):
    """Continuously calls classify_random_image until time is up."""
    while (datetime.now() - self.start_time).seconds < FLAGS.runtime:
      self.classify_random_image()

  def start(self):
    """Creates and launches worker threads and waits for them to finish."""
    threads = []
    for _ in range(FLAGS.num_threads):
      threads.append(threading.Thread(target=self.run_worker_thread))

    self.start_time = datetime.now()
    for t in threads:
      t.start()

    for t in threads:
      t.join()
    self.end_time = datetime.now()

  def print_results(self, out=sys.stdout):
    """Prints tests results, to stdout by default."""
    actual_runtime = (self.end_time - self.start_time).total_seconds()
    req_per_second = self.num_completed_requests / actual_runtime
    out.write('Completed requests: %s\n' % self.num_completed_requests)
    out.write('Failed requests: %s\n' % self.num_failed_requests)
    out.write('Runtime: %s\n' % actual_runtime)
    out.write('Number of threads: %s\n' % FLAGS.num_threads)
    out.write('Throughput: %s\n' % req_per_second)
    out.write('Latency:\n')
    for latency in self.latencies:
      out.write(str(latency) + '\n')


def main(argv):
  """Runs the test and prints results to stdout."""
  del argv
  load_test = TfServingClientWorkload()
  load_test.start()
  load_test.print_results()


if __name__ == '__main__':
  app.run(main)
