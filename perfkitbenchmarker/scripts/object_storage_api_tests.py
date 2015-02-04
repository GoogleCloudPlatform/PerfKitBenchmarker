#!/usr/bin/env python

# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A Test Script to be copied to the test VM to perform various tests to
   object storage via APIs exposed by the provider.

   Note: it is intentional that this test script is NOT dependant on the PKB
   package so we do not have to copy the entire PKB package to test VM just to
   run this script.
"""

import json
import logging
import sys
from threading import Thread
import string
import random
import time

import boto
import gflags as flags
import gcs_oauth2_boto_plugin  # noqa

FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'storage', 'GCS', ['GCS', 'S3', 'AZURE'], 'The target storage to test.')

flags.DEFINE_string('host', None, 'The hostname of the storage endpoint.')

flags.DEFINE_string('bucket', None,
                    'The name of the bucket to test with. Caller is '
                    'responsible to create an empty bucket for a particular '
                    'invocation of this test and then clean-up the bucket '
                    'after this test returns.')

flags.DEFINE_enum(
    'scenario', 'OneByteRW', ['OneByteRW', 'ListConsistency',
                              'SingleStreamThroughput'],
    'The various scenarios to test. OneByteRW: read and write of single byte. '
    'ListConsistency: List-after-write and list-after-update consistency. '
    'SingleStreamThroughput: Throughput of single stream large object RW. (not '
    'implemented yet)')

flags.DEFINE_integer('iterations', 1, 'The number of iterations to run for the '
                     'particular test scenario. Currently only applicable to '
                     'the ListConsistency scenario, ignored in others.')

STORAGE_TO_SCHEMA_DICT = {'GCS': 'gs', 'S3': 's3'}

# If more than 5% of our upload or download operations fail for an iteration,
# there is an availability issue with the service provider or the connection
# between the test VM and the service provider. For this reason, we consider
# this particular iteration invalid, and we will print out an error message
# instead of a set of benchmark numbers (which will be incorrect).
FAILURE_TOLERANCE = 0.05

# Here are some constants used by various benchmark tests below.

# The number of objects used by the one byte RW benchmarks.
ONE_BYTE_OBJECT_COUNT = 1000

# The maximum amount of seconds we are willing to wait for list results to be
# consistent in this benchmark
LIST_CONSISTENCY_WAIT_TIME_LIMIT = 300

# Total number of objects we will provision before we do the list
# This covers 3 pages of List (one page == 1000 objects)
LIST_CONSISTENCY_OBJECT_COUNT = 2100

# Provisioning is done in parallel threads to reduce test iteration time!
# This number specifies the thread count.
LIST_CONSISTENCY_THREAD_COUNT = 100

# Size of large objects used in single stream throughput benchmarking.
LARGE_OBJECT_SIZE_BYTES = 100 * 1024 * 1024

# The number of large objects we use in single stream throughput benchmarking.
LARGE_OBJECT_COUNT = 5

LARGE_OBJECT_FAILURE_TOLERANCE = 0.2


# When a storage provider fails more than a threshold number of requests, we
# stop the benchmarking tests and raise a low availability error back to the
# caller.
class LowAvailabilityError(Exception):
    pass


def _PercentileCalculator(numbers):
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  result = {}
  result['p50'] = numbers_sorted[int(count * 0.5)]
  result['p90'] = numbers_sorted[int(count * 0.9)]
  result['p99'] = numbers_sorted[int(count * 0.99)]
  result['p99.9'] = numbers_sorted[int(count * 0.999)]
  return result


def _ListObjects(storage_schema, bucket, prefix, host_to_connect=None):
  bucket_uri = boto.storage_uri(bucket, storage_schema)
  if host_to_connect is not None:
    bucket_uri.connect(host=host_to_connect)

  list_result = []
  for k in bucket_uri.list_bucket(prefix=prefix):
    list_result.append(k.name)

  return list_result


def DeleteObjects(storage_schema, bucket, objects_to_delete,
                  host_to_connect=None):
  """Delete a bunch of objects.

  Args:
    storage_schema: The address schema identifying a storage. e.g., "gs"
    bucket: Name of the bucket.
    objects_to_delete: A list of names of objects to delete.
    host_to_connect: An optional endpoint string to connect to.
  """

  for object_name in objects_to_delete:
    object_path = '%s/%s' % (bucket, object_name)
    object_uri = boto.storage_uri(object_path, storage_schema)
    if host_to_connect is not None:
      object_uri.connect(host=host_to_connect)

    try:
      object_uri.delete_key()
    except:
      logging.exception('Caught exception while deleting object %s.',
                        object_path)


def WriteObjects(storage_schema, bucket, object_prefix, count,
                 size, objects_written, latency_results=None,
                 host_to_connect=None):
  """Write a number of objects to a storage provider.

  Args:
    storage_schema: The address schema identifying a storage. e.g., "gs"
    bucket: Name of the bucket to write to.
    object_prefix: The prefix of names of objects to be written.
    count: The total number of objects that need to be written.
    size: The size of each object in bytes.
    objects_written: A list of names of objects that have been successfully
        written by this function. Caller supplies the list and this function
        fills in the name of the objects.
    latency_results: An optional parameter that caller can supply to hold
        a latency numbers, in seconds, for each object that is successfully
        written.
    host_to_connect: An optional endpoint string to connect to.
  """
  payload_bytes = bytearray(size)
  for i in range(size):
    payload_bytes[i] = ord(random.choice(string.letters))

  payload_string = payload_bytes.decode('ascii')

  for i in range(count):
    object_name = '%s_%d' % (object_prefix, i)
    object_path = '%s/%s' % (bucket, object_name)
    object_uri = boto.storage_uri(object_path, storage_schema)
    # Note below, this does not really connect, it only sets the connection
    # property, the real http connnection is only established when doing the
    # actual upload.
    if host_to_connect is not None:
      object_uri.connect(host=host_to_connect)

    # Ready to go!
    start_time = time.time()

    # We write the object to storage provider and if successful we place the
    # name of the object to the objects_written list. When a write fails, the
    # object won't be placed into the objects_written list, and then it's up to
    # the caller to decide if they want to tolerate the failure and accept
    # the results, depending on test scenarios.
    try:
      object_uri.set_contents_from_string(payload_string)

      if latency_results is not None:
        latency_results.append(time.time() - start_time)

      objects_written.append(object_name)
    except:
      logging.exception('Caught exception while writing object %s.',
                        object_path)


def ReadObjects(storage_schema, bucket, objects_to_read, latency_results,
                host_to_connect=None):
  for object_name in objects_to_read:
    object_path = '%s/%s' % (FLAGS.bucket, object_name)
    object_uri = boto.storage_uri(object_path, storage_schema)
    object_uri.connect(host=host_to_connect)

    # Ready to go!
    start_time = time.time()

    try:
      object_uri.get_contents_as_string()

      latency_results.append(time.time() - start_time)
    except:
      logging.exception('Failed to read object %s', object_path)


def SingleStreamThroughputBenchmark(storage_schema, host_to_connect=None):
  """ A benchmark test for single stream upload and download throughput.

  Args:
    storage_schema: The schema of the storage provider to use, e.g., "gs"
    host_to_connect: An optional host endpoint to connect to.

  Raises:
    LowAvailabilityError: when the storage provider has failed a high number of
        our RW requests that exceeds a threshold (>5%), we raise this error
        instead of collecting performance numbers from this run.
  """
  object_prefix = 'pkb_single_stream_%f' % time.time()
  write_latency = []
  objects_written = []

  WriteObjects(storage_schema, FLAGS.bucket, object_prefix,
               LARGE_OBJECT_COUNT, LARGE_OBJECT_SIZE_BYTES, objects_written,
               latency_results=write_latency,
               host_to_connect=host_to_connect)

  try:
    if len(objects_written) < LARGE_OBJECT_COUNT * (
                              1 - LARGE_OBJECT_FAILURE_TOLERANCE):  # noqa
      raise LowAvailabilityError('Failed to write required number of large '
                                 'objects, exiting.')

    # Report the p50 as the final result (out of the default 5 run attempts)
    latency_percentiles = _PercentileCalculator(write_latency)
    logging.info('Single stream upload throughput in Bps: %s',
                 LARGE_OBJECT_SIZE_BYTES / float(latency_percentiles['p50']))

    read_latency = []
    ReadObjects(storage_schema, FLAGS.bucket, objects_written, read_latency,
                host_to_connect)
    if len(read_latency) < len(objects_written) * (
                           1 - LARGE_OBJECT_FAILURE_TOLERANCE):  # noqa
      raise LowAvailabilityError('Failed to read required number of objects, '
                                 'exiting.')

    latency_percentiles = _PercentileCalculator(read_latency)
    logging.info('Single stream download throughput in Bps: %s',
                 LARGE_OBJECT_SIZE_BYTES / float(latency_percentiles['p50']))
  finally:
    DeleteObjects(storage_schema, FLAGS.bucket, objects_written,
                  host_to_connect=host_to_connect)


def OneByteRWBenchmark(storage_schema, host_to_connect=None):
  """ A benchmark test for one byte object read and write. It uploads and
  downloads ONE_BYTE_OBJECT_COUNT number of 1-byte objects to the storage
  provider, keeps track of the latency of these operations, and print out the
  result in JSON format at the end of the test.

  Args:
    storage_schema: The schema of the storage provider to use, e.g., "gs"
    host_to_connect: An optional host endpoint to connect to.

  Raises:
    LowAvailabilityError: when the storage provider has failed a high number of
        our RW requests that exceeds a threshold (>5%), we raise this error
        instead of collecting performance numbers from this run.
  """

  # One byte write
  object_prefix = 'pkb_one_byte_%f' % time.time()

  one_byte_write_latency = []
  one_byte_objects_written = []

  WriteObjects(storage_schema, FLAGS.bucket, object_prefix,
               ONE_BYTE_OBJECT_COUNT, 1, one_byte_objects_written,
               latency_results=one_byte_write_latency,
               host_to_connect=host_to_connect)

  try:
    success_count = len(one_byte_objects_written)
    if success_count < ONE_BYTE_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
      raise LowAvailabilityError('Failed to write required number of objects, '
                                 'exiting.')

    logging.info('One byte upload - %s',
                 json.dumps(_PercentileCalculator(one_byte_write_latency),
                            sort_keys=True))

    # Now download these objects and measure the latencies.
    one_byte_read_latency = []
    ReadObjects(storage_schema, FLAGS.bucket, one_byte_objects_written,
                one_byte_read_latency, host_to_connect)

    success_count = len(one_byte_read_latency)
    if success_count < ONE_BYTE_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
      raise LowAvailabilityError('Failed to read required number of objects, '
                                 'exiting.')

    logging.info('One byte download - %s',
                 json.dumps(_PercentileCalculator(one_byte_read_latency),
                            sort_keys=True))
  finally:
    DeleteObjects(storage_schema, FLAGS.bucket, one_byte_objects_written,
                  host_to_connect=host_to_connect)


def ListConsistencyBenchmark(storage_schema, host_to_connect=None):
  """ A benchmark test to measure list-after-write consistency. It uploads
  a large number of 1-byte objects in a short amount of time, and then issues
  a list request. If the first list request returns all objects as expected,
  then the result is deemed consistent. Otherwise, it keeps issuing list request
  until all expected objects are returned, and the test reports the time
  it takes from the end of the last write to the time the list returns
  consistent result.

  Args:
    storage_schema: The schema of the storage provider to use, e.g., "gs"
    host_to_connect: An optional host endpoint to connect to.

  Returns:
    A dictionary that contains the test results:
      'is-list-consistent': True/False
      'list-latency': if list is consistent, what is its latency
      'inconsistency-window': if list is inconsistent, how long did it take to
          reach consistency.

  Raises:
    LowAvailabilityError: when the storage provider has failed a high number of
        our RW requests that exceeds a threshold (>5%), we raise this error
        instead of collecting performance numbers from this run.
  """

  # Provision the test with tons of one byte objects. Write them all at once.
  object_prefix = 'pkb_list_consistency_%f' % time.time()
  final_objects_written = []

  per_thread_objects_written = [[] for i in
                                 range(LIST_CONSISTENCY_THREAD_COUNT)]  # noqa

  threads = []

  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    my_prefix = '%s_%d' % (object_prefix, i)
    thread = Thread(target=WriteObjects,
                    args=(storage_schema, FLAGS.bucket, my_prefix,
                          LIST_CONSISTENCY_OBJECT_COUNT /
                          LIST_CONSISTENCY_THREAD_COUNT,
                          per_thread_objects_written[i], None, host_to_connect))
    thread.daemon = True
    thread.start()
    threads.append(thread)

  logging.debug('All threads started, waiting for them to end...')

  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    try:
      threads[i].join()
      final_objects_written += per_thread_objects_written[i]
    except:
      logging.exception('Caught exception waiting for the %dth thread.', i)
  logging.debug('All threads ended...')

  write_finish_time = time.time()

  final_count = len(final_objects_written)
  if final_count < LIST_CONSISTENCY_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
    raise LowAvailabilityError('Failed to provision required number of '
                               'objects, exiting.')

  logging.info('Done provisioning the objects, objects written %d. Now start '
               'doing the lists...', final_count)

  # Now list this bucket under this prefix, compare the list results with
  # objects_written. If they are not the same, keep doing it until they
  # are the same.

  total_wait_time = 0
  final_objects_written_set = set(final_objects_written)
  list_count = 0
  result_consistent = False
  list_latency = 0
  while total_wait_time < LIST_CONSISTENCY_WAIT_TIME_LIMIT:

    list_start_time = time.time()
    list_result = _ListObjects(storage_schema, FLAGS.bucket, object_prefix,
                               host_to_connect)
    list_count += 1
    list_latency = time.time() - list_start_time

    if final_objects_written_set.difference(set(list_result)):
      total_wait_time = time.time() - write_finish_time
      continue
    else:
      result_consistent = True
      break

  final_result = {}
  if result_consistent:
    logging.debug('Listed %d times until results are consistent.', list_count)
    if list_count == 1:
      logging.debug('List latency is: %f' % list_latency)

      final_result['is-list-consistent'] = True
      final_result['list-latency'] = list_latency
    else:
      logging.debug('List-after-write inconsistency window is: %f',
                    total_wait_time)

      final_result['is-list-consistent'] = False
      final_result['inconsistency-window'] = total_wait_time
  else:
    logging.debug('Results are still inconsistent after waiting for the max '
                  'limit!')
    final_result['is-list-consistent'] = False
    final_result['inconsistency-window'] = LIST_CONSISTENCY_WAIT_TIME_LIMIT

  # Delete the objects written by individual threads in concurrency:
  logging.info('One list-after-write iteration completed. result is %s',
               final_result)

  threads = []
  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    thread = Thread(target=DeleteObjects,
                    args=(storage_schema, FLAGS.bucket,
                          per_thread_objects_written[i], host_to_connect))
    thread.daemon = True
    thread.start()
    threads.append(thread)

  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    try:
      threads[i].join()
    except:
      logging.exception('Caught exception waiting for the %dth deletion '
                        'thread.', i)

  logging.info('Finished deleting')

  return final_result


def Main(argv=sys.argv):

  logging.basicConfig(level=logging.INFO)

  try:
    argv = FLAGS(argv)  # parse flags
  except flags.FlagsError as e:
    logging.error(
        '%s\nUsage: %s ARGS\n%s', e, sys.argv[0], FLAGS)
    sys.exit(1)

  if FLAGS.bucket is None:
    raise ValueError('Must specify a valid bucket for this test.')

  logging.info('Storage is %s, bucket is %s, scenario is %s',
               FLAGS.storage,
               FLAGS.bucket,
               FLAGS.scenario)

  host_to_connect = None
  if FLAGS.host is not None:
    logging.info('Will use user-specified host endpoint: %s', FLAGS.host)
    host_to_connect = FLAGS.host

  if FLAGS.storage == 'AZURE':
    raise NotImplementedError('Storage type Azure is not implemented yet.')

  storage_schema = STORAGE_TO_SCHEMA_DICT[FLAGS.storage]

  if FLAGS.scenario == 'OneByteRW':
    return OneByteRWBenchmark(storage_schema, host_to_connect)
  elif FLAGS.scenario == 'ListConsistency':
    list_latency = []
    list_inconsistency_window = []
    inconsistent_list_count = 0.0

    logging.info('Running list consistency tests for %d iterations...' %
                 FLAGS.iterations)
    for _ in range(FLAGS.iterations):
      result = ListConsistencyBenchmark(storage_schema, host_to_connect)
      if result['is-list-consistent']:
        list_latency.append(result['list-latency'])
      else:
        inconsistent_list_count += 1
        list_inconsistency_window.append(result['inconsistency-window'])

    # All iterations completed, ready to print out final stats.
    logging.info('\n\nFinal stats:')
    logging.info('List consistency percentage: %f',
                 100 * (1 - inconsistent_list_count / FLAGS.iterations))

    if len(list_inconsistency_window) > 0:
      logging.info('List inconsistency window: %s',
                   json.dumps(_PercentileCalculator(list_inconsistency_window),
                              sort_keys=True))

    if len(list_latency) > 0:
      logging.info('List latency: %s',
                   json.dumps(_PercentileCalculator(list_latency),
                              sort_keys=True))

    return 0
  elif FLAGS.scenario == 'SingleStreamThroughput':
    return SingleStreamThroughputBenchmark(storage_schema, host_to_connect)

if __name__ == '__main__':
  sys.exit(Main())
