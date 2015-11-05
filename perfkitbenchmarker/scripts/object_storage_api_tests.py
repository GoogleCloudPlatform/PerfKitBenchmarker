#!/usr/bin/env python

# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
from azure.storage.blob import BlobService

FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'storage_provider', 'GCS', ['GCS', 'S3', 'AZURE'],
    'The target storage provider to test.')

flags.DEFINE_string('host', None, 'The hostname of the storage endpoint.')

flags.DEFINE_string('bucket', None,
                    'The name of the bucket to test with. Caller is '
                    'responsible to create an empty bucket for a particular '
                    'invocation of this test and then clean-up the bucket '
                    'after this test returns.')

flags.DEFINE_string('azure_account', None,
                    'The name of the storage account for Azure.')

flags.DEFINE_string('azure_key', None,
                    'The key of the storage account for Azure.')

flags.DEFINE_enum(
    'scenario', 'OneByteRW', ['OneByteRW', 'ListConsistency',
                              'SingleStreamThroughput', 'CleanupBucket'],
    'The various scenarios to run. OneByteRW: read and write of single byte. '
    'ListConsistency: List-after-write and list-after-update consistency. '
    'SingleStreamThroughput: Throughput of single stream large object RW. '
    'CleanupBucket: Cleans up everything in a given bucket.')

flags.DEFINE_integer('iterations', 1, 'The number of iterations to run for the '
                     'particular test scenario. Currently only applicable to '
                     'the ListConsistency scenario, ignored in others.')

STORAGE_TO_SCHEMA_DICT = {'GCS': 'gs', 'S3': 's3', 'AZURE': 'azure'}

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
# This covers 5 pages of List (one page == 1000 objects)
LIST_CONSISTENCY_OBJECT_COUNT = 5000

# List-after-update (delete) consistency:
# We will randomly delete X% number of objects we have just written, and then
# do a list immediately to check consistency. The number below defines the X
LIST_AFTER_UPDATE_DELETION_RATIO = 0.1

# Provisioning is done in parallel threads to reduce test iteration time!
# This number specifies the thread count.
LIST_CONSISTENCY_THREAD_COUNT = 100

# Names of the list consistency scenarios we test in this benchmark
LIST_AFTER_WRITE_SCENARIO = 'list-after-write'
LIST_AFTER_UPDATE_SCENARIO = 'list-after-update'
LIST_RESULT_SUFFIX_CONSISTENT = '-consistent'
LIST_RESULT_SUFFIX_LATENCY = '-latency'
LIST_RESULT_SUFFIX_INCONSISTENCY_WINDOW = '-inconsistency-window'

# Size of large objects used in single stream throughput benchmarking.
# 100 MiB (100 * 2 ^ 20)
LARGE_OBJECT_SIZE_BYTES = 100 * 1024 * 1024

# The number of large objects we use in single stream throughput benchmarking.
LARGE_OBJECT_COUNT = 100

LARGE_OBJECT_FAILURE_TOLERANCE = 0.1

# A global variable initialized once for Azure Blob Service
_AZURE_BLOB_SERVICE = None


# When a storage provider fails more than a threshold number of requests, we
# stop the benchmarking tests and raise a low availability error back to the
# caller.
class LowAvailabilityError(Exception):
    pass


def _useBotoApi(storage_schema):
  """Decides if the caller should use the boto api given a storage schema.

  Args:
    storage_schema: The schema represents the storage provider.

  Returns:
    A boolean indicates whether or not to use boto API.
  """

  if storage_schema == 'gs' or storage_schema == 's3':
    return True
  else:
    return False


def PercentileCalculator(numbers):
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  result['p1'] = numbers_sorted[int(count * 0.01)]
  result['p5'] = numbers_sorted[int(count * 0.05)]
  result['p50'] = numbers_sorted[int(count * 0.5)]
  result['p90'] = numbers_sorted[int(count * 0.9)]
  result['p99'] = numbers_sorted[int(count * 0.99)]
  result['p99.9'] = numbers_sorted[int(count * 0.999)]
  if count > 0:
    average = total / float(count)
    result['average'] = average
    if count > 1:
      total_of_squares = sum([(i - average) ** 2 for i in numbers])
      result['stddev'] = (total_of_squares / (count - 1)) ** 0.5
    else:
      result['stddev'] = 0

  return result


def _ListObjects(storage_schema, bucket, prefix, host_to_connect=None):
  """List objects under a bucket given a prefix.

  Args:
    storage_schema: The address schema identifying a storage. e.g., "gs"
    bucket: Name of the bucket.
    prefix: A prefix to list from.
    host_to_connect: An optional endpoint string to connect to.

  Returns:
    A list of object names.
  """

  bucket_list_result = None
  if _useBotoApi(storage_schema):
    bucket_uri = boto.storage_uri(bucket, storage_schema)
    if host_to_connect is not None:
      bucket_uri.connect(host=host_to_connect)

    bucket_list_result = bucket_uri.list_bucket(prefix=prefix)
  else:
    bucket_list_result = _AZURE_BLOB_SERVICE.list_blobs(bucket, prefix=prefix)

  list_result = []
  for k in bucket_list_result:
    list_result.append(k.name)

  return list_result


def DeleteObjects(storage_schema, bucket, objects_to_delete,
                  host_to_connect=None,
                  objects_deleted=None):
  """Delete a bunch of objects.

  Args:
    storage_schema: The address schema identifying a storage. e.g., "gs"
    bucket: Name of the bucket.
    objects_to_delete: A list of names of objects to delete.
    host_to_connect: An optional endpoint string to connect to.
    objects_deleted: An optional list to record the objects that have been
        successfully deleted.
  """

  for object_name in objects_to_delete:
    try:
      if _useBotoApi(storage_schema):
        object_path = '%s/%s' % (bucket, object_name)
        object_uri = boto.storage_uri(object_path, storage_schema)
        if host_to_connect is not None:
          object_uri.connect(host=host_to_connect)

        object_uri.delete_key()
      else:
        _AZURE_BLOB_SERVICE.delete_blob(bucket, object_name)

      if objects_deleted is not None:
        objects_deleted.append(object_name)
    except:
      logging.exception('Caught exception while deleting object %s.',
                        object_name)


def CleanupBucket(storage_schema):
  """ Cleans-up EVERYTHING under a given bucket as specified by FLAGS.bucket.

  Args:
    Storage_schema: The address schema identifying a storage. e.g., "gs"
  """

  objects_to_cleanup = _ListObjects(storage_schema, FLAGS.bucket, prefix=None)
  while len(objects_to_cleanup) > 0:
    logging.info('Will delete %d objects.', len(objects_to_cleanup))
    DeleteObjects(storage_schema, FLAGS.bucket, objects_to_cleanup)
    objects_to_cleanup = _ListObjects(storage_schema, FLAGS.bucket, prefix=None)


def WriteObjects(storage_schema, bucket, object_prefix, count,
                 size, objects_written, latency_results=None,
                 bandwidth_results=None, host_to_connect=None):
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
        latency numbers, in seconds, for each object that is successfully
        written.
    bandwidth_results: An optional parameter that caller can supply to hold
        bandwidth numbers, in bytes per second, for each object that is
        successfully written.
    host_to_connect: An optional endpoint string to connect to.
  """
  payload_bytes = bytearray(size)
  # Use a while loop to fill up the byte array which is by default 100MB in size
  # don't use a for...range(100M), range(100M) will create a list of 100mm items
  # with each item being the default object size of 30 bytes or so, it will lead
  # to out of memory error.
  i = 0
  while i < size:
    payload_bytes[i] = ord(random.choice(string.letters))
    i += 1

  payload_string = payload_bytes.decode('ascii')

  for i in range(count):
    object_name = '%s_%d' % (object_prefix, i)

    # Ready to go!
    start_time = time.time()

    # We write the object to storage provider and if successful we place the
    # name of the object to the objects_written list. When a write fails, the
    # object won't be placed into the objects_written list, and then it's up to
    # the caller to decide if they want to tolerate the failure and accept
    # the results, depending on test scenarios.
    try:
      if _useBotoApi(storage_schema):
        object_path = '%s/%s' % (bucket, object_name)
        object_uri = boto.storage_uri(object_path, storage_schema)
        if host_to_connect is not None:
          object_uri.connect(host=host_to_connect)

        object_uri.set_contents_from_string(payload_string)
      else:
        _AZURE_BLOB_SERVICE.put_block_blob_from_bytes(bucket, object_name,
                                                      bytes(payload_bytes))

      latency = time.time() - start_time

      if latency_results is not None:
        latency_results.append(latency)

      if bandwidth_results is not None and latency > 0.0:
        bandwidth_results.append(size / latency)

      objects_written.append(object_name)
    except:
      logging.exception('Caught exception while writing object %s.',
                        object_name)


def ReadObjects(storage_schema, bucket, objects_to_read, latency_results=None,
                bandwidth_results=None, object_size=None, host_to_connect=None):
  """Read a bunch of objects.

  Args:
    storage_schema: The address schema identifying a storage. e.g., "gs"
    bucket: Name of the bucket.
    objects_to_read: A list of names of objects to read.
    latency_results: An optional list to receive latency results.
    bandwidth_results: An optional list to receive bandwidth results.
    object_size: Size of the object that will be read, used to calculate bw.
    host_to_connect: An optional endpoint string to connect to.
  """
  for object_name in objects_to_read:

    start_time = time.time()
    try:
      if _useBotoApi(storage_schema):
        object_path = '%s/%s' % (bucket, object_name)
        object_uri = boto.storage_uri(object_path, storage_schema)
        object_uri.connect(host=host_to_connect)
        object_uri.new_key().get_contents_as_string()
      else:
        _AZURE_BLOB_SERVICE.get_blob_to_bytes(bucket, object_name)

      latency = time.time() - start_time

      if latency_results is not None:
        latency_results.append(latency)

      if (bandwidth_results is not None and
          object_size is not None and latency > 0.0):
        bandwidth_results.append(object_size / latency)
    except:
      logging.exception('Failed to read object %s', object_name)


def DeleteObjectsConcurrently(storage_schema, per_thread_objects_to_delete,
                              host_to_connect, per_thread_objects_deleted):
  """Delete a bunch of objects concurrently.

  Args:
    storage_schema: The address schema identifying a storage. e.g., "gs"
    per_thread_objects_to_delete: A 2-d list of objects to delete per thread.

    host_to_connect: An optional endpoint string to connect to.
    per_thread_objects_deleted: A list to record the objects that have been
        successfully deleted per thread.
  """
  threads = []
  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    thread = Thread(target=DeleteObjects,
                    args=(storage_schema, FLAGS.bucket,
                          per_thread_objects_to_delete[i], host_to_connect,
                          per_thread_objects_deleted[i]))
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


def ListAndWaitForObjects(counting_start_time, expected_set_of_objects,
                          storage_schema, object_prefix, host_to_connect):
  """List objects and wait for consistency.

  Args:
    counting_start_time: The start time used to count for the inconsistency
        window.
    expected_set_of_objects: The set of expectation.
    storage_schema: The address schema identifying a storage. e.g., "gs"
    object_prefix: The prefix of objects to list from.
    host_to_connect: An optional endpoint string to connect to.

  Returns:
    result_consistent: Is the list consistent
    list_count: Count of the lists before the result is consistent.
    list_latency: Latency of the list request that is consistent.
    total_wait_time: Total time waited before it's consistent.
  """

  total_wait_time = 0
  list_count = 0
  result_consistent = False
  list_latency = 0
  while total_wait_time < LIST_CONSISTENCY_WAIT_TIME_LIMIT:
    list_start_time = time.time()
    list_result = _ListObjects(storage_schema, FLAGS.bucket, object_prefix,
                               host_to_connect)
    list_count += 1
    list_latency = time.time() - list_start_time

    if expected_set_of_objects.difference(set(list_result)):
      total_wait_time = time.time() - counting_start_time
      continue
    else:
      result_consistent = True
      break

  return result_consistent, list_count, list_latency, total_wait_time


def AnalyzeListResults(final_result, result_consistent, list_count,
                       list_latency, total_wait_time, list_scenario):
  """Analyze the results of list consistency test, and fill in the final result.

  Args:
    final_result: The final result array to fill in.
    result_consistent: Is the final result consistent.
    list_count: The number of lists done.
    list_latency: The latency of the lists in seconds.
    total_wait_time: Time spent waiting for the list to be consistent (seconds).
    list_scenario: The scenario that was tested: list-after-update,
        list-after-write.
  """
  result_string_consistency = '%s%s' % (list_scenario,
                                        LIST_RESULT_SUFFIX_CONSISTENT)
  result_string_latency = '%s%s' % (list_scenario, LIST_RESULT_SUFFIX_LATENCY)
  result_string_inconsistency_window = '%s%s' % (
      list_scenario, LIST_RESULT_SUFFIX_INCONSISTENCY_WINDOW)

  if result_consistent:
    logging.debug('Listed %d times until results are consistent.', list_count)
    if list_count == 1:
      logging.debug('List latency is: %f', list_latency)
      final_result[result_string_consistency] = True
      final_result[result_string_latency] = list_latency
    else:
      logging.debug('List inconsistency window is: %f', total_wait_time)

      final_result[result_string_consistency] = False
      final_result[result_string_inconsistency_window] = total_wait_time
  else:
    logging.debug('Results are still inconsistent after waiting for the max '
                  'limit!')
    final_result[result_string_consistency] = False
    final_result[result_string_inconsistency_window] = (
        LIST_CONSISTENCY_WAIT_TIME_LIMIT)


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
  write_bandwidth = []
  objects_written = []

  WriteObjects(storage_schema, FLAGS.bucket, object_prefix,
               LARGE_OBJECT_COUNT, LARGE_OBJECT_SIZE_BYTES, objects_written,
               bandwidth_results=write_bandwidth,
               host_to_connect=host_to_connect)

  try:
    if len(objects_written) < LARGE_OBJECT_COUNT * (
                              1 - LARGE_OBJECT_FAILURE_TOLERANCE):  # noqa
      raise LowAvailabilityError('Failed to write required number of large '
                                 'objects, exiting.')

    logging.info('Single stream upload individual results in Bps:')
    for bandwidth in write_bandwidth:
      logging.info('%f', bandwidth)
    logging.info('Single stream upload throughput in Bps: %s',
                 json.dumps(PercentileCalculator(write_bandwidth),
                            sort_keys=True))

    read_bandwidth = []
    ReadObjects(storage_schema, FLAGS.bucket, objects_written,
                bandwidth_results=read_bandwidth,
                object_size=LARGE_OBJECT_SIZE_BYTES,
                host_to_connect=host_to_connect)
    if len(read_bandwidth) < len(objects_written) * (
        1 - LARGE_OBJECT_FAILURE_TOLERANCE):  # noqa
      raise LowAvailabilityError('Failed to read required number of objects, '
                                 'exiting.')

    logging.info('Single stream download individual results in Bps:')
    for bandwidth in read_bandwidth:
      logging.info('%f', bandwidth)
    logging.info('Single stream download throughput in Bps: %s',
                 json.dumps(PercentileCalculator(read_bandwidth),
                            sort_keys=True))

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

    logging.info('Individual results of one byte upload:')
    for latency in one_byte_write_latency:
      logging.info('%f', latency)
    logging.info('One byte upload - %s',
                 json.dumps(PercentileCalculator(one_byte_write_latency),
                            sort_keys=True))

    # Now download these objects and measure the latencies.
    one_byte_read_latency = []
    ReadObjects(storage_schema, FLAGS.bucket, one_byte_objects_written,
                one_byte_read_latency, host_to_connect)

    success_count = len(one_byte_read_latency)
    if success_count < ONE_BYTE_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
      raise LowAvailabilityError('Failed to read required number of objects, '
                                 'exiting.')

    logging.info('Individual results of one byte download:')
    for latency in one_byte_read_latency:
      logging.info('%f', latency)
    logging.info('One byte download - %s',
                 json.dumps(PercentileCalculator(one_byte_read_latency),
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
                          1,
                          per_thread_objects_written[i], None, None,
                          host_to_connect))
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
  result_consistent, list_count, list_latency, total_wait_time = (
      ListAndWaitForObjects(write_finish_time, set(final_objects_written),
                            storage_schema, object_prefix, host_to_connect))

  final_result = {}
  AnalyzeListResults(final_result, result_consistent, list_count, list_latency,
                     total_wait_time, LIST_AFTER_WRITE_SCENARIO)

  logging.info('One list-after-write iteration completed. result is %s',
               final_result)
  if not result_consistent:
    # There is no point continuing testing the list-after-update consistency if
    # list-after-write is still not consistent after waiting for extended
    # period of time.
    logging.info('Not doing list-after-update tests because results are still '
                 'not consistent after max wait time for list-after-write.')
    return final_result

  logging.info('Start benchmarking list-after-update consistency.')

  # Now delete some objects and do list again, this measures list-after-update
  # consistency
  per_thread_objects_to_delete = [
      [] for i in range(LIST_CONSISTENCY_THREAD_COUNT)]

  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    for j in range(len(per_thread_objects_written[i])):
      # Delete about 30% of the objects written so far.
      if random.Random() < LIST_AFTER_UPDATE_DELETION_RATIO:
        per_thread_objects_to_delete[i].append(per_thread_objects_written[i][j])

  # Now issue the delete concurrently.
  per_thread_objects_deleted = [
      [] for i in range(LIST_CONSISTENCY_THREAD_COUNT)]

  DeleteObjectsConcurrently(storage_schema,
                            per_thread_objects_to_delete,
                            host_to_connect,
                            per_thread_objects_deleted)

  delete_finish_time = time.time()
  final_expectation = []
  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    for k in range(len(per_thread_objects_deleted[i])):
      per_thread_objects_written[i].remove(per_thread_objects_deleted[i][k])
    final_expectation += per_thread_objects_written[i]

  result_consistent, list_count, list_latency, total_wait_time = (
      ListAndWaitForObjects(delete_finish_time, set(final_expectation),
                            storage_schema, object_prefix, host_to_connect))

  AnalyzeListResults(final_result, result_consistent, list_count, list_latency,
                     total_wait_time, LIST_AFTER_UPDATE_SCENARIO)

  logging.info('One list-after-update iteration completed. result is %s',
               final_result)

  # Final clean up: delete the objects still remaining.
  DeleteObjectsConcurrently(storage_schema,
                            per_thread_objects_written,
                            host_to_connect,
                            per_thread_objects_deleted)
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

  logging.info('Storage provider is %s, bucket is %s, scenario is %s',
               FLAGS.storage_provider,
               FLAGS.bucket,
               FLAGS.scenario)

  host_to_connect = None
  if FLAGS.host is not None:
    logging.info('Will use user-specified host endpoint: %s', FLAGS.host)
    host_to_connect = FLAGS.host

  if FLAGS.storage_provider == 'AZURE':
    if FLAGS.azure_key is None or FLAGS.azure_account is None:
      raise ValueError('Must specify azure account and key')
    else:
      global _AZURE_BLOB_SERVICE
      _AZURE_BLOB_SERVICE = BlobService(FLAGS.azure_account, FLAGS.azure_key)
      # There are DNS lookup issues with the provider Azure when doing
      # "high" number of concurrent requests using multiple threads. The error
      # came from getaddrinfo() called by the azure python library. By reducing
      # the concurrent thread count to 10 or below, the issue can be mitigated.
      # If we lower the thread count, we need to lower the total object count
      # too so the time to write these object remains short
      global LIST_CONSISTENCY_THREAD_COUNT
      LIST_CONSISTENCY_THREAD_COUNT = 10
      global LIST_CONSISTENCY_OBJECT_COUNT
      LIST_CONSISTENCY_OBJECT_COUNT = 1000

  storage_schema = STORAGE_TO_SCHEMA_DICT[FLAGS.storage_provider]

  if FLAGS.scenario == 'OneByteRW':
    return OneByteRWBenchmark(storage_schema, host_to_connect)
  elif FLAGS.scenario == 'ListConsistency':
    list_latency = {}
    list_inconsistency_window = {}
    inconsistent_list_count = {}
    for scenario in [LIST_AFTER_WRITE_SCENARIO, LIST_AFTER_UPDATE_SCENARIO]:
      list_latency[scenario] = []
      list_inconsistency_window[scenario] = []
      inconsistent_list_count[scenario] = 0.0

    logging.info('Running list consistency tests for %d iterations...',
                 FLAGS.iterations)
    for _ in range(FLAGS.iterations):
      result = ListConsistencyBenchmark(storage_schema, host_to_connect)
      # Analyze the result for both scenarios.
      for scenario in [LIST_AFTER_WRITE_SCENARIO, LIST_AFTER_UPDATE_SCENARIO]:
        result_consistent = '%s%s' % (scenario, LIST_RESULT_SUFFIX_CONSISTENT)
        if result_consistent in result:
          if result[result_consistent]:
            list_latency[scenario].append(
                result['%s%s' % (scenario, LIST_RESULT_SUFFIX_LATENCY)])
          else:
            inconsistent_list_count[scenario] += 1
            list_inconsistency_window[scenario].append(
                result['%s%s' % (scenario,
                                 LIST_RESULT_SUFFIX_INCONSISTENCY_WINDOW)])

    # All iterations completed, ready to print out final stats.
    logging.info('\n\nFinal stats:')
    for scenario in [LIST_AFTER_WRITE_SCENARIO, LIST_AFTER_UPDATE_SCENARIO]:
      logging.info('%s consistency percentage: %f', scenario,
                   100 *
                   (1 - inconsistent_list_count[scenario] / FLAGS.iterations))

      if len(list_inconsistency_window[scenario]) > 0:
        logging.info('%s inconsistency window: %s', scenario,
                     json.dumps(PercentileCalculator(
                         list_inconsistency_window[scenario]),
                         sort_keys=True))

      if len(list_latency[scenario]) > 0:
        logging.info('%s latency: %s', scenario,
                     json.dumps(PercentileCalculator(list_latency[scenario]),
                                sort_keys=True))

    return 0
  elif FLAGS.scenario == 'SingleStreamThroughput':
    return SingleStreamThroughputBenchmark(storage_schema, host_to_connect)
  elif FLAGS.scenario == 'CleanupBucket':
    return CleanupBucket(storage_schema)

if __name__ == '__main__':
  sys.exit(Main())
