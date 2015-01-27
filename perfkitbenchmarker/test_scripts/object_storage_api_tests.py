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
import time

import gflags as flags
import boto
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


# When a storage provider fails more than a threshold number of requests, we
# stop the benchmarking tests and raise a low availability error back to the
# caller.
class LowAvailabilityError(Exception):
    pass


def _PercentileCalculator(numbers):
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  result = {}
  result['tp50'] = numbers_sorted[int(count * 0.5)]
  result['tp90'] = numbers_sorted[int(count * 0.9)]
  result['tp99'] = numbers_sorted[int(count * 0.99)]
  result['tp99.9'] = numbers_sorted[int(count * 0.999)]
  return result


def _ListObjects(storage_schema, bucket, prefix, host_to_connect=None):
  bucket_uri = boto.storage_uri(bucket, storage_schema)
  if host_to_connect is not None:
    bucket_uri.connect(host=host_to_connect)

  list_result = []
  for k in bucket_uri.list_bucket(prefix=prefix):
    list_result.append(k.name)

  return list_result


def WriteOneByteObjects(storage_schema, bucket, object_prefix, count,
                        objects_written, latency_results=None,
                        host_to_connect=None):
  """Write a number of one byte objects to a storage provider.

  Args:
    storage_schema: The address schema identifying a storage. e.g., "gs"
    bucket: Name of the bucket to write to.
    object_prefix: The prefix of names of objects to be written.
    count: The total number of objects that need to be written.
    objects_written: A list of names of objects that have been successfully
        written by this function. Caller supplies the list and this function
        fills in the name of the objects.
    latency_results: An optional parameter that caller can supply to hold
        a latency numbers, in seconds, for each object that is successfully
        written.
    host_to_connect: An optional endpoint string to connect to.
  """

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
      object_uri.set_contents_from_string('f')

      if latency_results is not None:
        latency_results.append(time.time() - start_time)

      objects_written.append(object_name)
    except:
      logging.exception('Caught exception while writing object %s.' %
                        object_path)


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
  ONE_BYTE_OBJECT_COUNT = 1000

  one_byte_write_latency = []
  one_byte_objects_written = []

  WriteOneByteObjects(storage_schema, FLAGS.bucket, object_prefix,
                      ONE_BYTE_OBJECT_COUNT, one_byte_objects_written,
                      latency_results=one_byte_write_latency,
                      host_to_connect=host_to_connect)

  success_count = len(one_byte_objects_written)
  if success_count < ONE_BYTE_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
    raise LowAvailabilityError('Failed to write required number of objects, '
                               'exiting.')

  logging.info('One byte upload - %s' %
               json.dumps(_PercentileCalculator(one_byte_write_latency),
                          sort_keys=True))

  # Now download these objects and measure the latencies.
  one_byte_read_latency = []

  for object_name in one_byte_objects_written:
    object_path = '%s/%s' % (FLAGS.bucket, object_name)
    object_uri = boto.storage_uri(object_path, storage_schema)
    object_uri.connect(host=host_to_connect)

    # Ready to go!
    start_time = time.time()

    try:
      object_uri.get_contents_as_string()

      one_byte_read_latency.append(time.time() - start_time)
    except:
      logging.exception('Failed to read object %s' % object_path)

  success_count = len(one_byte_read_latency)
  if success_count < ONE_BYTE_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
    raise LowAvailabilityError('Failed to read required number of objects, '
                               'exiting.')

  logging.info('One byte download - %s' %
               json.dumps(_PercentileCalculator(one_byte_read_latency),
                          sort_keys=True))


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

  Raises:
    LowAvailabilityError: when the storage provider has failed a high number of
        our RW requests that exceeds a threshold (>5%), we raise this error
        instead of collecting performance numbers from this run.
  """

  # The maximum amount of seconds we are willing to wait for list results to be
  # consistent in this benchmark
  LIST_CONSISTENCY_WAIT_TIME_LIMIT = 300

  # Total number of objects we will provision before we do the list
  LIST_CONSISTENCY_OBJECT_COUNT = 5000

  # Provisioning is done in parallel threads to reduce test iteration time!
  LIST_CONSISTENCY_THREAD_COUNT = 100

  # Provision the test with tons of one byte objects. Write them all at once.
  object_prefix = 'pkb_list_consistency_%f' % time.time()
  final_objects_written = []

  per_thread_objects_written = [[] for i in
                                 range(LIST_CONSISTENCY_THREAD_COUNT)]  # noqa

  threads = []

  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    my_prefix = '%s_%d' % (object_prefix, i)
    thread = Thread(target=WriteOneByteObjects,
                    args=(storage_schema, FLAGS.bucket, my_prefix,
                          LIST_CONSISTENCY_OBJECT_COUNT /
                          LIST_CONSISTENCY_THREAD_COUNT,
                          per_thread_objects_written[i], None, host_to_connect))
    thread.daemon = True
    thread.start()
    threads.append(thread)

  logging.info('All threads started, waiting for them to end...')

  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    try:
      threads[i].join()
      final_objects_written += per_thread_objects_written[i]
    except:
      logging.exception('Caught exception waiting for the %dth thread.' % i)
  logging.info('All threads ended...')

  write_finish_time = time.time()

  final_count = len(final_objects_written)
  if final_count < LIST_CONSISTENCY_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
    raise LowAvailabilityError('Failed to provision required number of '
                               'objects, exiting.')

  logging.info(('Done provisioning the objects, objects written %d. Now start '
                'doing the lists...') % final_count)

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

  if result_consistent:
    logging.info('Listed %d times until results are consistent.' % list_count)
    if list_count == 1:
      logging.info('List latency is: %f' % list_latency)
    else:
      logging.info('List-after-write inconsistency window is: %f' %
                   total_wait_time)
  else:
    logging.info('Results are still inconsistent after waiting for the max '
                 'limit!')


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

  logging.info('Storage is %s, bucket is %s, scenario is %s' % (FLAGS.storage,
                                                                FLAGS.bucket,
                                                                FLAGS.scenario))
  host_to_connect = None
  if FLAGS.host is not None:
    logging.info('Will use user-specified host endpoint: %s' % FLAGS.host)
    host_to_connect = FLAGS.host

  if FLAGS.storage == 'AZURE':
    raise NotImplementedError('Storage type Azure is not implemented yet.')

  storage_schema = STORAGE_TO_SCHEMA_DICT[FLAGS.storage]

  if FLAGS.scenario == 'OneByteRW':
    return OneByteRWBenchmark(storage_schema, host_to_connect)
  elif FLAGS.scenario == 'ListConsistency':
    for _ in range(FLAGS.iterations):
      ListConsistencyBenchmark(storage_schema, host_to_connect)
    return 0
  elif FLAGS.scenario == 'SingleStreamThroughput':
    raise NotImplementedError('Single Stream Throughput is not implemented.')

if __name__ == '__main__':
  sys.exit(Main())
