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

import gflags as flags
import logging
import boto
import sys
from threading import Thread
import time

FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'storage', 'GCS', ['GCS', 'S3', 'AZURE'], 'The target storage to test.')

flags.DEFINE_string('host', None, 'The hostname of the storage endpoint.')

flags.DEFINE_string('bucket', None,
                    'The name of the bucket to test with. Caller is responsible'
                    ' to create an empty bucket for a particular invocation of'
                    ' this test and then clean-up the bucket after this test'
                    ' returns.')

flags.DEFINE_enum(
    'scenario', 'OneByteRW', ['OneByteRW', 'ListConsistency',
                              'SingleStreamThroughput'],
    'The various scenarios to test.')

flags.DEFINE_integer('iterations', 1, 'The number of iterations to run for the'
                     ' particular test scenario. Currently only applicable to'
                     ' the ListConsistency scenario, ignored in others.')

# If more than 5% of our upload or download operations fail for an iteration,
# there is an availability issue with the service provider or the connection
# between the test VM and the service provider. For this reason, we consider
# this particular iteration invalid, and we will print out an error message
# instead of a set of benchmark numbers (which will be incorrect).
FAILURE_TOLERANCE = 0.05


def _TpCalc(numbers):
  if numbers is None:
    raise ValueError('Must pass in a list of numbers to this function')

  numbers.sort()
  count = len(numbers)
  output = ('tp50: %f, tp90: %f,'
            ' tp99: %f, tp99.9: %f') % (numbers[int(count * 0.5)],
                                        numbers[int(count * 0.9)],
                                        numbers[int(count * 0.99)],
                                        numbers[int(count * 0.999)])
  return output


def WriteOneByteObjects(storage_schema, bucket, object_prefix, count,
                        objects_written, latency_results=None,
                        host_to_connect=None):
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
    except Exception as e:
      print 'Caught exception while writing object %s. Exception is %s' % (
            object_path, e)


def listObjects(storage_schema, bucket, prefix, host_to_connect=None):
  bucket_uri = boto.storage_uri(bucket, storage_schema)
  if host_to_connect is not None:
    bucket_uri.connect(host=host_to_connect)

  list_result = []
  for k in bucket_uri.list_bucket(prefix=prefix):
    list_result.append(k.name)

  return list_result


def OneByteRWBenchmark(storage_schema, host_to_connect=None):

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
    raise Exception("Failed to write required number of objects, exiting.")

  print 'One byte upload - %s' % _TpCalc(one_byte_write_latency)

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
    except Exception as e:
      print 'Failed to read object %s, exception is %e' % (object_path, e)

  success_count = len(one_byte_read_latency)
  if success_count < ONE_BYTE_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
    raise Exception("Failed to read required number of objects, exiting.")

  print 'One byte download - %s' % _TpCalc(one_byte_read_latency)


def ListConsistencyBenchmark(storage_schema, host_to_connect=None):
  # The maximum amount of seconds we are willing to wait for list results to be
  # consistent in this benchmark
  CONSISTENCY_WAIT_TIME_LIMIT = 300

  # Total number of objects we will provision before we do the list
  TOTAL_OBJECT_COUNT = 5000

  # Provisioning is done in parallel threads to reduce test iteration time!
  TOTAL_THREAD_COUNT = 100

  # Provision the test with tons of one byte objects. Write them all at once.
  object_prefix = 'pkb_list_consistency_%f' % time.time()
  final_objects_written = []

  per_thread_objects_written = [[] for i in range(TOTAL_THREAD_COUNT)]
  threads = []

  for i in range(TOTAL_THREAD_COUNT):
    my_prefix = '%s_%d' % (object_prefix, i)
    thread = Thread(target=WriteOneByteObjects,
                    args=(storage_schema, FLAGS.bucket, my_prefix,
                          TOTAL_OBJECT_COUNT / TOTAL_THREAD_COUNT,
                          per_thread_objects_written[i], None, host_to_connect))
    thread.daemon = True
    thread.start()
    threads.append(thread)

  print 'All threads started, waiting for them to end...'

  for i in range(TOTAL_THREAD_COUNT):
    try:
      threads[i].join()
      final_objects_written += per_thread_objects_written[i]
    except:
      print 'Caught exception waiting for the %th thread.' % i
  print 'All threads ended...'

  write_finish_time = time.time()

  final_count = len(final_objects_written)
  if final_count < TOTAL_OBJECT_COUNT * (1 - FAILURE_TOLERANCE):
    raise Exception("Failed to provision required number of objects, exiting.")

  print ('Done provisioning the objects, objects written %d. Now start doing'
         ' the lists...') % final_count

  # Now list this bucket under this prefix, compare the list results with
  # objects_written. If they are not the same, keep doing it until they
  # are the same.

  total_wait_time = 0
  final_objects_written_set = set(final_objects_written)
  list_count = 0
  result_consistent = False
  list_latency = 0
  while total_wait_time < CONSISTENCY_WAIT_TIME_LIMIT:

    list_start_time = time.time()
    list_result = listObjects(storage_schema, FLAGS.bucket, object_prefix,
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
    print "Listed %d times until results are consistent." % list_count
    if list_count == 1:
      print "List latency is: %f" % list_latency
    else:
      print "list-after-write inconsistency window is: %f" % total_wait_time
  else:
    print "Results are still inconsistent after waiting for the max limit!"


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

  print 'Storage is %s, bucket is %s, scenario is %s' % (FLAGS.storage,
                                                         FLAGS.bucket,
                                                         FLAGS.scenario)
  host_to_connect = None
  storage_schema = None

  if FLAGS.host is not None:
    print 'Will use user-specified host endpoint: %s' % FLAGS.host
    host_to_connect = FLAGS.host

  if FLAGS.storage == 'GCS':
    storage_schema = 'gs'

  if FLAGS.storage == 'S3':
    storage_schema = 's3'

  if FLAGS.storage == 'AZURE':
    raise NotImplementedError('Storage type Azure is not implemented yet.')

  if FLAGS.scenario == 'OneByteRW':
    return OneByteRWBenchmark(storage_schema, host_to_connect)
  elif FLAGS.scenario == 'ListConsistency':
    for _ in range(FLAGS.iterations):
      ListConsistencyBenchmark(storage_schema, host_to_connect)
    return 0
  elif FLAGS.scenario == 'SingleStreamThroughput':
    raise NotImplementedError('Single Stream Throughput is not implemented.')

sys.exit(Main())
