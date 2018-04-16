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

import cStringIO
import json
import logging
import os
import sys
import multiprocessing as mp
from threading import Thread
import string
import random
import time

import yaml

from absl import flags

import azure_flags  # noqa
import s3_flags  # noqa

FLAGS = flags.FLAGS

flags.DEFINE_enum(
    'storage_provider', 'GCS', ['GCS', 'S3', 'AZURE'],
    'The target storage provider to test.')

flags.DEFINE_string('bucket', None,
                    'The name of the bucket to test with. Caller is '
                    'responsible to create an empty bucket for a particular '
                    'invocation of this test and then clean-up the bucket '
                    'after this test returns.')

flags.DEFINE_enum(
    'scenario', 'OneByteRW', ['OneByteRW', 'ListConsistency',
                              'SingleStreamThroughput', 'CleanupBucket',
                              'MultiStreamWrite', 'MultiStreamRead'],
    'The various scenarios to run. OneByteRW: read and write of single byte. '
    'ListConsistency: List-after-write and list-after-update consistency. '
    'SingleStreamThroughput: Throughput of single stream large object RW. '
    'CleanupBucket: Cleans up everything in a given bucket.'
    'MultiStreamWrite: Write objects with many streams at once.'
    'MultiStreamRead: Read objects with many streams at once.')

flags.DEFINE_integer('iterations', 1, 'The number of iterations to run for the '
                     'particular test scenario. Currently only applicable to '
                     'the ListConsistency scenario, ignored in others.')

flags.DEFINE_integer('objects_per_stream', 1000, 'The number of objects to '
                     'read and write per stream in the MultiStreamThroughput '
                     'scenario.')
flags.DEFINE_string('object_sizes', "{1024: 100.0}", 'The size of the objects '
                    'to use, as a distribution. Currently only applicable to '
                    'the MultiStreamThroughput scenario, ignored in others. '
                    'Must be a dict-based representation, even for a constant '
                    'distribution. Ex: {1024: 100.0}, (1KiB 100% of the time), '
                    '{1000: 50.0, 10000: 50.0} (1KB 50% of the time, 10KB 50% '
                    'of the time)')
flags.DEFINE_integer('num_streams', 10, 'The number of streams to use. Only '
                     'applies to the MultiStreamThroughput scenario.',
                     lower_bound=1)
flags.DEFINE_integer('stream_num_start', 1, 'The number of the first thread in '
                     'this process.')
flags.DEFINE_string('objects_written_file', None, 'The path where the '
                    'multistream write benchmark will save a list of the '
                    'objects it wrote, and the multistream read benchmark will '
                    'get a list of objects to read. If the scenario is '
                    'MultiStreamWrite and this file exists, it will be '
                    'deleted.')

flags.DEFINE_float('start_time', None, 'The time (as a POSIX timestamp) '
                   'to start the operation. Only applies to the '
                   'MultiStreamRead and MultiStreamWrite scenarios.')

flags.DEFINE_string('object_storage_class', None, 'The storage class to use '
                    'for uploads. Currently only applicable to AWS. For other '
                    'providers, storage class is determined by the bucket, '
                    'which is passed in by the --bucket parameter.')

flags.DEFINE_enum('object_naming_scheme', 'sequential_by_stream',
                  ['sequential_by_stream',
                   'approximately_sequential'],
                  'How objects will be named. Only applies to the '
                  'MultiStreamWrite benchmark. '
                  'sequential_by_stream: object names from each stream '
                  'will be sequential, but different streams will have '
                  'different name prefixes. '
                  'approximately_sequential: object names from all '
                  'streams will roughly increase together.')

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

BYTES_PER_KILOBYTE = 1024

# The multistream benchmarks log how many threads are still active
# every THREAD_STATUS_LOG_INTERVAL seconds.
THREAD_STATUS_LOG_INTERVAL = 10


# When a storage provider fails more than a threshold number of requests, we
# stop the benchmarking tests and raise a low availability error back to the
# caller.
class LowAvailabilityError(Exception):
    pass


# ### Utilities for workload generation ###

class SizeDistributionIterator(object):
  """Draw object sizes from a distribution.

  Args:
    dist: dict. The distribution to draw sizes from.
  """

  def __init__(self, dist):
    # This class chooses random numbers by using random.random() to
    # pick a float in [0.0, 100.0), then converting that into a choice
    # from our distribution. The data structure is two lists, sizes
    # and cutoffs. If the random number is in the range [cutoffs[i-1],
    # cutoffs[i]), then the random choice is sizes[i]. Cutoffs has an
    # implied starting element 0.0, and its last element must be
    # 100.0.
    self.sizes = []
    self.cutoffs = []

    # Sorting the (size, percent change) pairs by size makes for a
    # nicer interface and also simplifies unit testing.
    sorted_dist = sorted(dist.iteritems(), key=lambda x: x[0])

    total_percent = 0.0
    for size, percent in sorted_dist:
      total_percent += percent
      self.sizes.append(size)
      self.cutoffs.append(total_percent)

    # We store percents instead of probabilities because it avoids any
    # possible edge cases where the percentages would add to 100.0 but
    # their equivalent probabilities would have truncated
    # floating-point representations and would not add to 1.0.
    assert total_percent == 100.0

  # this is required by the Python iterator protocol
  def __iter__(self):
    return self

  def next(self):
    val = random.random() * 100.0

    # binary search has a lower asymptotic complexity than linear
    # scanning, but thanks to caches and sequential access, linear
    # scanning will probably be faster for any realistic distribution.
    for i in xrange(len(self.sizes)):
      if self.cutoffs[i] > val:
        return self.sizes[i]

    raise AssertionError("Random percent %s didn't match percent cutoff list %s"
                         % (val, self.cutoffs))

    return self.size


def MaxSizeInDistribution(dist):
  """Find the maximum object size in a distribution."""

  return max(dist.iterkeys())


# ### Utilities for data analysis ###

def PercentileCalculator(numbers):
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  result['p0.1'] = numbers_sorted[int(count * 0.001)]
  result['p1'] = numbers_sorted[int(count * 0.01)]
  result['p5'] = numbers_sorted[int(count * 0.05)]
  result['p10'] = numbers_sorted[int(count * 0.1)]
  result['p50'] = numbers_sorted[int(count * 0.5)]
  result['p90'] = numbers_sorted[int(count * 0.9)]
  result['p95'] = numbers_sorted[int(count * 0.95)]
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

# ### Object naming schemes ###


class ObjectNameIterator(object):
  """Provides new object names on demand.

  This class exists to easily support different object naming schemes.
  """

  def __iter__(self):
    """The iterator for this iterator.

    This method is required by the Python iterator protocol.
    """
    return self

  def next(self):
    """Generate a new name.

    Returns:
      The object name, as a string.
    """

    raise NotImplementedError()


class PrefixCounterIterator(ObjectNameIterator):
  def __init__(self, prefix):
    self.prefix = prefix
    self.counter = 0

  def next(self):
    name = '%s_%d' % (self.prefix, self.counter)
    self.counter = self.counter + 1
    return name


class PrefixTimestampSuffixIterator(ObjectNameIterator):
  def __init__(self, prefix, suffix):
    self.prefix = prefix
    self.suffix = suffix

  def next(self):
    return '%s_%f_%s' % (self.prefix, time.time(), self.suffix)


# ### Utilities for benchmarking ###

def CleanupBucket(service):
  """ Cleans-up EVERYTHING under a given bucket as specified by FLAGS.bucket.

  Args:
    service: the ObjectStorageServiceBase to use.
  """

  objects_to_cleanup = service.ListObjects(FLAGS.bucket, prefix=None)
  while len(objects_to_cleanup) > 0:
    logging.info('Will delete %d objects.', len(objects_to_cleanup))
    service.DeleteObjects(FLAGS.bucket, objects_to_cleanup)
    objects_to_cleanup = service.ListObjects(FLAGS.bucket, prefix=None)


def GenerateWritePayload(size):
  """Generate random data for use with WriteObjectFromBuffer.

  Args:
    size: the amount of data needed, in bytes.

  Returns:
    A string of the length requested, filled with random data.
  """

  payload_bytes = bytearray(size)
  # don't use a for...range(100M), range(100M) will create a list of 100mm items
  # with each item being the default object size of 30 bytes or so, it will lead
  # to out of memory error.
  for i in xrange(size):
    payload_bytes[i] = ord(random.choice(string.letters))

  return payload_bytes.decode('ascii')


def WriteObjects(service, bucket, object_prefix, count,
                 size, objects_written, latency_results=None,
                 bandwidth_results=None):
  """Write a number of objects to a storage service.

  Args:
    service: the ObjectStorageServiceBase object to use.
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
  """

  payload = GenerateWritePayload(size)
  handle = cStringIO.StringIO(payload)

  for i in xrange(count):
    object_name = '%s_%d' % (object_prefix, i)

    try:
      _, latency = service.WriteObjectFromBuffer(
          bucket, object_name, handle, size)

      objects_written.append(object_name)
      if latency_results is not None:
        latency_results.append(latency)
      if bandwidth_results is not None and latency > 0.0:
        bandwidth_results.append(size / latency)
    except Exception as e:
      logging.info('Caught exception %s while writing object %s' %
                   (e, object_name))


def ReadObjects(service, bucket, objects_to_read, latency_results=None,
                bandwidth_results=None, object_size=None,
                start_times=None):
  """Read a bunch of objects.

  Args:
    service: the ObjectStorageServiceBase object to use.
    bucket: Name of the bucket.
    objects_to_read: A list of names of objects to read.
    latency_results: An optional list to receive latency results.
    bandwidth_results: An optional list to receive bandwidth results.
    object_size: Size of the object that will be read, used to calculate bw.
    start_times: An optional list to receive start time results.
  """

  for object_name in objects_to_read:
    try:
      start_time, latency = service.ReadObject(bucket, object_name)

      if start_times is not None:
        start_times.append(start_time)

      if latency_results is not None:
        latency_results.append(latency)

      if (bandwidth_results is not None and
          object_size is not None and latency > 0.0):
        bandwidth_results.append(object_size / latency)
    except:
      logging.exception('Failed to read object %s', object_name)


def DeleteObjectsConcurrently(service, per_thread_objects_to_delete,
                              per_thread_objects_deleted):
  """Delete a bunch of objects concurrently.

  Args:
    service: the ObjectStorageServiceBase object to use.
    per_thread_objects_to_delete: A 2-d list of objects to delete per thread.
    per_thread_objects_deleted: A 2-d list to record the objects that have been
        successfully deleted per thread.
  """
  threads = []
  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    thread = Thread(target=service.DeleteObjects,
                    args=(FLAGS.bucket,
                          per_thread_objects_to_delete[i],
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


def ListAndWaitForObjects(service, counting_start_time,
                          expected_set_of_objects, object_prefix):
  """List objects and wait for consistency.

  Args:
    service: the ObjectStorageServiceBase object to use.
    counting_start_time: The start time used to count for the inconsistency
        window.
    expected_set_of_objects: The set of expectation.
    object_prefix: The prefix of objects to list from.

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
    list_result = service.ListObjects(FLAGS.bucket, object_prefix)
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


def SingleStreamThroughputBenchmark(service):
  """ A benchmark test for single stream upload and download throughput.

  Args:
    service: the ObjectStorageServiceBase object to use.

  Raises:
    LowAvailabilityError: when the storage service has failed a high number of
        our RW requests that exceeds a threshold (>5%), we raise this error
        instead of collecting performance numbers from this run.
  """
  object_prefix = 'pkb_single_stream_%f' % time.time()
  write_bandwidth = []
  objects_written = []

  WriteObjects(service, FLAGS.bucket, object_prefix,
               LARGE_OBJECT_COUNT, LARGE_OBJECT_SIZE_BYTES, objects_written,
               bandwidth_results=write_bandwidth)

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
    ReadObjects(service, FLAGS.bucket, objects_written,
                bandwidth_results=read_bandwidth,
                object_size=LARGE_OBJECT_SIZE_BYTES)
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
    service.DeleteObjects(FLAGS.bucket, objects_written)


def RunWorkerProcesses(worker, worker_args, per_process_args=None):
  """Run a worker function in many processes, then gather and return the results

  Args:
    worker: either WriteWorker or ReadWorker. The worker function to call.
    worker_args: a tuple. The arguments to pass to the worker function. The
      result queue and stream number will be appended as the last two arguments.
    per_process_args: if given, an array with length equal to the
      number of processes. Process number i will be passed
      per_process_args[i] after its regular arguments and before the
      result queue and stream number.

  Returns:
    A list of the results returned by the workers.
  """

  result_queue = mp.Queue()
  num_streams = FLAGS.num_streams

  logging.info('Creating %s processes', FLAGS.num_streams)
  if per_process_args is None:
    processes = [mp.Process(target=worker,
                            args=worker_args + (result_queue, i))
                 for i in xrange(num_streams)]
  else:
    processes = [mp.Process(target=worker,
                            args=worker_args +
                            (per_process_args[i],) +
                            (result_queue, i))
                 for i in xrange(num_streams)]
  logging.info('Processes created. Starting processes.')
  for process in processes:
    process.start()
  logging.info('Processes started.')

  # Wait for all the results. Each worker will put one result onto the queue
  results = [result_queue.get() for _ in range(num_streams)]
  logging.info('All processes complete.')
  return results


def MultiStreamWrites(service):
  """Run multi-stream write benchmark.

  Args:
    service: the ObjectStorageServiceBase object to use.

  This function writes objects to the storage service, potentially
  using multiple threads.

  It doesn't return anything, but it outputs two sets of results in
  two different ways. First, it writes a list of the names of all the
  objects it has written with their sizes to
  FLAGS.objects_written_file (if given). The format is

  [[object_name_1, object_size_1],
   [object_name_2, object_size_2],
   ...]

  Second, it writes records of the objects it wrote, along with timing
  information, to sys.stdout. The format is

  [{"operation": "upload", "start_time": start_time_1,
    "latency": latency_1, "size": size_1, "stream_num": stream_num_1},
   {"operation": "upload", "start_time": start_time_2,
    "latency": latency_2, "size": size_2, "stream_num": stream_num_2},
   ...]

  Both kinds of output are written as JSON, for easy serialization and
  deserialization.

  """

  size_distribution = yaml.load(FLAGS.object_sizes)

  payload = GenerateWritePayload(MaxSizeInDistribution(size_distribution))

  results = RunWorkerProcesses(
      WriteWorker,
      (service,
       payload,
       size_distribution,
       FLAGS.objects_per_stream,
       FLAGS.start_time,
       FLAGS.object_naming_scheme))

  # object_records is the data we leave on the VM for future reads. We
  # need to pass data to the reader so it will know what object names
  # it can read.
  if FLAGS.objects_written_file is not None:
    try:
      object_records = []
      for result in results:
        for name, size in zip(result['object_names'], result['sizes']):
          object_records.append([name, size])
      if os.path.exists(FLAGS.objects_written_file):
        os.remove(FLAGS.objects_written_file)
      with open(FLAGS.objects_written_file, 'w') as out:
        json.dump(object_records, out)
    except Exception as ex:
      # If we can't write our objects written, we still want to return
      # the data we collected, so keep going.
      logging.info('Got exception %s while trying to write objects written '
                   'file.', ex)

  logging.info('len(results) = %s', len(results))

  # streams is the data we send back to the controller.
  streams = []
  for result in results:
    result_keys = ('stream_num', 'start_times', 'latencies', 'sizes')
    streams.append({k: result[k] for k in result_keys})

  num_writes = sum([len(stream['start_times']) for stream in streams])
  num_writes_requested = FLAGS.objects_per_stream * FLAGS.num_streams
  min_writes_required = num_writes_requested * (1.0 - FAILURE_TOLERANCE)
  if num_writes < min_writes_required:
    raise LowAvailabilityError(
        'Wrote %s objects out of %s requested (%s requred)' %
        (num_writes, num_writes_requested, min_writes_required))

  json.dump(streams, sys.stdout, indent=0)


def MultiStreamReads(service):
  """Run multi-stream read benchmark.

  Args:
    service: the ObjectStorageServiceBase object to use.

  This function reads a list of object names and sizes from
  FLAGS.objects_written_file (in the format written by
  MultiStreamWrites and then reads the objects from the storage
  service, potentially using multiple threads.

  It doesn't directly return anything, but it writes its results to
  sys.stdout in the following format:

  [{"operation": "download", "start_time": start_time_1,
    "latency": latency_1, "size": size_1, "stream_num": stream_num_1},
   {"operation": "download", "start_time": start_time_1,
    "latency": latency_1, "size": size_1, "stream_num": stream_num_1},
   ...]

  """

  # Read the object records that the MultiStreamWriter left for us.
  if FLAGS.objects_written_file is None:
    raise ValueError('The MultiStreamRead benchmark needs a list of object '
                     'names to read from. Use '
                     '--objects_written_file=<filename>.')
  with open(FLAGS.objects_written_file, 'r') as object_file:
    object_records = json.load(object_file)

  num_workers = FLAGS.num_streams
  objects_by_worker = [object_records[i::num_workers]
                       for i in xrange(num_workers)]

  results = RunWorkerProcesses(
      ReadWorker,
      (service,
       FLAGS.start_time),
      per_process_args=objects_by_worker)

  # streams is the data we send back to the controller.
  streams = []
  for result in results:
    result_keys = ('stream_num', 'start_times', 'latencies', 'sizes')
    streams.append({k: result[k] for k in result_keys})

  num_reads = sum([len(stream['start_times']) for stream in streams])
  num_reads_requested = FLAGS.objects_per_stream * FLAGS.num_streams
  min_reads_required = num_reads_requested * (1.0 - FAILURE_TOLERANCE)
  if num_reads < min_reads_required:
    raise LowAvailabilityError(
        'Read %s objects out of %s requested (%s requred)' %
        (num_reads, num_reads_requested, min_reads_required))

  json.dump(streams, sys.stdout, indent=0)


def SleepUntilTime(when):
  """Sleep until a given time.

  Args:
    when: float. The time to sleep to, as a POSIX timestamp.
  """

  now = time.time()
  sleep_time = when - now
  if sleep_time > 0.0:
    time.sleep(sleep_time)
  elif sleep_time == 0.0:
    pass
  else:
    logging.info('Sleep time %s was too small', sleep_time)


def WriteWorker(service, payload,
                size_distribution, num_objects,
                start_time, naming_scheme, result_queue, worker_num):
  """Upload objects for the multi-stream writes benchmark.

  Args:
    service: the ObjectStorageServiceBase object to use.
    payload: a string. The bytes to upload.
    size_distribution: the distribution of object sizes to use.
    num_objects: the number of objects to upload.
    start_time: a POSIX timestamp. When to start uploading.
    naming_scheme: how to name objects. See flag.
    result_queue: a mp.Queue to record results in.
    worker_num: the thread number of this worker.
  """

  object_names = []
  start_times = []
  latencies = []
  sizes = []

  if naming_scheme == 'sequential_by_stream':
    name_iterator = PrefixCounterIterator(
        'pkb_write_worker_%f_%s' % (time.time(), worker_num))
  elif naming_scheme == 'approximately_sequential':
    name_iterator = PrefixTimestampSuffixIterator(
        'pkb_writes_%s' % start_time,
        '%s' % worker_num)
  size_iterator = SizeDistributionIterator(size_distribution)

  payload_handle = cStringIO.StringIO(payload)

  if start_time is not None:
    SleepUntilTime(start_time)

  for i in xrange(num_objects):
    object_name = name_iterator.next()
    object_size = size_iterator.next()

    try:
      start_time, latency = service.WriteObjectFromBuffer(
          FLAGS.bucket, object_name,
          payload_handle, object_size)

      object_names.append(object_name)
      start_times.append(start_time)
      latencies.append(latency)
      sizes.append(object_size)
    except Exception as e:
      logging.info('Worker %s caught exception %s while writing object %s' %
                   (worker_num, e, object_name))

  logging.info('Worker %s finished writing its objects' % worker_num)

  result_queue.put({'object_names': object_names,
                    'start_times': start_times,
                    'latencies': latencies,
                    'sizes': sizes,
                    'stream_num': worker_num + FLAGS.stream_num_start})


def ReadWorker(service, start_time, object_records,
               result_queue, worker_num):

  start_times = []
  latencies = []
  sizes = []

  if start_time is not None:
    SleepUntilTime(start_time)

  for name, size in object_records:
    try:
      start_time, latency = service.ReadObject(FLAGS.bucket, name)

      start_times.append(start_time)
      latencies.append(latency)
      sizes.append(size)
    except Exception as e:
      logging.info('Worker %s caught exception %s while reading object %s' %
                   (worker_num, e, name))


  result_queue.put({'start_times': start_times,
                    'latencies': latencies,
                    'sizes': sizes,
                    'stream_num': worker_num + FLAGS.stream_num_start})


def OneByteRWBenchmark(service):
  """ A benchmark test for one byte object read and write. It uploads and
  downloads ONE_BYTE_OBJECT_COUNT number of 1-byte objects to the storage
  service, keeps track of the latency of these operations, and print out the
  result in JSON format at the end of the test.

  Args:
    service: the ObjectStorageServiceBase object to use.

  Raises:
    LowAvailabilityError: when the storage service has failed a high number of
        our RW requests that exceeds a threshold (>5%), we raise this error
        instead of collecting performance numbers from this run.
  """

  # One byte write
  object_prefix = 'pkb_one_byte_%f' % time.time()

  one_byte_write_latency = []
  one_byte_objects_written = []

  WriteObjects(service, FLAGS.bucket, object_prefix,
               ONE_BYTE_OBJECT_COUNT, 1, one_byte_objects_written,
               latency_results=one_byte_write_latency)

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
    ReadObjects(service, FLAGS.bucket, one_byte_objects_written,
                one_byte_read_latency)

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
    service.DeleteObjects(FLAGS.bucket, one_byte_objects_written)


def ListConsistencyBenchmark(service):
  """ A benchmark test to measure list-after-write consistency. It uploads
  a large number of 1-byte objects in a short amount of time, and then issues
  a list request. If the first list request returns all objects as expected,
  then the result is deemed consistent. Otherwise, it keeps issuing list request
  until all expected objects are returned, and the test reports the time
  it takes from the end of the last write to the time the list returns
  consistent result.

  Args:
    service: the ObjectStorageServiceBase object to use.

  Returns:
    A dictionary that contains the test results:
      'is-list-consistent': True/False
      'list-latency': if list is consistent, what is its latency
      'inconsistency-window': if list is inconsistent, how long did it take to
          reach consistency.

  Raises:
    LowAvailabilityError: when the storage service has failed a high number of
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
                    args=(service, FLAGS.bucket, my_prefix,
                          LIST_CONSISTENCY_OBJECT_COUNT /
                          LIST_CONSISTENCY_THREAD_COUNT,
                          1,
                          per_thread_objects_written[i], None, None))
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
      ListAndWaitForObjects(service, write_finish_time,
                            set(final_objects_written), object_prefix))

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

  DeleteObjectsConcurrently(service,
                            per_thread_objects_to_delete,
                            per_thread_objects_deleted)

  delete_finish_time = time.time()
  final_expectation = []
  for i in range(LIST_CONSISTENCY_THREAD_COUNT):
    for k in range(len(per_thread_objects_deleted[i])):
      per_thread_objects_written[i].remove(per_thread_objects_deleted[i][k])
    final_expectation += per_thread_objects_written[i]

  result_consistent, list_count, list_latency, total_wait_time = (
      ListAndWaitForObjects(service, delete_finish_time,
                            set(final_expectation), object_prefix))

  AnalyzeListResults(final_result, result_consistent, list_count, list_latency,
                     total_wait_time, LIST_AFTER_UPDATE_SCENARIO)

  logging.info('One list-after-update iteration completed. result is %s',
               final_result)

  # Final clean up: delete the objects still remaining.
  DeleteObjectsConcurrently(service,
                            per_thread_objects_written,
                            per_thread_objects_deleted)
  return final_result


def Main(argv=sys.argv):
  logging.basicConfig(level=logging.INFO)

  try:
    argv = FLAGS(argv)  # parse flags
  except flags.Error as e:
    logging.error(
        '%s\nUsage: %s ARGS\n%s', e, sys.argv[0], FLAGS)
    sys.exit(1)

  if FLAGS.bucket is None:
    raise ValueError('Must specify a valid bucket for this test.')

  logging.info('Storage provider is %s, bucket is %s, scenario is %s',
               FLAGS.storage_provider,
               FLAGS.bucket,
               FLAGS.scenario)

  # This is essentially a dictionary lookup implemented in if
  # statements, but doing it this way allows us to not import the
  # modules of storage providers we're not using.
  if FLAGS.storage_provider == 'AZURE':
    import azure_service
    service = azure_service.AzureService()
  elif FLAGS.storage_provider == 'GCS':
    import gcs
    service = gcs.GCSService()
  elif FLAGS.storage_provider == 'S3':
    import s3
    service = s3.S3Service()
  else:
    raise ValueError('Invalid storage provider %s' % FLAGS.storage_provider)

  if FLAGS.storage_provider == 'AZURE':
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

  if FLAGS.scenario == 'OneByteRW':
    return OneByteRWBenchmark(service)
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
      result = ListConsistencyBenchmark(service)
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
    return SingleStreamThroughputBenchmark(service)
  elif FLAGS.scenario == 'CleanupBucket':
    return CleanupBucket(service)
  elif FLAGS.scenario == 'MultiStreamWrite':
    return MultiStreamWrites(service)
  elif FLAGS.scenario == 'MultiStreamRead':
    return MultiStreamReads(service)

if __name__ == '__main__':
  sys.exit(Main())
