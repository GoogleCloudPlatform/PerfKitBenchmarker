# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Object (blob) Storage benchmark tests.

There are two categories of tests here: 1) tests based on CLI tools, and 2)
tests that use APIs to access storage provider.

For 1), we aim to simulate one typical use case of common user using storage
provider: upload and downloads a set of files with different sizes from/to a
local directory.

For 2), we aim to measure more directly the performance of a storage provider
by accessing them via APIs. Here are the main scenarios covered in this
category:
  a: Single byte object upload and download, measures latency.
  b: List-after-write and list-after-update consistency measurement.
  c: Single stream large object upload and download, measures throughput.
"""

import datetime
import glob
import json
import logging
import os
import posixpath
import re
import threading
import time
import uuid

import numpy as np

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import sample
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.sample import PercentileCalculator  # noqa

flags.DEFINE_enum('storage', providers.GCP,
                  [providers.GCP, providers.AWS,
                   providers.AZURE, providers.OPENSTACK],
                  'storage provider (GCP/AZURE/AWS/OPENSTACK) to use.')

flags.DEFINE_string('object_storage_region', None,
                    'Storage region for object storage benchmark.')

flags.DEFINE_string('object_storage_gcs_multiregion', None,
                    'Storage multiregion for GCS in object storage benchmark.')

flags.DEFINE_string('object_storage_storage_class', None,
                    'Storage class to use in object storage benchmark.')

flags.DEFINE_enum('object_storage_scenario', 'all',
                  ['all', 'cli', 'api_data', 'api_namespace',
                   'api_multistream', 'api_multistream_writes',
                   'api_multistream_reads'],
                  'select all, or one particular scenario to run: \n'
                  'ALL: runs all scenarios. This is the default. \n'
                  'cli: runs the command line only scenario. \n'
                  'api_data: runs API based benchmarking for data paths. \n'
                  'api_namespace: runs API based benchmarking for namespace '
                  'operations. \n'
                  'api_multistream: runs API-based benchmarking with multiple '
                  'upload/download streams.\n'
                  'api_multistream_writes: runs API-based benchmarking with '
                  'multiple upload streams.')

flags.DEFINE_string('object_storage_bucket_name', None,
                    'If set, the bucket will be created with this name')

flags.DEFINE_enum('cli_test_size', 'normal',
                  ['normal', 'large'],
                  'size of the cli tests. Normal means a mixture of various \n'
                  'object sizes up to 32MiB (see '
                  'data/cloud-storage-workload.sh). \n'
                  'Large means all objects are of at least 1GiB.')

flags.DEFINE_integer('object_storage_multistream_objects_per_stream', 1000,
                     'Number of objects to send and/or receive per stream. '
                     'Only applies to the api_multistream scenario.',
                     lower_bound=1)
flag_util.DEFINE_yaml('object_storage_object_sizes', '1KB',
                      'Size of objects to send and/or receive. Only applies to '
                      'the api_multistream scenario. Examples: 1KB, '
                      '{1KB: 50%, 10KB: 50%}')
flags.DEFINE_integer('object_storage_streams_per_vm', 10,
                     'Number of independent streams per VM. Only applies to '
                     'the api_multistream scenario.',
                     lower_bound=1)

flags.DEFINE_integer('object_storage_list_consistency_iterations', 200,
                     'Number of iterations to perform for the api_namespace '
                     'list consistency benchmark. This flag is mainly for '
                     'regression testing in the benchmarks. Reduce the number '
                     'to shorten the execution time of the api_namespace '
                     'scenario. However, to get useful metrics from the '
                     'api_namespace scenario, a high number of iterations '
                     'should be used (>=200).')
flags.DEFINE_enum('object_storage_object_naming_scheme', 'sequential_by_stream',
                  ['sequential_by_stream',
                   'approximately_sequential'],
                  'How objects will be named. Only applies to the '
                  'api_multistream benchmark. '
                  'sequential_by_stream: object names from each stream '
                  'will be sequential, but different streams will have '
                  'different name prefixes. '
                  'approximately_sequential: object names from all '
                  'streams will roughly increase together.')
flags.DEFINE_string('object_storage_objects_written_file_prefix', None,
                    'If specified, the bucket and all of the objects will not '
                    'be deleted, and the list of object names will be written '
                    'to a file with the specified prefix in the following '
                    'format: <bucket>/<object>. This prefix can be passed to '
                    'this benchmark in a later run via via the '
                    'object_storage_read_objects_prefix flag. Only valid for '
                    'the api_multistream and api_multistream_writes scenarios. '
                    'The filename is appended with the date and time so that '
                    'later runs can be given a prefix and a minimum age of '
                    'objects. The later run will then use the oldest objects '
                    'available or fail if there is no file with an old enough '
                    'date. The prefix is also appended with the region so that '
                    'later runs will read objects from the same region.')
flags.DEFINE_string('object_storage_read_objects_prefix', None,
                    'If specified, no new bucket or objects will be created. '
                    'Instead, the benchmark will read the objects listed in '
                    'a file with the specified prefix that was written some '
                    'number of hours before (as specifed by '
                    'object_storage_read_objects_min_hours). Only valid for '
                    'the api_multistream_reads scenario.')
flags.DEFINE_integer('object_storage_read_objects_min_hours', 72, 'The minimum '
                     'number of hours from which to read objects that were '
                     'written on a previous run. Used in combination with '
                     'object_storage_read_objects_prefix.')
flags.DEFINE_boolean('object_storage_dont_delete_bucket', False,
                     'If True, the storage bucket won\'t be deleted. Useful '
                     'for running the api_multistream_reads scenario multiple '
                     'times against the same objects.')

flags.DEFINE_string('object_storage_worker_output', None,
                    'If set, the worker threads\' output will be written to the'
                    'path provided.')
flags.DEFINE_float('object_storage_latency_histogram_interval', None,
                   'If set, a latency histogram sample will be created with '
                   'buckets of the specified interval in seconds. Individual '
                   'histogram samples are created for each different object '
                   'size in the distribution, because it is easy to aggregate '
                   'the histograms during post-processing, but impossible to '
                   'go in the opposite direction.')

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'object_storage_service',
                  'description':
                  'Object/blob storage service benchmarks. Specify '
                  '--object_storage_scenario '
                  'to select a set of sub-benchmarks to run. default is all.',
                  'scratch_disk': False,
                  'num_machines': 1}

BENCHMARK_NAME = 'object_storage_service'
BENCHMARK_CONFIG = """
object_storage_service:
  description: >
      Object/blob storage service benchmarks. Specify
      --object_storage_scenario
      to select a set of sub-benchmarks to run. default is all.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
  flags:
    gcloud_scopes: https://www.googleapis.com/auth/devstorage.read_write
"""

DATA_FILE = 'cloud-storage-workload.sh'
# size of all data used in the CLI tests.
DATA_SIZE_IN_BYTES = 256.1 * 1024 * 1024
DATA_SIZE_IN_MBITS = 8 * DATA_SIZE_IN_BYTES / 1000 / 1000
LARGE_DATA_SIZE_IN_BYTES = 3 * 1024 * 1024 * 1024
LARGE_DATA_SIZE_IN_MBITS = 8 * LARGE_DATA_SIZE_IN_BYTES / 1000 / 1000

API_TEST_SCRIPT = 'object_storage_api_tests.py'
API_TEST_SCRIPTS_DIR = 'object_storage_api_test_scripts'

# Files that will be sent to the remote VM for API tests.
API_TEST_SCRIPT_FILES = ['object_storage_api_tests.py',
                         'object_storage_interface.py',
                         'azure_flags.py',
                         's3_flags.py']

# Various constants to name the result metrics.
THROUGHPUT_UNIT = 'Mbps'
LATENCY_UNIT = 'seconds'
NA_UNIT = 'na'
PERCENTILES_LIST = ['p0.1', 'p1', 'p5', 'p10', 'p50', 'p90', 'p95', 'p99',
                    'p99.9', 'average', 'stddev']

UPLOAD_THROUGHPUT_VIA_CLI = 'upload throughput via cli Mbps'
DOWNLOAD_THROUGHPUT_VIA_CLI = 'download throughput via cli Mbps'

CLI_TEST_ITERATION_COUNT = 100
LARGE_CLI_TEST_ITERATION_COUNT = 20

CLI_TEST_FAILURE_TOLERANCE = 0.05
# Azure does not parallelize operations in its CLI tools. We have to
# do the uploads or downloads of 100 test files sequentially, it takes
# a very long time for each iteration, so we are doing only 3 iterations.
CLI_TEST_ITERATION_COUNT_AZURE = 3

SINGLE_STREAM_THROUGHPUT = 'single stream %s throughput Mbps'

ONE_BYTE_LATENCY = 'one byte %s latency'

LIST_CONSISTENCY_SCENARIOS = ['list-after-write', 'list-after-update']
LIST_CONSISTENCY_PERCENTAGE = 'consistency percentage'
LIST_INCONSISTENCY_WINDOW = 'inconsistency window'
LIST_LATENCY = 'latency'

CONTENT_REMOVAL_RETRY_LIMIT = 5
# Some times even when a bucket is completely empty, the service provider would
# refuse to remove the bucket with "BucketNotEmpty" error until up to 1 hour
# later. We keep trying until we reach the one-hour limit. And this wait is
# necessary for some providers.
BUCKET_REMOVAL_RETRY_LIMIT = 120
RETRY_WAIT_INTERVAL_SECONDS = 30

# GCS has special region handling until we can remove it :(
DEFAULT_GCS_MULTIREGION = 'us'

# Keys for flag names and metadata values
OBJECT_STORAGE_REGION = 'object_storage_region'
REGIONAL_BUCKET_LOCATION = 'regional_bucket_location'
OBJECT_STORAGE_GCS_MULTIREGION = 'object_storage_gcs_multiregion'
GCS_MULTIREGION_LOCATION = 'gcs_multiregion_location'
DEFAULT = 'default'

# This accounts for the overhead of running RemoteCommand() on a VM.
MULTISTREAM_DELAY_PER_VM = 5.0 * units.second
# We wait this long for each stream. Note that this is multiplied by
# the number of streams per VM, not the total number of streams.
MULTISTREAM_DELAY_PER_STREAM = 0.1 * units.second
# And add a constant factor for PKB-side processing
MULTISTREAM_DELAY_CONSTANT = 10.0 * units.second

# The multistream write benchmark writes a file in the VM's /tmp with
# the objects it has written, which is used by the multistream read
# benchmark. This is the filename.
OBJECTS_WRITTEN_FILE = 'pkb-objects-written'

# If the gap between different stream starts and ends is above a
# certain proportion of the total time, we log a warning because we
# are throwing out a lot of information. We also put the warning in
# the sample metadata.
MULTISTREAM_STREAM_GAP_THRESHOLD = 0.2

# The API test script uses different names for providers than this
# script :(
STORAGE_TO_API_SCRIPT_DICT = {
    providers.GCP: 'GCS',
    providers.AWS: 'S3',
    providers.AZURE: 'AZURE'}

_SECONDS_PER_HOUR = 60 * 60


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


# Raised when we fail to remove a bucket or its content after many retries.
# TODO: add a new class of error "ObjectStorageError" to errors.py and remove
# this one.
class BucketRemovalError(Exception):
  pass


class NotEnoughResultsError(Exception):
  pass


class ColdDataError(Exception):
  """Exception indicating that the cold object data does not exist."""


def _JsonStringToPercentileResults(results, json_input, metric_name,
                                   metric_unit, metadata):
  """This function parses a percentile result string in Json format.

  Args:
    results: The final result set to put result in.
    json_input: The input in Json format about percentiles.
    metric_name: Name of the metric.
    metric_unit: Unit of the metric.
    metadata: The metadata to be included.
  """
  result = json.loads(json_input)
  for percentile in PERCENTILES_LIST:
    results.append(sample.Sample(
        ('%s %s') % (metric_name, percentile),
        float(result[percentile]),
        metric_unit,
        metadata))


def _GetClientLibVersion(vm, library_name):
  """ This function returns the version of client lib installed on a vm.

  Args:
    vm: the VM to get the client lib version from.
    library_name: the name of the client lib.

  Returns:
    The version string of the client.
  """
  version, _ = vm.RemoteCommand('pip show %s |grep Version' % library_name)
  logging.info('%s client lib version is: %s', library_name, version)
  return version


def MultiThreadStartDelay(num_vms, threads_per_vm):
  """Find how long in the future we can simultaneously start threads on VMs.

  Args:
    num_vms: number of VMs to start threads on.
    threads_per_vm: number of threads to start on each VM.

  Returns:
    A units.Quantity of time such that if we want to start
    threads_per_vm threads on num_vms VMs, we can start the threads
    sequentially, tell each of them to sleep for this number of
    seconds, and we expect that we will be able to start the last
    thread before the delay has finished.
  """

  return (
      MULTISTREAM_DELAY_CONSTANT +
      MULTISTREAM_DELAY_PER_VM * num_vms +
      MULTISTREAM_DELAY_PER_STREAM * threads_per_vm)


def _ProcessMultiStreamResults(start_times, latencies, sizes, operation,
                               all_sizes, results, metadata=None):
  """Read and process results from the api_multistream worker process.

  Results will be reported per-object size and combined for all
  objects.

  Args:
    start_times: a list of numpy arrays. Operation start times, as
      POSIX timestamps.
    latencies: a list of numpy arrays. Operation durations, in seconds.
    sizes: a list of numpy arrays. Object sizes used in each
      operation, in bytes.
    operation: 'upload' or 'download'. The operation the results are from.
    all_sizes: a sequence of integers. all object sizes in the
      distribution used, in bytes.
    results: a list to append Sample objects to.
    metadata: dict. Base sample metadata
  """

  num_streams = FLAGS.object_storage_streams_per_vm * FLAGS.num_vms

  assert len(start_times) == num_streams
  assert len(latencies) == num_streams
  assert len(sizes) == num_streams

  if metadata is None:
    metadata = {}
  metadata['num_streams'] = num_streams
  metadata['objects_per_stream'] = (
      FLAGS.object_storage_multistream_objects_per_stream)
  metadata['object_naming'] = FLAGS.object_storage_object_naming_scheme

  num_records = sum((len(start_time) for start_time in start_times))
  logging.info('Processing %s total operation records', num_records)

  stop_times = [start_time + latency
                for start_time, latency in zip(start_times, latencies)]

  last_start_time = max((start_time[0] for start_time in start_times))
  first_stop_time = min((stop_time[-1] for stop_time in stop_times))

  # Compute how well our synchronization worked
  first_start_time = min((start_time[0] for start_time in start_times))
  last_stop_time = max((stop_time[-1] for stop_time in stop_times))
  start_gap = last_start_time - first_start_time
  stop_gap = last_stop_time - first_stop_time
  if ((start_gap + stop_gap) / (last_stop_time - first_start_time) <
      MULTISTREAM_STREAM_GAP_THRESHOLD):
    logging.info(
        'First stream started %s seconds before last stream started', start_gap)
    logging.info(
        'Last stream ended %s seconds after first stream ended', stop_gap)
  else:
    logging.warning(
        'Difference between first and last stream start/end times was %s and '
        '%s, which is more than %s of the benchmark time %s.',
        start_gap, stop_gap, MULTISTREAM_STREAM_GAP_THRESHOLD,
        (last_stop_time - first_start_time))
    metadata['stream_gap_above_threshold'] = True

  # Find the indexes in each stream where all streams are active,
  # following Python's [inclusive, exclusive) index convention.
  active_start_indexes = []
  for start_time in start_times:
    for i in xrange(len(start_time)):
      if start_time[i] >= last_start_time:
        active_start_indexes.append(i)
        break
  active_stop_indexes = []
  for stop_time in stop_times:
    for i in xrange(len(stop_time) - 1, -1, -1):
      if stop_time[i] <= first_stop_time:
        active_stop_indexes.append(i + 1)
        break
  active_latencies = [
      latencies[i][active_start_indexes[i]:active_stop_indexes[i]]
      for i in xrange(num_streams)]
  active_sizes = [
      sizes[i][active_start_indexes[i]:active_stop_indexes[i]]
      for i in xrange(num_streams)]

  all_active_latencies = np.concatenate(active_latencies)
  all_active_sizes = np.concatenate(active_sizes)

  # Don't publish the full distribution in the metadata because doing
  # so might break regexp-based parsers that assume that all metadata
  # values are simple Python objects. However, do add an
  # 'object_size_B' metadata field even for the full results because
  # searching metadata is easier when all records with the same metric
  # name have the same set of metadata fields.
  distribution_metadata = metadata.copy()
  if len(all_sizes) == 1:
    distribution_metadata['object_size_B'] = all_sizes[0]
  else:
    distribution_metadata['object_size_B'] = 'distribution'

  latency_prefix = 'Multi-stream %s latency' % operation
  logging.info('Processing %s multi-stream %s results for the full '
               'distribution.', len(all_active_latencies), operation)
  _AppendPercentilesToResults(
      results,
      all_active_latencies,
      latency_prefix,
      LATENCY_UNIT,
      distribution_metadata)

  # Publish by-size and full-distribution stats even if there's only
  # one size in the distribution, because it simplifies postprocessing
  # of results.
  for size in all_sizes:
    this_size_metadata = metadata.copy()
    this_size_metadata['object_size_B'] = size
    logging.info('Processing multi-stream %s results for object size %s',
                 operation, size)
    _AppendPercentilesToResults(
        results,
        all_active_latencies[all_active_sizes == size],
        latency_prefix,
        LATENCY_UNIT,
        this_size_metadata)
    # Build the object latency histogram if user requested it
    if FLAGS.object_storage_latency_histogram_interval:
      histogram_interval = FLAGS.object_storage_latency_histogram_interval
      hist_latencies = [[l for l, s in zip(*w_l_s) if s == size]
                        for w_l_s in zip(latencies, sizes)]
      max_latency = max([max(l) for l in hist_latencies])
      # Note that int() floors for us
      num_histogram_buckets = int(max_latency / histogram_interval) + 1
      histogram_buckets = [0 for _ in range(num_histogram_buckets)]
      for worker_latencies in hist_latencies:
        for latency in worker_latencies:
          # Note that int() floors for us
          histogram_buckets[int(latency / histogram_interval)] += 1
      histogram_str = ','.join([str(c) for c in histogram_buckets])
      histogram_metadata = this_size_metadata.copy()
      histogram_metadata['interval'] = histogram_interval
      histogram_metadata['histogram'] = histogram_str
      results.append(sample.Sample(
          'Multi-stream %s latency histogram' % operation,
          0.0, 'histogram', metadata=histogram_metadata))

  # Throughput metrics
  total_active_times = [np.sum(latency) for latency in active_latencies]
  active_durations = [stop_times[i][active_stop_indexes[i] - 1] -
                      start_times[i][active_start_indexes[i]]
                      for i in xrange(num_streams)]
  total_active_sizes = [np.sum(size) for size in active_sizes]
  # 'net throughput (with gap)' is computed by taking the throughput
  # for each stream (total # of bytes transmitted / (stop_time -
  # start_time)) and then adding the per-stream throughputs. 'net
  # throughput' is the same, but replacing (stop_time - start_time)
  # with the sum of all of the operation latencies for that thread, so
  # we only divide by the time that stream was actually transmitting.
  results.append(sample.Sample(
      'Multi-stream ' + operation + ' net throughput',
      np.sum((size / active_time * 8
              for size, active_time
              in zip(total_active_sizes, total_active_times))),
      'bit / second', metadata=distribution_metadata))
  results.append(sample.Sample(
      'Multi-stream ' + operation + ' net throughput (with gap)',
      np.sum((size / duration * 8
              for size, duration in zip(total_active_sizes, active_durations))),
      'bit / second', metadata=distribution_metadata))
  results.append(sample.Sample(
      'Multi-stream ' + operation + ' net throughput (simplified)',
      sum([np.sum(size) for size in sizes]) /
      (last_stop_time - first_start_time) * 8,
      'bit / second', metadata=distribution_metadata))

  # QPS metrics
  results.append(sample.Sample(
      'Multi-stream ' + operation + ' QPS (any stream active)',
      num_records / (last_stop_time - first_start_time), 'operation / second',
      metadata=distribution_metadata))
  results.append(sample.Sample(
      'Multi-stream ' + operation + ' QPS (all streams active)',
      len(all_active_latencies) / (first_stop_time - last_start_time),
      'operation / second', metadata=distribution_metadata))

  # Statistics about benchmarking overhead
  gap_time = sum((active_duration - active_time
                  for active_duration, active_time
                  in zip(active_durations, total_active_times)))
  results.append(sample.Sample(
      'Multi-stream ' + operation + ' total gap time',
      gap_time, 'second', metadata=distribution_metadata))
  results.append(sample.Sample(
      'Multi-stream ' + operation + ' gap time proportion',
      gap_time / (first_stop_time - last_start_time) * 100.0,
      'percent', metadata=distribution_metadata))


def _DistributionToBackendFormat(dist):
  """Convert an object size distribution to the format needed by the backend.

  Args:
    dist: a distribution, given as a dictionary mapping size to
    frequency. Size will be a string with a quantity and a
    unit. Frequency will be a percentage, including a '%'
    character. dist may also be a string, in which case it represents
    a single object size which applies to 100% of objects.

  Returns:
    A dictionary giving an object size distribution. Sizes will be
    integers representing bytes. Frequencies will be floating-point
    numbers in [0,100], representing percentages.

  Raises:
    ValueError if dist is not a valid distribution.
  """

  if isinstance(dist, dict):
    val = {flag_util.StringToBytes(size):
           flag_util.StringToRawPercent(frequency)
           for size, frequency in dist.iteritems()}
  else:
    # We allow compact notation for point distributions. For instance,
    # '1KB' is an abbreviation for '{1KB: 100%}'.
    val = {flag_util.StringToBytes(dist): 100.0}

  # I'm requiring exact addition to 100, which can always be satisfied
  # with integer percentages. If we want to allow general decimal
  # percentages, all we have to do is replace this equality check with
  # approximate equality.
  if sum(val.itervalues()) != 100.0:
    raise ValueError("Frequencies in %s don't add to 100%%!" % dist)

  return val


class APIScriptCommandBuilder(object):
  """Builds command lines for the API test script.

  Attributes:
    test_script_path: the path to the API test script on the remote machine.
    storage: the storage provider to use, in the format expected by
      the test script.
    service: the ObjectStorageService object corresponding to the
      storage provider.
  """

  def __init__(self, test_script_path, storage, service):
    self.test_script_path = test_script_path
    self.storage = storage
    self.service = service

  def BuildCommand(self, args):
    """Build a command string for the API test script.

    Args:
      args: a list of strings. These will become space-separated
      arguments to the test script.

    Returns:
      A string that can be passed to vm.RemoteCommand.
    """

    cmd_parts = [
        self.test_script_path,
        '--storage_provider=%s' % self.storage
    ] + args + self.service.APIScriptArgs()
    if FLAGS.object_storage_storage_class is not None:
      cmd_parts += ['--object_storage_class',
                    FLAGS.object_storage_storage_class]

    return ' '.join(cmd_parts)


class UnsupportedProviderCommandBuilder(APIScriptCommandBuilder):
  """A dummy command builder for unsupported providers.

  When a provider isn't supported by the API test script yet, we
  create this command builder for them. It will let us run the CLI
  benchmark on that provider, but if the user tries to run an API
  benchmark, it will throw an error.

  Attributes:
    provider: the name of the unsupported provider.
  """

  def __init__(self, provider):
    self.provider = provider

  def BuildCommand(self, args):
    raise NotImplementedError('API tests are not supported on provider %s.' %
                              self.provider)


def OneByteRWBenchmark(results, metadata, vm, command_builder,
                       service, bucket_name):
  """A benchmark for small object latency.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  one_byte_rw_cmd = command_builder.BuildCommand([
      '--bucket=%s' % bucket_name,
      '--scenario=OneByteRW'])

  _, raw_result = vm.RemoteCommand(one_byte_rw_cmd)
  logging.info('OneByteRW raw result is %s', raw_result)

  for up_and_down in ['upload', 'download']:
    search_string = 'One byte %s - (.*)' % up_and_down
    result_string = re.findall(search_string, raw_result)
    sample_name = ONE_BYTE_LATENCY % up_and_down

    if len(result_string) > 0:
      _JsonStringToPercentileResults(results,
                                     result_string[0],
                                     sample_name,
                                     LATENCY_UNIT,
                                     metadata)
    else:
      raise ValueError('Unexpected test outcome from OneByteRW api test: '
                       '%s.' % raw_result)


def SingleStreamThroughputBenchmark(results, metadata, vm, command_builder,
                                    service, bucket_name):
  """A benchmark for large object throughput.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  single_stream_throughput_cmd = command_builder.BuildCommand([
      '--bucket=%s' % bucket_name,
      '--scenario=SingleStreamThroughput'])

  _, raw_result = vm.RemoteCommand(single_stream_throughput_cmd)
  logging.info('SingleStreamThroughput raw result is %s', raw_result)

  for up_and_down in ['upload', 'download']:
    search_string = 'Single stream %s throughput in Bps: (.*)' % up_and_down
    result_string = re.findall(search_string, raw_result)
    sample_name = SINGLE_STREAM_THROUGHPUT % up_and_down

    if not result_string:
      raise ValueError('Unexpected test outcome from '
                       'SingleStreamThroughput api test: %s.' % raw_result)

    # Convert Bytes per second to Mega bits per second
    # We use MB (10^6) to be consistent with network
    # bandwidth convention.
    result = json.loads(result_string[0])
    for percentile in PERCENTILES_LIST:
      results.append(sample.Sample(
          ('%s %s') % (sample_name, percentile),
          8 * float(result[percentile]) / 1000 / 1000,
          THROUGHPUT_UNIT,
          metadata))


def ListConsistencyBenchmark(results, metadata, vm, command_builder,
                             service, bucket_name):
  """A benchmark for bucket list consistency.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  list_consistency_cmd = command_builder.BuildCommand([
      '--bucket=%s' % bucket_name,
      '--iterations=%d' % FLAGS.object_storage_list_consistency_iterations,
      '--scenario=ListConsistency'])

  _, raw_result = vm.RemoteCommand(list_consistency_cmd)
  logging.info('ListConsistency raw result is %s', raw_result)

  for scenario in LIST_CONSISTENCY_SCENARIOS:
    metric_name = '%s %s' % (scenario, LIST_CONSISTENCY_PERCENTAGE)
    search_string = '%s: (.*)' % metric_name
    result_string = re.findall(search_string, raw_result)

    if not result_string:
      raise ValueError(
          'Cannot get percentage from ListConsistency test.')

    results.append(sample.Sample(
        metric_name,
        (float)(result_string[0]),
        NA_UNIT,
        metadata))

    # Parse the list inconsistency window if there is any.
    metric_name = '%s %s' % (scenario, LIST_INCONSISTENCY_WINDOW)
    search_string = '%s: (.*)' % metric_name
    result_string = re.findall(search_string, raw_result)
    _JsonStringToPercentileResults(results,
                                   result_string[0],
                                   metric_name,
                                   LATENCY_UNIT,
                                   metadata)

    # Also report the list latency. These latencies are from the lists
    # that were consistent.
    metric_name = '%s %s' % (scenario, LIST_LATENCY)
    search_string = '%s: (.*)' % metric_name
    result_string = re.findall(search_string, raw_result)
    _JsonStringToPercentileResults(results,
                                   result_string[0],
                                   metric_name,
                                   LATENCY_UNIT,
                                   metadata)


def LoadWorkerOutput(output):
  """Load output from worker processes to our internal format.

  Args:
    output: list of strings. The stdouts of all worker processes.

  Returns:
    A tuple of start_time, latency, size. Each of these is a list of
    numpy arrays, one array per worker process. start_time[i],
    latency[i], and size[i] together form a table giving the start
    time, latency, and size (bytes transmitted or received) of all
    send/receive operations for worker i.

    start_time holds POSIX timestamps, stored as np.float64. latency
    holds times in seconds, stored as np.float64. size holds sizes in
    bytes, stored as np.int64.

    Example:
      start_time[i]  latency[i]  size[i]
      -------------  ----------  -------
               0.0         0.5      100
               1.0         0.7      200
               2.3         0.3      100

  Raises:
    AssertionError, if an individual worker's input includes
    overlapping operations, or operations that don't move forward in
    time, or if the input list isn't in stream number order.
  """

  start_times = []
  latencies = []
  sizes = []

  for worker_out in output:
    json_out = json.loads(worker_out)

    for stream in json_out:
      assert len(stream['start_times']) == len(stream['latencies'])
      assert len(stream['latencies']) == len(stream['sizes'])

      start_times.append(np.asarray(stream['start_times'], dtype=np.float64))
      latencies.append(np.asarray(stream['latencies'], dtype=np.float64))
      sizes.append(np.asarray(stream['sizes'], dtype=np.int64))

  return start_times, latencies, sizes


def _RunMultiStreamProcesses(vms, command_builder, cmd_args, streams_per_vm):
  """Runs all of the multistream read or write processes and doesn't return
     until they complete.

  Args:
    vms: the VMs to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    cmd_args: arguments for the command_builder.
    streams_per_vm: number of threads per vm.
  """

  output = [None] * len(vms)

  def RunOneProcess(vm_idx):
    logging.info('Running on VM %s.', vm_idx)
    cmd = command_builder.BuildCommand(
        cmd_args + ['--stream_num_start=%s' % (vm_idx * streams_per_vm)])
    out, _ = vms[vm_idx].RobustRemoteCommand(cmd, should_log=False)
    output[vm_idx] = out

  # Each vm/process has a thread managing it.
  threads = [
      threading.Thread(target=RunOneProcess, args=(vm_idx,))
      for vm_idx in xrange(len(vms))]
  for thread in threads:
    thread.start()
  logging.info('Started %s processes.', len(vms))
  # Wait for the threads to finish
  for thread in threads:
    thread.join()
  logging.info('All processes complete.')
  return output


def _DatetimeNow():
  """Returns datetime.datetime.now()."""
  return datetime.datetime.now()


def _ColdObjectsWrittenFilename():
  """Generates a name for the objects_written_file.

  Returns:
    The name of the objects_written_file if it should be created, or None.
  """
  if FLAGS.object_storage_objects_written_file_prefix:
    # Note this format is required by _ColdObjectsWrittenFileAgeHours.
    datetime_suffix = _DatetimeNow().strftime('%Y%m%d-%H%M')
    return '%s-%s-%s-%s' % (
        FLAGS.object_storage_objects_written_file_prefix,
        FLAGS.object_storage_region,
        uuid.uuid4(),  # Add a UUID to support parallel runs that upload data.
        datetime_suffix)
  return None


def _ColdObjectsWrittenFileAgeHours(filename):
  """Determines the age in hours of an objects_written_file.

  Args:
    filename: The name of the file.

  Returns:
    The age of the file in hours (based on the name), or None.
  """
  # Parse the year, month, day, hour, and minute from the filename based on the
  # way it is written in _ColdObjectsWrittenFilename.
  match = re.search(r'(\d\d\d\d)(\d\d)(\d\d)-(\d\d)(\d\d)$', filename)
  if not match:
    return None
  year, month, day, hour, minute = (int(item) for item in match.groups())
  write_datetime = datetime.datetime(year, month, day, hour, minute)
  write_timedelta = _DatetimeNow() - write_datetime
  return write_timedelta.total_seconds() / _SECONDS_PER_HOUR


def _MultiStreamOneWay(results, metadata, vms, command_builder,
                       service, bucket_name, operation):
  """Measures multi-stream latency and throughput in one direction.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vms: the VMs to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: The provider's ObjectStorageService
    bucket_name: the primary bucket to benchmark.
    operation: 'upload' or 'download'

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  objects_written_file = posixpath.join(vm_util.VM_TMP_DIR,
                                        OBJECTS_WRITTEN_FILE)

  size_distribution = _DistributionToBackendFormat(
      FLAGS.object_storage_object_sizes)
  logging.info('Distribution %s, backend format %s.',
               FLAGS.object_storage_object_sizes, size_distribution)

  streams_per_vm = FLAGS.object_storage_streams_per_vm

  start_time = (
      time.time() +
      MultiThreadStartDelay(FLAGS.num_vms, streams_per_vm).m_as('second'))

  logging.info('Start time is %s', start_time)

  cmd_args = [
      '--bucket=%s' % bucket_name,
      '--objects_per_stream=%s' % (
          FLAGS.object_storage_multistream_objects_per_stream),
      '--num_streams=%s' % streams_per_vm,
      '--start_time=%s' % start_time,
      '--objects_written_file=%s' % objects_written_file]

  if operation == 'upload':
    cmd_args += [
        '--object_sizes="%s"' % size_distribution,
        '--object_naming_scheme=%s' % FLAGS.object_storage_object_naming_scheme,
        '--scenario=MultiStreamWrite']
  elif operation == 'download':
    cmd_args += ['--scenario=MultiStreamRead']
  else:
    raise Exception('Value of operation must be \'upload\' or \'download\'.'
                    'Value is: \'' + operation + '\'')

  output = _RunMultiStreamProcesses(vms, command_builder, cmd_args,
                                    streams_per_vm)
  start_times, latencies, sizes = LoadWorkerOutput(output)
  if FLAGS.object_storage_worker_output:
    with open(FLAGS.object_storage_worker_output, 'w') as out_file:
      out_file.write(json.dumps(output))
  _ProcessMultiStreamResults(start_times, latencies, sizes, operation,
                             list(size_distribution.iterkeys()), results,
                             metadata=metadata)

  # Write the objects written file if the flag is set and this is an upload
  objects_written_path_local = _ColdObjectsWrittenFilename()
  if operation == 'upload' and objects_written_path_local is not None:
    # Get the objects written from all the VMs
    # Note these are JSON lists with the following format:
    # [[object1_name, object1_size],[object2_name, object2_size],...]
    outs = vm_util.RunThreaded(
        lambda vm: vm.RemoteCommand('cat ' + objects_written_file), vms)
    maybe_storage_account = ''
    maybe_resource_group = ''
    if FLAGS.storage == 'Azure':
      maybe_storage_account = '"azure_storage_account": "%s", ' % \
                              service.storage_account.name
      maybe_resource_group = '"azure_resource_group": "%s", ' % \
                             service.resource_group.name
    # Merge the objects written from all the VMs into a single string
    objects_written_json = \
        '{%s%s"bucket_name": "%s", "objects_written": %s}' % \
        (maybe_storage_account, maybe_resource_group, bucket_name,
         '[' + ','.join([out for out, _ in outs]) + ']')
    # Write the file
    with open(objects_written_path_local, 'w') as objects_written_file_local:
      objects_written_file_local.write(objects_written_json)


def MultiStreamRWBenchmark(results, metadata, vms, command_builder,
                           service, bucket_name):

  """A benchmark for multi-stream read/write latency and throughput.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vms: the VMs to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: The provider's ObjectStorageService
    bucket_name: the primary bucket to benchmark.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  logging.info('Starting multi-stream write test on %s VMs.', len(vms))

  _MultiStreamOneWay(results, metadata, vms, command_builder, service,
                     bucket_name, 'upload')

  logging.info('Finished multi-stream write test. Starting '
               'multi-stream read test.')

  _MultiStreamOneWay(results, metadata, vms, command_builder, service,
                     bucket_name, 'download')

  logging.info('Finished multi-stream read test.')


def MultiStreamWriteBenchmark(results, metadata, vms, command_builder,
                              service, bucket_name):

  """A benchmark for multi-stream write latency and throughput.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vms: the VMs to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: The provider's ObjectStorageService
    bucket_name: the primary bucket to benchmark.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  logging.info('Starting multi-stream write test on %s VMs.', len(vms))

  _MultiStreamOneWay(results, metadata, vms, command_builder, service,
                     bucket_name, 'upload')

  logging.info('Finished multi-stream write test.')


def MultiStreamReadBenchmark(results, metadata, vms, command_builder,
                             service, bucket_name, read_objects):

  """A benchmark for multi-stream read latency and throughput.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vms: the VMs to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: The provider's ObjectStorageService
    bucket_name: the primary bucket to benchmark.
    read_objects: List of lists of [object_name, object_size]. In the outermost
      list, each element corresponds to a VM's worker process.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  logging.info('Starting multi-stream read test on %s VMs.', len(vms))

  assert read_objects is not None, (
      'api_multistream_reads scenario requires the '
      'object_storage_read_objects_prefix flag to be set.')

  # Send over the objects written file
  try:
    # Write the per-VM objects-written-files
    assert len(read_objects) == len(vms), (
        'object_storage_read_objects_prefix file specified requires exactly '
        '%d VMs, but %d were provisioned.' % (len(read_objects), len(vms)))
    for vm, vm_objects_written in zip(vms, read_objects):
      # Note that each file is written with a unique name so that parallel runs
      # don't overwrite the same local file. They are pushed to the VM to a file
      # named OBJECTS_WRITTEN_FILE.
      tmp_objects_written_path = os.path.join(vm_util.GetTempDir(),
                                              '%s-%s' % (OBJECTS_WRITTEN_FILE,
                                                         vm.name))
      with open(tmp_objects_written_path, 'w') as objects_written_file:
        objects_written_file.write(json.dumps(vm_objects_written))
      vm.PushFile(tmp_objects_written_path,
                  posixpath.join(vm_util.VM_TMP_DIR, OBJECTS_WRITTEN_FILE))
  except Exception as e:
    raise Exception("Failed to upload the objects written files to the VMs: "
                    "%s" % e)

  _MultiStreamOneWay(results, metadata, vms, command_builder, service,
                     bucket_name, 'download')

  logging.info('Finished multi-stream read test.')


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(DATA_FILE)


def _AppendPercentilesToResults(output_results, input_results, metric_name,
                                metric_unit, metadata):
  # PercentileCalculator will (correctly) raise an exception on empty
  # input, but an empty input list makes semantic sense here.
  if len(input_results) == 0:
    return

  percentiles = PercentileCalculator(input_results)
  for percentile in PERCENTILES_LIST:
    output_results.append(sample.Sample(('%s %s') % (metric_name, percentile),
                                        percentiles[percentile],
                                        metric_unit,
                                        metadata))


def CLIThroughputBenchmark(output_results, metadata, vm, command_builder,
                           service, bucket):
  """A benchmark for CLI tool throughput.

  We will upload and download a set of files from/to a local directory
  via cli tools and observe the throughput.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.

  Raises:
    NotEnoughResultsError: if we failed too many times to upload or download.
  """

  data_directory = '/tmp/run/data'
  download_directory = '/tmp/run/temp'

  # The real solution to the iteration count issue is dynamically
  # choosing the number of iterations based on how long they
  # take. This will work for now, though.
  if FLAGS.storage == providers.AZURE:
    iteration_count = CLI_TEST_ITERATION_COUNT_AZURE
  elif FLAGS.cli_test_size == 'normal':
    iteration_count = CLI_TEST_ITERATION_COUNT
  else:
    iteration_count = LARGE_CLI_TEST_ITERATION_COUNT

  # The CLI-based tests require some provisioning on the VM first.
  vm.RemoteCommand(
      'cd /tmp/run/; bash cloud-storage-workload.sh %s' % FLAGS.cli_test_size)

  # CLI tool based tests.
  cli_upload_results = []
  cli_download_results = []
  if FLAGS.cli_test_size == 'normal':
    data_size_in_mbits = DATA_SIZE_IN_MBITS
    file_names = ['file-%s.dat' % i for i in range(100)]
  else:
    data_size_in_mbits = LARGE_DATA_SIZE_IN_MBITS
    file_names = ['file_large_3gib.dat']

  for _ in range(iteration_count):
    try:
      service.EmptyBucket(bucket)
    except Exception:
      pass

    try:
      _, res = service.CLIUploadDirectory(vm, data_directory,
                                          file_names, bucket)
    except errors.VirtualMachine.RemoteCommandError:
      logging.info('failed to upload, skip this iteration.')
      continue

    throughput = data_size_in_mbits / vm_util.ParseTimeCommandResult(res)
    logging.info('cli upload throughput %f', throughput)
    cli_upload_results.append(throughput)

    try:
      vm.RemoveFile(posixpath.join(download_directory, '*'))
    except Exception:
      pass

    try:
      _, res = service.CLIDownloadBucket(vm, bucket,
                                         file_names, download_directory)
    except errors.VirtualMachine.RemoteCommandError:
      logging.info('failed to download, skip this iteration.')
      continue

    throughput = data_size_in_mbits / vm_util.ParseTimeCommandResult(res)
    logging.info('cli download throughput %f', throughput)
    cli_download_results.append(throughput)

  expected_successes = iteration_count * (1 - CLI_TEST_FAILURE_TOLERANCE)

  if (len(cli_download_results) < expected_successes or
      len(cli_upload_results) < expected_successes):
    raise NotEnoughResultsError('Failed to complete the required number of '
                                'iterations.')

  # Report various percentiles.
  metrics_prefix = ''
  if FLAGS.cli_test_size != 'normal':
    metrics_prefix = '%s ' % FLAGS.cli_test_size

  _AppendPercentilesToResults(output_results,
                              cli_upload_results,
                              '%s%s' % (metrics_prefix,
                                        UPLOAD_THROUGHPUT_VIA_CLI),
                              THROUGHPUT_UNIT,
                              metadata)
  _AppendPercentilesToResults(output_results,
                              cli_download_results,
                              '%s%s' % (metrics_prefix,
                                        DOWNLOAD_THROUGHPUT_VIA_CLI),
                              THROUGHPUT_UNIT,
                              metadata)


def PrepareVM(vm, service):
  vm.Install('pip')
  vm.RemoteCommand('sudo pip install absl-py')
  vm.RemoteCommand('sudo pip install pyyaml')

  vm.Install('openssl')

  # Prepare data on vm, create a run directory in temporary directory, and add
  # permission.
  vm.RemoteCommand('sudo mkdir -p /tmp/run/')
  vm.RemoteCommand('sudo chmod 777 /tmp/run/')

  vm.RemoteCommand('sudo mkdir -p /tmp/run/temp/')
  vm.RemoteCommand('sudo chmod 777 /tmp/run/temp/')

  file_path = data.ResourcePath(DATA_FILE)
  vm.PushFile(file_path, '/tmp/run/')

  for file_name in API_TEST_SCRIPT_FILES + service.APIScriptFiles():
    path = data.ResourcePath(os.path.join(API_TEST_SCRIPTS_DIR, file_name))
    logging.info('Uploading %s to %s', path, vm)
    vm.PushFile(path, '/tmp/run/')

  service.PrepareVM(vm)


def CleanupVM(vm, service):
  service.CleanupVM(vm)
  vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall absl-py')
  vm.RemoteCommand('sudo rm -rf /tmp/run/')
  objects_written_file = posixpath.join(vm_util.VM_TMP_DIR,
                                        OBJECTS_WRITTEN_FILE)
  vm.RemoteCommand('rm -f %s' % objects_written_file)


def Prepare(benchmark_spec):
  """Prepare vm with cloud provider tool and prepare vm with data file.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Raises:
    ColdDataError: If this benchmark is reading cold data, but the data isn't
        cold enough (as configured by object_storage_read_objects_min_hours).
  """
  # We would like to always cleanup server side states when exception happens.
  benchmark_spec.always_call_cleanup = True

  # Load the objects to read file if specified
  benchmark_spec.read_objects = None
  if FLAGS.object_storage_read_objects_prefix is not None:
    # By taking a glob, we choose an arbitrary file that is old enough, assuming
    # there is ever more than one.
    search_prefix = '%s-%s*' % (
        FLAGS.object_storage_read_objects_prefix,
        FLAGS.object_storage_region)
    read_objects_filenames = glob.glob(search_prefix)
    logging.info('Considering object files %s*: %s', search_prefix,
                 read_objects_filenames)
    for filename in read_objects_filenames:
      age_hours = _ColdObjectsWrittenFileAgeHours(filename)
      if age_hours and age_hours > FLAGS.object_storage_read_objects_min_hours:
        read_objects_filename = filename
        break
    else:
      raise ColdDataError(
          'Object data older than %d hours does not exist. Current cold data '
          'files include the following: %s' % (
              FLAGS.object_storage_read_objects_min_hours,
              read_objects_filenames))

    with open(read_objects_filename) as read_objects_file:
      # Format of json structure is:
      # {"bucket_name": <bucket_name>,
      #  ... any other provider-specific context needed
      #  "objects_written": <objects_written_array>}
      benchmark_spec.read_objects = json.loads(read_objects_file.read())
      benchmark_spec.read_objects_filename = read_objects_filename
      benchmark_spec.read_objects_age_hours = age_hours

    # When this benchmark reads these files, the data will be deleted. Delete
    # the file that specifies the data too.
    if not FLAGS.object_storage_dont_delete_bucket:
      os.remove(read_objects_filename)

    assert benchmark_spec.read_objects is not None, (
        'Failed to read the file specified by '
        '--object_storage_read_objects_prefix')

  # Load the provider and its object storage service
  providers.LoadProvider(FLAGS.storage)

  service = object_storage_service.GetObjectStorageClass(FLAGS.storage)()
  if (FLAGS.storage == 'Azure' and
      FLAGS.object_storage_read_objects_prefix is not None):
    # Storage provider is azure and we are reading existing objects.
    # Need to prepare the ObjectStorageService with the existing storage
    # account and resource group associated with the bucket containing our
    # objects
    service.PrepareService(
        FLAGS.object_storage_region,
        # On Azure, use an existing storage account if we
        # are reading existing objects
        (benchmark_spec.read_objects['azure_storage_account'],
         benchmark_spec.read_objects['azure_resource_group']))
  else:
    service.PrepareService(FLAGS.object_storage_region)

  vms = benchmark_spec.vms
  vm_util.RunThreaded(lambda vm: PrepareVM(vm, service), vms)

  if benchmark_spec.read_objects is not None:
    # Using an existing bucket
    bucket_name = benchmark_spec.read_objects['bucket_name']
    if FLAGS.object_storage_bucket_name is not None:
      logging.warning('--object_storage_bucket_name ignored because '
                      '--object_storage_read_objects was specified')
  else:
    # Make the bucket(s)
    bucket_name = FLAGS.object_storage_bucket_name or 'pkb%s' % FLAGS.run_uri
    if FLAGS.storage == 'GCP' and FLAGS.object_storage_gcs_multiregion:
      # Use a GCS multiregional bucket
      multiregional_service = gcs.GoogleCloudStorageService()
      multiregional_service.PrepareService(FLAGS.object_storage_gcs_multiregion
                                           or DEFAULT_GCS_MULTIREGION)
      multiregional_service.MakeBucket(bucket_name)
    else:
      # Use a regular bucket
      service.MakeBucket(bucket_name)

  # Save the service and the bucket name for later
  benchmark_spec.service = service
  benchmark_spec.bucket_name = bucket_name


def Run(benchmark_spec):
  """Run storage benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    Total throughput in the form of tuple. The tuple contains
        the sample metric (string), value (float), unit (string).
  """
  logging.info('Start benchmarking object storage service, '
               'scenario is %s, storage provider is %s.',
               FLAGS.object_storage_scenario, FLAGS.storage)

  service = benchmark_spec.service
  bucket_name = benchmark_spec.bucket_name

  metadata = {'storage_provider': FLAGS.storage}

  vms = benchmark_spec.vms

  if FLAGS[OBJECT_STORAGE_REGION].present:
    metadata[REGIONAL_BUCKET_LOCATION] = FLAGS.object_storage_region
  else:
    metadata[REGIONAL_BUCKET_LOCATION] = DEFAULT

  if FLAGS[OBJECT_STORAGE_GCS_MULTIREGION].present:
    metadata[GCS_MULTIREGION_LOCATION] = FLAGS.object_storage_gcs_multiregion
  else:
    metadata[GCS_MULTIREGION_LOCATION] = DEFAULT

  metadata.update(service.Metadata(vms[0]))

  results = []
  test_script_path = '/tmp/run/%s' % API_TEST_SCRIPT
  try:
    command_builder = APIScriptCommandBuilder(
        test_script_path, STORAGE_TO_API_SCRIPT_DICT[FLAGS.storage], service)
  except KeyError:
    command_builder = UnsupportedProviderCommandBuilder(FLAGS.storage)

  for name, benchmark in [('cli', CLIThroughputBenchmark),
                          ('api_data', OneByteRWBenchmark),
                          ('api_data', SingleStreamThroughputBenchmark),
                          ('api_namespace', ListConsistencyBenchmark)]:
    if FLAGS.object_storage_scenario in {name, 'all'}:
      benchmark(results, metadata, vms[0], command_builder,
                service, bucket_name)

  # MultiStreamRW and MultiStreamWrite support multiple VMs, so they have a
  # slightly different calling convention than the others.
  for name, benchmark in [('api_multistream', MultiStreamRWBenchmark),
                          ('api_multistream_writes',
                           MultiStreamWriteBenchmark)]:
    if FLAGS.object_storage_scenario in {name, 'all'}:
      benchmark(results, metadata, vms, command_builder, benchmark_spec.service,
                bucket_name)

  # MultiStreamRead has the additional 'read_objects' parameter
  if FLAGS.object_storage_scenario in {'api_multistream_reads', 'all'}:
    metadata['cold_objects_filename'] = benchmark_spec.read_objects_filename
    metadata['cold_objects_age_hours'] = benchmark_spec.read_objects_age_hours
    MultiStreamReadBenchmark(results, metadata, vms, command_builder,
                             benchmark_spec.service, bucket_name,
                             benchmark_spec.read_objects['objects_written'])

  # Clear the bucket if we're not saving the objects for later
  # This is needed for long running tests, or else the objects would just pile
  # up after each run.
  keep_bucket = (FLAGS.object_storage_objects_written_file_prefix is not None or
                 FLAGS.object_storage_dont_delete_bucket)
  if not keep_bucket:
    service.EmptyBucket(bucket_name)

  return results


def Cleanup(benchmark_spec):
  """Clean up storage bucket/container and clean up vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """

  service = benchmark_spec.service
  bucket_name = benchmark_spec.bucket_name
  vms = benchmark_spec.vms

  vm_util.RunThreaded(lambda vm: CleanupVM(vm, service), vms)

  # Only clean up bucket if we're not saving the objects for a later run
  keep_bucket = (FLAGS.object_storage_objects_written_file_prefix is not None or
                 FLAGS.object_storage_dont_delete_bucket)
  if not keep_bucket:
    service.DeleteBucket(bucket_name)
    service.CleanupService()
