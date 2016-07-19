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

Documentation: https://goto.google.com/perfkitbenchmarker-storage
"""

import itertools
import json
import logging
import os
import posixpath
import re
import threading
import time

import pandas as pd

from perfkitbenchmarker import object_storage_multistream_analysis as analysis
from perfkitbenchmarker import providers
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import object_storage_service
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
                   'api_multistream'],
                  'select all, or one particular scenario to run: \n'
                  'ALL: runs all scenarios. This is the default. \n'
                  'cli: runs the command line only scenario. \n'
                  'api_data: runs API based benchmarking for data paths. \n'
                  'api_namespace: runs API based benchmarking for namespace '
                  'operations. \n'
                  'api_multistream: runs API-based benchmarking with multiple '
                  'upload/download streams.')

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
flags.DEFINE_integer('object_storage_multistream_num_streams', 10,
                     'Number of independent streams to send and/or receive on. '
                     'Only applies to the api_multistream scenario.',
                     lower_bound=1)

flags.DEFINE_integer('object_storage_list_consistency_iterations', 200,
                     'Number of iterations to perform for the api_namespace '
                     'list consistency benchmark. This flag is mainly for '
                     'regression testing in the benchmarks. Reduce the number '
                     'to shorten the execution time of the api_namespace '
                     'scenario. However, to get useful metrics from the '
                     'api_namespace scenario, a high number of iterations '
                     'should be used (>=200).')


FLAGS = flags.FLAGS

# User a scratch disk here to simulate what most users would do when they
# use CLI tools to interact with the storage provider.
BENCHMARK_INFO = {'name': 'object_storage_service',
                  'description':
                  'Object/blob storage service benchmarks. Specify '
                  '--object_storage_scenario '
                  'to select a set of sub-benchmarks to run. default is all.',
                  'scratch_disk': True,
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
      disk_spec: *default_500_gb
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


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


# Raised when we fail to remove a bucket or its content after many retries.
# TODO: add a new class of error "ObjectStorageError" to errors.py and remove
# this one.
class BucketRemovalError(Exception):
    pass


class NotEnoughResultsError(Exception):
    pass


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


def _ProcessMultiStreamResults(raw_result, operation, sizes,
                               results, metadata=None):
  """Read and process results from the api_multistream worker process.

  Results will be reported per-object size and combined for all
  objects.

  Args:
    raw_result: list of strings. The stdouts of the worker processes.
    operation: 'upload' or 'download'. The operation the results are from.
    sizes: the object sizes used in the benchmark, in bytes.
    results: a list to append Sample objects to.
    metadata: dict. Base sample metadata
  """

  num_streams = FLAGS.object_storage_multistream_num_streams

  if metadata is None:
    metadata = {}
  metadata['num_streams'] = num_streams
  metadata['objects_per_stream'] = (
      FLAGS.object_storage_multistream_objects_per_stream)

  records = pd.DataFrame({'operation': [],
                          'start_time': [],
                          'latency': [],
                          'size': [],
                          'stream_num': []})
  for proc_result in raw_result:
    proc_json = json.loads(proc_result)
    records = records.append(pd.DataFrame(proc_json))
  records = records.reset_index()

  logging.info('Records:\n%s', records)
  logging.info('All latencies positive:%s',
               (records['latency'] > 0).all())

  any_streams_active, all_streams_active = analysis.GetStreamActiveIntervals(
      records['start_time'], records['latency'], records['stream_num'])
  start_gap, stop_gap = analysis.StreamStartAndEndGaps(
      records['start_time'], records['latency'], all_streams_active)
  if ((start_gap + stop_gap) / any_streams_active.duration <
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
        any_streams_active.duration)
    metadata['stream_gap_above_threshold'] = True

  records_in_interval = records[
      analysis.FullyInInterval(records['start_time'],
                               records['latency'],
                               all_streams_active)]

  # Don't publish the full distribution in the metadata because doing
  # so might break regexp-based parsers that assume that all metadata
  # values are simple Python objects. However, do add an
  # 'object_size_B' metadata field even for the full results because
  # searching metadata is easier when all records with the same metric
  # name have the same set of metadata fields.
  distribution_metadata = metadata.copy()
  distribution_metadata['object_size_B'] = 'distribution'

  latency_prefix = 'Multi-stream %s latency' % operation
  logging.info('Processing %s multi-stream %s results for the full '
               'distribution.', len(records_in_interval), operation)
  _AppendPercentilesToResults(
      results,
      records_in_interval['latency'],
      latency_prefix,
      LATENCY_UNIT,
      distribution_metadata)

  logging.info('Processing %s multi-stream %s results for net throughput',
               len(records), operation)
  throughput_stats = analysis.ThroughputStats(
      records_in_interval['start_time'],
      records_in_interval['latency'],
      records_in_interval['size'],
      records_in_interval['stream_num'],
      num_streams)
  # A special throughput statistic that uses all the records, not
  # restricted to the interval.
  throughput_stats['net throughput (simplified)'] = (
      records['size'].sum() * 8 / any_streams_active.duration
      * units.bit / units.second)
  gap_stats = analysis.GapStats(
      records['start_time'],
      records['latency'],
      records['stream_num'],
      all_streams_active,
      num_streams)
  logging.info('Benchmark overhead was %s percent of total benchmark time',
               gap_stats['gap time proportion'].magnitude)

  for name, value in itertools.chain(throughput_stats.iteritems(),
                                     gap_stats.iteritems()):
    results.append(sample.Sample(
        'Multi-stream ' + operation + ' ' + name,
        value.magnitude, str(value.units), metadata=distribution_metadata))

  # Publish by-size and full-distribution stats even if there's only
  # one size in the distribution, because it simplifies postprocessing
  # of results.
  for size in sizes:
    this_size_records = records_in_interval[records_in_interval['size'] == size]
    this_size_metadata = metadata.copy()
    this_size_metadata['object_size_B'] = size
    logging.info('Processing %s multi-stream %s results for object size %s',
                 len(records), operation, size)
    _AppendPercentilesToResults(
        results,
        this_size_records['latency'],
        latency_prefix,
        LATENCY_UNIT,
        this_size_metadata)


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
                       service, bucket_name, regional_bucket_name):
  """A benchmark for small object latency.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.
    regional_bucket_name: the secondary bucket to benchmark.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  buckets = [bucket_name]
  if regional_bucket_name is not None:
    buckets.append(regional_bucket_name)

  for bucket in buckets:
    one_byte_rw_cmd = command_builder.BuildCommand([
        '--bucket=%s' % bucket,
        '--scenario=OneByteRW'])

    _, raw_result = vm.RemoteCommand(one_byte_rw_cmd)
    logging.info('OneByteRW raw result is %s', raw_result)

    for up_and_down in ['upload', 'download']:
      search_string = 'One byte %s - (.*)' % up_and_down
      result_string = re.findall(search_string, raw_result)
      sample_name = ONE_BYTE_LATENCY % up_and_down
      if bucket == regional_bucket_name:
        sample_name = 'regional %s' % sample_name

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
                                    service, bucket_name, regional_bucket_name):
  """A benchmark for large object throughput.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.
    regional_bucket_name: the secondary bucket to benchmark.

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
                             service, bucket_name, regional_bucket_name):
  """A benchmark for bucket list consistency.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.
    regional_bucket_name: the secondary bucket to benchmark.

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


def MultiStreamRWBenchmark(results, metadata, vm, command_builder,
                           service, bucket_name, regional_bucket_name):
  """A benchmark for multi-stream latency and throughput.

  Args:
    results: the results array to append to.
    metadata: a dictionary of metadata to add to samples.
    vm: the VM to run the benchmark on.
    command_builder: an APIScriptCommandBuilder.
    service: an ObjectStorageService.
    bucket_name: the primary bucket to benchmark.
    regional_bucket_name: the secondary bucket to benchmark.

  Raises:
    ValueError if an unexpected test outcome is found from the API
    test script.
  """

  logging.info('Starting multi-stream write test.')

  objects_written_file = posixpath.join(vm_util.VM_TMP_DIR,
                                        OBJECTS_WRITTEN_FILE)

  size_distribution = _DistributionToBackendFormat(
      FLAGS.object_storage_object_sizes)

  def StartMultiStreamProcess(cmd_args, proc_idx, out_array):
    cmd = command_builder.BuildCommand(
        cmd_args + ['--stream_num_start=%s' % proc_idx])
    out, _ = vm.RobustRemoteCommand(cmd, should_log=True)
    out_array[proc_idx] = out

  def RunMultiStreamProcesses(command):
    output = [None] * FLAGS.object_storage_multistream_num_streams
    # Each process has a thread managing it.
    threads = [
        threading.Thread(target=StartMultiStreamProcess,
                         args=(command, i, output))
        for i in xrange(FLAGS.object_storage_multistream_num_streams)]
    for thread in threads:
      thread.start()
    logging.info('Started %s processes.',
                 FLAGS.object_storage_multistream_num_streams)
    for thread in threads:
      thread.join()
    logging.info('All processes complete.')
    return output

  streams_per_vm = FLAGS.object_storage_multistream_num_streams // FLAGS.num_vms

  write_start_time = (
      time.time() +
      MultiThreadStartDelay(FLAGS.num_vms, streams_per_vm).m_as('second'))

  logging.info('Write start time is %s', write_start_time)

  multi_stream_write_args = [
      '--bucket=%s' % bucket_name,
      '--objects_per_stream=%s' % (
          FLAGS.object_storage_multistream_objects_per_stream),
      '--object_sizes="%s"' % size_distribution,
      '--num_streams=1',
      '--start_time=%s' % write_start_time,
      '--objects_written_file=%s' % objects_written_file,
      '--scenario=MultiStreamWrite']

  write_out = RunMultiStreamProcesses(multi_stream_write_args)
  _ProcessMultiStreamResults(write_out, 'upload',
                             size_distribution.iterkeys(), results,
                             metadata=metadata)

  logging.info('Finished multi-stream write test. Starting multi-stream '
               'read test.')

  read_start_time = (
      time.time() +
      MultiThreadStartDelay(FLAGS.num_vms, streams_per_vm).m_as('second'))

  logging.info('Read start time is %s', read_start_time)

  multi_stream_read_args = [
      '--bucket=%s' % bucket_name,
      '--objects_per_stream=%s' % (
          FLAGS.object_storage_multistream_objects_per_stream),
      '--num_streams=1',
      '--start_time=%s' % read_start_time,
      '--objects_written_file=%s' % objects_written_file,
      '--scenario=MultiStreamRead']
  try:
    read_out = RunMultiStreamProcesses(multi_stream_read_args)
    _ProcessMultiStreamResults(read_out, 'download',
                               size_distribution.iterkeys(), results,
                               metadata=metadata)
  except Exception as ex:
    logging.info('MultiStreamRead test failed with exception %s. Still '
                 'recording write data.', ex.msg)

  logging.info('Finished multi-stream read test.')


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(DATA_FILE)


def _AppendPercentilesToResults(output_results, input_results, metric_name,
                                metric_unit, metadata):
  percentiles = PercentileCalculator(input_results)
  for percentile in PERCENTILES_LIST:
    output_results.append(sample.Sample(('%s %s') % (metric_name, percentile),
                                        percentiles[percentile],
                                        metric_unit,
                                        metadata))


def CLIThroughputBenchmark(output_results, metadata, vm, command_builder,
                           service, bucket, regional_bucket):
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
    regional_bucket_name: the secondary bucket to benchmark.

  Raises:
    NotEnoughResultsError: if we failed too many times to upload or download.
  """

  data_directory = '%s/run/data' % vm.GetScratchDir()
  download_directory = '%s/run/temp' % vm.GetScratchDir()

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
      'cd %s/run/; bash cloud-storage-workload.sh %s' % (vm.GetScratchDir(),
                                                         FLAGS.cli_test_size))

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
  vm.RemoteCommand('sudo pip install python-gflags==2.0')
  vm.RemoteCommand('sudo pip install pyyaml')

  vm.Install('openssl')

  # Prepare data on vm, create a run directory on scratch drive, and add
  # permission.
  scratch_dir = vm.GetScratchDir()
  vm.RemoteCommand('sudo mkdir -p %s/run/' % scratch_dir)
  vm.RemoteCommand('sudo chmod 777 %s/run/' % scratch_dir)

  vm.RemoteCommand('sudo mkdir -p %s/run/temp/' % scratch_dir)
  vm.RemoteCommand('sudo chmod 777 %s/run/temp/' % scratch_dir)

  file_path = data.ResourcePath(DATA_FILE)
  vm.PushFile(file_path, '%s/run/' % scratch_dir)

  for file_name in API_TEST_SCRIPT_FILES + service.APIScriptFiles():
    path = data.ResourcePath(os.path.join(API_TEST_SCRIPTS_DIR, file_name))
    logging.info('Uploading %s to %s', path, vm)
    vm.PushFile(path, '%s/run/' % scratch_dir)


def CleanupVM(vm):
  vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall python-gflags')
  vm.RemoteCommand('rm -rf %s/run/' % vm.GetScratchDir())
  objects_written_file = posixpath.join(vm_util.VM_TMP_DIR,
                                        OBJECTS_WRITTEN_FILE)
  vm.RemoteCommand('rm -f %s' % objects_written_file)


def Prepare(benchmark_spec):
  """Prepare vm with cloud provider tool and prepare vm with data file.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """

  providers.LoadProvider(FLAGS.storage)

  service = object_storage_service.GetObjectStorageClass(FLAGS.storage)()
  service.PrepareService(FLAGS.object_storage_region)

  vms = benchmark_spec.vms
  PrepareVM(vms[0], service)
  service.PrepareVM(vms[0])

  # We would like to always cleanup server side states when exception happens.
  benchmark_spec.always_call_cleanup = True

  # Make the bucket(s)
  bucket_name = 'pkb%s' % FLAGS.run_uri
  if FLAGS.storage != 'GCP':
    service.MakeBucket(bucket_name)
    buckets = [bucket_name]
  else:
    # TODO(nlavine): make GCP bucket name handling match other
    # providers. Leaving it inconsistent for now to match previous
    # behavior, but should change it after a reasonable deprecation
    # period.
    multiregional_service = gcs.GoogleCloudStorageService()
    multiregional_service.PrepareService(FLAGS.object_storage_gcs_multiregion
                                         or DEFAULT_GCS_MULTIREGION)
    multiregional_service.MakeBucket(bucket_name)

    region = FLAGS.object_storage_region or gcs.DEFAULT_GCP_REGION
    regional_bucket_name = 'pkb%s-%s' % (FLAGS.run_uri, region)
    regional_service = gcs.GoogleCloudStorageService()
    regional_service.PrepareService(region)
    regional_service.MakeBucket(regional_bucket_name)
    buckets = [bucket_name, regional_bucket_name]

  # Save the service and the buckets for later
  benchmark_spec.service = service
  benchmark_spec.buckets = buckets


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
  buckets = benchmark_spec.buckets

  metadata = {'storage provider': FLAGS.storage}

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
  test_script_path = '%s/run/%s' % (vms[0].GetScratchDir(), API_TEST_SCRIPT)
  try:
    command_builder = APIScriptCommandBuilder(
        test_script_path, STORAGE_TO_API_SCRIPT_DICT[FLAGS.storage], service)
  except KeyError:
    command_builder = UnsupportedProviderCommandBuilder(FLAGS.storage)
  regional_bucket_name = buckets[1] if len(buckets) == 2 else None

  for name, benchmark in [('cli', CLIThroughputBenchmark),
                          ('api_data', OneByteRWBenchmark),
                          ('api_data', SingleStreamThroughputBenchmark),
                          ('api_namespace', ListConsistencyBenchmark),
                          ('api_multistream', MultiStreamRWBenchmark)]:
    if FLAGS.object_storage_scenario in {name, 'all'}:
      benchmark(results, metadata, vms[0], command_builder,
                service, buckets[0], regional_bucket_name)

  return results


def Cleanup(benchmark_spec):
  """Clean up storage bucket/container and clean up vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """

  service = benchmark_spec.service
  buckets = benchmark_spec.buckets
  vms = benchmark_spec.vms

  service.CleanupVM(vms[0])
  CleanupVM(vms[0])

  for bucket in buckets:
    service.DeleteBucket(bucket)

  service.CleanupService()
