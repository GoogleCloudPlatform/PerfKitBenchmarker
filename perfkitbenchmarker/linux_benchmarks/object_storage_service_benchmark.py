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

import json
import logging
import os
import re
import time

from perfkitbenchmarker import providers
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.sample import PercentileCalculator  # noqa

flags.DEFINE_enum('storage', providers.GCP,
                  [providers.GCP, providers.AWS,
                   providers.AZURE, providers.OPENSTACK],
                  'storage provider (GCP/AZURE/AWS/OPENSTACK) to use.')

flags.DEFINE_string('object_storage_region', None,
                    'Storage region for object storage benchmark.')

flags.DEFINE_enum('object_storage_scenario', 'all',
                  ['all', 'cli', 'api_data', 'api_namespace'],
                  'select all, or one particular scenario to run: \n'
                  'ALL: runs all scenarios. This is the default. \n'
                  'cli: runs the command line only scenario. \n'
                  'api_data: runs API based benchmarking for data paths. \n'
                  'api_namespace: runs API based benchmarking for namespace '
                  'operations.')

flags.DEFINE_enum('cli_test_size', 'normal',
                  ['normal', 'large'],
                  'size of the cli tests. Normal means a mixture of various \n'
                  'object sizes up to 32MiB (see '
                  'data/cloud-storage-workload.sh). \n'
                  'Large means all objects are of at least 1GiB.')

flags.DEFINE_string('object_storage_credential_file', None,
                    'Directory of credential file.')

flags.DEFINE_string('boto_file_location', None,
                    'The location of the boto file.')

flags.DEFINE_string('azure_lib_version', None,
                    'Use a particular version of azure client lib, e.g.: 1.0.2')

flags.DEFINE_boolean('openstack_swift_insecure', False,
                     'Allow swiftclient to access Swift service without \n'
                     'having to verify the SSL certificate')

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

AWS_CREDENTIAL_LOCATION = '.aws'
GCE_CREDENTIAL_LOCATION = '.config/gcloud/credentials'
AZURE_CREDENTIAL_LOCATION = '.azure'

DEFAULT_BOTO_LOCATION = '~/.boto'
BOTO_LIB_VERSION = 'boto_lib_version'

SWIFTCLIENT_LIB_VERSION = 'python-swiftclient_lib_version'

OBJECT_STORAGE_CREDENTIAL_DEFAULT_LOCATION = {
    providers.GCP: '~/' + GCE_CREDENTIAL_LOCATION,
    providers.AWS: '~/' + AWS_CREDENTIAL_LOCATION,
    providers.AZURE: '~/' + AZURE_CREDENTIAL_LOCATION,
    providers.OPENSTACK: '~/'}

DATA_FILE = 'cloud-storage-workload.sh'
# size of all data used in the CLI tests.
DATA_SIZE_IN_BYTES = 256.1 * 1024 * 1024
DATA_SIZE_IN_MBITS = 8 * DATA_SIZE_IN_BYTES / 1000 / 1000
LARGE_DATA_SIZE_IN_BYTES = 3 * 1024 * 1024 * 1024
LARGE_DATA_SIZE_IN_MBITS = 8 * LARGE_DATA_SIZE_IN_BYTES / 1000 / 1000

API_TEST_SCRIPT = 'object_storage_api_tests.py'

# The default number of iterations to run for the list consistency benchmark.
LIST_CONSISTENCY_ITERATIONS = 200

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

DEFAULT_GCP_REGION = 'us-central1'
DEFAULT_AWS_REGION = 'us-east-1'
DEFAULT_AZURE_REGION = 'East US'

# The endpoints in this table are subdomains of 'amazonaws.com'. So
# where the table says 's3-us-west-2', you should connect to
# 's3-us-west-2.amazonaws.com'. This table comes from
# http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
AWS_S3_REGION_TO_ENDPOINT_TABLE = {
    'us-east-1': 's3-external-1',
    'us-west-2': 's3-us-west-2',
    'us-west-1': 's3-us-west-1',
    'eu-west-1': 's3-eu-west-1',
    'eu-central-1': 's3-eu-central-1',
    'ap-southeast-1': 's3-ap-southeast-1',
    'ap-southeast-2': 's3-ap-southeast-2',
    'ap-northeast-1': 's3-ap-northeast-1',
    'sa-east-1': 's3-sa-east-1'
}
AWS_S3_ENDPOINT_SUFFIX = '.amazonaws.com'


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


def _MakeAzureCommandSuffix(account_name, account_key, for_cli):
  """ This function returns a suffix for Azure command.

  Args:
    account_name: The name of the Azure storage account.
    account_key: The key to access the account.
    for_cli: If true, the suffix can be passed to the Azure cli tool; if false,
      the suffix created will be used to call our own test script for api-based
      tests.

  returns:
    A string represents a command suffix.
  """

  if for_cli:
    return (' -a %s -k %s') % (account_name, account_key)
  else:
    return (' --azure_account=%s --azure_key=%s') % (account_name, account_key)


def _MakeSwiftCommandPrefix(auth_url, tenant_name, username, password):
  """This function returns a prefix for Swift CLI command.

  Args:
    auth_url: URL for obtaining auth token.
    tenant_name: tenant name for obtaining auth token.
    username: username for obtaining auth token.
    password: password for obtaining auth token.

  Returns:
    string represents a command prefix.
  """
  options = ('--os-auth-url', auth_url,
             '--os-tenant-name', tenant_name,
             '--os-username', username,
             '--os-password', password,
             '--insecure' if FLAGS.openstack_swift_insecure else '',)
  return ' '.join(options)


def ApiBasedBenchmarks(results, metadata, vm, storage, test_script_path,
                       bucket_name, regional_bucket_name=None,
                       azure_command_suffix=None, host_to_connect=None):
    """This function contains all api-based benchmarks.
       It uses the value of the global flag "object_storage_scenario" to
       decide which scenario to run inside this function. The caller simply
       invokes this function without having to worry about which scenario to
       select.

    Args:
      vm: The vm being used to run the benchmark.
      results: The results array to append to.
      storage: The storage provider to run: S3 or GCS or Azure.
      test_script_path: The complete path to the test script on the target VM.
      bucket_name: The name of the bucket caller has created for this test.
      regional_bucket_name: The name of the "regional" bucket, if applicable.
      azure_command_suffix: A suffix for all Azure related test commands.
      host_to_connect: An optional endpoint string to connect to.

    Raises:
      ValueError: unexpected test outcome is found from the API test script.
    """
    if FLAGS.object_storage_scenario == 'cli':
      # User only wants to run the CLI based tests, do nothing here:
      return

    def BuildBenchmarkScriptCommand(args):
      """Build a command string for the API test script.

      Args:
        A list of strings. These will become space-separated arguments to the
          test script.

      Returns:
        A string that can be passed to vm.RemoteCommand.
      """

      cmd_parts = [
          test_script_path,
          '--storage_provider=%s' % storage
      ] + args
      if azure_command_suffix is not None:
        cmd_parts += [azure_command_suffix]
      if host_to_connect is not None:
        cmd_parts += ['--host', host_to_connect]

      return ' '.join(cmd_parts)

    if (FLAGS.object_storage_scenario == 'all' or
        FLAGS.object_storage_scenario == 'api_data'):
      # One byte RW latency
      buckets = [bucket_name]
      if regional_bucket_name is not None:
        buckets.append(regional_bucket_name)

      for bucket in buckets:
        one_byte_rw_cmd = BuildBenchmarkScriptCommand([
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

      # Single stream large object throughput metrics
      single_stream_throughput_cmd = BuildBenchmarkScriptCommand([
          '--bucket=%s' % bucket_name,
          '--scenario=SingleStreamThroughput'])

      _, raw_result = vm.RemoteCommand(single_stream_throughput_cmd)
      logging.info('SingleStreamThroughput raw result is %s', raw_result)

      for up_and_down in ['upload', 'download']:
        search_string = 'Single stream %s throughput in Bps: (.*)' % up_and_down
        result_string = re.findall(search_string, raw_result)
        sample_name = SINGLE_STREAM_THROUGHPUT % up_and_down

        if len(result_string) > 0:
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
        else:
          raise ValueError('Unexpected test outcome from '
                           'SingleStreamThroughput api test: %s.' % raw_result)

    if (FLAGS.object_storage_scenario == 'all' or
        FLAGS.object_storage_scenario == 'api_namespace'):
      # list-after-write consistency metrics
      list_consistency_cmd = BuildBenchmarkScriptCommand([
          '--bucket=%s' % bucket_name,
          '--iterations=%d' % LIST_CONSISTENCY_ITERATIONS,
          '--scenario=ListConsistency'])

      _, raw_result = vm.RemoteCommand(list_consistency_cmd)
      logging.info('ListConsistency raw result is %s', raw_result)

      for scenario in LIST_CONSISTENCY_SCENARIOS:
        metric_name = '%s %s' % (scenario, LIST_CONSISTENCY_PERCENTAGE)
        search_string = '%s: (.*)' % metric_name
        result_string = re.findall(search_string, raw_result)
        if len(result_string) > 0:
          results.append(sample.Sample(metric_name,
                                       (float)(result_string[0]),
                                       NA_UNIT,
                                       metadata))
        else:
          raise ValueError(
              'Cannot get percentage from ListConsistency test.')

        # Parse the list inconsistency window if there is any.
        metric_name = '%s %s' % (scenario, LIST_INCONSISTENCY_WINDOW)
        search_string = '%s: (.*)' % metric_name
        result_string = re.findall(search_string, raw_result)
        if len(result_string) > 0:
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
        if len(result_string) > 0:
          _JsonStringToPercentileResults(results,
                                         result_string[0],
                                         metric_name,
                                         LATENCY_UNIT,
                                         metadata)


def DeleteBucketWithRetry(vm, remove_content_cmd, remove_bucket_cmd):
  """ Delete a bucket and all its contents robustly.

      First we try to recursively delete its content with retries, if failed,
      we raise the error. If successful, we move on to remove the empty bucket.
      Due to eventual consistency issues, some provider may still think the
      bucket is not empty, so we will add a few more retries when we attempt to
      remove the empty bucket.

      Args:
        vm: the vm to run the command.
        remove_content_cmd: the command line to run to remove objects in the
            bucket.
        remove_bucket_cmd: the command line to run to remove the empty bucket.

      Raises:
        BucketRemovalError: when we failed multiple times to remove the content
            or the bucket itself.
  """
  retry_limit = 0
  for cmd in [remove_content_cmd, remove_bucket_cmd]:
    if cmd is remove_content_cmd:
      retry_limit = CONTENT_REMOVAL_RETRY_LIMIT
    else:
      retry_limit = BUCKET_REMOVAL_RETRY_LIMIT

    removal_successful = False
    logging.info('Performing removal action, cmd is %s', cmd)
    for i in range(retry_limit):
      try:
          vm.RemoteCommand(cmd)
          removal_successful = True
          logging.info('Successfully performed the removal operation.')
          break
      except Exception as e:
        logging.error('Failed to perform the removal op. Number '
                      'of attempts: %d. Error is %s', i + 1, e)
        time.sleep(RETRY_WAIT_INTERVAL_SECONDS)
        pass

    if not removal_successful:
      if cmd is remove_content_cmd:
        logging.error('Exceeded max retry limit for removing the content of '
                      'bucket. But we will try to delete the bucket anyway.')
      else:
        logging.error('Exceeded max retry limit for removing the empty bucket')
        raise BucketRemovalError('Failed to remove the bucket')


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


def _CliBasedTests(output_results, metadata, vm, iteration_count,
                   clean_up_bucket_cmd, upload_cmd,
                   cleanup_local_temp_cmd, download_cmd):
  """ Performs tests via cli tools

    We will upload and download a set of files from/to a local directory via
    cli tools and observe the throughput.

    Args:
      output_results: The collection to put results in.
      metadata: The metadata to be included in the result.
      vm: The vm to run the tests.
      iteration_count: The number of iterations to run for this test.
      clean_up_bucket_cmd: The command to run to cleanup the bucket.
      upload_cmd: The command to run to upload the objects.
      cleanup_local_temp_cmd: The command to run to cleanup the local temp dir.
      download_cmd: The command to run to download the content.

    Raises:
      NotEnoughResultsError: if we failed too many times to upload or download.
  """
  if (FLAGS.object_storage_scenario != 'all' and
      FLAGS.object_storage_scenario != 'cli'):
    # User does not want to run this scenario, do nothing.
    return

  # CLI tool based tests.
  cli_upload_results = []
  cli_download_results = []
  data_size_in_mbits = 0
  if FLAGS.cli_test_size == 'normal':
    data_size_in_mbits = DATA_SIZE_IN_MBITS
  else:
    data_size_in_mbits = LARGE_DATA_SIZE_IN_MBITS

  for _ in range(iteration_count):
    vm.RemoteCommand(clean_up_bucket_cmd, ignore_failure=True)

    upload_successful = False
    try:
      _, res = vm.RemoteCommand(upload_cmd)
      upload_successful = True
    except:
      logging.info('failed to upload, skip this iteration.')
      pass

    if upload_successful:
      logging.debug(res)
      throughput = data_size_in_mbits / vm_util.ParseTimeCommandResult(res)

      # Output some log traces to show we are making progress
      logging.info('cli upload throughput %f', throughput)
      cli_upload_results.append(throughput)

      download_successful = False
      vm.RemoteCommand(cleanup_local_temp_cmd, ignore_failure=True)
      try:
        _, res = vm.RemoteCommand(download_cmd)
        download_successful = True
      except:
        logging.info('failed to download, skip this iteration.')
        pass

      if download_successful:
        logging.debug(res)
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


class S3StorageBenchmark(object):
  """S3 version of storage benchmark."""

  def Prepare(self, vm):
    """Prepare vm with AWS s3 tool and create a bucket using vm.

    Documentation: http://aws.amazon.com/cli/
    Args:
      vm: The vm being used to run the benchmark.
    """
    vm.RemoteCommand('sudo pip install awscli')

    vm.PushFile(FLAGS.object_storage_credential_file, AWS_CREDENTIAL_LOCATION)
    vm.PushFile(FLAGS.boto_file_location, DEFAULT_BOTO_LOCATION)

    vm.bucket_name = 'pkb%s' % FLAGS.run_uri
    vm.storage_region = FLAGS.object_storage_region or DEFAULT_AWS_REGION

    vm.RemoteCommand(
        'aws s3 mb s3://%s --region=%s' % (vm.bucket_name, vm.storage_region))

  def Run(self, vm, metadata):
    """Run upload/download on vm with s3 tool.

    Args:
      vm: The vm being used to run the benchmark.
      metadata: the metadata to be stored with the results.

    Returns:
      A list of lists containing results of the tests. Each scenario outputs
      results to a list of the following format:
        name of the scenario, value, unit of the value, metadata
        e.g.,
        'one byte object upload latency p50', 0.800, 'seconds', 'storage=gcs'

      Then the final return value is the list of the above list that reflect
      the results of all scenarios run here.
    """
    metadata[BOTO_LIB_VERSION] = _GetClientLibVersion(vm, 'boto')
    results = []
    scratch_dir = vm.GetScratchDir()

    # CLI tool based tests.

    clean_up_bucket_cmd = 'aws s3 rm s3://%s --recursive' % vm.bucket_name
    upload_cmd = 'time aws s3 sync %s/run/data/ s3://%s/' % (scratch_dir,
                                                             vm.bucket_name)
    cleanup_local_temp_cmd = 'rm %s/run/temp/*' % scratch_dir
    download_cmd = 'time aws s3 sync s3://%s/ %s/run/temp/' % (
                   vm.bucket_name, scratch_dir)

    if FLAGS.cli_test_size == 'normal':
      iteration_count = CLI_TEST_ITERATION_COUNT
    else:
      iteration_count = LARGE_CLI_TEST_ITERATION_COUNT

    _CliBasedTests(results, metadata, vm, iteration_count,
                   clean_up_bucket_cmd, upload_cmd, cleanup_local_temp_cmd,
                   download_cmd)

    # Now tests the storage provider via APIs
    test_script_path = '%s/run/%s' % (scratch_dir, API_TEST_SCRIPT)
    hostname = AWS_S3_REGION_TO_ENDPOINT_TABLE[vm.storage_region]
    ApiBasedBenchmarks(results, metadata, vm, 'S3', test_script_path,
                       vm.bucket_name,
                       host_to_connect=hostname + AWS_S3_ENDPOINT_SUFFIX)

    return results

  def Cleanup(self, vm):
    """Clean up S3 bucket and uninstall packages on vm.

    Args:
      vm: The vm needs cleanup.
    """
    remove_content_cmd = 'aws s3 rm s3://%s --recursive' % vm.bucket_name
    remove_bucket_cmd = 'aws s3 rb s3://%s' % vm.bucket_name
    DeleteBucketWithRetry(vm, remove_content_cmd, remove_bucket_cmd)

    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall awscli')
    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall python-gflags')


class AzureBlobStorageBenchmark(object):
  """Azure Blob version of storage benchmark."""

  def Prepare(self, vm):
    """Prepare vm with Azure CLI tool and create a storage container using vm.

    Documentation: http://azure.microsoft.com/en-us/documentation/articles/
      xplat-cli/
    Args:
      vm: The vm being used to run the benchmark.
    """

    vm.Install('node_js')
    vm.RemoteCommand('sudo npm install azure-cli -g')

    vm.PushFile(FLAGS.object_storage_credential_file, AZURE_CREDENTIAL_LOCATION)
    region = FLAGS.object_storage_region or DEFAULT_AZURE_REGION
    vm.RemoteCommand(
        'azure storage account create --type ZRS -l \'%s\' ''"pkb%s"' %
        (region, FLAGS.run_uri),
        ignore_failure=False)
    vm.azure_account = ('pkb%s' % FLAGS.run_uri)

    output, _ = (
        vm.RemoteCommand(
            'azure storage account keys list %s' %
            vm.azure_account))
    key = re.findall(r'Primary:* (.+)', output)
    vm.azure_key = key[0]

    azure_command_suffix = _MakeAzureCommandSuffix(vm.azure_account,
                                                   vm.azure_key,
                                                   True)

    vm.RemoteCommand('azure storage container create pkb%s %s' % (
        FLAGS.run_uri, azure_command_suffix))

    vm.bucket_name = 'pkb%s' % FLAGS.run_uri
    vm.RemoteCommand('azure storage blob list %s %s' % (
        vm.bucket_name, azure_command_suffix))

  def Run(self, vm, metadata):
    """Run upload/download on vm with Azure CLI tool.

    Args:
      vm: The vm being used to run the benchmark.
      metadata: the metadata to be stored with the results.

    Returns:
      A list of lists containing results of the tests. Each scenario outputs
      results to a list of the following format:
        name of the scenario, value, unit of the value, metadata
        e.g.,
        'one byte object upload latency p50', 0.800, 'seconds', 'storage=gcs'

      Then the final return value is the list of the above list that reflect
      the results of all scenarios run here.

    """
    metadata['azure_lib_version'] = _GetClientLibVersion(vm, 'azure')
    results = []

    # CLI tool based tests.
    scratch_dir = vm.GetScratchDir()
    test_script_path = '%s/run/%s' % (scratch_dir, API_TEST_SCRIPT)
    cleanup_bucket_cmd = ('%s --bucket=%s --storage_provider=AZURE '
                          ' --scenario=CleanupBucket %s' %
                          (test_script_path,
                           vm.bucket_name,
                           _MakeAzureCommandSuffix(vm.azure_account,
                                                   vm.azure_key,
                                                   False)))
    if FLAGS.cli_test_size == 'normal':
      upload_cmd = ('time for i in {0..99}; do azure storage blob upload '
                    '%s/run/data/file-$i.dat %s %s; done' %
                    (scratch_dir,
                     vm.bucket_name,
                     _MakeAzureCommandSuffix(vm.azure_account,
                                             vm.azure_key,
                                             True)))
    else:
      upload_cmd = ('time azure storage blob upload '
                    '%s/run/data/file_large_3gib.dat %s %s' %
                    (scratch_dir,
                     vm.bucket_name,
                     _MakeAzureCommandSuffix(vm.azure_account,
                                             vm.azure_key,
                                             True)))

    cleanup_local_temp_cmd = 'rm %s/run/temp/*' % scratch_dir

    if FLAGS.cli_test_size == 'normal':
      download_cmd = ('time for i in {0..99}; do azure storage blob download '
                      '%s file-$i.dat %s/run/temp/file-$i.dat %s; done' % (
                          vm.bucket_name,
                          scratch_dir,
                          _MakeAzureCommandSuffix(vm.azure_account,
                                                  vm.azure_key,
                                                  True)))
    else:
      download_cmd = ('time azure storage blob download %s '
                      'file_large_3gib.dat '
                      '%s/run/temp/file_large_3gib.dat %s' % (
                          vm.bucket_name,
                          scratch_dir,
                          _MakeAzureCommandSuffix(vm.azure_account,
                                                  vm.azure_key,
                                                  True)))

    _CliBasedTests(results, metadata, vm, CLI_TEST_ITERATION_COUNT_AZURE,
                   cleanup_bucket_cmd, upload_cmd, cleanup_local_temp_cmd,
                   download_cmd)

    ApiBasedBenchmarks(results, metadata, vm, 'AZURE', test_script_path,
                       vm.bucket_name, regional_bucket_name=None,
                       azure_command_suffix=_MakeAzureCommandSuffix(
                           vm.azure_account, vm.azure_key, False))

    return results

  def Cleanup(self, vm):
    """Clean up Azure storage container and uninstall packages on vm.

    Args:
      vm: The vm needs cleanup.
    """
    test_script_path = '%s/run/%s' % (vm.GetScratchDir(), API_TEST_SCRIPT)
    remove_content_cmd = ('%s --bucket=%s --storage_provider=AZURE '
                          ' --scenario=CleanupBucket %s' %
                          (test_script_path, vm.bucket_name,
                           _MakeAzureCommandSuffix(vm.azure_account,
                                                   vm.azure_key,
                                                   False)))

    remove_bucket_cmd = ('azure storage container delete -q %s %s' % (
                         vm.bucket_name,
                         _MakeAzureCommandSuffix(vm.azure_account,
                                                 vm.azure_key,
                                                 True)))
    DeleteBucketWithRetry(vm, remove_content_cmd, remove_bucket_cmd)

    vm.RemoteCommand('azure storage account delete -q pkb%s' %
                     FLAGS.run_uri)


class GoogleCloudStorageBenchmark(object):
  """Google Cloud Storage version of storage benchmark."""

  def Prepare(self, vm):
    """Prepare vm with gsutil tool and create a bucket using vm.

    Args:
      vm: The vm being used to run the benchmark.
    """
    vm.Install('wget')
    vm.RemoteCommand(
        'wget '
        'https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz')
    vm.RemoteCommand('tar xvf google-cloud-sdk.tar.gz')
    vm.RemoteCommand('bash ./google-cloud-sdk/install.sh '
                     '--disable-installation-options '
                     '--usage-report=false '
                     '--rc-path=.bash_profile '
                     '--path-update=true '
                     '--bash-completion=true')

    try:
      vm.RemoteCommand('mkdir .config')
    except errors.VirtualMachine.RemoteCommandError:
      # If ran on existing machines, .config folder may already exists.
      pass
    vm.PushFile(FLAGS.object_storage_credential_file, '.config/gcloud')
    vm.PushFile(FLAGS.boto_file_location, DEFAULT_BOTO_LOCATION)

    vm.gsutil_path, _ = vm.RemoteCommand('which gsutil', login_shell=True)
    vm.gsutil_path = vm.gsutil_path.split()[0]

    vm.bucket_name = 'pkb%s' % FLAGS.run_uri

    vm.RemoteCommand('%s mb gs://%s' % (vm.gsutil_path, vm.bucket_name))

    region = FLAGS.object_storage_region or DEFAULT_GCP_REGION
    vm.regional_bucket_name = '%s-%s' % (vm.bucket_name,
                                         region.lower())
    vm.RemoteCommand('%s mb -c DRA -l %s gs://%s' % (vm.gsutil_path,
                                                     region.upper(),
                                                     vm.regional_bucket_name))

    # Detect if we need to install crcmod for gcp.
    # See "gsutil help crc" for details.
    raw_result, _ = vm.RemoteCommand('%s version -l' % vm.gsutil_path)
    logging.info('gsutil version -l raw result is %s', raw_result)
    search_string = 'compiled crcmod: True'
    result_string = re.findall(search_string, raw_result)
    if len(result_string) == 0:
      logging.info('compiled crcmod is not available, installing now...')
      try:
        # Try uninstall first just in case there is a pure python version of
        # crcmod on the system already, this is required by gsutil doc:
        # https://cloud.google.com/storage/docs/
        # gsutil/addlhelp/CRC32CandInstallingcrcmod
        vm.RemoteCommand('/usr/bin/yes |sudo pip uninstall crcmod')
      except:
        logging.info('pip uninstall crcmod failed, could be normal if crcmod '
                     'is not available at all.')
        pass
      vm.Install('crcmod')
      vm.installed_crcmod = True
    else:
      logging.info('compiled crcmod is available, not installing again.')
      vm.installed_crcmod = False


  def Run(self, vm, metadata):
    """Run upload/download on vm with gsutil tool.

    Args:
      vm: The vm being used to run the benchmark.
      metadata: the metadata to be stored with the results.

    Returns:
      A list of lists containing results of the tests. Each scenario outputs
      results to a list of the following format:
        name of the scenario, value, unit of the value, metadata
        e.g.,
        'one byte object upload latency p50', 0.800, 'seconds', 'storage=gcs'

      Then the final return value is the list of the above list that reflect
      the results of all scenarios run here.
    """
    results = []
    metadata['pkb_installed_crcmod'] = vm.installed_crcmod
    metadata[BOTO_LIB_VERSION] = _GetClientLibVersion(vm, 'boto')
    # CLI tool based tests.
    scratch_dir = vm.GetScratchDir()
    clean_up_bucket_cmd = '%s rm gs://%s/*' % (vm.gsutil_path, vm.bucket_name)
    upload_cmd = 'time %s -m cp %s/run/data/* gs://%s/' % (vm.gsutil_path,
                                                           scratch_dir,
                                                           vm.bucket_name)
    cleanup_local_temp_cmd = 'rm %s/run/temp/*' % scratch_dir
    download_cmd = 'time %s -m cp gs://%s/* %s/run/temp/' % (vm.gsutil_path,
                                                             vm.bucket_name,
                                                             scratch_dir)
    if FLAGS.cli_test_size == 'normal':
      iteration_count = CLI_TEST_ITERATION_COUNT
    else:
      iteration_count = LARGE_CLI_TEST_ITERATION_COUNT

    _CliBasedTests(results, metadata, vm, iteration_count,
                   clean_up_bucket_cmd, upload_cmd, cleanup_local_temp_cmd,
                   download_cmd)

    # API-based benchmarking of GCS
    test_script_path = '%s/run/%s' % (scratch_dir, API_TEST_SCRIPT)
    ApiBasedBenchmarks(results, metadata, vm, 'GCS', test_script_path,
                       vm.bucket_name, vm.regional_bucket_name)

    return results


  def Cleanup(self, vm):
    """Clean up Google Cloud Storage bucket and uninstall packages on vm.

    Args:
      vm: The vm needs cleanup.
    """
    for bucket in [vm.bucket_name, vm.regional_bucket_name]:
      remove_content_cmd = '%s -m rm -r gs://%s/*' % (vm.gsutil_path,
                                                      bucket)
      remove_bucket_cmd = '%s rb gs://%s' % (vm.gsutil_path, bucket)
      DeleteBucketWithRetry(vm, remove_content_cmd, remove_bucket_cmd)

    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall python-gflags')


class SwiftStorageBenchmark(object):
  """OpenStack Swift version of storage benchmark."""

  def __init__(self):
    self.swift_command_prefix = ''

  def Prepare(self, vm):
    """Prepare vm with swift cli and create a swift container using vm.

    Args:
      vm: The vm being used to run the benchmark.
    """
    # Upgrade to latest urllib3 version fixes SSL SNMI error
    vm.InstallPackages('python-urllib3')
    vm.RemoteCommand('sudo pip install python-keystoneclient')
    vm.RemoteCommand('sudo pip install python-swiftclient')
    vm.bucket_name = 'pkb%s' % FLAGS.run_uri
    self.swift_command_prefix = _MakeSwiftCommandPrefix(vm.client.url,
                                                        vm.client.tenant,
                                                        vm.client.user,
                                                        vm.client.password)
    vm.RemoteCommand('swift %s post %s' % (self.swift_command_prefix,
                                           vm.bucket_name))

  def Run(self, vm, metadata):
    """Run upload/download on vm with swift cli.

    Args:
      vm: The vm being used to run the benchmark.
      metadata: the metadata to be stored with the results.

    Returns:
      A list of lists containing results of the tests. Each scenario outputs
      results to a list of the following format:
        name of the scenario ,value, unit of the value, metadata
        e.g.,
        'one byte object upload latency p50', 0.800, 'seconds', 'storage=swift'

      Then the final return value is the list of the above list that reflect
      the results of all scenarios ran here.
    """
    metadata[SWIFTCLIENT_LIB_VERSION] = _GetClientLibVersion(
        vm, 'python-swiftclient')
    results = []
    scratch_dir = vm.GetScratchDir()

    # CLI tool based tests

    clean_up_container_cmd = ('swift %s delete %s'
                              % (self.swift_command_prefix, vm.bucket_name))
    upload_cmd = ('time '
                  'swift %(prefix)s upload %(container)s %(directory)s'
                  % {'container': vm.bucket_name,
                     'directory': '%s/run/data/' % scratch_dir,
                     'prefix': self.swift_command_prefix})

    clean_up_local_temp_cmd = 'rm -r %s/run/temp/*' % scratch_dir
    download_cmd = ('time '
                    'swift %(prefix)s download %(container)s -D %(directory)s'
                    % {'container': vm.bucket_name,
                       'directory': '%s/run/temp/' % scratch_dir,
                       'prefix': self.swift_command_prefix})

    iteration_count = (CLI_TEST_ITERATION_COUNT
                       if FLAGS.cli_test_size == 'normal'
                       else LARGE_CLI_TEST_ITERATION_COUNT)

    _CliBasedTests(results, metadata, vm, iteration_count,
                   clean_up_container_cmd, upload_cmd, clean_up_local_temp_cmd,
                   download_cmd)

    if FLAGS.object_storage_scenario in ('all', 'api_data',):
      logging.warn('Skipping API-based scenario for object storage service. '
                   'Reason: Not yet implemented for OpenStack Swift')

    return results

  def Cleanup(self, vm):
    """Clean up OpenStack Swift container and uninstall packages on vm.

    Args:
      vm: The vm that needs clean up.
    """
    remove_content_cmd = ('swift %s delete %s'
                          % (self.swift_command_prefix, vm.bucket_name))
    # Command above delete both container and its objects
    remove_container_cmd = 'echo noop-container-delete'
    DeleteBucketWithRetry(vm, remove_content_cmd, remove_container_cmd)

    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall python-swiftclient')
    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall python-keystoneclient')
    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall python-gflags')


OBJECT_STORAGE_BENCHMARK_DICTIONARY = {
    providers.GCP: GoogleCloudStorageBenchmark(),
    providers.AWS: S3StorageBenchmark(),
    providers.AZURE: AzureBlobStorageBenchmark(),
    providers.OPENSTACK: SwiftStorageBenchmark()}


def Prepare(benchmark_spec):
  """Prepare vm with cloud provider tool and prepare vm with data file.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """

  vms = benchmark_spec.vms
  if not FLAGS.object_storage_credential_file:
    FLAGS.object_storage_credential_file = (
        OBJECT_STORAGE_CREDENTIAL_DEFAULT_LOCATION[
            FLAGS.storage])
  FLAGS.object_storage_credential_file = os.path.expanduser(
      FLAGS.object_storage_credential_file)

  if FLAGS.storage == providers.OPENSTACK:
    openstack_creds_set = ('OS_AUTH_URL' in os.environ,
                           'OS_TENANT_NAME' in os.environ,
                           'OS_USERNAME' in os.environ,
                           'OS_PASSWORD' in os.environ,)
    if not all(openstack_creds_set):
      raise errors.Benchmarks.MissingObjectCredentialException(
          'OpenStack credentials not found in environment variables')
  else:
    if not (os.path.isfile(FLAGS.object_storage_credential_file) or
            os.path.isdir(FLAGS.object_storage_credential_file)):
      raise errors.Benchmarks.MissingObjectCredentialException(
          'Credential cannot be found in %s',
          FLAGS.object_storage_credential_file)

  if not FLAGS.boto_file_location:
    FLAGS.boto_file_location = DEFAULT_BOTO_LOCATION
  FLAGS.boto_file_location = os.path.expanduser(FLAGS.boto_file_location)

  if not os.path.isfile(FLAGS.boto_file_location):
    if FLAGS.storage not in (providers.AZURE,
                             providers.OPENSTACK):
      raise errors.Benchmarks.MissingObjectCredentialException(
          'Boto file cannot be found in %s but it is required for gcs or s3.',
          FLAGS.boto_file_location)

  vms[0].Install('pip')
  vms[0].RemoteCommand('sudo pip install python-gflags==2.0')
  azure_version_string = ''
  if FLAGS.azure_lib_version is not None:
    azure_version_string = '==%s' % FLAGS.azure_lib_version
  vms[0].RemoteCommand('sudo pip install azure%s' % azure_version_string)
  vms[0].Install('gcs_boto_plugin')

  OBJECT_STORAGE_BENCHMARK_DICTIONARY[FLAGS.storage].Prepare(vms[0])

  # We would like to always cleanup server side states when exception happens.
  benchmark_spec.always_call_cleanup = True

  # Prepare data on vm, create a run directory on scratch drive, and add
  # permission.
  scratch_dir = vms[0].GetScratchDir()
  vms[0].RemoteCommand('sudo mkdir %s/run/' % scratch_dir)
  vms[0].RemoteCommand('sudo chmod 777 %s/run/' % scratch_dir)

  vms[0].RemoteCommand('sudo mkdir %s/run/temp/' % scratch_dir)
  vms[0].RemoteCommand('sudo chmod 777 %s/run/temp/' % scratch_dir)

  file_path = data.ResourcePath(DATA_FILE)
  vms[0].PushFile(file_path, '%s/run/' % scratch_dir)

  api_test_script_path = data.ResourcePath(API_TEST_SCRIPT)
  vms[0].PushFile(api_test_script_path, '%s/run/' % scratch_dir)


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

  metadata = {'storage provider': FLAGS.storage}

  vms = benchmark_spec.vms

  # The client tool based tests requires some provisioning on the VMs first.
  vms[0].RemoteCommand(
      'cd %s/run/; bash cloud-storage-workload.sh %s' % (vms[0].GetScratchDir(),
                                                         FLAGS.cli_test_size))
  results = OBJECT_STORAGE_BENCHMARK_DICTIONARY[FLAGS.storage].Run(vms[0],
                                                                   metadata)
  print results
  return results


def Cleanup(benchmark_spec):
  """Clean up storage bucket/container and clean up vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  OBJECT_STORAGE_BENCHMARK_DICTIONARY[FLAGS.storage].Cleanup(vms[0])
  vms[0].RemoteCommand('rm -rf %s/run/' % vms[0].GetScratchDir())
