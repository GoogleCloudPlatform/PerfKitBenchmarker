# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Use cURL to upload and download data to object storage in parallel.

Consistent with object_storage_service multistream scenario.

Due to the difficulty of signing to requests to S3 by hand
(https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html).
The benchmark uses insecure short lived buckets and should be used with caution.

TODO(pclay): Consider signing requests and not using public buckets.
"""

import logging
from typing import List, Tuple

from absl import flags
import numpy as np
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import object_storage_service_benchmark

BENCHMARK_NAME = 'object_storage_curl'

BENCHMARK_CONFIG = """
object_storage_curl:
  description: Use cURL to upload and download data to object storage in parallel.
  vm_groups:
    default:
      vm_spec: *default_single_core
  flags:
    # Required
    object_storage_multistream_objects_per_stream: 1
    object_storage_streams_per_vm: 10
"""

# Blocksize for dd to pipe data into uploads.
DD_BLOCKSIZE = 4000

# Magic strings
_UPLOAD = 'upload'
_DOWNLOAD = 'download'
_START_TIME = 'START_TIME'
_CURL_RESULTS = 'CURL_RESULTS'

flags.DEFINE_string('object_storage_curl_object_size', '1MB',
                    'Size of objects to upload / download. Similar to '
                    '--object_storage_object_sizes, but only takes a single '
                    'size.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Validate some unsupported flags."""
  if (flag_util.StringToBytes(FLAGS.object_storage_curl_object_size) <
      DD_BLOCKSIZE):
    raise errors.Config.InvalidValue(
        '--object_storage_curl_object_size must be larger than 4KB')
  # TODO(pclay): Consider supporting multiple objects per stream.
  if FLAGS.object_storage_multistream_objects_per_stream != 1:
    raise errors.Config.InvalidValue(
        'object_storage_curl only supports 1 object per stream')
  if FLAGS.object_storage_object_naming_scheme != 'sequential_by_stream':
    raise errors.Config.InvalidValue(
        'object_storage_curl only supports sequential_by_stream naming.')


# PyType does not currently support returning Abstract classes
# TODO(user): stop suppressing
# pytype: disable=not-instantiable
def _GetService() -> object_storage_service.ObjectStorageService:
  """Get a ready to use instance of ObjectStorageService."""
  # TODO(pclay): consider using FLAGS.storage to allow cross cloud testing?
  cloud = FLAGS.cloud
  providers.LoadProvider(cloud)
  service = object_storage_service.GetObjectStorageClass(cloud)()
  # This method is idempotent with default args and safe to call in each phase.
  service.PrepareService(FLAGS.object_storage_region)
  return service


def _GetBucketName() -> str:
  return FLAGS.object_storage_bucket_name or 'pkb%s' % FLAGS.run_uri


def Prepare(benchmark_spec):
  """Create and ACL bucket and install curl."""
  # We would like to always cleanup server side states when exception happens.
  benchmark_spec.always_call_cleanup = True

  service = _GetService()
  bucket_name = _GetBucketName()

  service.MakeBucket(bucket_name)

  vms = benchmark_spec.vms
  vm_util.RunThreaded(lambda vm: vm.InstallPackages('curl'), vms)


def Run(benchmark_spec) -> List[sample.Sample]:
  """Run storage benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    The same samples as object_storage_multistream.
  """
  service = _GetService()
  bucket = _GetBucketName()

  object_bytes = flag_util.StringToBytes(FLAGS.object_storage_curl_object_size)
  blocks = object_bytes // DD_BLOCKSIZE
  streams_per_vm = FLAGS.object_storage_streams_per_vm

  generate_data_cmd = (
      'openssl aes-256-ctr -iter 1 -pass file:/dev/urandom -in /dev/zero'
      f' | dd bs={DD_BLOCKSIZE} count={blocks} iflag=fullblock')
  def StartTimeCmd(index):
    return f"date '+{_START_TIME} {index} %s.%N'"

  def CurlCmd(operation, index):
    return (
        f"curl -fsw '{_CURL_RESULTS} {index} %{{time_total}} "
        # Pad size with zero to force curl to populate it.
        f"%{{response_code}} 0%{{size_{operation}}}\n' -o /dev/null")

  def Upload(vm):
    commands = []
    for object_index in range(streams_per_vm):
      object_name = f'{vm.name}_{object_index}'
      url = service.GetUploadUrl(bucket, object_name)
      http_method = service.UPLOAD_HTTP_METHOD
      headers = service.GetHttpAuthorizationHeaders(
          http_method, bucket, object_name)
      headers_string = ' '.join(f"-H '{header}'" for header in headers)
      commands.append(
          f'{StartTimeCmd(object_index)}; {generate_data_cmd} | '
          f'{CurlCmd(_UPLOAD, object_index)} -X {http_method} {headers_string} '
          f"--data-binary @- '{url}'")
    stdout, _ = vm.RemoteCommandsInParallel(commands)
    return stdout

  def Download(vm):
    commands = []
    for object_index in range(streams_per_vm):
      object_name = f'{vm.name}_{object_index}'
      url = service.GetDownloadUrl(bucket, object_name)
      headers = service.GetHttpAuthorizationHeaders('GET', bucket, object_name)
      headers_string = ' '.join(f"-H '{header}'" for header in headers)
      commands.append(
          f'{StartTimeCmd(object_index)}; '
          f"{CurlCmd(_DOWNLOAD, object_index)} {headers_string} '{url}'")
    stdout, _ = vm.RemoteCommandsInParallel(commands)
    return stdout

  vms = benchmark_spec.vms
  samples = []
  for operation, func in [(_UPLOAD, Upload), (_DOWNLOAD, Download)]:
    output = vm_util.RunThreaded(func, vms)
    start_times, latencies = _LoadWorkerOutput(output)
    object_storage_service_benchmark.ProcessMultiStreamResults(
        start_times,
        latencies,
        all_sizes=[object_bytes],
        sizes=[np.array([object_bytes])] * streams_per_vm * len(vms),
        operation=operation,
        results=samples,
        # We do not retry curl. We simply do not report failing latencies.
        # This under-reports both latency and throughput. Since this benchmark
        # is intended to measure throughput this is reasonable.
        allow_failing_streams=True)
  return samples


def Cleanup(_):
  service = _GetService()
  bucket_name = _GetBucketName()
  if not FLAGS.object_storage_dont_delete_bucket:
    service.DeleteBucket(bucket_name)
    service.CleanupService()


def _LoadWorkerOutput(
    output: List[str]) -> Tuple[List[np.ndarray], List[np.ndarray]]:
  """Parse the output of Upload and Download functions.

  The output of Upload and Download is
  # START_TIME <index> <Unix start time in seconds>
  START_TIME   0       12345.6789
  # CURL_RESULTS <index> <latency in s> <HTTP code> <bytes transmitted>
  CURL_RESULTS   0       1.2345         200         01000

  Lines of output are not ordered and may be interleaved.

  Args:
    output: the output of each upload or download command

  Returns:
    the start times and latencies of the curl commands

  Raises:
    ValueError if output is unexpected.
    Exception if the curl request failed with a 4XX code.
  """
  start_times = []
  latencies = []

  for worker_out in output:
    worker_start_times = [None] * FLAGS.object_storage_streams_per_vm
    worker_latencies = [None] * FLAGS.object_storage_streams_per_vm
    for line in worker_out.strip().split('\n'):
      try:
        line_type, index, value, *curl_data = line.split()
        if line_type == _START_TIME:
          assert not curl_data
          worker_start_times[int(index)] = float(value)
        elif line_type == _CURL_RESULTS:
          response_code, bytes_transmitted = curl_data
          bytes_transmitted = int(bytes_transmitted)
          if response_code == '200':
            bytes_expected = flag_util.StringToBytes(
                FLAGS.object_storage_curl_object_size)
            if bytes_transmitted != bytes_expected:
              raise ValueError(
                  f'cURL transmitted {bytes_transmitted}'
                  f' instead of {bytes_expected}.')
            # curl 7.74 used μs instead of seconds. Not used in major OS types.
            # https://github.com/curl/curl/issues/6321
            assert '.' in value, 'Invalid curl output.'
            worker_latencies[int(index)] = float(value)
          elif response_code.startswith('4'):
            raise Exception(
                f'cURL command failed with HTTP Code {response_code}')
          else:
            logging.warning('cURL command failed with HTTP code %s. '
                            'Not reporting latency.', response_code)
        else:
          raise ValueError(f'Unexpected line start: {line_type}.')
      # Always show raw line when there is a parsing error.
      except (ValueError, AssertionError) as e:
        raise ValueError(f'Unexpected output:\n{line}') from e

    for start_time, latency in zip(worker_start_times, worker_latencies):
      if latency:
        start_times.append(np.array([start_time], dtype=np.float64))
        latencies.append(np.array([latency], dtype=np.float64))

  return start_times, latencies
