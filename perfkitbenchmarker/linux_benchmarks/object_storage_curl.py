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

import itertools
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

flags.DEFINE_string('object_storage_curl_object_size', '1MB',
                    'Size of objects to upload / download. Similar to '
                    '--object_storage_object_sizes, but only takes a single '
                    'size.')
flags.DEFINE_bool('object_storage_curl_i_am_ok_with_public_read_write_buckets',
                  False, 'Acknowledge that this bucket will create buckets '
                  'which are publicly readable and writable. Required to run '
                  'this benchmark.')

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
  if not FLAGS.object_storage_curl_i_am_ok_with_public_read_write_buckets:
    raise errors.Config.InvalidValue(
        'This benchmark uses public read/write object storage bucket.\n'
        'You must explicitly pass '
        '--object_storage_curl_i_am_ok_with_public_read_write_buckets to '
        'acknowledge that it will be created.\n'
        'If PKB is interrupted, you should ensure it is cleaned up.')


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
  service.MakeBucketPubliclyReadable(bucket_name, also_make_writable=True)

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

  start_time_cmd = "date '+%s.%N'"
  generate_data_cmd = (
      'openssl aes-256-ctr -iter 1 -pass file:/dev/urandom -in /dev/zero'
      f' | dd bs={DD_BLOCKSIZE} count={blocks} iflag=fullblock')
  # TODO(pclay): consider adding size_down/upload to verify we are actually
  # reading the data.
  curl_cmd = "curl -fsw '%{time_total}' -o /dev/null"

  def Upload(vm, object_index):
    object_name = f'{vm.name}_{object_index}'
    url = service.GetUploadUrl(bucket=bucket, object_name=object_name)
    stdout, _ = vm.RemoteCommand(
        f'{start_time_cmd}; {generate_data_cmd} | '
        f"{curl_cmd} -X {service.UPLOAD_HTTP_METHOD} --data-binary @- '{url}'")
    return stdout

  def Download(vm, object_index):
    object_name = f'{vm.name}_{object_index}'
    url = service.GetDownloadUrl(bucket=bucket, object_name=object_name)
    stdout, _ = vm.RemoteCommand(f"{start_time_cmd}; {curl_cmd} '{url}'")
    return stdout

  vms = benchmark_spec.vms
  streams_per_vm = FLAGS.object_storage_streams_per_vm
  samples = []
  for operation, func in (('upload', Upload), ('download', Download)):
    output = vm_util.RunThreaded(
        func,
        [(args, {}) for args in itertools.product(vms, range(streams_per_vm))])
    start_times, latencies = _LoadWorkerOutput(output)
    object_storage_service_benchmark.ProcessMultiStreamResults(
        start_times,
        latencies,
        all_sizes=[object_bytes],
        sizes=[np.array([object_bytes])] * streams_per_vm * len(vms),
        operation=operation,
        results=samples)
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
  12345.6789 # Unix start time as float in seconds
  1.2345     # Latency of curl in seconds

  Args:
    output: the output of each upload or download command

  Returns:
    the start times and latencies of the curl commands
  """
  start_times = []
  latencies = []

  for worker_out in output:
    start_time_str, latency_str = worker_out.strip().split('\n')
    # curl 7.74 used μs instead of seconds. Not used in major OS types.
    # https://github.com/curl/curl/issues/6321
    assert '.' in latency_str, 'Invalid curl output.'

    start_times.append(np.array([start_time_str], dtype=np.float64))
    latencies.append(np.array([latency_str], dtype=np.float64))

  return start_times, latencies
