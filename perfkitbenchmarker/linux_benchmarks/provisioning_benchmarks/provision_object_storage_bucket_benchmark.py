# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Benchmark for timing provisioning objext storage buckets."""

import time
from typing import List

from absl import flags

from perfkitbenchmarker import configs
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import sample


BENCHMARK_NAME = 'provision_object_storage_bucket'

BENCHMARK_CONFIG = """
provision_object_storage_bucket:
  description: >
      Time spinning up and deleting an Object Storage Bucket
"""

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _GetService() -> object_storage_service.ObjectStorageService:  # pytype: disable=not-instantiable
  """Get a ready to use instance of ObjectStorageService."""
  cloud = FLAGS.cloud
  providers.LoadProvider(cloud)
  service = object_storage_service.GetObjectStorageClass(cloud)()
  # Prepare service generally just sets location and is always idempotent, so it
  # is safe to call repeatedly.
  if not FLAGS.object_storage_region:
    raise ValueError('--object_storage_region is required')
  service.PrepareService(FLAGS.object_storage_region)
  return service


def _GetBucketName() -> str:
  return FLAGS.object_storage_bucket_name or 'pkb%s' % FLAGS.run_uri


def Prepare(_) -> None:
  pass


def Run(_) -> List[sample.Sample]:
  """Run the benchmark."""
  service = _GetService()
  bucket = _GetBucketName()

  start_time = time.time()
  service.MakeBucket(bucket, tag_bucket=False)
  bucket_created_time = time.time()
  # upload an empty file to enxure the bucket is actually functional.
  # This command failing after the bucket is ready would be absolutely, totally,
  # and in all other ways inconceivable, but it is possible the first upload
  # might take extra time.
  service.Copy('/dev/null', bucket)
  object_created_time = time.time()

  metadata = {'object_storage_region': FLAGS.object_storage_region}

  samples = [
      sample.Sample(
          'Time to Create Bucket',
          bucket_created_time - start_time,
          'seconds',
          metadata),
      sample.Sample(
          'Time to Create Bucket and Object',
          object_created_time - start_time,
          'seconds',
          metadata),
  ]
  return samples


def Cleanup(_):
  # TODO(pclay): Time deletion too?
  service = _GetService()
  bucket = _GetBucketName()
  service.DeleteBucket(bucket)
