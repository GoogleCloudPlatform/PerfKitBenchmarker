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

"""An interface to S3, using the boto library."""

import logging
import time

from absl import flags

import boto3

import object_storage_interface

FLAGS = flags.FLAGS


class S3Service(object_storage_interface.ObjectStorageServiceBase):
  """An interface to AWS S3, using the boto library."""

  def __init__(self):
    self.client = boto3.client('s3', region_name=FLAGS.region)

  def ListObjects(self, bucket, prefix):
    bucket_name = FLAGS.access_point_hostname or bucket
    return self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

  def DeleteObjects(self, bucket, objects_to_delete, objects_deleted=None):
    bucket_name = FLAGS.access_point_hostname or bucket
    for object_name in objects_to_delete:
      response = self.client.delete_object(Bucket=bucket_name, Key=object_name)
      if response['ResponseMetadata']['DeleteMarker']:
        if objects_deleted is not None:
          objects_deleted.append(object_name)
      else:
        logging.exception(
            'Encountered error while deleting object %s. '
            'Response metadata: %s', object_name, response)

  def WriteObjectFromBuffer(self, bucket, object_name, stream, size):
    start_time = time.time()
    bucket_name = FLAGS.access_point_hostname or bucket
    stream.seek(0)
    self.client.upload_fileobj(stream, bucket_name, object_name)
    latency = time.time() - start_time
    return start_time, latency

  def ReadObject(self, bucket, object_name):
    start_time = time.time()
    bucket_name = FLAGS.access_point_hostname or bucket
    self.client.get_object(Bucket=bucket_name, Key=object_name)
    latency = time.time() - start_time
    return start_time, latency
