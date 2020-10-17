# Lint as: python2, python3
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

"""An interface to Google Cloud Storage, using the python library."""

from __future__ import absolute_import

import logging
import time

from absl import flags
# This is the path that we SCP object_storage_interface to.
from providers import object_storage_interface
from google.cloud import storage

FLAGS = flags.FLAGS


class GcsService(object_storage_interface.ObjectStorageServiceBase):
  """An interface to Google Cloud Storage, using the python library."""

  def __init__(self):
    self.client = storage.Client()

  def ListObjects(self, bucket_name, prefix):
    bucket = storage.bucket.Bucket(self.client, bucket_name)
    return [obj.name for obj in self.client.list_blobs(bucket, prefix=prefix)]

  def DeleteObjects(self,
                    bucket_name,
                    objects_to_delete,
                    objects_deleted=None,
                    delay_time=0,
                    object_sizes=None):
    start_times = []
    latencies = []
    sizes = []
    bucket = storage.bucket.Bucket(self.client, bucket_name)
    for index, object_name in enumerate(objects_to_delete):
      time.sleep(delay_time)
      try:
        start_time = time.time()
        obj = storage.blob.Blob(object_name, bucket)
        obj.delete(client=self.client)
        latency = time.time() - start_time
        start_times.append(start_time)
        latencies.append(latency)
        if objects_deleted is not None:
          objects_deleted.append(object_name)
        if object_sizes:
          sizes.append(object_sizes[index])
      except Exception as e:  # pylint: disable=broad-except
        logging.exception('Caught exception while deleting object %s: %s',
                          object_name, e)
    return start_times, latencies, sizes

  def BulkDeleteObjects(self, bucket_name, objects_to_delete, delay_time):
    bucket = storage.bucket.Bucket(self.client, bucket_name)
    time.sleep(delay_time)
    start_time = time.time()
    bucket.delete_blobs(objects_to_delete)
    latency = time.time() - start_time
    return start_time, latency

  def WriteObjectFromBuffer(self, bucket_name, object_name, stream, size):
    stream.seek(0)
    start_time = time.time()
    data = str(stream.read(size))
    bucket = storage.bucket.Bucket(self.client, bucket_name)
    obj = storage.blob.Blob(object_name, bucket)
    obj.upload_from_string(data, client=self.client)
    latency = time.time() - start_time
    return start_time, latency

  def ReadObject(self, bucket_name, object_name):
    start_time = time.time()
    bucket = storage.bucket.Bucket(self.client, bucket_name)
    obj = storage.blob.Blob(object_name, bucket)
    obj.download_as_string(client=self.client)
    latency = time.time() - start_time
    return start_time, latency
