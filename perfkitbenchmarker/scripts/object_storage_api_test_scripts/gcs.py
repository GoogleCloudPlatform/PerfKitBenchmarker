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

import logging
import time

from absl import flags
import object_storage_interface
from google.cloud import storage

FLAGS = flags.FLAGS


class GCSService(object_storage_interface.ObjectStorageServiceBase):
  """An interface to Google Cloud Storage, using the python library."""

  def __init__(self):
    self.client = storage.Client()

  def ListObjects(self, bucket_name, prefix):
    bucket = storage.bucket.Bucket(self.client, bucket_name)
    return [obj.name for obj in self.client.list_blobs(bucket, prefix=prefix)]

  def DeleteObjects(self, bucket_name, objects_to_delete, objects_deleted=None):
    bucket = storage.bucket.Bucket(self.client, bucket_name)
    for object_name in objects_to_delete:
      obj = storage.blob.Blob(object_name, bucket)
      try:
        obj.delete(client=self.client)
        if objects_deleted is not None:
          objects_deleted.append(object_name)
      except Exception as e:  # pylint: disable=broad-except
        logging.exception('Caught exception while deleting object %s: %s',
                          object_name, e)

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
