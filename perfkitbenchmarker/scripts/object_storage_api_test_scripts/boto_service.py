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

"""An interface to boto-based object storage APIs."""

import logging
import time

import boto

import object_storage_interface


class BotoService(object_storage_interface.ObjectStorageServiceBase):
  def __init__(self, storage_schema, host_to_connect=None):
    self.storage_schema = storage_schema
    self.host_to_connect = host_to_connect

  def _StorageURI(self, bucket, object=None):
    """Return a storage_uri for the given resource.

    Args:
      bucket: the name of a bucket.
      object: the name of an object, if given.

    Returns: a storage_uri. If object is given, the uri will be for
    the bucket-object combination. If object is not given, the uri
    will be for the bucket.
    """

    if object is not None:
      path = '%s/%s' % (bucket, object)
    else:
      path = bucket
    storage_uri = boto.storage_uri(path, self.storage_schema)
    if self.host_to_connect is not None:
      storage_uri.connect(host=self.host_to_connect)
    return storage_uri

  def ListObjects(self, bucket, prefix):
    bucket_uri = self._StorageURI(bucket)
    return [obj.name for obj in bucket_uri.list_bucket(prefix=prefix)]

  def DeleteObjects(self, bucket, objects_to_delete, objects_deleted=None):
    for object_name in objects_to_delete:
      try:
        object_uri = self._StorageURI(bucket, object_name)
        object_uri.delete_key()
        if objects_deleted is not None:
          objects_deleted.append(object_name)
      except:
        logging.exception('Caught exception while deleting object %s.',
                          object_name)

  # Not implementing WriteObjectFromBuffer because the implementation
  # is different for GCS and S3.

  def ReadObject(self, bucket, object):
    start_time = time.time()
    object_uri = self._StorageURI(bucket, object)
    object_uri.new_key().get_contents_as_string()
    latency = time.time() - start_time
    return start_time, latency
