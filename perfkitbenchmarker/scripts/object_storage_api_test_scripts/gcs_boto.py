# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""An interface to Google Cloud Storage, using the boto library."""


import logging
import time

from absl import flags
import boto
import gcs_oauth2_boto_plugin  # noqa
# This is the path that we SCP object_storage_interface to.
from providers import object_storage_interface

FLAGS = flags.FLAGS


class GcsServiceBoto(object_storage_interface.ObjectStorageServiceBase):
  """An interface to Google Cloud Storage, using the boto library."""

  def __init__(self):
    pass

  def _StorageURI(self, bucket, object_name=None):
    """Return a storage_uri for the given resource.

    Args:
      bucket: the name of a bucket.
      object_name: the name of an object, if given.

    Returns:
      A storage_uri. If object is given, the uri will be for the bucket-object
      combination. If object is not given, the uri will be for the bucket.
    """

    if object_name is not None:
      path = '%s/%s' % (bucket, object_name)
    else:
      path = bucket
    storage_uri = boto.storage_uri(path, 'gs')
    return storage_uri

  def _CreateHeader(self):
    # Local use of header for debugging
    header = {}
    return header

  def ListObjects(self, bucket, prefix):
    bucket_uri = self._StorageURI(bucket)
    return [obj.name for obj in bucket_uri.list_bucket(prefix=prefix)]

  def DeleteObjects(self,
                    bucket,
                    objects_to_delete,
                    objects_deleted=None,
                    delay_time=0,
                    object_sizes=None):
    start_times = []
    latencies = []
    sizes = []
    for index, object_name in enumerate(objects_to_delete):
      try:
        time.sleep(delay_time)
        start_time = time.time()
        object_uri = self._StorageURI(bucket, object_name)
        object_uri.delete_key(headers=self._CreateHeader())
        latency = time.time() - start_time
        start_times.append(start_time)
        latencies.append(latency)
        if objects_deleted is not None:
          objects_deleted.append(object_name)
        if object_sizes:
          sizes.append(object_sizes[index])
      except:  # pylint:disable=bare-except
        logging.exception('Caught exception while deleting object %s.',
                          object_name)
    return start_times, latencies, sizes

  def BulkDeleteObjects(self, bucket, objects_to_delete, delay_time):
    # GCS Boto currently does not support Bulk delete
    start_times, latencies, _ = self.DeleteObjects(
        bucket, objects_to_delete, delay_time=delay_time)
    return min(start_times), sum(latencies)

  def WriteObjectFromBuffer(self, bucket, object_name, stream, size):
    start_time = time.time()
    stream.seek(0)
    object_uri = self._StorageURI(bucket, object_name)
    object_uri.set_contents_from_file(
        stream, size=size, headers=self._CreateHeader())
    latency = time.time() - start_time
    return start_time, latency

  def ReadObject(self, bucket, object_name):
    start_time = time.time()
    object_uri = self._StorageURI(bucket, object_name)
    object_uri.new_key().get_contents_as_string(headers=self._CreateHeader())
    latency = time.time() - start_time
    return start_time, latency
