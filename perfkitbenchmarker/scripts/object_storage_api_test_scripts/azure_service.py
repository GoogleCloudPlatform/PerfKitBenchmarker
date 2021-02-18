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

"""An interface to the Azure Blob Storage API."""


import logging
import time

from absl import flags
import azure.storage.blob
# This is the path that we SCP object_storage_interface to.
from providers import object_storage_interface

FLAGS = flags.FLAGS


class AzureService(object_storage_interface.ObjectStorageServiceBase):
  """An interface to Azure Blob Storage, using the python library."""

  def __init__(self):
    if FLAGS.azure_key is None or FLAGS.azure_account is None:
      raise ValueError('Must specify azure account and key.')
    self.blob_service = azure.storage.blob.BlobService(FLAGS.azure_account,
                                                       FLAGS.azure_key)

  def ListObjects(self, bucket, prefix):
    return [obj.name
            for obj in self.blob_service.list_blobs(bucket, prefix=prefix)]

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
        self.blob_service.delete_blob(bucket, object_name)
        latency = time.time() - start_time
        start_times.append(start_time)
        latencies.append(latency)
        if objects_deleted is not None:
          objects_deleted.append(object_name)
        if object_sizes:
          sizes.append(object_sizes[index])
      except:
        logging.exception('Caught exception while deleting object %s.',
                          object_name)
    return start_times, latencies, sizes

  def BulkDeleteObjects(self, bucket, objects_to_delete, delay_time):
    # This version of Azure Blob APIs do not support Bulk Delete
    # TODO(user): Update to latest version of Azure Blob Storage API
    start_times, latencies, _ = self.DeleteObjects(
        bucket, objects_to_delete, delay_time=delay_time)
    return min(start_times), sum(latencies)

  def WriteObjectFromBuffer(self, bucket, object, stream, size):
    stream.seek(0)
    start_time = time.time()
    self.blob_service.put_block_blob_from_file(
        bucket, object, stream, count=size)
    latency = time.time() - start_time
    return start_time, latency

  def ReadObject(self, bucket, object):
    start_time = time.time()
    self.blob_service.get_blob_to_bytes(bucket, object)
    latency = time.time() - start_time
    return start_time, latency
