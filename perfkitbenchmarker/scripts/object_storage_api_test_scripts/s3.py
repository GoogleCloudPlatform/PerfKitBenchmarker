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
# This is the path that we SCP object_storage_interface to.
from providers import object_storage_interface
import six

FLAGS = flags.FLAGS


class S3Service(object_storage_interface.ObjectStorageServiceBase):
  """An interface to AWS S3, using the boto library."""

  def __init__(self):
    self.client = boto3.client('s3', region_name=FLAGS.region)

  def ListObjects(self, bucket, prefix):
    return self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)

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
        self.client.delete_object(Bucket=bucket, Key=object_name)
        latency = time.time() - start_time
        start_times.append(start_time)
        latencies.append(latency)
        if objects_deleted:
          objects_deleted.append(object_name)
        if object_sizes:
          sizes.append(object_sizes[index])
      except Exception as e:  # pylint: disable=broad-except
        logging.exception('Caught exception while deleting object %s: %s',
                          object_name, e)
    return start_times, latencies, sizes

  def BulkDeleteObjects(self, bucket, objects_to_delete, delay_time):
    objects_to_delete_dict = {}
    objects_to_delete_dict['Objects'] = []
    for object_name in objects_to_delete:
      objects_to_delete_dict['Objects'].append({'Key': object_name})
    time.sleep(delay_time)
    start_time = time.time()
    self.client.delete_objects(Bucket=bucket, Delete=objects_to_delete_dict)
    latency = time.time() - start_time
    return start_time, latency

  def WriteObjectFromBuffer(self, bucket, object_name, stream, size):
    start_time = time.time()
    stream.seek(0)
    obj = six.BytesIO(stream.read(size))
    self.client.put_object(Body=obj, Bucket=bucket, Key=object_name)
    latency = time.time() - start_time
    return start_time, latency

  def ReadObject(self, bucket, object_name):
    start_time = time.time()
    s3_response_object = self.client.get_object(
        Bucket=bucket, Key=object_name)
    s3_response_object['Body'].read()
    latency = time.time() - start_time
    return start_time, latency
