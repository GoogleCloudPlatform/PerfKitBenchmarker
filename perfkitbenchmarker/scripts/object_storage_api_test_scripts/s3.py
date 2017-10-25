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

import boto_service

FLAGS = flags.FLAGS


class S3Service(boto_service.BotoService):
  def __init__(self):
    if FLAGS.host is not None:
      logging.info('Will use user-specified host endpoint: %s', FLAGS.host)
    super(S3Service, self).__init__('s3', host_to_connect=FLAGS.host)

  def WriteObjectFromBuffer(self, bucket, object, stream, size):
    stream.seek(0)
    start_time = time.time()
    object_uri = self._StorageURI(bucket, object)
    # We need to access the raw key object so we can set its storage
    # class
    key = object_uri.new_key()
    if FLAGS.object_storage_class is not None:
      key._set_storage_class(FLAGS.object_storage_class)
    key.set_contents_from_file(stream, size=size)
    latency = time.time() - start_time
    return start_time, latency
