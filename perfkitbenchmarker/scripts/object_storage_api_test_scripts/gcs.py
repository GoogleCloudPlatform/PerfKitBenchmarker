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

"""An interface to Google Cloud Storage, using the boto library."""

import time

from absl import flags
import gcs_oauth2_boto_plugin  # noqa

import boto_service

FLAGS = flags.FLAGS


class GCSService(boto_service.BotoService):
  def __init__(self):
    super(GCSService, self).__init__('gs', host_to_connect=None)

  def WriteObjectFromBuffer(self, bucket, object, stream, size):
    stream.seek(0)
    start_time = time.time()
    object_uri = self._StorageURI(bucket, object)
    object_uri.set_contents_from_file(stream, size=size)
    latency = time.time() - start_time
    return start_time, latency
