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

import asyncio
import collections
import io
import multiprocessing
import time

from google.cloud.storage.asyncio import async_appendable_object_writer
from google.cloud.storage.asyncio import async_grpc_client
from google.cloud.storage.asyncio import async_multi_range_downloader
from providers import gcs


class SynchronousGcsClient:
  """A synchronous wrapper for the async GCS client.

  AsyncGrpcClient is quite fragile, it must only be interacted with from a
  single event loop and the event loop must not be shared between theads or
  processes.

  This wrapper abstracts away the async nature of the client. It still needs to
  be called from the same thread.
  """

  def __init__(self):
    async def _ClientMustBeCreatedInSameLoop():
      return async_grpc_client.AsyncGrpcClient()

    self.loop = asyncio.new_event_loop()
    self.grpc_client = self.loop.run_until_complete(
        _ClientMustBeCreatedInSameLoop()
    )

  async def _WriteAsync(self, bucket_name, object_name, stream, size):
    """Creates, writes to, and finalizes an appendable object."""
    writer = async_appendable_object_writer.AsyncAppendableObjectWriter(
        self.grpc_client, bucket_name, object_name
    )
    await writer.open()
    await writer.append(stream.read(size))
    # TODO(pclay): Wire in optional support for finalizing the object.

  def Write(self, bucket_name, object_name, stream, size):
    return self.loop.run_until_complete(
        self._WriteAsync(bucket_name, object_name, stream, size)
    )

  async def _ReadAsync(self, bucket_name, object_name):
    """Downloads the entire content of an object using a multi-range downloader."""
    mrd = async_multi_range_downloader.AsyncMultiRangeDownloader(
        self.grpc_client, bucket_name, object_name
    )
    try:
      await mrd.open()
      # 0, 0 is the entire object.
      await mrd.download_ranges([(0, 0, io.BytesIO())])
    finally:
      if mrd.is_stream_open:
        await mrd.close()

  def Read(self, bucket_name, object_name):
    return self.loop.run_until_complete(
        self._ReadAsync(bucket_name, object_name)
    )


class GcsGrpcService(gcs.GcsService):
  """An interface to Google Cloud Storage, using the gRPC python library.

  Defends SynchronousGcsClient against being called from multiple processes.
  Extends GcsService to share Delete/List functionality, which is gRPC specific.
  """

  def __init__(self):
    super().__init__()
    self.clients = collections.defaultdict(SynchronousGcsClient)

  def get_client(self):
    pid = multiprocessing.current_process().pid
    return self.clients[pid]

  def WriteObjectFromBuffer(self, bucket_name, object_name, stream, size):
    stream.seek(0)
    client = self.get_client()
    start_time = time.time()
    client.Write(bucket_name, object_name, stream, size)
    latency = time.time() - start_time
    return start_time, latency

  def ReadObject(self, bucket_name, object_name):
    client = self.get_client()
    start_time = time.time()
    client.Read(bucket_name, object_name)
    latency = time.time() - start_time
    return start_time, latency
