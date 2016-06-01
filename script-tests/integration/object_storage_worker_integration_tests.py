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

"""Integration tests for the object_storage_service benchmark worker process."""

import time
import unittest

import object_storage_interface
import object_storage_api_tests  # noqa: importing for flags

import validate_service


class MockObjectStorageService(object_storage_interface.ObjectStorageServiceBase):  # noqa
  def __init__(self):
    self.bucket = None
    self.objects = {}

  def _CheckBucket(self, bucket):
    """Make sure that we are only passed one bucket name.

    Args:
      bucket: the name of a bucket.

    Raises: ValueError, if this object has been passed a different
    bucket name previously.
    """

    if self.bucket is None:
      self.bucket = bucket
    elif self.bucket != bucket:
      raise ValueError(
          'MockObjectStorageService passed two bucket names: %s and %s' %
          (self.bucket, bucket))

  def ListObjects(self, bucket, prefix):
    self._CheckBucket(bucket)

    return [value
            for name, value in self.objects.iteritems()
            if name.startswith(prefix)]

  def DeleteObjects(self, bucket, objects_to_delete, objects_deleted=None):
    self._CheckBucket(bucket)

    for name in objects_to_delete:
      if name in self.objects:
        del self.objects[name]
        if objects_deleted is not None:
          objects_deleted.append(name)

  def WriteObjectFromBuffer(self, bucket, object, stream, size):
    self._CheckBucket(bucket)

    stream.seek(0)
    self.objects[object] = stream.read(size)

    return time.time(), 0.001

  def ReadObject(self, bucket, object):
    self._CheckBucket(bucket)

    self.objects[object]

    return time.time(), 0.001


class TestScenarios(unittest.TestCase):
  """Test that the benchmark scenarios complete.

  Specifically, given a correctly operating service
  (MockObjectStorageService), verify that the benchmarking scenarios
  run to completion without raising an exception.
  """

  def setUp(self):
    self.FLAGS = object_storage_api_tests.FLAGS
    self.FLAGS([])
    self.objects_written_file = self.FLAGS.objects_written_file
    self.FLAGS.objects_written_file = '/tmp/objects-written'

  def tearDown(self):
    self.FLAGS.objects_written_file = self.objects_written_file

  def testOneByteRW(self):
    object_storage_api_tests.OneByteRWBenchmark(MockObjectStorageService())

  def testListConsistency(self):
    object_storage_api_tests.ListConsistencyBenchmark(
        MockObjectStorageService())

  def testSingleStreamThroughput(self):
    object_storage_api_tests.SingleStreamThroughputBenchmark(
        MockObjectStorageService())

  def testCleanupBucket(self):
    object_storage_api_tests.CleanupBucket(MockObjectStorageService())

  def testMultiStreamWriteAndRead(self):
    service = MockObjectStorageService()

    # Have to sequence MultiStreamWrites and MultiStreamReads because
    # MultiStreamReads will read from the objects_written_file that
    # MultiStreamWrites generates.
    object_storage_api_tests.MultiStreamWrites(service)
    object_storage_api_tests.MultiStreamReads(service)


class TestValidateService(unittest.TestCase):
  """Validate the ValidateService script."""

  def setUp(self):
    self.FLAGS = object_storage_api_tests.FLAGS
    self.FLAGS([])
    self.objects_written_file = self.FLAGS.objects_written_file
    self.FLAGS.objects_written_file = '/tmp/objects-written'

  def testValidateService(self):
    validate_service.ValidateService(MockObjectStorageService())



if __name__ == '__main__':
  unittest.main()
