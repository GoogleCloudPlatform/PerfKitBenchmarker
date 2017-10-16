#!/usr/bin/env python

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

"""A script to validate ObjectStorageServiceBase implementations."""

import cStringIO
import logging
import sys

from absl import flags

import object_storage_api_tests

FLAGS = flags.FLAGS


def ValidateService(service):
  object_names = ['object_' + str(i) for i in range(10)]
  payload = object_storage_api_tests.GenerateWritePayload(100)
  handle = cStringIO.StringIO(payload)

  logging.info('Starting test.')

  # Write objects
  for name in object_names:
    service.WriteObjectFromBuffer(FLAGS.bucket, name, handle, 100)

  logging.info('Wrote 10 100B objects to %s.', FLAGS.bucket)

  # Read the objects back
  for name in object_names:
    service.ReadObject(FLAGS.bucket, name)

  logging.info('Read objects back.')

  # List the objects
  names = service.ListObjects(FLAGS.bucket, 'object_')
  if sorted(names) != object_names:
    logging.error('ListObjects returned %s, but should have returned %s',
                  names, object_names)

  logging.info('Listed object names.')

  # Delete the objects
  deleted = []
  service.DeleteObjects(FLAGS.bucket, object_names, objects_deleted=deleted)
  if sorted(deleted) != object_names:
    logging.error('DeleteObjects returned %s, but should have returned %s',
                  deleted, object_names)

  logging.info('Deleted objects. Test is complete.')


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  FLAGS(sys.argv)

  service = (
      object_storage_api_tests._STORAGE_TO_SERVICE_DICT[FLAGS.storage_provider]())  # noqa
  ValidateService(service)
