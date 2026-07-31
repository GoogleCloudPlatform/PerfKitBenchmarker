# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
import json
import os
import unittest
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcs
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GoogleCloudStorageServiceTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.run_dir = self.create_tempdir().full_path
    mock_get_run_dir_path = mock.patch.object(
        temp_dir, 'GetRunDirPath', return_value=self.run_dir
    )
    mock_get_run_dir_path.start()
    self.addCleanup(mock_get_run_dir_path.stop)

  @mock.patch.object(
      vm_util, 'IssueCommand', return_value=('fake_stdout', 'fake_stderr', 0)
  )
  def testMakeBucketNoTtl(self, mock_issue):
    service = gcs.GoogleCloudStorageService()
    service.PrepareService(location='us-central1')
    service.MakeBucket('test-bucket', tag_bucket=False)

    self.assertEqual(mock_issue.call_count, 1)
    command = mock_issue.call_args[0][0]
    self.assertEqual(
        command,
        [
            'gcloud',
            'storage',
            'buckets',
            'create',
            '--location',
            'us-central1',
            '--default-storage-class',
            'regional',
            'gs://test-bucket',
        ],
    )

  @mock.patch.object(
      vm_util, 'IssueCommand', return_value=('fake_stdout', 'fake_stderr', 0)
  )
  @flagsaver.flagsaver(object_ttl_days=3)
  def testMakeBucketWithTtl(self, mock_issue):
    service = gcs.GoogleCloudStorageService()
    service.PrepareService(location='us-central1')
    service.MakeBucket('test-bucket', tag_bucket=False)

    self.assertEqual(mock_issue.call_count, 1)
    command = mock_issue.call_args[0][0]

    self.assertEqual(len(command), 10)
    self.assertEqual(
        command[:8],
        [
            'gcloud',
            'storage',
            'buckets',
            'create',
            '--location',
            'us-central1',
            '--default-storage-class',
            'regional',
        ],
    )
    self.assertTrue(command[8].startswith('--lifecycle-file='))
    self.assertEqual(command[9], 'gs://test-bucket')

    lifecycle_file_path = command[8].split('=', 1)[1]
    self.assertTrue(os.path.exists(lifecycle_file_path))
    with open(lifecycle_file_path) as f:
      content = json.load(f)

    expected_content = {
        'rule': [{
            'action': {'type': 'Delete'},
            'condition': {'age': 3},
        }]
    }
    self.assertEqual(content, expected_content)


if __name__ == '__main__':
  unittest.main()
