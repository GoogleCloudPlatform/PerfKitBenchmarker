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
"""Tests for the AWS Lustre service."""

import json
import unittest
from absl import flags
from absl.testing import parameterized
import mock
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_lustre_service
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_AWS_ZONE = 'us-east-1d'
_FILE_SYSTEM_ID = 'fs-0670433f7cdf85af8'


class AwsLustreServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'test_uri'
    disk_spec = mock.create_autospec(disk.BaseLustreDiskSpec)
    self.lustre = aws_lustre_service.AwsLustreService(disk_spec, _AWS_ZONE)
    self.lustre.id = _FILE_SYSTEM_ID

  @mock.patch.object(vm_util, 'IssueCommand')
  def testIsReadyAvailable(self, mock_issue_command):
    mock_issue_command.return_value = (
        json.dumps({
            'FileSystems': [{
                'FileSystemId': _FILE_SYSTEM_ID,
                'Lifecycle': 'AVAILABLE',
            }]
        }),
        '',
        0,
    )
    self.assertTrue(self.lustre._IsReady())

  @mock.patch.object(vm_util, 'IssueCommand')
  def testIsReadyCreating(self, mock_issue_command):
    mock_issue_command.return_value = (
        json.dumps({
            'FileSystems': [{
                'FileSystemId': _FILE_SYSTEM_ID,
                'Lifecycle': 'CREATING',
            }]
        }),
        '',
        0,
    )
    self.assertFalse(self.lustre._IsReady())

  @parameterized.named_parameters(
      (
          'FailedWithDetails',
          'FAILED',
          {'Message': 'Capacity must be at least 4800 GiB.'},
          'Capacity must be at least 4800 GiB.',
      ),
      (
          'MisconfiguredWithDetails',
          'MISCONFIGURED',
          {'Message': 'Subnet not found.'},
          'Subnet not found.',
      ),
  )
  @mock.patch.object(vm_util, 'IssueCommand')
  def testIsReadyRaisesCreationErrorOnFailure(
      self, status, failure_details, expected_msg, mock_issue_command
  ):
    mock_issue_command.return_value = (
        json.dumps({
            'FileSystems': [{
                'FileSystemId': _FILE_SYSTEM_ID,
                'Lifecycle': status,
                'FailureDetails': failure_details,
            }]
        }),
        '',
        0,
    )
    with self.assertRaises(errors.Resource.CreationError) as cm:
      self.lustre._IsReady()
    self.assertIn(expected_msg, str(cm.exception))
    self.assertIn(status, str(cm.exception))

  @mock.patch.object(vm_util, 'IssueCommand')
  def testIsReadyRaisesInsufficientCapacity(self, mock_issue_command):
    mock_issue_command.return_value = (
        json.dumps({
            'FileSystems': [{
                'FileSystemId': _FILE_SYSTEM_ID,
                'Lifecycle': 'FAILED',
                'FailureDetails': {
                    'Message': (
                        'There is insufficient capacity in the AZ to create'
                        ' the file system.'
                    )
                },
            }]
        }),
        '',
        0,
    )
    with self.assertRaises(
        errors.Benchmarks.InsufficientCapacityCloudFailure
    ) as cm:
      self.lustre._IsReady()
    self.assertIn('insufficient capacity', str(cm.exception).lower())

  @mock.patch.object(vm_util, 'IssueCommand')
  def testCommandFailsWithReturnCode(self, mock_issue_command):
    mock_issue_command.return_value = (
        '',
        (
            'An error occurred (BadRequest) when calling the CreateFileSystem'
            ' operation'
        ),
        255,
    )
    with self.assertRaises(errors.Error) as cm:
      self.lustre._Command('create-file-system', [])
    self.assertIn('return code 255', str(cm.exception))
    self.assertIn('BadRequest', str(cm.exception))


if __name__ == '__main__':
  unittest.main()
