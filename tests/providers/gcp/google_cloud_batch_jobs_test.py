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
import time
import unittest
from absl import flags
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import google_cloud_batch_jobs as gcb_jobs
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GoogleCloudBatchJobTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'run_uri'
    FLAGS.project = 'fake-project'

    self.enter_context(
        mock.patch(
            'perfkitbenchmarker.providers.gcp.util.GetDefaultProject',
            return_value='fake-project',
        )
    )
    self.enter_context(
        mock.patch(
            'perfkitbenchmarker.providers.gcp.util.GetProjectNumber',
            return_value='123456789',
        )
    )
    self.enter_context(
        mock.patch.object(vm_util, 'GetTempDir', return_value='/tmp')
    )

    self.mock_open = self.enter_context(
        mock.patch('builtins.open', mock.mock_open())
    )
    self.mock_exists = self.enter_context(
        mock.patch.object(os.path, 'exists', return_value=True)
    )
    self.mock_remove = self.enter_context(mock.patch.object(os, 'remove'))
    self.enter_context(mock.patch.object(time, 'sleep'))

    self.job_spec = mock.Mock()
    self.job_spec.job_region = 'us-central1'
    self.job_spec.job_type = 'GoogleCloudBatchJob'

    self.mock_registry = mock.Mock()
    self.mock_registry.GetOrBuild.return_value = (
        'us-central1-docker.pkg.dev/fake/image'
    )
    self.job = gcb_jobs.GoogleCloudBatchJob(
        self.job_spec, container_registry=self.mock_registry
    )
    self.job.name = 'pkb-job-run_uri'
    self.job.container_image = 'fake-image'
    self.job.task_count = 1

  def testInitialization(self):
    self.assertEqual(self.job.region, 'us-central1')
    self.assertEqual(self.job.machine_type, 'c4-standard-2')

  def testLifecycle(self):
    self.job.Create()

    self.assertEqual(self.job.config_path, '/tmp/pkb-job-run_uri_config.json')

    write_args = self.mock_open().write.call_args_list
    written_data = ''.join(call[0][0] for call in write_args)
    config = json.loads(written_data)
    self.assertEqual(
        config['taskGroups'][0]['taskSpec']['runnables'][0]['container'][
            'imageUri'
        ],
        'us-central1-docker.pkg.dev/fake/image:latest',
    )
    self.assertEqual(
        config['allocationPolicy']['instances'][0]['policy']['machineType'],
        'c4-standard-2',
    )

    mock_cmd = self.MockIssueCommand({
        'batch jobs submit': [('{}', '', 0)],
        'batch jobs describe': [
            ('{"status": {"state": "QUEUED"}}', '', 0),
            (
                (
                    '{"status": {"state": "RUNNING", "statusEvents": [{"type":'
                    ' "STATUS_CHANGED", "description": "to RUNNING",'
                    ' "eventTime": "2026-06-26T20:10:00Z"}]}}'
                ),
                '',
                0,
            ),
            (
                (
                    '{"status": {"state": "SUCCEEDED", "statusEvents":'
                    ' [{"type": "STATUS_CHANGED", "description": "to'
                    ' RUNNING", "eventTime": "2026-06-26T20:10:00Z"}]}}'
                ),
                '',
                0,
            ),
            ('{"status": {"state": "SUCCEEDED"}}', '', 0),
            ('Job not found', 'Job not found', 1),
        ],
        'batch jobs delete': [('', '', 0)],
    })
    self.enter_context(mock.patch.object(time, 'time', return_value=123456.0))

    self.job.Execute()

    self.assertEqual(self.job.start_timestamp, 1782504600.0)
    self.assertIn('submit', mock_cmd.all_commands)

    self.job.Delete()
    self.mock_remove.assert_called_once_with(self.job.config_path)
    self.assertIn('pkb-job-run_uri', mock_cmd.GetCommandWithSubstring('delete'))

  def testExistsEdgeCases(self):
    self.job.submit_timestamp = None
    self.mock_exists.return_value = False
    self.assertFalse(self.job._Exists())

    self.job.submit_timestamp = 123456.0
    self.MockIssueCommand({
        'batch jobs describe': [
            ('{"status": {"state": "DELETION_IN_PROGRESS"}}', '', 0)
        ]
    })
    self.assertFalse(self.job._Exists())

    self.MockIssueCommand(
        {'batch jobs describe': [('Job not found', 'Job not found', 1)]}
    )
    self.assertFalse(self.job._Exists())


if __name__ == '__main__':
  unittest.main()
