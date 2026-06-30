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
from absl.testing import flagsaver
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

    # Mock GCP util methods that query live metadata/config
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

    # Mock temporary directory for config generation
    self.enter_context(
        mock.patch.object(vm_util, 'GetTempDir', return_value='/tmp')
    )

    # Construct Job Spec and Job instance
    self.job_spec = mock.Mock()
    self.job_spec.job_region = 'us-central1'
    self.job_spec.job_type = 'GoogleCloudBatchJob'

    self.job = gcb_jobs.GoogleCloudBatchJob(
        self.job_spec, container_registry=mock.Mock()
    )
    self.job.name = 'pkb-job-run_uri'
    self.job.container_image = 'fake-image'
    self.job.task_count = 1

  def testInitialization(self):
    self.assertEqual(self.job.region, 'us-central1')
    self.assertEqual(self.job.project, 'fake-project')
    self.assertEqual(self.job.project_number, '123456789')
    self.assertEqual(self.job.machine_type, 'c4-standard-2')

    # Verify constructor metadata updates
    self.assertEqual(self.job.metadata['Product_ID'], 'GoogleCloudBatchJob')
    self.assertEqual(self.job.metadata['region'], 'us-central1')
    self.assertEqual(self.job.metadata['machine_type'], 'c4-standard-2')

  @flagsaver.flagsaver(machine_type='custom-machine')
  def testInitializationCustomMachine(self):
    job = gcb_jobs.GoogleCloudBatchJob(
        self.job_spec, container_registry=mock.Mock()
    )
    self.assertEqual(job.machine_type, 'custom-machine')
    self.assertEqual(job.metadata['machine_type'], 'custom-machine')

  def testCreate(self):
    # Mock open to avoid writing to actual disk during tests, but we will
    # capture the written JSON content using mock_open!
    mock_open_instance = mock.mock_open()
    with mock.patch('builtins.open', mock_open_instance):
      self.job._Create()

    self.assertEqual(self.job.config_path, '/tmp/pkb-job-run_uri_config.json')

    # Assert on dynamic metadata returned by GetResourceMetadata()
    metadata = self.job.GetResourceMetadata()
    self.assertEqual(metadata['config_path'], self.job.config_path)
    self.assertEqual(metadata['container_image'], 'fake-image')

    # Verify written JSON content
    write_args = mock_open_instance().write.call_args_list
    written_data = ''.join(call[0][0] for call in write_args)
    config = json.loads(written_data)

    self.assertEqual(
        config['taskGroups'][0]['taskSpec']['runnables'][0]['container'][
            'imageUri'
        ],
        'fake-image',
    )
    self.assertEqual(
        config['allocationPolicy']['instances'][0]['policy']['machineType'],
        'c4-standard-2',
    )

  @mock.patch.object(os.path, 'exists', return_value=True)
  @mock.patch.object(os, 'remove')
  def testDelete(self, mock_remove, mock_exists):
    mock_cmd = self.MockIssueCommand({'batch jobs delete': [('', '', 0)]})
    self.job.config_path = '/tmp/pkb-job-run_uri_config.json'
    self.job._Delete()

    mock_remove.assert_called_once_with(self.job.config_path)
    delete_call = mock_cmd.func_to_mock.call_args_list[0][0][0]
    self.assertIn('delete', delete_call)
    self.assertIn('pkb-job-run_uri', delete_call)

  def testExistsBeforeSubmit(self):
    self.job.submit_timestamp = None
    self.job.config_path = '/tmp/pkb-job-run_uri_config.json'

    with mock.patch.object(os.path, 'exists', return_value=True):
      self.assertTrue(self.job._Exists())
    with mock.patch.object(os.path, 'exists', return_value=False):
      self.assertFalse(self.job._Exists())

  def testExistsAfterSubmit(self):
    self.job.submit_timestamp = 123456.0
    mock_cmd = self.MockIssueCommand(
        {'batch jobs describe': [('{"status": {"state": "RUNNING"}}', '', 0)]}
    )

    self.assertTrue(self.job._Exists())
    describe_call = mock_cmd.func_to_mock.call_args_list[0][0][0]
    self.assertIn('describe', describe_call)

  def testExistsDeletionInProgress(self):
    self.job.submit_timestamp = 123456.0
    self.MockIssueCommand({
        'batch jobs describe': [
            ('{"status": {"state": "DELETION_IN_PROGRESS"}}', '', 0)
        ]
    })

    # Treating it as False avoids blocking teardown on metadata-only cleanup
    self.assertFalse(self.job._Exists())

  def testExistsNotFound(self):
    self.job.submit_timestamp = 123456.0
    self.MockIssueCommand(
        {'batch jobs describe': [('Job not found', 'Job not found', 1)]}
    )

    self.assertFalse(self.job._Exists())

  @mock.patch.object(time, 'time', return_value=123456.0)
  def testExecuteAndTimestampParsing(self, mock_time):
    # Mock submit response
    mock_submit_stdout = '{}'
    # Mock describe polling responses
    mock_describe_queued = '{"status": {"state": "QUEUED"}}'
    mock_describe_running = json.dumps({
        'status': {
            'state': 'RUNNING',
            'statusEvents': [{
                'type': 'STATUS_CHANGED',
                'description': 'Job state changed from QUEUED to RUNNING',
                'eventTime': '2026-06-26T20:10:00Z',
            }],
        }
    })
    mock_describe_succeeded = json.dumps({
        'createTime': '2026-06-26T20:00:00Z',
        'status': {
            'state': 'SUCCEEDED',
            'statusEvents': [
                {
                    'type': 'STATUS_CHANGED',
                    'description': 'Job state changed from QUEUED to RUNNING',
                    'eventTime': '2026-06-26T20:10:00Z',
                },
                {
                    'type': 'STATUS_CHANGED',
                    'description': (
                        'Job state changed from RUNNING to SUCCEEDED'
                    ),
                    'eventTime': '2026-06-26T20:15:00Z',
                },
            ],
        },
    })

    mock_cmd = self.MockIssueCommand({
        'batch jobs submit': [(mock_submit_stdout, '', 0)],
        'batch jobs describe': [
            (mock_describe_queued, '', 0),
            (mock_describe_running, '', 0),
            (mock_describe_succeeded, '', 0),
        ],
    })

    self.job.config_path = '/tmp/pkb-job-run_uri_config.json'
    self.job.Execute()

    # Verify submit command
    submit_call = mock_cmd.func_to_mock.call_args_list[0][0][0]
    self.assertIn('submit', submit_call)

    # Verify describe calls (1 submit + 3 polls = 4 total calls)
    self.assertEqual(mock_cmd.func_to_mock.call_count, 4)

    # RUNNING event '2026-06-26T20:10:00Z' -> 1782504600.0
    self.assertEqual(self.job.start_timestamp, 1782504600.0)

  def testLifecycleMetadata(self):
    mock_registry = mock.Mock()
    mock_registry.GetOrBuild.return_value = (
        'us-central1-docker.pkg.dev/fake/image'
    )

    job = gcb_jobs.GoogleCloudBatchJob(
        self.job_spec, container_registry=mock_registry
    )
    job.name = 'pkb-job-run_uri'
    job.task_count = 1

    mock_open_instance = mock.mock_open()
    with mock.patch('builtins.open', mock_open_instance), mock.patch.object(
        os.path, 'exists', return_value=True
    ):
      job.Create()

    metadata = job.GetResourceMetadata()
    self.assertEqual(
        metadata['container_image'],
        'us-central1-docker.pkg.dev/fake/image:latest',
    )
    self.assertEqual(
        metadata['config_path'], '/tmp/pkb-job-run_uri_config.json'
    )


if __name__ == '__main__':
  unittest.main()
