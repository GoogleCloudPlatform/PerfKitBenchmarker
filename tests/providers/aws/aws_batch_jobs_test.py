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
import unittest
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker.providers.aws import aws_batch_jobs as ab_jobs
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util as aws_util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class AwsBatchJobTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'run_uri'
    FLAGS.zone = ['us-east-1a']

    self.enter_context(
        mock.patch.object(aws_util, 'GetAccount', return_value='123456789012')
    )

    self.mock_network = mock.Mock(spec=aws_network.AwsNetwork)
    self.mock_network.subnet = mock.Mock(id='subnet-1234')
    self.mock_network.regional_network = mock.Mock()
    self.mock_network.regional_network.vpc = mock.Mock(
        default_security_group_id='sg-5678'
    )
    self.enter_context(
        mock.patch.object(
            aws_network.AwsNetwork,
            'GetNetworkFromNetworkSpec',
            return_value=self.mock_network,
        )
    )

    self.job_spec = mock.Mock()
    self.job_spec.job_region = 'us-east-1'
    self.job_spec.job_type = 'AwsBatchJob'

  def _CreateJob(self) -> ab_jobs.AwsBatchJob:
    job = ab_jobs.AwsBatchJob(self.job_spec, container_registry=mock.Mock())
    job.name = 'pkb-job-run_uri'
    job.container_image = 'fake-image'
    return job

  def testInitialization(self):
    job = self._CreateJob()
    self.assertEqual(job.region, 'us-east-1')
    self.assertEqual(job.account, '123456789012')
    self.assertEqual(job.compute_type, 'FARGATE')
    self.assertIsNone(job.start_timestamp)

  def testExecuteAndTimestampParsing(self):
    job = self._CreateJob()
    mock_submit_stdout = '{"jobId": "job-1234"}'
    mock_describe_stdout = json.dumps({
        'jobs': [{
            'jobId': 'job-1234',
            'status': 'SUCCEEDED',
            'createdAt': 1782504000000,  # 2026-06-26T20:00:00Z in ms
            'startedAt': 1782504600000,  # 2026-06-26T20:10:00Z in ms
            'stoppedAt': 1782504900000,
        }]
    })

    mock_cmd = self.MockIssueCommand({
        'batch submit-job': [(mock_submit_stdout, '', 0)],
        'batch describe-jobs': [(mock_describe_stdout, '', 0)],
    })

    job.job_queue.arn = 'arn:queue'
    job.job_definition.arn = 'arn:def'

    job.Execute()

    self.assertEqual(mock_cmd.func_to_mock.call_count, 2)
    self.assertEqual(job.start_timestamp, 1782504600.0)

  @flagsaver.flagsaver(aws_batch_compute_type='EC2', machine_type='m8i.large')
  def testCreateComputeEnvironment(self):
    job = self._CreateJob()
    mock_cmd = self.MockIssueCommand({
        'ec2 modify-subnet-attribute': [('', '', 0)],
        'batch create-compute-environment': [
            ('{"computeEnvironmentArn": "arn:ce"}', '', 0)
        ],
        'batch describe-compute-environments': [
            ('{"computeEnvironments": [{"status": "VALID"}]}', '', 0)
        ],
    })

    job.compute_env.Create()

    create_cmd = None
    for call in mock_cmd.func_to_mock.call_args_list:
      cmd = call[0][0]
      if 'create-compute-environment' in cmd:
        create_cmd = cmd
        break
    self.assertIsNotNone(create_cmd)
    idx = create_cmd.index('--compute-resources')
    resources = json.loads(create_cmd[idx + 1])
    self.assertEqual(resources['minvCpus'], 0)
    self.assertEqual(resources['securityGroupIds'], ['sg-5678'])


if __name__ == '__main__':
  unittest.main()
