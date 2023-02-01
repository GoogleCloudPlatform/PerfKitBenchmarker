# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_dpb_emr."""

import json
import unittest
from unittest import mock

from absl import flags

from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_dpb_emr
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'
AWS_ZONE_US_EAST_1A = 'us-east-1a'
FLAGS = flags.FLAGS
JOB_RUN_MOCK_PAYLOAD = {
    'jobRun': {
        'applicationId': 'foobar',
        'jobRunId': 'bazquux',
        'arn': (
            'arn:aws:emr-serverless:us-east-1:1234567:/applications/foobar/jobruns/bazquux'
        ),
        'createdBy': 'arn:aws:iam::1234567:user/perfkitbenchmarker',
        'createdAt': 1675193231.789,
        'updatedAt': 1675194602.299,
        'executionRole': 'arn:aws:iam::1234567:role/MyRole',
        'state': 'SUCCESS',
        'stateDetails': '',
        'releaseLabel': 'emr-6.8.0',
        'jobDriver': {
            'sparkSubmit': {
                'entryPoint': 's3://test/hello.py',
                'entryPointArguments': [],
                'sparkSubmitParameters': (
                    '--conf spark.dynamicAllocation.enabled=FALSE '
                    '--conf spark.executor.cores=4 '
                    '--conf spark.driver.cores=4 '
                    '--conf spark.executor.memory=14G '
                    '--conf spark.executor.instances=4 '
                    '--conf spark.emr-serverless.driver.disk=42G '
                    '--conf spark.emr-serverless.executor.disk=42G'
                )
            }
        },
        'tags': {},
        'totalResourceUtilization': {
            'vCPUHour': 59.422,
            'memoryGBHour': 237.689,
            'storageGBHour': 1901.511
        }
    }
}


SERVERLESS_SPEC = mock.Mock(
    static_dpb_service_instance=None,
    version='fake-4.2',
    emr_serverless_core_count=4,
    emr_serverless_executor_count=4,
    emr_serverless_memory=14,
    worker_group=mock.Mock(disk_spec=mock.Mock(disk_size=42)))


class LocalAwsDpbEmr(aws_dpb_emr.AwsDpbEmr):

  def __init__(self):
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(
        util.GetRegionFromZone(FLAGS.dpb_service_zone))


class AwsDpbEmrTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsDpbEmrTestCase, self).setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = AWS_ZONE_US_EAST_1A
    FLAGS.zones = [AWS_ZONE_US_EAST_1A]
    self.issue_cmd_mock = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', autospec=True))

  def testEmrServerlessCalculateCost(self):
    emr_serverless = aws_dpb_emr.AwsDpbEmrServerless(SERVERLESS_SPEC)

    create_application_response = {'applicationId': 'foobar'}
    get_application_response = {'application': {'state': 'STARTED'}}
    start_job_run_response = {
        'applicationId': 'foobar',
        'jobRunId': 'bazquux',
        'arn': (
            'arn:aws:emr-serverless:us-east-1:1234567:/applications/foobar/jobruns/bazquux'
        )
    }
    self.issue_cmd_mock.side_effect = [
        (json.dumps(create_application_response), '', 0),
        (json.dumps(get_application_response), '', 0),
        (json.dumps(start_job_run_response), '', 0),
        (json.dumps(JOB_RUN_MOCK_PAYLOAD), '', 0)
    ]

    emr_serverless.SubmitJob(
        pyspark_file='s3://test/hello.py',
        job_type=dpb_service.BaseDpbService.PYSPARK_JOB_TYPE)
    self.assertEqual(emr_serverless.CalculateCost(), 4.711576935499999)


if __name__ == '__main__':
  unittest.main()
