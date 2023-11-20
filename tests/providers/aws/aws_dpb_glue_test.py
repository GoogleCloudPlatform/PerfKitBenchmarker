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

import copy
import json
from typing import Any, Optional
import unittest
from unittest import mock

from absl import flags

from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_dpb_glue
from perfkitbenchmarker.providers.aws import aws_dpb_glue_prices
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'
AWS_ZONE_US_EAST_1A = 'us-east-1a'
FLAGS = flags.FLAGS
_BASE_JOB_RUN_PAYLOAD = {
    'JobRun': {
        'Id': 'jr_01234567890abcdef',
        'Attempt': 0,
        'JobName': 'pkb-deadbeef-0',
        'StartedOn': 1675103057.784,
        'LastModifiedOn': 1675105738.096,
        'CompletedOn': 1675105738.096,
        'JobRunState': 'SUCCEEDED',
        'Arguments': {
            '--pkb_main': 'hello',
            '--pkb_args': '[]'
        },
        'PredecessorRuns': [],
        'AllocatedCapacity': 32,
        'ExecutionTime': 2672,
        'Timeout': 2880,
        'MaxCapacity': 32.0,
        'WorkerType': 'G.2X',
        'NumberOfWorkers': 4,
        'LogGroupName': '/aws-glue/jobs',
        'GlueVersion': '3.0'
    }
}


def _GetJobRunMockPayload(
    dpu_seconds: Optional[float],
    max_capacity: Optional[float],
    execution_time: Optional[float]
) -> dict[str, Any]:
  payload = copy.deepcopy(_BASE_JOB_RUN_PAYLOAD)
  if dpu_seconds is not None:
    payload['JobRun']['DPUSeconds'] = dpu_seconds
  if max_capacity is not None:
    payload['JobRun']['MaxCapacity'] = max_capacity
  if execution_time is not None:
    payload['JobRun']['ExecutionTime'] = execution_time

  return payload


GLUE_SPEC = mock.Mock(
    static_dpb_service_instance=None,
    version='3.0',
    worker_count=4,
    worker_group=mock.Mock(vm_spec=mock.Mock(machine_type='G.2X')))


class AwsDpbEmrTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsDpbEmrTestCase, self).setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = AWS_ZONE_US_EAST_1A
    FLAGS.zones = [AWS_ZONE_US_EAST_1A]
    self.issue_cmd_mock = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', autospec=True))

  def testGlueCalculateLastJobCost(self):
    dpb_glue = aws_dpb_glue.AwsDpbGlue(GLUE_SPEC)

    create_job_response = {'Name': 'pkb-deadbeef-0'}
    start_job_run_response = {'JobRunId': 'jr_01234567890abcdef'}
    self.issue_cmd_mock.side_effect = [
        (json.dumps(create_job_response), '', 0),
        (json.dumps(start_job_run_response), '', 0),
        (json.dumps(
            _GetJobRunMockPayload(dpu_seconds=None, max_capacity=32.0,
                                  execution_time=2672)), '', 0)
    ]

    with mock.patch.object(aws_dpb_glue_prices, 'GLUE_PRICES'):
      # The actual prices are expected to change over time, but we don't want to
      # update the test every time.
      aws_dpb_glue_prices.GLUE_PRICES = {'us-east-1': 0.44}
      dpb_glue.SubmitJob(
          pyspark_file='s3://test/hello.py',
          job_type=dpb_constants.PYSPARK_JOB_TYPE,
          job_arguments=[])

    self.assertEqual(dpb_glue.CalculateLastJobCost(), 10.45048888888889)

  def testGluePricesSchema(self):
    for region, price in aws_dpb_glue_prices.GLUE_PRICES.items():
      self.assertIsInstance(region, str)
      self.assertIsInstance(price, float)

  def testMetadata(self):
    dpb_glue = aws_dpb_glue.AwsDpbGlue(GLUE_SPEC)
    expected_metadata = {
        'dpb_service': 'glue',
        'dpb_version': '3.0',
        'dpb_service_version': 'glue_3.0',
        'dpb_cluster_shape': 'G.2X',
        'dpb_cluster_size': 4,
        'dpb_hdfs_type': 'default-disk',
        'dpb_disk_size': '128',
        'dpb_service_zone': 'us-east-1a',
        'dpb_job_properties': '',
    }
    self.assertEqual(dpb_glue.GetResourceMetadata(), expected_metadata)


if __name__ == '__main__':
  unittest.main()
