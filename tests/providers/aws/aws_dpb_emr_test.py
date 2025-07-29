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
import time
from typing import Any
import unittest
from unittest import mock

from absl import flags
from absl.testing import parameterized
import freezegun
from perfkitbenchmarker import disk
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_dpb_emr
from perfkitbenchmarker.providers.aws import aws_dpb_emr_serverless_prices
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'
AWS_ZONE_US_EAST_1A = 'us-east-1a'
FLAGS = flags.FLAGS

_BASE_JOB_RUN_PAYLOAD: dict[str, Any] = {
    'jobRun': {
        'applicationId': 'foobar',
        'jobRunId': 'bazquux',
        'arn': 'arn:aws:emr-serverless:us-east-1:1234567:/applications/foobar/jobruns/bazquux',
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
                ),
            }
        },
        'tags': {},
    }
}

_EMR_SERVERLESS_PRICES = {
    'us-east-1': {
        'vcpu_hours': 0.052624,
        'memory_gb_hours': 0.0057785,
        'storage_gb_hours': 0.000111,
    },
}

_EMR_SERVERLESS_CREATE_APPLICATION_RESPONSE = {'applicationId': 'foobar'}
_EMR_SERVERLESS_GET_APPLICATION_RESPONSE = {'application': {'state': 'STARTED'}}
_EMR_SERVERLESS_START_JOB_RUN_RESPONSE = {
    'applicationId': 'foobar',
    'jobRunId': 'bazquux',
    'arn': 'arn:aws:emr-serverless:us-east-1:1234567:/applications/foobar/jobruns/bazquux',
}


def _GetEmrSpec():
  return mock.Mock(
      static_dpb_service_instance=None,
      worker_count=2,
      version='fake-version',
      worker_group=mock.Mock(
          vm_spec=mock.Mock(machine_type='fake-machine-type'),
          disk_spec=mock.Mock(disk_type='gp2', disk_size=42),
      ),
  )


def _GetJobRunMockPayload(
    vcpu_hour: float | None = None,
    memory_gb_hour: float | None = None,
    storage_gb_hour: float | None = None,
) -> dict[str, Any]:
  payload = copy.deepcopy(_BASE_JOB_RUN_PAYLOAD)
  if vcpu_hour is not None:
    payload['jobRun'].setdefault('totalResourceUtilization', {})[
        'vCPUHour'
    ] = vcpu_hour
  if memory_gb_hour is not None:
    payload['jobRun'].setdefault('totalResourceUtilization', {})[
        'memoryGBHour'
    ] = memory_gb_hour
  if storage_gb_hour is not None:
    payload['jobRun'].setdefault('totalResourceUtilization', {})[
        'storageGBHour'
    ] = storage_gb_hour
  return payload


SERVERLESS_SPEC = mock.Mock(
    static_dpb_service_instance=None,
    version='fake-4.2',
    emr_serverless_core_count=4,
    emr_serverless_executor_count=4,
    emr_serverless_memory=14,
    worker_group=mock.Mock(disk_spec=mock.Mock(disk_size=42)),
)


class LocalAwsDpbEmr(aws_dpb_emr.AwsDpbEmr):

  def __init__(self):
    self.storage_service = s3.S3Service()
    self.storage_service.PrepareService(
        util.GetRegionFromZone(FLAGS.dpb_service_zone)
    )


class AwsDpbEmrTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = AWS_ZONE_US_EAST_1A
    FLAGS.zones = [AWS_ZONE_US_EAST_1A]
    self.issue_cmd_mock = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', autospec=True)
    )

  @parameterized.named_parameters(
      dict(testcase_name='St1', disk_type=aws_disk.ST1, hdfs_type='HDD (ST1)'),
      dict(testcase_name='Gp2', disk_type=aws_disk.GP2, hdfs_type='SSD (GP2)'),
      dict(testcase_name='Ssd', disk_type=disk.LOCAL, hdfs_type='Local SSD'),
  )
  @mock.patch.object(aws_network.AwsNetwork, 'GetNetworkFromNetworkSpec')
  def testEmrMetadata(self, _, disk_type, hdfs_type):
    spec = _GetEmrSpec()
    spec.worker_group.disk_spec.disk_type = disk_type
    cluster = aws_dpb_emr.AwsDpbEmr(spec)
    expected_metadata = {
        'dpb_service': 'emr',
        'dpb_version': 'fake-version',
        'dpb_service_version': 'emr_fake-version',
        'dpb_cluster_id': 'pkb-fakeru',
        'dpb_cluster_shape': 'fake-machine-type',
        'dpb_cluster_size': 2,
        'dpb_hdfs_type': hdfs_type,
        'dpb_disk_size': 42,
        'dpb_service_zone': 'us-east-1a',
        'dpb_job_properties': '',
        'dpb_cluster_properties': '',
        'dpb_dynamic_allocation': True,
        'dpb_extra_jars': '',
    }
    self.assertEqual(cluster.GetResourceMetadata(), expected_metadata)

  @mock.patch.object(
      aws_dpb_emr_serverless_prices,
      'EMR_SERVERLESS_PRICES',
      new=_EMR_SERVERLESS_PRICES,
  )
  def testEmrServerlessCalculateLastJobCosts(self):
    emr_serverless = aws_dpb_emr.AwsDpbEmrServerless(SERVERLESS_SPEC)
    self.issue_cmd_mock.side_effect = [
        (json.dumps(_EMR_SERVERLESS_CREATE_APPLICATION_RESPONSE), '', 0),
        (json.dumps(_EMR_SERVERLESS_GET_APPLICATION_RESPONSE), '', 0),
        (json.dumps(_EMR_SERVERLESS_START_JOB_RUN_RESPONSE), '', 0),
        (
            json.dumps(
                _GetJobRunMockPayload(
                    vcpu_hour=59.422,
                    memory_gb_hour=237.689,
                    storage_gb_hour=1901.511,
                )
            ),
            '',
            0,
        ),
    ]
    emr_serverless.SubmitJob(
        pyspark_file='s3://test/hello.py',
        job_type=dpb_constants.PYSPARK_JOB_TYPE,
    )
    expected_costs = dpb_service.JobCosts(
        total_cost=4.711576935499999,
        compute_cost=3.1270233279999995,
        memory_cost=1.3734858865,
        storage_cost=0.21106772099999999,
        compute_units_used=59.422,
        memory_units_used=237.689,
        storage_units_used=1901.511,
        compute_unit_cost=0.052624,
        memory_unit_cost=0.0057785,
        storage_unit_cost=0.000111,
        compute_unit_name='vCPU*hr',
        memory_unit_name='GB*hr',
        storage_unit_name='GB*hr',
    )
    self.assertEqual(emr_serverless.CalculateLastJobCosts(), expected_costs)

  def testEmrServerlessPricesSchema(self):
    # Checking schema of EMR_SERVERLESS_PRICES
    emr_serverless_prices = aws_dpb_emr_serverless_prices.EMR_SERVERLESS_PRICES
    for region, price_dict in emr_serverless_prices.items():
      self.assertIsInstance(region, str)
      self.assertIsInstance(price_dict['vcpu_hours'], float)
      self.assertIsInstance(price_dict['memory_gb_hours'], float)
      self.assertIsInstance(price_dict['storage_gb_hours'], float)

  @freezegun.freeze_time('2024-02-16', auto_tick_seconds=60.0)
  @mock.patch.object(time, 'sleep')
  @mock.patch.object(
      aws_dpb_emr_serverless_prices,
      'EMR_SERVERLESS_PRICES',
      new=_EMR_SERVERLESS_PRICES,
  )
  def testEmrServerlessUsageMetricsNotAvailableRightAway(self, *_):
    emr_serverless = aws_dpb_emr.AwsDpbEmrServerless(SERVERLESS_SPEC)
    self.issue_cmd_mock.side_effect = [
        (json.dumps(_EMR_SERVERLESS_CREATE_APPLICATION_RESPONSE), '', 0),
        (json.dumps(_EMR_SERVERLESS_GET_APPLICATION_RESPONSE), '', 0),
        (json.dumps(_EMR_SERVERLESS_START_JOB_RUN_RESPONSE), '', 0),
        (json.dumps(_GetJobRunMockPayload()), '', 0),
        (
            json.dumps(
                _GetJobRunMockPayload(
                    vcpu_hour=59.422,
                    memory_gb_hour=237.689,
                    storage_gb_hour=1901.511,
                )
            ),
            '',
            0,
        ),
    ]
    emr_serverless.SubmitJob(
        pyspark_file='s3://test/hello.py',
        job_type=dpb_constants.PYSPARK_JOB_TYPE,
    )
    expected_costs = dpb_service.JobCosts(
        total_cost=4.711576935499999,
        compute_cost=3.1270233279999995,
        memory_cost=1.3734858865,
        storage_cost=0.21106772099999999,
        compute_units_used=59.422,
        memory_units_used=237.689,
        storage_units_used=1901.511,
        compute_unit_cost=0.052624,
        memory_unit_cost=0.0057785,
        storage_unit_cost=0.000111,
        compute_unit_name='vCPU*hr',
        memory_unit_name='GB*hr',
        storage_unit_name='GB*hr',
    )
    self.assertEqual(emr_serverless.CalculateLastJobCosts(), expected_costs)

  @freezegun.freeze_time('2024-02-16', auto_tick_seconds=60.0)
  @mock.patch.object(time, 'sleep')
  @mock.patch.object(
      aws_dpb_emr_serverless_prices,
      'EMR_SERVERLESS_PRICES',
      new=_EMR_SERVERLESS_PRICES,
  )
  def testEmrServerlessUsageMetricsNeverAvailable(self, *_):
    emr_serverless = aws_dpb_emr.AwsDpbEmrServerless(SERVERLESS_SPEC)
    self.issue_cmd_mock.side_effect = [
        (json.dumps(_EMR_SERVERLESS_CREATE_APPLICATION_RESPONSE), '', 0),
        (json.dumps(_EMR_SERVERLESS_GET_APPLICATION_RESPONSE), '', 0),
        (json.dumps(_EMR_SERVERLESS_START_JOB_RUN_RESPONSE), '', 0),
    ] + [(json.dumps(_GetJobRunMockPayload()), '', 0)] * 100
    emr_serverless.SubmitJob(
        pyspark_file='s3://test/hello.py',
        job_type=dpb_constants.PYSPARK_JOB_TYPE,
    )
    expected_costs = dpb_service.JobCosts()
    self.assertEqual(emr_serverless.CalculateLastJobCosts(), expected_costs)

  def testEmrServerlessMetadata(self):
    emr_serverless = aws_dpb_emr.AwsDpbEmrServerless(SERVERLESS_SPEC)
    expected_metadata = {
        'dpb_service': 'emr_serverless',
        'dpb_version': 'fake-4.2',
        'dpb_service_version': 'emr_serverless_fake-4.2',
        'dpb_cluster_shape': 'emr-serverless-4',
        'dpb_cluster_size': '4',
        'dpb_hdfs_type': 'default-disk',
        'dpb_memory_per_node': 14,
        'dpb_disk_size': 42,
        'dpb_service_zone': 'us-east-1a',
        'dpb_job_properties': 'spark.dynamicAllocation.enabled=FALSE,spark.executor.cores=4,spark.driver.cores=4,spark.executor.memory=14G,spark.executor.instances=4,spark.emr-serverless.driver.disk=42G,spark.emr-serverless.executor.disk=42G',
    }
    self.assertEqual(emr_serverless.GetResourceMetadata(), expected_metadata)


if __name__ == '__main__':
  unittest.main()
