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
"""Tests for perfkitbenchmarker.providers.gcp.gcp_dpb_dataproc."""

import unittest
from absl import flags
import mock
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataproc
from perfkitbenchmarker.providers.gcp import gcs
from tests import pkb_common_test_case

TEST_RUN_URI = 'fakeru'
GCP_ZONE_US_CENTRAL1_A = 'us-central1-a'
GCP_REGION = 'us-central1'
BUCKET_NAME = 'foo'
PROJECT = 'fake-project'
STAGING_BUCKET = 'fake-bucket'
SERVERLESS_MOCK_BATCH = """
{
    "state": "SUCCEEDED",
    "stateHistory": [
        {
            "state": "PENDING",
            "stateStartTime": "2022-02-03T02:24:38.810357Z"
        },
        {
            "state": "RUNNING",
            "stateStartTime": "2022-02-03T02:25:51.092538Z"
        }
    ],
    "stateTime": "2022-02-03T02:27:04.152374Z"
}
"""

FLAGS = flags.FLAGS

CLUSTER_SPEC = mock.Mock(
    static_dpb_service_instance=None,
    worker_count=2,
    version='fake-version',
    applications=['foo-component', 'bar-component'],
    worker_group=mock.Mock(
        vm_spec=mock.Mock(machine_type='fake-machine-type', num_local_ssds=2),
        disk_spec=mock.Mock(disk_type='pd-ssd', disk_size=42),
    ),
)

DPGKE_CLUSTER_SPEC = mock.Mock(
    static_dpb_service_instance=None,
    gke_cluster_name='gke-cluster',
    gke_cluster_location='gke-cluster-loc',
    version='preview-0.3',
    gke_cluster_nodepools='name:pool-name,role:driver,min:3',
)

SERVERLESS_SPEC = mock.Mock(
    static_dpb_service_instance=None,
    version='fake-4.2',
    dataproc_serverless_core_count=4,
    dataproc_serverless_initial_executors=4,
    dataproc_serverless_min_executors=2,
    dataproc_serverless_max_executors=10,
    dataproc_serverless_memory=10000,
    dataproc_serverless_memory_overhead=4000,
    worker_group=mock.Mock(
        disk_spec=mock.Mock(
            disk_size=42,
        ),
    ),
)


class LocalGcpDpbDataproc(gcp_dpb_dataproc.GcpDpbDataproc):

  def __init__(self):
    # Bypass GCS initialization in Dataproc's constructor
    dpb_service.BaseDpbService.__init__(self, CLUSTER_SPEC)
    self.project = PROJECT
    self.region = self.dpb_service_zone.rsplit('-', 1)[0]
    self.storage_service = gcs.GoogleCloudStorageService()
    self.storage_service.PrepareService(location=self.region)


class GcpDpbDataprocTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcpDpbDataprocTestCase, self).setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = GCP_ZONE_US_CENTRAL1_A
    FLAGS.zones = [GCP_ZONE_US_CENTRAL1_A]

  @mock.patch.object(
      vm_util, 'IssueCommand', return_value=('fake_stdout', 'fake_stderr', 0)
  )
  def testCreate(self, mock_issue):
    cluster = LocalGcpDpbDataproc()
    cluster._Create()
    self.assertEqual(mock_issue.call_count, 1)
    command_string = ' '.join(mock_issue.call_args[0][0])
    self.assertIn('gcloud dataproc clusters create pkb-fakeru', command_string)
    self.assertIn('--image-version fake-version', command_string)
    self.assertIn('--master-boot-disk-size 42GB', command_string)
    self.assertIn('--master-boot-disk-type pd-ssd', command_string)
    self.assertIn('--master-machine-type fake-machine-type', command_string)
    self.assertIn('--num-master-local-ssds 2', command_string)
    self.assertIn('--worker-boot-disk-size 42GB', command_string)
    self.assertIn('--worker-boot-disk-type pd-ssd', command_string)
    self.assertIn('--worker-machine-type fake-machine-type', command_string)
    self.assertIn('--num-worker-local-ssds 2', command_string)
    self.assertIn('--num-workers 2', command_string)
    self.assertIn(
        '--optional-components foo-component,bar-component', command_string
    )
    self.assertIn('--project fake-project ', command_string)
    self.assertIn('--region us-central1', command_string)
    self.assertIn('--zone us-central1-a', command_string)

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      return_value=(
          'fake_stdout',
          (
              "The zone 'projects/fake-project/zones/us-central1-a' "
              'does not have enough resources available to fulfill the request.'
          ),
          1,
      ),
  )
  def testCreateResourceExhausted(self, mock_issue):
    cluster = LocalGcpDpbDataproc()
    with self.assertRaises(errors.Benchmarks.InsufficientCapacityCloudFailure):
      cluster._Create()
    self.assertEqual(mock_issue.call_count, 1)


class LocalGcpDpbDPGKE(gcp_dpb_dataproc.GcpDpbDpgke):

  def __init__(self, spec=DPGKE_CLUSTER_SPEC):
    # Bypass GCS initialization in Dataproc's constructor
    gcp_dpb_dataproc.GcpDpbDpgke.__init__(self, spec)
    self.project = PROJECT
    self.region = GCP_REGION


class GcpDpbDPGKETestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcpDpbDPGKETestCase, self).setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = GCP_ZONE_US_CENTRAL1_A
    FLAGS.dpb_service_bucket = STAGING_BUCKET

  @mock.patch.object(
      vm_util, 'IssueCommand', return_value=('fake_stdout', 'fake_stderr', 0)
  )
  def testCreate(self, mock_issue):
    cluster = LocalGcpDpbDPGKE()
    cluster._Create()
    self.assertEqual(mock_issue.call_count, 1)
    command_string = ' '.join(mock_issue.call_args[0][0])
    self.assertIn(
        'gcloud alpha dataproc clusters gke create pkb-fakeru', command_string
    )
    self.assertIn('--gke-cluster gke-cluster ', command_string)
    self.assertIn('--namespace pkb-fakeru ', command_string)
    self.assertIn('--gke-cluster-location gke-cluster-loc ', command_string)
    self.assertIn('--pools name=pool-name,role=driver,min=3 ', command_string)
    self.assertIn('--project fake-project ', command_string)
    self.assertIn('--region us-central1 ', command_string)
    self.assertIn('--image-version preview-0.3 ', command_string)
    self.assertIn('--staging-bucket fake-bucket', command_string)

  def testMissingAttrs(self):
    cluster_spec = mock.Mock(
        spec=[
            'version',
        ],
        static_dpb_service_instance=None,
        gke_cluster_nodepools='',
    )
    with self.assertRaises(errors.Setup.InvalidSetupError) as ex:
      LocalGcpDpbDPGKE(spec=cluster_spec)
    self.assertIn(
        "['gke_cluster_name', 'gke_cluster_nodepools', 'gke_cluster_location']"
        ' must be provided for provisioning DPGKE.',
        str(ex.exception),
    )


class GcpDpbDataprocServerlessTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = TEST_RUN_URI
    FLAGS.dpb_service_zone = GCP_ZONE_US_CENTRAL1_A

  @mock.patch.object(
      vm_util, 'IssueCommand', return_value=(SERVERLESS_MOCK_BATCH, '', 0)
  )
  def testSubmitJob(self, mock_issue):
    service = gcp_dpb_dataproc.GcpDpbDataprocServerless(SERVERLESS_SPEC)
    result = service.SubmitJob(
        pyspark_file=(
            'gs://pkb-fab5770b/spark_sql_test_scripts/spark_sql_runner.py'
        ),
        job_arguments=[
            '--sql-scripts',
            'gs://pkb-fab5770b/2.sql',
            '--report-dir',
            'gs://pkb-fab5770b/report-1643853399069',
            '--table-metadata',
            'gs://pkb-fab5770b/metadata.json',
        ],
        job_jars=[],
        job_type='pyspark',
    )
    self.assertEqual(result.run_time, 73.059836)
    self.assertEqual(result.pending_time, 72.282181)
    self.assertEqual(mock_issue.call_count, 2)
    mock_issue.assert_has_calls([
        mock.call(
            [
                'gcloud',
                'dataproc',
                'batches',
                'submit',
                'pyspark',
                'gs://pkb-fab5770b/spark_sql_test_scripts/spark_sql_runner.py',
                '--batch',
                'pkb-fakeru-0',
                '--format',
                'json',
                '--labels',
                '',
                '--properties',
                (
                    '^@^spark.executor.cores=4@'
                    'spark.driver.cores=4@'
                    'spark.executor.instances=4@'
                    'spark.dynamicAllocation.minExecutors=2@'
                    'spark.dynamicAllocation.maxExecutors=10@'
                    'spark.dataproc.driver.disk.size=42g@'
                    'spark.dataproc.executor.disk.size=42g@'
                    'spark.driver.memory=10000m@'
                    'spark.executor.memory=10000m@'
                    'spark.driver.memoryOverhead=4000m@'
                    'spark.executor.memoryOverhead=4000m'
                ),
                '--quiet',
                '--region',
                'us-central1',
                '--version',
                'fake-4.2',
                '--',
                '--sql-scripts',
                'gs://pkb-fab5770b/2.sql',
                '--report-dir',
                'gs://pkb-fab5770b/report-1643853399069',
                '--table-metadata',
                'gs://pkb-fab5770b/metadata.json',
            ],
            raise_on_failure=False,
            timeout=None,
        ),
        mock.call(
            [
                'gcloud',
                'dataproc',
                'batches',
                'describe',
                'pkb-fakeru-0',
                '--format',
                'json',
                '--quiet',
                '--region',
                'us-central1',
            ],
            raise_on_failure=False,
            timeout=None,
        ),
    ])


if __name__ == '__main__':
  unittest.main()
