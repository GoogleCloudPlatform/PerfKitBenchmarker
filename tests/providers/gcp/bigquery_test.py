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
"""Tests for perfkitbenchmarker.providers.gcp.bigquery."""

import json
import unittest
from unittest import mock
from absl import flags
from absl.testing import parameterized
from perfkitbenchmarker.providers.gcp import bigquery
from tests import pkb_common_test_case

PACKAGE_NAME = 'PACKAGE_NAME'
DATASET_ID = 'DATASET_ID'
PROJECT_ID = 'PROJECT_ID'
QUERY_NAME = 'QUERY_NAME'
_TEST_RUN_URI = 'fakeru'
_GCP_ZONE_US_CENTRAL_1_C = 'us-central1-c'

_BASE_BIGQUERY_SPEC = {
    'type': 'bigquery',
    'cluster_identifier': 'bigquerypkb.tpcds_100G',
}

FLAGS = flags.FLAGS


EDW_SERVICE_SPEC = mock.Mock(
    snapshot=None,
    concurrency=5,
    node_type=None,
    node_count=1,
    endpoint=None,
    db=None,
    user=None,
    password=None,
    type='bqfederated',
    cluster_identifier='proj.dataset',
)


class FakeRemoteVM:

  def Install(self, package_name):
    if package_name != 'google_cloud_sdk':
      raise RuntimeError


class FakeRemoteVMForCliClientInterfacePrepare:
  """Class to setup a Fake VM that prepares a Client VM (CLI Client)."""

  def __init__(self):
    self.valid_install_package_list = ['pip', 'google_cloud_sdk']
    self.valid_remote_command_list = [
        'sudo pip install absl-py',
        (
            '/tmp/pkb/google-cloud-sdk/bin/gcloud auth activate-service-account'
            ' SERVICE_ACCOUNT --key-file=SERVICE_ACCOUNT_KEY_FILE'
        ),
        'chmod 755 script_runner.sh',
        'echo "\nMaxSessions 100" | sudo tee -a /etc/ssh/sshd_config',
    ]

  def Install(self, package_name):
    if package_name not in self.valid_install_package_list:
      raise RuntimeError

  def RemoteCommand(self, command):
    if command not in self.valid_remote_command_list:
      raise RuntimeError

  def InstallPreprovisionedPackageData(
      self, package_name, filenames, install_path
  ):
    if package_name != 'PACKAGE_NAME':
      raise RuntimeError

  def PushFile(self, source_path):
    pass


class FakeRemoteVMForCliClientInterfaceExecuteQuery:
  """Class to setup a Fake VM that executes script on Client VM (CLI Client)."""

  def RemoteCommand(self, command):
    if command == 'echo "\nMaxSessions 100" | sudo tee -a /etc/ssh/sshd_config':
      return None, None

    expected_command = (
        'python script_driver.py --script={} --bq_project_id={}'
        ' --bq_dataset_id={}'
    ).format(QUERY_NAME, PROJECT_ID, DATASET_ID)
    if command != expected_command:
      raise RuntimeError
    response_object = {QUERY_NAME: {'job_id': 'JOB_ID', 'execution_time': 1.0}}
    response = json.dumps(response_object)
    return response, None


class FakeRemoteVMForJavaClientInterfacePrepare:
  """Class to setup a Fake VM that prepares a Client VM (JAVA Client)."""

  def __init__(self):
    self.valid_install_package_list = ['openjdk']

  def Install(self, package_name):
    if package_name != 'openjdk':
      raise RuntimeError

  def RemoteCommand(self, command):
    if command == 'echo "\nMaxSessions 100" | sudo tee -a /etc/ssh/sshd_config':
      return None, None
    else:
      raise RuntimeError

  def InstallPreprovisionedPackageData(
      self, package_name, filenames, install_path
  ):
    if package_name != 'PACKAGE_NAME':
      raise RuntimeError


class FakeRemoteVMForJavaClientInterfaceExecuteQuery:
  """Class to setup a Fake VM that executes script on Client VM (JAVA Client)."""

  def RemoteCommand(self, command):
    if command == 'echo "\nMaxSessions 100" | sudo tee -a /etc/ssh/sshd_config':
      return None, None

    expected_command = (
        'java -Xmx6g -cp bq-jdbc-simba-client-1.8-temp-labels.jar '
        'com.google.cloud.performance.edw.Single --project {} '
        '--credentials_file {} --dataset {} --query_file '
        '{}'
    ).format(PROJECT_ID, 'SERVICE_ACCOUNT_KEY_FILE', DATASET_ID, QUERY_NAME)
    if command != expected_command:
      raise RuntimeError
    response_object = {
        'query_wall_time_in_secs': 1.0,
        'details': {'job_id': 'JOB_ID'},
    }
    response = json.dumps(response_object)
    return response, None


class FakeBenchmarkSpec:
  """Fake BenchmarkSpec to use for setting client interface attributes."""

  def __init__(self, client_vm):
    self.name = PACKAGE_NAME
    self.vms = [client_vm]


class BigqueryTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.cloud = 'GCP'
    FLAGS.run_uri = _TEST_RUN_URI
    FLAGS.zones = [_GCP_ZONE_US_CENTRAL_1_C]

  def testGetBigQueryClientInterfaceGeneric(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertEqual(interface.project_id, PROJECT_ID)
    self.assertEqual(interface.dataset_id, DATASET_ID)

  def testGetBigQueryClientInterfaceCli(self):
    FLAGS.bq_client_interface = 'CLI'
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.CliClientInterface)

  def testGetBigQueryClientInterfaceJava(self):
    FLAGS.bq_client_interface = 'JAVA'
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)

  def testGenericClientInterfaceGetMetada(self):
    FLAGS.bq_client_interface = 'CLI'
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertDictEqual(interface.GetMetadata(), {'client': 'CLI'})
    FLAGS.bq_client_interface = 'JAVA'
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertDictEqual(interface.GetMetadata(), {'client': 'JAVA'})

  def testCliClientInterfacePrepare(self):
    FLAGS.bq_client_interface = 'CLI'
    FLAGS.gcp_service_account_key_file = 'SERVICE_ACCOUNT_KEY_FILE'
    FLAGS.gcp_service_account = 'SERVICE_ACCOUNT'
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.CliClientInterface)
    bm_spec = FakeBenchmarkSpec(FakeRemoteVMForCliClientInterfacePrepare())
    interface.SetProvisionedAttributes(bm_spec)
    interface.Prepare(PACKAGE_NAME)

  def testCliClientInterfaceExecuteQuery(self):
    FLAGS.bq_client_interface = 'CLI'
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.CliClientInterface)
    bm_spec = FakeBenchmarkSpec(FakeRemoteVMForCliClientInterfaceExecuteQuery())
    interface.SetProvisionedAttributes(bm_spec)
    performance, details = interface.ExecuteQuery(QUERY_NAME)
    self.assertEqual(performance, 1.0)
    self.assertDictEqual(details, {'client': 'CLI', 'job_id': 'JOB_ID'})

  def testJavaClientInterfacePrepare(self):
    FLAGS.bq_client_interface = 'JAVA'
    FLAGS.gcp_service_account_key_file = 'SERVICE_ACCOUNT_KEY_FILE'
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)
    bm_spec = FakeBenchmarkSpec(FakeRemoteVMForJavaClientInterfacePrepare())
    interface.SetProvisionedAttributes(bm_spec)
    interface.Prepare(PACKAGE_NAME)

  def testJavaClientInterfaceExecuteQuery(self):
    FLAGS.bq_client_interface = 'JAVA'
    FLAGS.gcp_service_account_key_file = 'SERVICE_ACCOUNT_KEY_FILE'

    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)

    bm_spec = FakeBenchmarkSpec(
        FakeRemoteVMForJavaClientInterfaceExecuteQuery()
    )
    interface.SetProvisionedAttributes(bm_spec)
    performance, details = interface.ExecuteQuery(QUERY_NAME)
    self.assertEqual(performance, 1.0)
    self.assertDictEqual(details, {'client': 'JAVA', 'job_id': 'JOB_ID'})

  @parameterized.named_parameters(
      dict(
          testcase_name='NoLocationNoTableFormat',
          cluster_identifier=(
              'mybqfederated.tpcds1000_parquet_compressed_partitioned_gcs'
          ),
          expected_fields={
              'format': 'parquet',
              'table_format': 'None',
              'compression': 'compressed',
              'partitioning': 'partitioned',
              'storage': 'gcs',
              'location': 'us',
          },
      ),
      dict(
          testcase_name='NoTableFormat',
          cluster_identifier=(
              'mybqfederated.tpcds1000_parquet_snappy_part_gcs_uscentral1'
          ),
          expected_fields={
              'format': 'parquet',
              'table_format': 'None',
              'compression': 'snappy',
              'partitioning': 'part',
              'storage': 'gcs',
              'location': 'uscentral1',
          },
      ),
      dict(
          testcase_name='WithTableFormat',
          cluster_identifier=(
              'mybqfederated.tpcds1000_parquet_iceberg_snappy_part_gcs_us'
          ),
          expected_fields={
              'format': 'parquet',
              'table_format': 'iceberg',
              'compression': 'snappy',
              'partitioning': 'part',
              'storage': 'gcs',
              'location': 'us',
          },
      ),
      dict(
          testcase_name='UnparseableClusterId',
          cluster_identifier='mybqfederated.yolo',
          expected_fields={
              'format': 'unknown',
              'table_format': 'unknown',
              'compression': 'unknown',
              'partitioning': 'unknown',
              'storage': 'unknown',
              'location': 'unknown',
          },
      ),
  )
  def testBqFederatedGetDataDetail(
      self,
      cluster_identifier: str,
      expected_fields: dict[str, str],
  ):
    EDW_SERVICE_SPEC.cluster_identifier = cluster_identifier
    edw = bigquery.Bqfederated(EDW_SERVICE_SPEC)
    data_details = edw.GetDataDetails()
    self.assertEqual(data_details, data_details | expected_fields)


if __name__ == '__main__':
  unittest.main()
