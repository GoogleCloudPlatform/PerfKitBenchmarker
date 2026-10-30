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
from typing import Any
import unittest
from unittest import mock
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import edw_ai_extract_lib
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import bigquery
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from tests import pkb_common_test_case

PACKAGE_NAME = 'PACKAGE_NAME'
DATASET_ID = 'DATASET_ID'
PROJECT_ID = 'PROJECT_ID'
QUERY_NAME = 'QUERY_NAME'
_TEST_RUN_URI = 'fakeru'
_GCP_ZONE_US_CENTRAL_1_C = 'us-central1-c'
QUERY_STREAMS = [['QUERY_1', 'QUERY_2'], ['QUERY_2', 'QUERY_3']]
THROUGHPUT_LABELS = {
    'run_uri': _TEST_RUN_URI,
    'type': 'tpt',
    'iteration': '1',
    'flavor': 'tpcds',
    'scale': '1',
    'minimal_run_key': f'pkb-{_TEST_RUN_URI}-1',
}

THROUGHPUT_RESPONSE_OBJECT = {
    'throughput_start': 1,
    'throughput_end': 10,
    'throughput_wall_time_in_secs': 2.0,
    'all_streams_performance_array': [
        {
            'stream_start': 1,
            'stream_end': 10,
            'stream_wall_time_in_secs': 2.0,
            'stream_performance_array': [
                {
                    'query_wall_time_in_secs': 1.0,
                    'query_end': 5,
                    'query': 'QUERY_1',
                    'query_start': 1,
                    'details': {'job_id': 'JOB_ID_1'},
                },
                {
                    'query_wall_time_in_secs': 1.0,
                    'query_end': 10,
                    'query': 'QUERY_2',
                    'query_start': 5,
                    'details': {'job_id': 'JOB_ID_2'},
                },
            ],
        },
        {
            'stream_start': 2,
            'stream_end': 10,
            'stream_wall_time_in_secs': 2.0,
            'stream_performance_array': [
                {
                    'query_wall_time_in_secs': 1.0,
                    'query_end': 5,
                    'query': 'QUERY_2',
                    'query_start': 1,
                    'details': {'job_id': 'JOB_ID_3'},
                },
                {
                    'query_wall_time_in_secs': 1.0,
                    'query_end': 10,
                    'query': 'QUERY_3',
                    'query_start': 5,
                    'details': {'job_id': 'JOB_ID_4'},
                },
            ],
        },
    ],
}

_BASE_BIGQUERY_SPEC = {
    'type': 'bigquery',
    'cluster_identifier': 'bigquerypkb.tpcds_100G',
}


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


class FakeRemoteVMForJavaClientInterfaceExecuteThroughput:
  """Class to setup a Fake VM that executes script on Client VM (JAVA Client)."""

  def RemoteCommand(self, command):
    if command == 'echo "\nMaxSessions 100" | sudo tee -a /etc/ssh/sshd_config':
      return None, None

    expected_command = (
        'java -Xmx6g -cp bq-jdbc-simba-client-1.8-temp-labels.jar '
        'com.google.cloud.performance.edw.Throughput --project {} '
        '--credentials_file {} --dataset {} --query_streams {}'
    ).format(
        PROJECT_ID,
        'SERVICE_ACCOUNT_KEY_FILE',
        DATASET_ID,
        ' '.join([','.join(stream) for stream in QUERY_STREAMS]),
    )
    if command != expected_command:
      raise RuntimeError
    response_object = THROUGHPUT_RESPONSE_OBJECT
    response = json.dumps(response_object)
    return response, None


class FakeRemoteVMForJavaClientInterfaceExecuteThroughputWithLabels:
  """Class to setup a Fake VM that executes script on Client VM (JAVA Client)."""

  def RemoteCommand(self, command):
    if command == 'echo "\nMaxSessions 100" | sudo tee -a /etc/ssh/sshd_config':
      return None, None

    expected_command = (
        'java -Xmx6g -cp bq-jdbc-simba-client-1.8-temp-labels.jar '
        'com.google.cloud.performance.edw.Throughput --project {} '
        '--credentials_file {} --dataset {} --query_streams {}'
    ).format(
        PROJECT_ID,
        'SERVICE_ACCOUNT_KEY_FILE',
        DATASET_ID,
        ' '.join([','.join(stream) for stream in QUERY_STREAMS]),
    ) + ''.join(
        map(lambda x: f' --label {x[0]}={x[1]}', THROUGHPUT_LABELS.items())
    )

    if command != expected_command:
      raise RuntimeError
    response_object = THROUGHPUT_RESPONSE_OBJECT
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
    self.enter_context(
        flagsaver.flagsaver(
            cloud='GCP',
            run_uri=_TEST_RUN_URI,
            zones=[_GCP_ZONE_US_CENTRAL_1_C],
        )
    )

  def testGetBigQueryClientInterfaceGeneric(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertEqual(interface.project_id, PROJECT_ID)
    self.assertEqual(interface.dataset_id, DATASET_ID)

  @flagsaver.flagsaver((gcp_flags.BQ_CLIENT_INTERFACE, 'CLI'))
  def testGetBigQueryClientInterfaceCli(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.CliClientInterface)

  @flagsaver.flagsaver((gcp_flags.BQ_CLIENT_INTERFACE, 'JAVA'))
  def testGetBigQueryClientInterfaceJava(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)

  def testGenericClientInterfaceGetMetada(self):
    with flagsaver.flagsaver((gcp_flags.BQ_CLIENT_INTERFACE, 'CLI')):
      interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
      self.assertDictEqual(interface.GetMetadata(), {'client': 'CLI'})
    with flagsaver.flagsaver((gcp_flags.BQ_CLIENT_INTERFACE, 'JAVA')):
      interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
      self.assertDictEqual(interface.GetMetadata(), {'client': 'JAVA'})

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'CLI'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'SERVICE_ACCOUNT_KEY_FILE'),
      (gcp_flags.GCP_SERVICE_ACCOUNT, 'SERVICE_ACCOUNT'),
  )
  def testCliClientInterfacePrepare(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.CliClientInterface)
    bm_spec = FakeBenchmarkSpec(FakeRemoteVMForCliClientInterfacePrepare())
    interface.SetProvisionedAttributes(bm_spec)
    interface.Prepare(PACKAGE_NAME)

  @flagsaver.flagsaver((gcp_flags.BQ_CLIENT_INTERFACE, 'CLI'))
  def testCliClientInterfaceExecuteQuery(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.CliClientInterface)
    bm_spec = FakeBenchmarkSpec(FakeRemoteVMForCliClientInterfaceExecuteQuery())
    interface.SetProvisionedAttributes(bm_spec)
    performance, details = interface.ExecuteQuery(QUERY_NAME)
    self.assertEqual(performance, 1.0)
    self.assertDictEqual(details, {'client': 'CLI', 'job_id': 'JOB_ID'})

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'JAVA'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'SERVICE_ACCOUNT_KEY_FILE'),
  )
  def testJavaClientInterfacePrepare(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)
    bm_spec = FakeBenchmarkSpec(FakeRemoteVMForJavaClientInterfacePrepare())
    interface.SetProvisionedAttributes(bm_spec)
    interface.Prepare(PACKAGE_NAME)

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'JAVA'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'SERVICE_ACCOUNT_KEY_FILE'),
  )
  def testJavaClientInterfaceExecuteQuery(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)

    bm_spec = FakeBenchmarkSpec(
        FakeRemoteVMForJavaClientInterfaceExecuteQuery()
    )
    interface.SetProvisionedAttributes(bm_spec)
    performance, details = interface.ExecuteQuery(QUERY_NAME)
    self.assertEqual(performance, 1.0)
    self.assertDictEqual(details, {'client': 'JAVA', 'job_id': 'JOB_ID'})

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'JAVA'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'SERVICE_ACCOUNT_KEY_FILE'),
  )
  def testJavaClientInterfaceExecuteThroughputWithoutLabels(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)

    bm_spec = FakeBenchmarkSpec(
        FakeRemoteVMForJavaClientInterfaceExecuteThroughput()
    )
    interface.SetProvisionedAttributes(bm_spec)
    response = interface.ExecuteThroughput(QUERY_STREAMS)
    self.assertDictEqual(json.loads(response), THROUGHPUT_RESPONSE_OBJECT)

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'JAVA'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'SERVICE_ACCOUNT_KEY_FILE'),
  )
  def testJavaClientInterfaceExecuteThroughputWithLabels(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.JavaClientInterface)

    bm_spec = FakeBenchmarkSpec(
        FakeRemoteVMForJavaClientInterfaceExecuteThroughputWithLabels()
    )
    interface.SetProvisionedAttributes(bm_spec)
    response = interface.ExecuteThroughput(QUERY_STREAMS, THROUGHPUT_LABELS)
    self.assertDictEqual(json.loads(response), THROUGHPUT_RESPONSE_OBJECT)

  @parameterized.named_parameters(
      dict(
          testcase_name='PrefersActivePhysicalBytes',
          bq_show_json={
              'numActivePhysicalBytes': str(34 * 1024 * 1024 * 1024),
              'numTotalPhysicalBytes': str(364 * 1024 * 1024 * 1024),
              'numBytes': str(944 * 1024 * 1024 * 1024),
              'numRows': '1000000000',
          },
          expected_size=34.0,
          expected_rows=1000000000,
      ),
      dict(
          testcase_name='FallsBackToTotalPhysicalBytesWhenActiveAbsent',
          bq_show_json={
              'numTotalPhysicalBytes': str(10 * 1024 * 1024 * 1024),
              'numBytes': str(50 * 1024 * 1024 * 1024),
              'numRows': '100',
          },
          expected_size=10.0,
          expected_rows=100,
      ),
      dict(
          testcase_name='FallsBackToTotalPhysicalBytesWhenActiveIsNone',
          bq_show_json={
              'numActivePhysicalBytes': None,
              'numTotalPhysicalBytes': str(10 * 1024 * 1024 * 1024),
              'numRows': '100',
          },
          expected_size=10.0,
          expected_rows=100,
      ),
      dict(
          testcase_name='ZeroActivePhysicalBytesDoesNotFallBackToTotal',
          bq_show_json={
              'numActivePhysicalBytes': '0',
              'numTotalPhysicalBytes': str(10 * 1024 * 1024 * 1024),
              'numRows': '0',
          },
          expected_size=0.0,
          expected_rows=0,
      ),
      dict(
          testcase_name='FallsBackToNumBytesWhenPhysicalBytesAbsent',
          bq_show_json={
              'numBytes': str(50 * 1024 * 1024 * 1024),
              'numRows': '100',
          },
          expected_size=50.0,
          expected_rows=100,
      ),
  )
  def testGenericClientInterfaceGetTableStats(
      self, bq_show_json, expected_size, expected_rows
  ):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.MockIssueCommand({'bq show': [(json.dumps(bq_show_json), '', 0)]})

    size, rows = interface.GetTableStats('hits')
    self.assertEqual(size, expected_size)
    self.assertEqual(rows, expected_rows)

  @parameterized.named_parameters(
      dict(testcase_name='EmptyResponse', bq_show_json={}),
      dict(
          testcase_name='MissingNumRows',
          bq_show_json={'numActivePhysicalBytes': '1024'},
      ),
      dict(
          testcase_name='MissingAllBytes',
          bq_show_json={'numRows': '100'},
      ),
  )
  def testGenericClientInterfaceGetTableStatsRaisesErrorWhenMissing(
      self, bq_show_json
  ):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.MockIssueCommand(
        {
            'bq show': [
                (json.dumps({}), '', 0),
            ]
        }
    )
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)

    with self.assertRaises(ValueError):
      interface.GetTableStats('hits')

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

  def testSplitClusterIdentifier(self):
    self.assertEqual(
        bigquery._SplitClusterIdentifier('project.dataset'),
        ('project', 'dataset'),
    )
    self.assertEqual(
        bigquery._SplitClusterIdentifier('catalog.namespace.dataset'),
        ('catalog', 'namespace.dataset'),
    )

  @parameterized.named_parameters(
      (
          'missing_text_response',
          {
              'query_wall_time_in_secs': 5.0,
              'details': {
                  'query_results': {
                      'generated_sql': 'SELECT * FROM foo',
                      'retrieved_data': [['a', 'b']],
                  }
              },
          },
          (
              "Conversational Analytics query failed: 'text_response' is"
              ' missing or empty. Got: None'
          ),
      ),
      (
          'empty_text_response',
          {
              'query_wall_time_in_secs': 5.0,
              'details': {
                  'query_results': {
                      'text_response': '',
                      'generated_sql': 'SELECT * FROM foo',
                      'retrieved_data': [['a', 'b']],
                  }
              },
          },
          (
              "Conversational Analytics query failed: 'text_response' is"
              " missing or empty. Got: ''"
          ),
      ),
      (
          'missing_generated_sql',
          {
              'query_wall_time_in_secs': 5.0,
              'details': {
                  'query_results': {
                      'text_response': 'Fake answer',
                      'retrieved_data': [['a', 'b']],
                  }
              },
          },
          (
              "Conversational Analytics query failed: 'generated_sql' is"
              ' missing or empty. Got: None'
          ),
      ),
      (
          'empty_generated_sql',
          {
              'query_wall_time_in_secs': 5.0,
              'details': {
                  'query_results': {
                      'text_response': 'Fake answer',
                      'generated_sql': '',
                      'retrieved_data': [['a', 'b']],
                  }
              },
          },
          (
              "Conversational Analytics query failed: 'generated_sql' is"
              " missing or empty. Got: ''"
          ),
      ),
      (
          'missing_retrieved_data',
          {
              'query_wall_time_in_secs': 5.0,
              'details': {
                  'query_results': {
                      'text_response': 'Fake answer',
                      'generated_sql': 'SELECT * FROM foo',
                  }
              },
          },
          (
              "Conversational Analytics query failed: 'retrieved_data' is"
              ' missing or empty. Got: None'
          ),
      ),
      (
          'empty_retrieved_data',
          {
              'query_wall_time_in_secs': 5.0,
              'details': {
                  'query_results': {
                      'text_response': 'Fake answer',
                      'generated_sql': 'SELECT * FROM foo',
                      'retrieved_data': [],
                  }
              },
          },
          (
              "Conversational Analytics query failed: 'retrieved_data' is"
              ' missing or empty. Got: []'
          ),
      ),
  )
  @flagsaver.flagsaver(
      (
          gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE,
          '/path/to/SERVICE_ACCOUNT_KEY_FILE',
      ),
      (bigquery.BQ_CA_DATA_AGENT, 'AGENT_ID'),
  )
  def testConversationalAnalyticsClientInterfaceExecuteQueryValidationErrors(
      self, response_dict, expected_error_msg
  ):
    interface = bigquery.ConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)

    mock_vm.RemoteCommand.side_effect = [
        (None, None),  # For directory check in CreateRemoteFile
        (json.dumps(response_dict), None),  # For executing query
    ]

    execution_time, metadata = interface.ExecuteQuery(QUERY_NAME)
    self.assertEqual(execution_time, -1.0)
    self.assertEqual(metadata['error'], expected_error_msg)

    # Assertions
    expected_query_file = interface._GetQueryFileName(QUERY_NAME)
    mock_vm.RemoteCommand.assert_has_calls([
        mock.call('[ -d . ] || mkdir -p .'),
        mock.call(
            'source .venv/bin/activate && python3 bq_ca_driver.py single '
            f'--project={PROJECT_ID} --agent=AGENT_ID '
            '--credentials_file=SERVICE_ACCOUNT_KEY_FILE --print_results '
            f'--query_file={expected_query_file}'
        ),
    ])
    mock_vm.PushFile.assert_called_once_with(mock.ANY, expected_query_file)

  @flagsaver.flagsaver((
      gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE,
      '/path/to/SERVICE_ACCOUNT_KEY_FILE',
  ))
  def testGetQueryFileName(self):
    interface = bigquery.ConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    query = 'What is the total sales in 2023?'
    filename = interface._GetQueryFileName(query)
    self.assertTrue(filename.startswith('./What_is_the_total_sales_in_202'))
    self.assertTrue(filename.endswith('.txt'))
    # Length should be: 2 (for ./) + 30 (for sanitized prefix) + 1 (for _) + 8
    # (for hash) + 4 (for .txt) = 45
    self.assertLen(filename, 45)

  @flagsaver.flagsaver((
      gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE,
      '/path/to/SERVICE_ACCOUNT_KEY_FILE',
  ))
  def testParseConversationalAnalyticsResultsSuccess(self):
    interface = bigquery.ConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    results = {
        'query_wall_time_in_secs': 5.0,
        'details': {
            'job_id': 'job_123',
            'query_results': {
                'text_response': 'The total sales is $1000.',
                'generated_sql': 'SELECT sum(sales) FROM t',
                'retrieved_data': [['1000']],
                'thoughts': ['thought 1'],
                'progress_messages': ['progress 1'],
                'time_to_first_token_secs': 1.0,
                'total_stream_time_secs': 4.0,
            },
        },
    }
    execution_time, metadata = interface._ParseConversationalAnalyticsResults(
        results, 'test query'
    )
    self.assertEqual(execution_time, 5.0)
    self.assertEqual(metadata['question'], 'test query')
    self.assertEqual(metadata['text_response'], 'The total sales is $1000.')
    self.assertEqual(metadata['generated_sql'], 'SELECT sum(sales) FROM t')
    self.assertEqual(metadata['predict_data'], [['1000']])
    self.assertEqual(metadata['thoughts'], ['thought 1'])
    self.assertEqual(metadata['progress_messages'], ['progress 1'])
    self.assertEqual(metadata['time_to_first_token_secs'], 1.0)
    self.assertEqual(metadata['total_stream_time_secs'], 4.0)
    self.assertEqual(metadata['job_id'], 'job_123')
    self.assertNotIn('error', metadata)

  @flagsaver.flagsaver(
      (
          gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE,
          '/path/to/SERVICE_ACCOUNT_KEY_FILE',
      ),
      (bigquery.BQ_CA_DATA_AGENT, 'AGENT_ID'),
  )
  def testConversationalAnalyticsClientInterfaceExecuteQuerySuccess(self):
    interface = bigquery.ConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)

    response_dict = {
        'query_wall_time_in_secs': 5.0,
        'details': {
            'job_id': 'job_123',
            'query_results': {
                'text_response': 'The total sales is $1000.',
                'generated_sql': 'SELECT sum(sales) FROM t',
                'retrieved_data': [['1000']],
            },
        },
    }

    mock_vm.RemoteCommand.side_effect = [
        (None, None),  # For directory check in CreateRemoteFile
        (json.dumps(response_dict), None),  # For executing query
    ]

    execution_time, metadata = interface.ExecuteQuery(QUERY_NAME)
    self.assertEqual(execution_time, 5.0)
    self.assertEqual(metadata['text_response'], 'The total sales is $1000.')

    expected_query_file = interface._GetQueryFileName(QUERY_NAME)
    mock_vm.RemoteCommand.assert_has_calls([
        mock.call('[ -d . ] || mkdir -p .'),
        mock.call(
            'source .venv/bin/activate && python3 bq_ca_driver.py single '
            f'--project={PROJECT_ID} --agent=AGENT_ID '
            '--credentials_file=SERVICE_ACCOUNT_KEY_FILE --print_results '
            f'--query_file={expected_query_file}'
        ),
    ])
    mock_vm.PushFile.assert_called_once_with(mock.ANY, expected_query_file)

  @flagsaver.flagsaver(
      (
          gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE,
          '/path/to/SERVICE_ACCOUNT_KEY_FILE',
      ),
      (bigquery.BQ_CA_DATA_AGENT, 'AGENT_ID'),
  )
  def testConversationalAnalyticsClientInterfaceExecuteQueryInvalidJson(self):
    interface = bigquery.ConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)

    mock_vm.RemoteCommand.side_effect = [
        (None, None),  # For directory check in CreateRemoteFile
        ('invalid json', None),  # For executing query
    ]

    with self.assertRaises(errors.Benchmarks.RunError):
      interface.ExecuteQuery(QUERY_NAME)

  @parameterized.named_parameters(
      (
          'full_resource_path',
          'projects/other-project/locations/us-central1/reservations/test-res',
          'other-project:us-central1.test-res',
          'other-project.region-us-central1',
          1234,
          1234,
      ),
      (
          'full_resource_path_multi_region_us',
          'projects/other-project/locations/us/reservations/test-res',
          'other-project:US.test-res',
          'other-project.region-us',
          1234,
          1234,
      ),
      (
          'full_resource_path_multi_region_eu',
          'projects/other-project/locations/eu/reservations/test-res',
          'other-project:EU.test-res',
          'other-project.region-eu',
          1234,
          1234,
      ),
      (
          'colon_dot_path',
          'other-project:us-central1.test-res',
          'other-project:us-central1.test-res',
          'other-project.region-us-central1',
          '5678',
          5678,
      ),
      (
          'colon_dot_path_multi_region_us',
          'other-project:us.test-res',
          'other-project:US.test-res',
          'other-project.region-us',
          5678,
          5678,
      ),
      (
          'colon_dot_path_multi_region_eu',
          'other-project:eu.test-res',
          'other-project:EU.test-res',
          'other-project.region-eu',
          5678,
          5678,
      ),
      (
          'colon_dot_path_already_uppercase',
          'other-project:US.test-res',
          'other-project:US.test-res',
          'other-project.region-us',
          5678,
          5678,
      ),
      (
          'short_name',
          'test-res',
          'mybqfederated:US.test-res',
          'mybqfederated.region-us',
          9012,
          9012,
      ),
      (
          'reservation_not_set',
          None,
          '',
          'mybqfederated.region-us',
          1234,
          1234,
      ),
      (
          'none_query_result',
          None,
          '',
          'mybqfederated.region-us',
          None,
          0,
      ),
  )
  def testGetAutoscaleSlotSeconds(
      self,
      flag_value,
      expected_reservation_id,
      expected_res_proj_region,
      query_result_val,
      expected_cost,
  ):
    bq = bigquery.Bigquery(EDW_SERVICE_SPEC)
    bq.client_interface.client_vm = mock.MagicMock()
    with (
        mock.patch.object(bq, 'GetDatasetRegion', return_value='us'),
        mock.patch.object(
            bigquery.edw_service,
            'EDW_BQ_RESERVATION',
            mock.MagicMock(value=flag_value),
        ),
        mock.patch.object(
            bq.client_interface,
            'RunQueryWithResults',
            return_value=json.dumps({
                'details': {
                    'query_results': {'billed_slot_seconds': [query_result_val]}
                }
            }),
        ),
    ):
      cost = bq.GetAutoscaleSlotSeconds('test_run_123')

    self.assertEqual(cost, expected_cost)
    bq.client_interface.client_vm.RenderTemplate.assert_called_once_with(
        bigquery.data.ResourcePath('edw/bigquery/run_cost_query.sql.j2'),
        'cost_query_test_run_123',
        {
            'run_identifier': 'test_run_123',
            'project_dot_region': f'{bq.client_interface.project_id}.region-us',
            'reservation_project_dot_region': expected_res_proj_region,
            'reservation_id': expected_reservation_id,
        },
    )

  @parameterized.named_parameters(
      ('us_lowercase', 'us', 'US'),
      ('us_uppercase', 'US', 'US'),
      ('eu_lowercase', 'eu', 'EU'),
      ('eu_uppercase', 'EU', 'EU'),
      ('regional', 'us-central1', 'us-central1'),
      ('empty', '', ''),
      ('none', None, None),
  )
  def testNormalizeReservationLocation(self, location, expected):
    self.assertEqual(
        bigquery._NormalizeReservationLocation(location), expected
    )

  @flagsaver.flagsaver((gcp_flags.BQ_CLIENT_INTERFACE, 'PYTHON'))
  def testGetBigQueryClientInterfacePython(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.PythonClientInterface)

  @flagsaver.flagsaver((gcp_flags.BQ_CLIENT_INTERFACE, 'PYTHON'))
  def testPythonClientInterfaceGetMetadataDefault(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertDictEqual(
        interface.GetMetadata(),
        {
            'client': 'PYTHON',
            'edw_bq_api_timeout': 120,
            'edw_bq_reservation': 'default',
        },
    )

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'PYTHON'),
      (edw_service.EDW_BQ_RESERVATION, 'projects/p/locations/l/reservations/r'),
      (edw_service.EDW_BQ_QUERY_RESULTS_FORMAT, 'ARROW'),
      (edw_service.EDW_BQ_API_TIMEOUT, 120),
  )
  def testPythonClientInterfaceGetMetadataWithCustomFlags(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertDictEqual(
        interface.GetMetadata(),
        {
            'client': 'PYTHON',
            'edw_bq_reservation': 'projects/p/locations/l/reservations/r',
            'edw_bq_query_results_format': 'ARROW',
            'edw_bq_api_timeout': 120,
        },
    )

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'PYTHON'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'key.json'),
  )
  def testPythonClientInterfaceExecuteQueryDefault(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.PythonClientInterface)
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)
    mock_vm.RobustRemoteCommand.return_value = (
        json.dumps({
            'query_wall_time_in_secs': 2.5,
            'details': {'job_id': 'JOB_123'},
        }),
        None,
    )
    execution_time, details = interface.ExecuteQuery(QUERY_NAME)
    self.assertEqual(execution_time, 2.5)
    self.assertDictEqual(
        details,
        {
            'client': 'PYTHON',
            'edw_bq_reservation': 'default',
            'job_id': 'JOB_123',
            'edw_bq_api_timeout': 120,
        },
    )
    mock_vm.RobustRemoteCommand.assert_called_once_with(
        '.venv/bin/python bq_python_driver.py single --project PROJECT_ID'
        ' --credentials_file key.json --dataset DATASET_ID --query_file'
        ' QUERY_NAME --feature_config default --api_timeout 120'
    )

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'PYTHON'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, '/path/to/key.json'),
      (edw_service.EDW_BQ_FEATURE_CONFIG, 'job_optional'),
      (edw_service.EDW_BQ_RESERVATION, 'projects/p/locations/l/reservations/r'),
      (edw_service.EDW_BQ_QUERY_RESULTS_FORMAT, 'ARROW'),
      (edw_service.EDW_BQ_API_TIMEOUT, 120),
  )
  def testPythonClientInterfaceExecuteQueryWithFlags(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.PythonClientInterface)
    interface.destination = 'project.dataset.table'
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)
    mock_vm.RobustRemoteCommand.return_value = (
        json.dumps({
            'query_wall_time_in_secs': 1.2,
            'details': {'job_id': 'JOB_456'},
        }),
        None,
    )
    execution_time, details = interface.ExecuteQuery(
        QUERY_NAME, print_results=True
    )
    self.assertEqual(execution_time, 1.2)
    self.assertDictEqual(
        details,
        {
            'client': 'PYTHON',
            'edw_bq_reservation': 'projects/p/locations/l/reservations/r',
            'edw_bq_query_results_format': 'ARROW',
            'edw_bq_api_timeout': 120,
            'job_id': 'JOB_456',
        },
    )
    mock_vm.RobustRemoteCommand.assert_called_once_with(
        '.venv/bin/python bq_python_driver.py single --project PROJECT_ID'
        ' --credentials_file key.json --dataset DATASET_ID --query_file'
        ' QUERY_NAME --feature_config job_optional --reservation'
        ' projects/p/locations/l/reservations/r --query_results_format ARROW'
        ' --api_timeout 120 --print_results --destination'
        ' project.dataset.table'
    )

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'PYTHON'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'key.json'),
      (edw_service.EDW_BQ_FEATURE_CONFIG, 'job_optional'),
      (edw_service.EDW_BQ_RESERVATION, 'projects/p/locations/l/reservations/r'),
      (edw_service.EDW_BQ_QUERY_RESULTS_FORMAT, 'ARROW'),
      (edw_service.EDW_BQ_API_TIMEOUT, 120),
  )
  def testPythonClientInterfaceExecuteThroughput(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.PythonClientInterface)
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)
    mock_vm.RobustRemoteCommand.return_value = (
        json.dumps(THROUGHPUT_RESPONSE_OBJECT),
        None,
    )
    response = interface.ExecuteThroughput(QUERY_STREAMS, THROUGHPUT_LABELS)
    self.assertDictEqual(json.loads(response), THROUGHPUT_RESPONSE_OBJECT)
    mock_vm.RobustRemoteCommand.assert_called_once_with(
        '.venv/bin/python bq_python_driver.py throughput --project PROJECT_ID'
        ' --credentials_file key.json --dataset DATASET_ID'
        f" --query_streams='{json.dumps(QUERY_STREAMS)}'"
        ' --feature_config job_optional --labels'
        f" '{json.dumps(THROUGHPUT_LABELS)}'"
        ' --reservation projects/p/locations/l/reservations/r'
        ' --query_results_format ARROW --api_timeout 120'
    )

  @flagsaver.flagsaver(
      (gcp_flags.BQ_CLIENT_INTERFACE, 'PYTHON'),
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'key.json'),
      (edw_service.EDW_BQ_QUERY_RESULTS_FORMAT, 'ARROW'),
      (edw_service.EDW_BQ_API_TIMEOUT, 120),
  )
  def testPythonClientInterfaceRunQueryWithResults(self):
    interface = bigquery.GetBigQueryClientInterface(PROJECT_ID, DATASET_ID)
    self.assertIsInstance(interface, bigquery.PythonClientInterface)
    mock_vm = mock.MagicMock()
    bm_spec = FakeBenchmarkSpec(mock_vm)
    interface.SetProvisionedAttributes(bm_spec)
    mock_vm.RobustRemoteCommand.return_value = (
        json.dumps({'results': [1, 2, 3]}),
        None,
    )
    response = interface.RunQueryWithResults(QUERY_NAME)
    self.assertEqual(json.loads(response), {'results': [1, 2, 3]})
    mock_vm.RobustRemoteCommand.assert_called_once_with(
        '.venv/bin/python bq_python_driver.py single --project PROJECT_ID'
        ' --credentials_file key.json --dataset DATASET_ID --query_file'
        ' QUERY_NAME --print_results --query_results_format ARROW'
        ' --api_timeout 120'
    )

  @flagsaver.flagsaver((gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'key.json'))
  def testClaudeGetMcpConfigIncludesCredentials(self):
    interface = bigquery.ClaudeConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    interface.mcp_toolbox_path = '/home/perfkit/toolbox'
    interface.key_file_path = '/home/perfkit/key.json'

    config = json.loads(interface.GetMcpConfig())

    self.assertEqual(
        config['mcpServers']['google-data-cloud-genai-mcp-server']['env'][
            'GOOGLE_APPLICATION_CREDENTIALS'
        ],
        '/home/perfkit/key.json',
    )

  @flagsaver.flagsaver((gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, 'key.json'))
  def testClaudeGetMcpConfigRaisesWhenKeyFilePathNotSet(self):
    interface = bigquery.ClaudeConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    interface.mcp_toolbox_path = '/home/perfkit/toolbox'

    with self.assertRaisesRegex(RuntimeError, 'key_file_path is not set'):
      interface.GetMcpConfig()

  @flagsaver.flagsaver(
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, '/local/path/key.json')
  )
  def testClaudePrepareSetsKeyFilePathFromHomeDir(self):
    interface = bigquery.ClaudeConversationalAnalyticsClientInterface(
        PROJECT_ID, DATASET_ID
    )
    mock_vm = mock.MagicMock()
    mock_vm.RemoteCommand.return_value = ('/home/perfkit\n', '')
    interface.SetProvisionedAttributes(FakeBenchmarkSpec(mock_vm))

    with (
        mock.patch.object(
            bigquery.mcp_toolbox_for_db,
            'Install',
            return_value='/home/perfkit/toolbox',
        ),
        mock.patch.object(interface.python_client_interface, 'Prepare'),
        mock.patch.object(edw_service.vm_util, 'CreateRemoteFile'),
    ):
      interface.Prepare(PACKAGE_NAME)

    self.assertEqual(interface.key_file_path, '/home/perfkit/key.json')


_AI_EXTRACT_DOCS = edw_ai_extract_lib.AiExtractDocuments(
    dataset_id='paystub',
    storage_prefix='gs://bucket/bqmmdlab/paystub/',
    paths={
        '0001': 'documents/0001.pdf',
        '0002': 'documents/0002.pdf',
    },
)

_GEMINI_SETTINGS = bigquery.GeminiAiExtractSettings(
    'gemini-3.5-flash',
    'gemini-3.7-flash',
    0.0,
    40,
    1.0,
    'MEDIUM',
    8,
)


class _FakeAiExtractSchema:
  """Minimal schema stub satisfying AiExtractSchema."""

  def NestedTemplate(self) -> dict[str, Any]:
    return {'name': "employee's name", 'item': [{'amount': 'amount'}]}

  def GeminiResponseSchema(self) -> dict[str, Any]:
    return {'type': 'OBJECT', 'properties': {'name': {'type': 'STRING'}}}

  def SnowflakeResponseFormat(self) -> dict[str, Any]:
    return {}

  def SnowflakeResponseToCanonical(
      self, response: dict[str, Any]
  ) -> dict[str, Any]:
    return response

  def DatabricksSchema(self) -> dict[str, Any]:
    return {}

  def DatabricksResponseToCanonical(
      self, response: dict[str, Any]
  ) -> dict[str, Any]:
    return response


class GeminiAiExtractTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.remote_files: dict[str, str] = {}
    self.enter_context(
        mock.patch.object(
            vm_util,
            'CreateRemoteFile',
            autospec=True,
            side_effect=lambda vm, text, path, write_mode='w': (
                self.remote_files.__setitem__(path, text)
            ),
        )
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'GetTempDir',
            autospec=True,
            return_value=self.create_tempdir().full_path,
        )
    )

  def _CreateClient(
      self, key_file: str
  ) -> tuple[bigquery.GeminiAiExtractClientInterface, mock.MagicMock]:
    mock_vm = mock.MagicMock()
    client = bigquery.GeminiAiExtractClientInterface(
        'proj', key_file, _GEMINI_SETTINGS
    )
    client.SetProvisionedAttributes(FakeBenchmarkSpec(mock_vm))
    return client, mock_vm

  def _SetDriverOutput(
      self,
      mock_vm: mock.MagicMock,
      robust_stdout: str,
      records: list[dict[str, Any]],
  ) -> None:
    pulled_text = ''.join(json.dumps(record) + '\n' for record in records)

    def _PullFile(local_path: str, remote_path: str) -> None:
      del remote_path
      with open(local_path, 'w') as f:
        f.write(pulled_text)

    mock_vm.RobustRemoteCommand.return_value = (robust_stdout, '')
    mock_vm.PullFile.side_effect = _PullFile

  def testGeminiDriverRecordToResultOk(self):
    raw = {
        'doc_id': '0001',
        'status': 'ok',
        'error': None,
        'pass1_json': {'name': 'Jane'},
        'raw_text_p1': '{}',
        'raw_text_p2': '{}',
        'audit': {'0': {'confidence': 'High'}},
        'confidence': {'name': ['High']},
        'usage': {
            'pass1_input_tokens': 1000,
            'pass1_output_tokens': 100,
            'pass2_input_tokens': 2000,
            'pass2_output_tokens': 200,
            'pass1_latency_sec': 1.0,
            'pass2_latency_sec': 2.0,
        },
    }

    result = bigquery.GeminiDriverRecordToResult(raw, _GEMINI_SETTINGS)

    with self.subTest('error'):
      self.assertIsNone(result.error)
    with self.subTest('prediction'):
      self.assertEqual(result.prediction, {'name': 'Jane'})
    with self.subTest('confidence'):
      self.assertEqual(result.confidence, {'name': ['High']})
    with self.subTest('usage'):
      self.assertEqual(
          result.usage,
          dict(
              raw['usage'],
              pass1_model='gemini-3.5-flash',
              pass2_model='gemini-3.7-flash',
          ),
      )

  def testGeminiDriverRecordToResultError(self):
    raw = {
        'doc_id': '0001',
        'status': 'error',
        'error': 'pass1 JSON error',
        'pass1_json': {'name': 'Jane'},
        'raw_text_p1': 'garbage',
        'raw_text_p2': '',
        'audit': {},
        'confidence': {'name': ['High']},
        'usage': {
            'pass1_input_tokens': 1000,
            'pass1_output_tokens': 100,
            'pass2_input_tokens': 0,
            'pass2_output_tokens': 0,
            'pass1_latency_sec': 1.0,
            'pass2_latency_sec': 0.0,
        },
    }

    errored = bigquery.GeminiDriverRecordToResult(raw, _GEMINI_SETTINGS)

    with self.subTest('error'):
      self.assertEqual(errored.error, 'pass1 JSON error')
    with self.subTest('prediction'):
      self.assertEqual(errored.prediction, {})
    with self.subTest('confidence'):
      self.assertIsNone(errored.confidence)

  @parameterized.named_parameters(
      ('error_without_message', 'error', None),
      ('ok_with_error_message', 'ok', 'x'),
      ('unknown_status', '?', None),
  )
  def testGeminiDriverRecordRejectsInconsistentStatus(self, status, error):
    raw = {'doc_id': '0001', 'status': status, 'error': error}
    with self.assertRaisesRegex(ValueError, 'Invalid driver record'):
      bigquery.GeminiDriverRecordToResult(raw, _GEMINI_SETTINGS)

  def testGeminiPreparePushesLocalKeyFile(self):
    client, mock_vm = self._CreateClient('/keys/k.json')

    client.Prepare('edw_common')

    mock_vm.PushFile.assert_called_once_with('/keys/k.json')
    mock_vm.InstallPreprovisionedPackageData.assert_not_called()
    mock_vm.Install.assert_called_once_with('pip')
    mock_vm.PushDataFile.assert_called_once_with(
        'edw/bigquery/clients/python/bq_ai_extract_driver.py'
    )

  def testGeminiPrepareInstallsPreprovisionedKeyFile(self):
    client, mock_vm = self._CreateClient('k.json')

    client.Prepare('edw_common')

    mock_vm.InstallPreprovisionedPackageData.assert_called_once_with(
        'edw_common', ['k.json'], ''
    )
    mock_vm.PushFile.assert_not_called()

  def testGeminiExtract(self):
    usage = {
        'pass1_input_tokens': 1000,
        'pass1_output_tokens': 100,
        'pass2_input_tokens': 2000,
        'pass2_output_tokens': 200,
        'pass1_latency_sec': 1.0,
        'pass2_latency_sec': 3.0,
    }
    records = [
        {
            'doc_id': '0001',
            'status': 'ok',
            'error': None,
            'pass1_json': {'name': 'Jane', 'item': [{'amount': '10'}]},
            'raw_text_p1': '{}',
            'raw_text_p2': '{}',
            'audit': {},
            'confidence': {
                'name': ['High'],
                'item.amount': ['Need human review'],
            },
            'usage': usage,
        },
        {
            'doc_id': '0002',
            'status': 'error',
            'error': 'pass1 JSON error: x',
            'pass1_json': {},
            'raw_text_p1': 'garbage',
            'raw_text_p2': '',
            'audit': {},
            'confidence': {},
            'usage': dict(
                usage,
                pass2_input_tokens=0,
                pass2_output_tokens=0,
                pass2_latency_sec=0.0,
            ),
        },
    ]
    client, mock_vm = self._CreateClient('/keys/k.json')
    self._SetDriverOutput(
        mock_vm,
        robust_stdout='log line\n{"extract_wall_time_sec": 7.5}\n',
        records=records,
    )
    schema = _FakeAiExtractSchema()

    run = client.Extract(_AI_EXTRACT_DOCS, schema)

    mock_vm.RobustRemoteCommand.assert_called_once_with(
        '.venv/bin/python bq_ai_extract_driver.py'
        ' --project=proj --location=global --credentials_file=k.json'
        ' --pass1_model=gemini-3.5-flash --pass2_model=gemini-3.7-flash'
        ' --temperature=0.0 --top_k=40 --top_p=1.0 --thinking_level=MEDIUM'
        ' --num_threads=8 --request_file=ai_extract_request.json'
        ' --output_file=ai_extract_records.jsonl'
    )
    request = json.loads(self.remote_files['ai_extract_request.json'])
    with self.subTest('request_payload'):
      self.assertEqual(
          request,
          {
              'documents': [
                  {
                      'doc_id': '0001',
                      'uri': 'gs://bucket/bqmmdlab/paystub/documents/0001.pdf',
                  },
                  {
                      'doc_id': '0002',
                      'uri': 'gs://bucket/bqmmdlab/paystub/documents/0002.pdf',
                  },
              ],
              'nested_template': schema.NestedTemplate(),
              'response_schema': schema.GeminiResponseSchema(),
          },
      )
    with self.subTest('wall_time'):
      self.assertEqual(run.wall_time_sec, 7.5)
    with self.subTest('document_results'):
      self.assertEqual(
          [r.error for r in run.document_results], [None, 'pass1 JSON error: x']
      )
      self.assertIsNone(run.document_results[1].confidence)
    with self.subTest('usage_metadata'):
      self.assertEqual(
          run.usage_metadata,
          {
              'pass1_input_tokens': 2000,
              'pass1_output_tokens': 200,
              'pass2_input_tokens': 2000,
              'pass2_output_tokens': 200,
          },
      )
    with self.subTest('extra_metrics'):
      metrics = {
          name: (value, unit) for name, value, unit in run.extra_metrics
      }
      self.assertEqual(metrics['pass1_latency'], (1.0, 'sec'))
      self.assertEqual(metrics['pass2_latency'], (3.0, 'sec'))
      self.assertEqual(metrics['pass1_input_tokens'], (2000.0, 'tokens'))
      self.assertEqual(metrics['pass2_output_tokens'], (200.0, 'tokens'))

  def testGeminiExtractAllFailedReportsMissingLatency(self):
    usage = {
        'pass1_input_tokens': 1000,
        'pass1_output_tokens': 100,
        'pass2_input_tokens': 0,
        'pass2_output_tokens': 0,
        'pass1_latency_sec': 4.0,
        'pass2_latency_sec': 0.0,
    }
    records = [
        {
            'doc_id': '0001',
            'status': 'error',
            'error': 'pass1 JSON error: x',
            'pass1_json': {},
            'raw_text_p1': 'garbage',
            'raw_text_p2': '',
            'audit': {},
            'confidence': {},
            'usage': usage,
        },
        {
            'doc_id': '0002',
            'status': 'error',
            'error': 'pass1 JSON error: y',
            'pass1_json': {},
            'raw_text_p1': 'garbage',
            'raw_text_p2': '',
            'audit': {},
            'confidence': {},
            'usage': usage,
        },
    ]
    client, mock_vm = self._CreateClient('/keys/k.json')
    self._SetDriverOutput(
        mock_vm,
        robust_stdout='{"extract_wall_time_sec": 2.0}\n',
        records=records,
    )

    run = client.Extract(_AI_EXTRACT_DOCS, _FakeAiExtractSchema())

    with self.subTest('document_errors'):
      self.assertEqual(
          [r.error for r in run.document_results],
          ['pass1 JSON error: x', 'pass1 JSON error: y'],
      )
    with self.subTest('missing_latency_sentinel'):
      metrics = {
          name: (value, unit) for name, value, unit in run.extra_metrics
      }
      self.assertEqual(metrics['pass1_latency'], (-1.0, 'sec'))
      self.assertEqual(metrics['pass2_latency'], (-1.0, 'sec'))
      self.assertEqual(metrics['pass1_input_tokens'], (2000.0, 'tokens'))

  def _CreateBigqueryService(self) -> bigquery.Bigquery:
    spec = mock.Mock(
        snapshot=None,
        concurrency=5,
        node_type=None,
        node_count=1,
        endpoint=None,
        db=None,
        user=None,
        password=None,
        type='bigquery',
        cluster_identifier='proj.dataset',
    )
    return bigquery.Bigquery(spec)

  @flagsaver.flagsaver(
      (gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, '/keys/k.json'),
      (bigquery._AI_EXTRACT_PASS1_MODEL, 'p1'),
      (bigquery._AI_EXTRACT_NUM_THREADS, 3),
  )
  def testBigqueryGetAiExtractClientInterface(self):
    service = self._CreateBigqueryService()

    client = service.GetAiExtractClientInterface()

    self.assertIsInstance(client, bigquery.GeminiAiExtractClientInterface)
    with self.subTest('project_and_key'):
      self.assertEqual(client.project_id, 'proj')
      self.assertEqual(client.key_file, '/keys/k.json')
    with self.subTest('settings_and_metadata'):
      self.assertEqual(client.settings.pass1_model, 'p1')
      self.assertEqual(client.settings.pass2_model, 'gemini-3.8-flash')
      self.assertEqual(client.settings.num_threads, 3)
      self.assertEqual(client.GetMetadata()['pass1_model'], 'p1')

  @flagsaver.flagsaver((gcp_flags.GCP_SERVICE_ACCOUNT_KEY_FILE, None))
  def testBigqueryGetAiExtractClientInterfaceRequiresKeyFile(self):
    service = self._CreateBigqueryService()

    with self.assertRaisesRegex(
        errors.Config.InvalidValue, 'gcp_service_account_key_file'
    ):
      service.GetAiExtractClientInterface()


if __name__ == '__main__':
  unittest.main()
