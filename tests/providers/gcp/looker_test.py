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
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.linux_benchmarks import edw_conversational_analytics_benchmark  # pylint: disable=unused-import
from perfkitbenchmarker.providers.gcp import looker
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_BASE_URL = 'https://instance.looker.com'
_MODEL_NAME = 'test_model'
_AGENT_ID = 'projects/test-project/locations/us-central1/dataAgents/test-agent'
_QUERY_NAME = 'test_query.sql'
_QUESTION = 'What is the total revenue?'


class PythonClientInterfaceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_set_provisioned_attributes(self):
    # Arrange
    interface = looker.PythonClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )
    mock_vm = mock.MagicMock()
    bm_spec = mock.Mock(name='test_benchmark', vms=[mock_vm])
    bm_spec.name = 'test_benchmark'

    # Act
    interface.SetProvisionedAttributes(bm_spec)

    # Assert
    self.assertEqual(interface.benchmark_name, 'test_benchmark')
    self.assertEqual(interface.client_vm, mock_vm)

  def test_get_metadata(self):
    # Arrange
    interface = looker.PythonClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )
    expected_metadata = {
        'client': 'PYTHON',
        'looker_base_url': _BASE_URL,
        'looker_model_name': _MODEL_NAME,
    }

    # Act & Assert
    self.assertEqual(interface.GetMetadata(), expected_metadata)

  def test_prepare(self):
    # Arrange
    interface = looker.PythonClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )
    mock_vm = mock.MagicMock()
    interface.client_vm = mock_vm
    interface.benchmark_name = 'test_benchmark'

    # Act
    interface.Prepare('edw_common')

    # Assert
    mock_vm.InstallPreprovisionedBenchmarkData.assert_called_once_with(
        'test_benchmark', [looker.LOOKER_CLIENT_SECRET_FILE], '.'
    )
    mock_vm.Install.assert_called_once_with('pip')
    mock_vm.RemoteCommand.assert_has_calls([
        mock.call(
            'sudo apt-get -qq update && DEBIAN_FRONTEND=noninteractive'
            ' sudo apt-get -qq install python3.12-venv'
        ),
        mock.call('python3 -m venv .venv'),
        mock.call(
            'source .venv/bin/activate && pip install looker-sdk absl-py'
        ),
    ])

  @mock.patch('builtins.open', mock.mock_open(read_data='test_secret\n'))
  @mock.patch.object(
      looker.data,
      'ResourcePath',
      return_value='/path/looker_client_secret.txt',
  )
  def test_load_looker_client_secret(self, mock_resource_path):
    secret = looker._LoadLookerClientSecret()
    self.assertEqual(secret, 'test_secret')

  @mock.patch('builtins.open', mock.mock_open(read_data='   \n'))
  @mock.patch.object(
      looker.data,
      'ResourcePath',
      return_value='/path/looker_client_secret.txt',
  )
  def test_load_looker_client_secret_empty_raises(self, mock_resource_path):
    with self.assertRaises(ValueError):
      looker._LoadLookerClientSecret()

  @parameterized.named_parameters(
      ('with_print_results', True),
      ('without_print_results', False),
  )
  def test_execute_query(self, print_results: bool):
    # Arrange
    interface = looker.PythonClientInterface(
        base_url=_BASE_URL,
        model_name=_MODEL_NAME,
        client_id='test_client_id',
    )
    mock_vm = mock.MagicMock()
    stdout_payload = json.dumps({
        'query_wall_time_in_secs': 2.5,
        'details': {'query_id': '12345'},
    })
    mock_vm.RobustRemoteCommand.return_value = (stdout_payload, None)
    interface.client_vm = mock_vm

    # Act
    wall_time, details = interface.ExecuteQuery(
        _QUERY_NAME, print_results=print_results
    )

    # Assert
    self.assertEqual(wall_time, 2.5)
    self.assertEqual(
        details,
        {
            'client': 'PYTHON',
            'looker_base_url': _BASE_URL,
            'looker_model_name': _MODEL_NAME,
            'query_id': '12345',
        },
    )
    cmd = mock_vm.RobustRemoteCommand.call_args[0][0]
    self.assertIn(
        f'.venv/bin/python {looker.LOOKER_PYTHON_CLIENT_FILE} single', cmd
    )
    self.assertIn(f'--base_url={_BASE_URL}', cmd)
    self.assertIn(f'--model_name={_MODEL_NAME}', cmd)
    self.assertIn('--client_id=test_client_id', cmd)
    self.assertIn(f'--query_file={_QUERY_NAME}', cmd)
    if print_results:
      self.assertIn('--print_results', cmd)
    else:
      self.assertNotIn('--print_results', cmd)


class ConversationalAnalyticsClientInterfaceTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def test_inheritance_and_properties(self):
    # Arrange & Act
    interface = looker.ConversationalAnalyticsClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )

    # Assert
    self.assertIsInstance(
        interface, edw_service.BaseConversationalAnalyticsClientInterface
    )
    self.assertIsInstance(interface, looker.PythonClientInterface)
    self.assertTrue(interface.fetches_results_immediately)

  def test_get_conversational_analytics_command(self):
    # Arrange & Act
    with flagsaver.flagsaver((looker.LOOKER_CA_DATA_AGENT, _AGENT_ID)):
      interface = looker.ConversationalAnalyticsClientInterface(
          base_url=_BASE_URL,
          model_name=_MODEL_NAME,
          client_id='test_client_id',
      )
      cmd = interface._GetConversationalAnalyticsCommand('./query.txt')

      # Assert
      self.assertIn(
          f'.venv/bin/python {looker.LOOKER_CA_CLIENT_FILE} single', cmd
      )
      self.assertIn(f'--base_url={_BASE_URL}', cmd)
      self.assertIn('--client_id=test_client_id', cmd)
      self.assertIn(f'--agent_id={_AGENT_ID}', cmd)
      self.assertIn('--query_file=./query.txt', cmd)
      self.assertIn('--print_results', cmd)

  def test_execute_query_success(self):
    # Arrange
    with flagsaver.flagsaver((looker.LOOKER_CA_DATA_AGENT, _AGENT_ID)):
      interface = looker.ConversationalAnalyticsClientInterface(
          base_url=_BASE_URL, model_name=_MODEL_NAME
      )
      mock_vm = mock.MagicMock()
      interface.client_vm = mock_vm

      response_dict = {
          'query_wall_time_in_secs': 4.2,
          'details': {
              'job_id': 'job-456',
              'query_results': {
                  'text_response': 'Total revenue is $500',
                  'generated_sql': 'SELECT 500',
                  'retrieved_data': [['500']],
                  'thoughts': ['Analyzing revenue'],
                  'progress_messages': ['Executing SQL'],
                  'time_to_first_token_secs': 0.8,
                  'total_stream_time_secs': 3.5,
              },
          },
      }
      mock_vm.RemoteCommand.side_effect = [
          (None, None),  # For directory check in CreateRemoteFile
          (json.dumps(response_dict), None),  # For executing query
      ]

      # Act
      execution_time, metadata = interface.ExecuteQuery(_QUESTION)

      # Assert
      self.assertEqual(execution_time, 4.2)
      expected_metadata = {
          'question': _QUESTION,
          'text_response': 'Total revenue is $500',
          'generated_sql': 'SELECT 500',
          'predict_data': [['500']],
          'job_id': 'job-456',
          'thoughts': ['Analyzing revenue'],
          'progress_messages': ['Executing SQL'],
          'time_to_first_token_secs': 0.8,
          'total_stream_time_secs': 3.5,
      }
      self.assertEqual(metadata, expected_metadata)

  def test_execute_query_invalid_json(self):
    # Arrange
    with flagsaver.flagsaver((looker.LOOKER_CA_DATA_AGENT, _AGENT_ID)):
      interface = looker.ConversationalAnalyticsClientInterface(
          base_url=_BASE_URL, model_name=_MODEL_NAME
      )
      mock_vm = mock.MagicMock()
      interface.client_vm = mock_vm

      mock_vm.RemoteCommand.side_effect = [
          (None, None),  # For directory check in CreateRemoteFile
          ('non-json output', None),  # For executing query
      ]

      # Act & Assert
      with self.assertRaises(errors.Benchmarks.RunError):
        interface.ExecuteQuery(_QUESTION)

  @parameterized.named_parameters(
      (
          'missing_text_response',
          {
              'query_wall_time_in_secs': 5.0,
              'details': {
                  'query_results': {
                      'generated_sql': 'SELECT 1',
                      'retrieved_data': [['1']],
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
                      'generated_sql': 'SELECT 1',
                      'retrieved_data': [['1']],
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
                      'text_response': 'Answer',
                      'retrieved_data': [['1']],
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
                      'text_response': 'Answer',
                      'generated_sql': '',
                      'retrieved_data': [['1']],
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
                      'text_response': 'Answer',
                      'generated_sql': 'SELECT 1',
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
                      'text_response': 'Answer',
                      'generated_sql': 'SELECT 1',
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
  def test_execute_query_validation_errors(
      self, response_dict, expected_error_msg
  ):
    # Arrange
    with flagsaver.flagsaver((looker.LOOKER_CA_DATA_AGENT, _AGENT_ID)):
      interface = looker.ConversationalAnalyticsClientInterface(
          base_url=_BASE_URL, model_name=_MODEL_NAME
      )
      mock_vm = mock.MagicMock()
      interface.client_vm = mock_vm

      mock_vm.RemoteCommand.side_effect = [
          (None, None),  # For directory check in CreateRemoteFile
          (json.dumps(response_dict), None),  # For executing query
      ]

      # Act
      execution_time, metadata = interface.ExecuteQuery(_QUESTION)

      # Assert
      self.assertEqual(execution_time, -1.0)
      self.assertEqual(metadata['error'], expected_error_msg)


class LookerServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def test_init(self):
    # Arrange & Act
    with flagsaver.flagsaver(
        (looker.LOOKER_BASE_URL, _BASE_URL),
        (looker.LOOKER_MODEL_NAME, _MODEL_NAME),
    ):
      spec = mock.Mock()
      service = looker.Looker(spec)

      # Assert
      self.assertEqual(service.CLOUD, provider_info.GCP)
      self.assertEqual(service.SERVICE_TYPE, 'looker')
      self.assertEqual(service.QUERY_SET, 'looker')
      self.assertEqual(service.base_url, _BASE_URL)
      self.assertEqual(service.model_name, _MODEL_NAME)
      self.assertIsInstance(
          service.client_interface, looker.PythonClientInterface
      )
      self.assertEqual(service.client_interface.base_url, _BASE_URL)
      self.assertEqual(service.client_interface.model_name, _MODEL_NAME)

  def test_get_metadata(self):
    # Arrange & Act
    with flagsaver.flagsaver(
        (looker.LOOKER_BASE_URL, _BASE_URL),
        (looker.LOOKER_MODEL_NAME, _MODEL_NAME),
    ):
      spec = mock.Mock()
      service = looker.Looker(spec)
      metadata = service.GetMetadata()

      # Assert
      self.assertEqual(metadata['looker_base_url'], _BASE_URL)
      self.assertEqual(metadata['looker_model_name'], _MODEL_NAME)
      self.assertEqual(metadata['client'], 'PYTHON')

  def test_get_conversational_analytics_client_interface(self):
    # Arrange & Act
    with flagsaver.flagsaver(
        (looker.LOOKER_BASE_URL, _BASE_URL),
        (looker.LOOKER_MODEL_NAME, _MODEL_NAME),
    ):
      spec = mock.Mock()
      service = looker.Looker(spec)
      ca_interface = service.GetConversationalAnalyticsClientInterface()

      # Assert
      self.assertIsInstance(
          ca_interface, looker.ConversationalAnalyticsClientInterface
      )
      self.assertEqual(ca_interface.base_url, _BASE_URL)
      self.assertEqual(ca_interface.model_name, _MODEL_NAME)


if __name__ == '__main__':
  unittest.main()
