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


class ClaudeConversationalAnalyticsClientInterfaceTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def test_inheritance_and_properties(self):
    # Arrange & Act
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )

    # Assert
    self.assertIsInstance(
        interface, edw_service.BaseClaudeConversationalAnalyticsClientInterface
    )
    self.assertTrue(interface.fetches_results_immediately)

  def test_set_provisioned_attributes(self):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
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
    self.assertEqual(
        interface.python_client_interface.benchmark_name, 'test_benchmark'
    )
    self.assertEqual(interface.python_client_interface.client_vm, mock_vm)

  def test_construct_claude_system_prompt_ecomm(self):
    # Arrange
    with flagsaver.flagsaver((edw_service.CA_DATASET, 'ecomm')):
      interface = looker.ClaudeConversationalAnalyticsClientInterface(
          base_url=_BASE_URL, model_name=_MODEL_NAME
      )

      # Act
      prompt = interface._ConstructClaudeSystemPrompt()

      # Assert
      self.assertIn('thelook_adwords', prompt)
      self.assertIn('events', prompt)

  def test_construct_claude_system_prompt_call_center(self):
    # Arrange
    with flagsaver.flagsaver((edw_service.CA_DATASET, 'call_center')):
      interface = looker.ClaudeConversationalAnalyticsClientInterface(
          base_url=_BASE_URL, model_name=_MODEL_NAME
      )

      # Act
      prompt = interface._ConstructClaudeSystemPrompt()

      # Assert
      self.assertIn('call_center', prompt)
      self.assertIn('transcript', prompt)

  def test_inject_looker_client_secret(self):
    # Arrange
    mock_vm = mock.MagicMock()

    # Act
    looker._InjectLookerClientSecret(
        mock_vm,
        '/home/user/looker_client_secret.txt',
        '/home/user/claude_ca/.mcp.json',
    )

    # Assert
    mock_vm.RemoteCommand.assert_called_once()
    cmd = mock_vm.RemoteCommand.call_args.args[0]
    self.assertIn('/home/user/looker_client_secret.txt', cmd)
    self.assertIn('/home/user/claude_ca/.mcp.json', cmd)
    self.assertIn('LOOKER_CLIENT_SECRET', cmd)

  def test_get_mcp_config(self):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL,
        model_name=_MODEL_NAME,
        client_id='test_client_id',
    )
    interface.mcp_toolbox_path = '/usr/local/bin/toolbox'

    # Act
    config_json = interface.GetMcpConfig()
    config = json.loads(config_json)

    # Assert
    expected_env = {
        'LOOKER_BASE_URL': _BASE_URL,
        'LOOKER_CLIENT_ID': 'test_client_id',
        'LOOKER_CLIENT_SECRET': looker.LOOKER_CLIENT_SECRET_PLACEHOLDER,
        'LOOKER_VERIFY_SSL': 'true',
    }
    self.assertEqual(
        config['mcpServers']['looker-toolbox']['env'], expected_env
    )

  def test_get_mcp_config_raises_when_path_not_set(self):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )

    # Act & Assert
    with self.assertRaises(RuntimeError):
      interface.GetMcpConfig()

  def test_get_claude_dir(self):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )
    interface.claude_dir = '/home/user/claude_ca'

    # Act & Assert
    self.assertEqual(interface.GetClaudeDir(), '/home/user/claude_ca')

  def test_get_claude_dir_raises_when_not_set(self):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )

    # Act & Assert
    with self.assertRaises(RuntimeError):
      interface.GetClaudeDir()

  def test_get_query_file_name(self):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )
    interface.claude_dir = '/home/user/claude_ca'

    # Act
    filename = interface._GetQueryFileName('test_query')

    # Assert
    self.assertTrue(filename.startswith('/home/user/claude_ca/'))
    self.assertTrue(filename.endswith('.txt'))

  def test_get_conversational_analytics_command_default(self):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL, model_name=_MODEL_NAME
    )
    interface.claude_dir = '/home/user/claude_ca'

    # Act
    cmd = interface._GetConversationalAnalyticsCommand(
        '/home/user/claude_ca/query.txt'
    )

    # Assert
    self.assertIn(f'python3 {looker.CLAUDE_PYTHON_DRIVER_FILE} single', cmd)
    self.assertIn('--allowed_tools=mcp__looker-toolbox__*', cmd)
    self.assertIn('--print_results', cmd)
    self.assertNotIn('--model=', cmd)

  def test_get_conversational_analytics_command_with_custom_model(self):
    # Arrange
    with flagsaver.flagsaver(
        (looker.LOOKER_CA_CLAUDE_MODEL, 'claude-sonnet-4-5')
    ):
      interface = looker.ClaudeConversationalAnalyticsClientInterface(
          base_url=_BASE_URL, model_name=_MODEL_NAME
      )
      interface.claude_dir = '/home/user/claude_ca'

      # Act
      cmd = interface._GetConversationalAnalyticsCommand(
          '/home/user/claude_ca/query.txt'
      )

      # Assert
      self.assertIn('--model=claude-sonnet-4-5', cmd)

  @mock.patch.object(
      looker.mcp_toolbox_for_db,
      'Install',
      return_value='/usr/local/bin/toolbox',
      autospec=True,
  )
  @mock.patch.object(looker, '_InjectLookerClientSecret', autospec=True)
  def test_prepare(self, mock_inject_secret, mock_toolbox_install):
    # Arrange
    interface = looker.ClaudeConversationalAnalyticsClientInterface(
        base_url=_BASE_URL,
        model_name=_MODEL_NAME,
        client_id='test_client_id',
    )
    mock_vm = mock.MagicMock()
    mock_vm.RemoteCommand.return_value = ('/home/user\n', '')
    bm_spec = mock.Mock(name='test_benchmark', vms=[mock_vm])
    bm_spec.name = 'test_benchmark'
    interface.SetProvisionedAttributes(bm_spec)

    # Act
    interface.Prepare('edw_common')

    # Assert
    mock_toolbox_install.assert_called_once_with(mock_vm)
    mock_inject_secret.assert_called_once_with(
        mock_vm,
        '/home/user/looker_client_secret.txt',
        '/home/user/claude_ca/.mcp.json',
    )


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

  def test_get_conversational_analytics_client_interface_claude(self):
    # Arrange & Act
    with flagsaver.flagsaver(
        (looker.LOOKER_BASE_URL, _BASE_URL),
        (looker.LOOKER_MODEL_NAME, _MODEL_NAME),
        (looker.LOOKER_CLIENT_ID, 'test_client_id'),
        (looker.LOOKER_CA_CLIENT, 'claude'),
    ):
      spec = mock.Mock()
      service = looker.Looker(spec)
      ca_interface = service.GetConversationalAnalyticsClientInterface()

      # Assert
      self.assertIsInstance(
          ca_interface, looker.ClaudeConversationalAnalyticsClientInterface
      )
      self.assertEqual(ca_interface.base_url, _BASE_URL)
      self.assertEqual(ca_interface.model_name, _MODEL_NAME)
      self.assertEqual(ca_interface.client_id, 'test_client_id')


if __name__ == '__main__':
  unittest.main()
