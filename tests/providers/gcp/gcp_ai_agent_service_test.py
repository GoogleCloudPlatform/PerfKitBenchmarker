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
import unittest
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_ai_agent_service
from perfkitbenchmarker.resources import ai_agent_service
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GcpAiAgentServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(run_uri='123'))
    self.enter_context(flagsaver.flagsaver(project='my-project'))
    self.enter_context(flagsaver.flagsaver(zone=['us-central1-a']))
    self.enter_context(flagsaver.flagsaver(default_timeout=0))
    self.mock_vm = mock.create_autospec(virtual_machine.BaseVirtualMachine)
    self.mock_spec = mock.MagicMock(agent='test_agent', framework='adk')

  def test_get_ai_agent_service_class(self):
    self.assertIs(
        ai_agent_service.GetAiAgentServiceClass('GCP', 'client_vm'),
        gcp_ai_agent_service.GcpClientVmAiAgentService,
    )
    self.assertIs(
        ai_agent_service.GetAiAgentServiceClass('GCP', 'custom_job'),
        gcp_ai_agent_service.VertexAiCustomJobAiAgentService,
    )
    self.assertIs(
        ai_agent_service.GetAiAgentServiceClass('GCP', 'agent_engine'),
        gcp_ai_agent_service.VertexAiAgentEngineAiAgentService,
    )

  def test_client_vm_initialization(self):
    service = gcp_ai_agent_service.GcpClientVmAiAgentService(
        self.mock_vm, self.mock_spec
    )
    self.assertEqual(service.CLOUD, 'GCP')
    self.assertEqual(service.DEPLOYMENT_TYPE, 'client_vm')
    self.assertEqual(service.project, 'my-project')
    self.assertEqual(service.region, 'us-central1')

  def test_client_vm_execute_success(self):
    service = gcp_ai_agent_service.GcpClientVmAiAgentService(
        self.mock_vm, self.mock_spec
    )
    service.UploadRunConfigToClientVm = mock.MagicMock()
    service.Execute(
        output_dir='gs://my-bucket/output',
        prompt='test prompt',
        session_id='session_1',
    )
    self.mock_vm.RobustRemoteCommand.assert_called_once()
    called_command = self.mock_vm.RobustRemoteCommand.call_args[0][0]
    self.assertIn(
        'python3 run_local_agent.py --config_file "run_config_session_1.yaml"',
        called_command,
    )

  def test_agent_engine_execute_success(self):
    service = gcp_ai_agent_service.VertexAiAgentEngineAiAgentService(
        self.mock_vm, self.mock_spec
    )
    service._remote_agent_name = 'test_agent'
    service.UploadRunConfigToClientVm = mock.MagicMock()
    self.mock_vm.RemoteCommandWithReturnCode.return_value = (
        'stdout output',
        '',
        0,
    )
    service.Execute(
        output_dir='gs://my-bucket/output',
        prompt='test prompt',
        session_id='session_1',
    )
    self.mock_vm.RemoteCommandWithReturnCode.assert_called_once()
    called_command = self.mock_vm.RemoteCommandWithReturnCode.call_args[0][0]
    self.assertIn(
        'python3 run_agent_engine.py --config_file "run_config_session_1.yaml"',
        called_command,
    )

  def test_agent_engine_execute_quota_error(self):
    service = gcp_ai_agent_service.VertexAiAgentEngineAiAgentService(
        self.mock_vm, self.mock_spec
    )
    service._remote_agent_name = 'test_agent'
    service.UploadRunConfigToClientVm = mock.MagicMock()
    self.mock_vm.RemoteCommandWithReturnCode.return_value = (
        '',
        "API Error: {'error': {'code': 429, 'message': 'Resource exhausted'}}",
        1,
    )
    with self.assertRaises(vm_util.TimeoutExceededRetryError) as ctx:
      service.Execute(
          output_dir='gs://my-bucket/output',
          prompt='test prompt',
          session_id='session_1',
      )
    self.assertIsInstance(
        ctx.exception.__cause__, errors.Benchmarks.QuotaFailure
    )

  def test_agent_engine_execute_generic_error(self):
    service = gcp_ai_agent_service.VertexAiAgentEngineAiAgentService(
        self.mock_vm, self.mock_spec
    )
    service._remote_agent_name = 'test_agent'
    service.UploadRunConfigToClientVm = mock.MagicMock()
    self.mock_vm.RemoteCommandWithReturnCode.return_value = (
        '',
        'Some generic unexpected remote failure',
        1,
    )
    with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
      service.Execute(
          output_dir='gs://my-bucket/output',
          prompt='test prompt',
          session_id='session_1',
      )


if __name__ == '__main__':
  unittest.main()
