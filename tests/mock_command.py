# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

"""A fancier mock for BaseVirtualMachine.RemoteCommand.

Allows for specifying different responses for different commands.
Usage:
  Within test class:
    self.vm = (
      mock.create_autospec(linux_virtual_machine.BaseLinuxVirtualMachine))
    self.mock_cmd = mock_command.MockRemoteCommand(
        {
            'command1': [('response1', 'stdout'), ('response2', 'stdout')],
            'command2': [('response3', 'stdout')],
        },
        self.vm,
    )
    # Act - Some test code calls vm.RemoteCommand..
    # Assert.
    self.assertEqual(self.mock_cmd.progress_through_calls['command1'], 2)
    # Or regular asserts on vm.RemoteCommand
    self.vm.RemoteCommand.assert_any_call('command1')
"""

import collections
from unittest import mock
from perfkitbenchmarker import virtual_machine


class MockCommand:
  """A mock for BaseVirtualMachine.RemoteCommand & vm_util.IssueCommand.

  Attributes:
    progress_through_calls: A dictionary of how many times each call has been
      made.
    call_to_response: A dictionary of commands to a list of responses. Commands
      just need to be a substring of the actual command. Each response is given
      in order, like with mock's normal iterating side_effect.
  """

  def __init__(
      self,
      call_to_response: dict[str, list[tuple[str, str]]],
      mock_command_function: mock.MagicMock,
  ):
    self.progress_through_calls = collections.defaultdict(int)
    self.call_to_response = call_to_response

    def mock_remote_command(
        cmd: str, **kwargs
    ) -> tuple[str, str]:
      del kwargs  # Unused but matches type signature.
      for call in self.call_to_response:
        if call in cmd:
          call_num = self.progress_through_calls[call]
          if len(call_to_response[call]) <= call_num:
            call_num = len(call_to_response[call]) - 1
          response = call_to_response[call][call_num]
          self.progress_through_calls[call] += 1
          return response
      return '', ''
    mock_command_function.side_effect = mock_remote_command


class MockRemoteCommand(MockCommand):
  """A mock for BaseVirtualMachine.RemoteCommand.

  Attributes:
    progress_through_calls: A dictionary of how many times each call has been
      made.
    call_to_response: A dictionary of commands to a list of responses. Commands
      just need to be a substring of the actual command. Each response is given
      in order, like with mock's normal iterating side_effect.
  """

  def __init__(
      self,
      call_to_response: dict[str, list[tuple[str, str]]],
      vm: virtual_machine.BaseVirtualMachine,
  ):
    super().__init__(call_to_response, vm.RemoteCommand)
