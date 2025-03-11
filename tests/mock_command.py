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

"""A fancier mock for RemoteCommand & IssueCommand.

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
import logging
from typing import Any
from unittest import mock
from absl.testing import absltest
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util


# stdout, stderr, returncode
ReturnValues = tuple[str, str] | tuple[str, str, int]


class MockCommand:
  """A mock for BaseVirtualMachine.RemoteCommand & vm_util.IssueCommand.

  Attributes:
    progress_through_calls: A dictionary of how many times each call has been
      made.
    call_to_response: A dictionary of commands to a list of responses. Commands
      just need to be a substring of the actual command. Each response is given
      in order, like with mock's normal iterating side_effect.
    default_return_value: The value to return if no command is found.
  """

  def __init__(
      self,
      call_to_response: dict[str, list[ReturnValues]],
      mock_command_function: Any,
      default_return_value: ReturnValues = ('', ''),
  ):
    self.progress_through_calls = collections.defaultdict(int)
    self.call_to_response = call_to_response
    self.default_return_value = default_return_value

    mock_command_function.side_effect = self.mock_remote_command

  def mock_remote_command(self, cmd: str | list[str], **kwargs) -> ReturnValues:
    """Mocks a command, returning the next response for the command."""
    if isinstance(cmd, list):
      try:
        cmd = ' '.join(cmd)
      except TypeError as ex:
        logging.warning(
            'Tried joining command %s but not all elements were strings. Got'
            ' exception: %s',
            cmd,
            ex,
        )
        raise ex
    for call in self.call_to_response:
      if call in cmd:
        call_num = self.progress_through_calls[call]
        if len(self.call_to_response[call]) <= call_num:
          call_num = len(self.call_to_response[call]) - 1
        response = self.call_to_response[call][call_num]
        if not response and isinstance(self.call_to_response[call], tuple):
          # Tester passed in one tuple rather than a list of tuples.
          response = self.call_to_response[call]
        self.progress_through_calls[call] += 1
        if (
            (
                (len(response) == 3 and response[2] != 0)
                or (len(response) == 2 and response[1])
            )
            and kwargs.get('raise_on_failure', True)
            and not kwargs.get('ignore_failure', False)
        ):
          raise errors.VmUtil.IssueCommandError(response[1])
        return response
    return self.default_return_value


class MockRemoteCommand(MockCommand):
  """A mock for BaseVirtualMachine.RemoteCommand."""

  def __init__(
      self,
      call_to_response: dict[str, list[tuple[str, str]]],
      vm: virtual_machine.BaseVirtualMachine,
  ):
    super().__init__(call_to_response, vm.RemoteCommand)


class MockIssueCommand(MockCommand):
  """A mock for vm_util.IssueCommand."""

  def __init__(
      self,
      call_to_response: dict[str, list[tuple[str, str, int]]],
      test_case: absltest.TestCase,
  ):
    self.func_to_mock = test_case.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
        )
    )
    super().__init__(call_to_response, self.func_to_mock, ('', '', 0))
