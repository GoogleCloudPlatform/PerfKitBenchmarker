# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""OS Mixin class, which defines abstract properties of an OS.

"Mixin" because most real implementations will inherit from a child of both
BaseOSMixin & BaseVirtualMachine. eg CentOs7BasedAzureVirtualMachine inherits
from AzureVirtualMachine which inherits from BaseVirtualMachine & CentOs7Mixin
which inherits from BaseOSMixin. Implementation & details are somewhat
confusingly coupled with BaseVirtualMachine.
"""

import abc
from typing import Tuple

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS


class CommandInterface(metaclass=abc.ABCMeta):
  """A base class for running simple commands.

  Only supports a very limited set of functions (commands & file IO) which could
  be run on either the Runner VM or a Client VM.
  """

  def RunCommand(
      self,
      command: str | list[str],
      ignore_failure: bool = False,
      should_pre_log: bool = True,
      stack_level: int = 1,
      timeout: float | None = None,
      **kwargs,
  ) -> Tuple[str, str, int]:
    """Runs a command.

    Additional args can be supplied & are passed to lower level functions but
    aren't required.

    Args:
      command: A valid bash command in string or list form.
      ignore_failure: Ignore any failure if set to true.
      should_pre_log: Whether to print the command being run or not.
      stack_level: Number of stack frames to skip & get an "interesting" caller,
        for logging. 1 skips this function, 2 skips this & its caller, etc..
      timeout: The time to wait in seconds for the command before exiting. None
        means no timeout.
      **kwargs: Additional command arguments.

    Returns:
      A tuple of stdout and stderr from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    raise NotImplementedError()

  def WriteTemporaryFile(self, file_contents: str) -> str:
    """Writes a temporary file to the VM.

    Args:
      file_contents: The contents of the file.

    Returns:
      The full filename.
    """
    raise NotImplementedError()

  def PrepareResourcePath(
      self, resource_name: str, search_user_paths: bool = True
  ) -> str:
    """Prepares a resource from local loaders & returns path on machine.

    Loaders are searched in order until the resource is found.
    If no loader provides 'resource_name', an exception is thrown.

    If 'search_user_paths' is true, the directories specified by
    "--data_search_paths" are consulted before the default paths.

    Args:
      resource_name: string. Name of a resource.
      search_user_paths: boolean. Whether paths from "--data_search_paths"
        should be searched before the default paths.

    Returns:
      A path to the resource on the local or remote machine's filesystem.

    Raises:
      ResourceNotFound: When resource was not found.
    """
    raise NotImplementedError()


class VmUtilCommandInterface(CommandInterface):
  """Implemenation for CommandInterface that wraps vm_util commands."""

  def RunCommand(
      self,
      command: str | list[str],
      ignore_failure: bool = False,
      should_pre_log: bool = True,
      stack_level: int = 1,
      timeout: float | None = None,
      **kwargs,
  ) -> Tuple[str, str, int]:
    """Runs a command.

    Additional args can be supplied & are passed to lower level functions but
    aren't required.

    Args:
      command: A valid bash command in string or list form.
      ignore_failure: Ignore any failure if set to true.
      should_pre_log: Whether to print the command being run or not.
      stack_level: Number of stack frames to skip & get an "interesting" caller,
        for logging. 1 skips this function, 2 skips this & its caller, etc..
      timeout: The time to wait in seconds for the command before exiting. None
        means no timeout.
      **kwargs: Additional command arguments.

    Returns:
      A tuple of stdout, stderr, & return code from running the command.

    Raises:
      RemoteCommandError: If there was a problem issuing the command.
    """
    if isinstance(command, str):
      cmd_list = command.split(' ')
    else:
      cmd_list = command
    stack_level += 1
    if 'raise_on_failure' in kwargs:
      raise_on_failure = kwargs['raise_on_failure']
      del kwargs['raise_on_failure']
    else:
      raise_on_failure = not ignore_failure
    try:
      return vm_util.IssueCommand(
          cmd=cmd_list,
          timeout=timeout,
          should_pre_log=should_pre_log,
          stack_level=stack_level,
          raise_on_failure=raise_on_failure,
          **kwargs,
      )
    except errors.VmUtil.IssueCommandError as ex:
      raise errors.VirtualMachine.RemoteCommandError(str(ex)) from ex
    except errors.VmUtil.IssueCommandTimeoutError as ex:
      raise errors.VirtualMachine.RemoteCommandError(str(ex)) from ex

  def WriteTemporaryFile(self, file_contents: str) -> str:
    """Writes a temporary file to the VM.

    Args:
      file_contents: The contents of the file.

    Returns:
      The full filename.
    """
    with vm_util.NamedTemporaryFile(mode='w', delete=False) as tf:
      tf.write(file_contents)
      tf.close()
      return tf.name

  def PrepareResourcePath(
      self, resource_name: str, search_user_paths: bool = True
  ) -> str:
    """Prepares a resource from local loaders & returns path on machine.

    Loaders are searched in order until the resource is found.
    If no loader provides 'resource_name', an exception is thrown.

    If 'search_user_paths' is true, the directories specified by
    "--data_search_paths" are consulted before the default paths.

    Args:
      resource_name: string. Name of a resource.
      search_user_paths: boolean. Whether paths from "--data_search_paths"
        should be searched before the default paths.

    Returns:
      A path to the resource on the local or remote machine's filesystem.

    Raises:
      ResourceNotFound: When resource was not found.
    """
    return data.ResourcePath(resource_name, search_user_paths)
