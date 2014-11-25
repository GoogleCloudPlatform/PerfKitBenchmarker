# Copyright 2014 Google Inc. All rights reserved.
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

"""A common location for all perfkitbenchmarker-defined exceptions."""

import pprint


class Error(Exception):
  pass


class VirtualMachine(object):
  """Errors raised by virtual_machine.py."""

  class RemoteExceptionError(Error):
    pass

  class VirtualMachineError(Error):
    """An error raised when VM is having an issue."""

    @classmethod
    def FromDebugInfo(cls, info, error_message):
      """Create VirtualMachineError class from debug information.

      Args:
        info: A dictionary containing debug information (such as traceroute
            info).
        error_message: the error message from the originating code.

      Returns:
        a cls exception class

      Raises:
        TypeError: if info is not an instance of dictionary.
      """
      if isinstance(info, dict):
        info = VirtualMachine.VirtualMachineError.FormatDebugInfo(
            info, error_message)
        return cls(info)
      raise TypeError('The argument of FromDebugInfo should be an instance '
                      'of dictionary.')

    @staticmethod
    def FormatDebugInfo(info, error_message):
      """A function to return a string in human readable format.

      Args:
        info: A dictionary containing debug information (such as traceroute
            info).
        error_message: the error message from the originating code.

      Returns:
        A human readable string of debug information.
      """
      sep = '\n%s\n' % ('-' * 65)

      def AddHeader(error, header, message):
        error += '{sep}{header}\n{message}\n'.format(
            sep=sep, header=header, message=message)
        return error

      def AddKeyIfExists(result, header, key):
        if key in info:
          result = AddHeader(result, header, info[key])
          del info[key]
        return result

      result = AddHeader('', 'error_message:',
                         error_message) if error_message else ''
      result = AddKeyIfExists(result, 'traceroute:', 'traceroute')
      return AddHeader(result, 'Debug Info:', pprint.pformat(info))

  class VmStateError(VirtualMachineError):
    pass


class VmUtil(object):
  """Errors raised by vm_utils.py."""

  class SshConnectionError(VirtualMachine.VirtualMachineError):
    """An error raised when VM is running but not SSHable."""
    pass

  class RestConnectionError(Error):
    pass

  class IpParsingError(Error):
    pass

  class UserSetupError(Error):
    pass


# TODO(user) Rename this class to match changes to perfkitbenchmarker_lib
class PerfKitBenchmarkerLib(object):
  """Errors raised by perfkitbenchmarker_lib.py."""

  class ThreadException(Error):
    pass

  class CalledProcessException(Error):
    pass


class Benchmarks(object):
  """Errors raised by individual benchmark."""

  class PrepareException(Error):
    pass

  class MissingObjectCredentialException(Error):
    pass

  class RunError(Error):
    pass
