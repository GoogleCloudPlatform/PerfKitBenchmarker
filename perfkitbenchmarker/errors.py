# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
from typing import Type

from perfkitbenchmarker import benchmark_status


class Error(Exception):
  STATUS = benchmark_status.FailedSubstatus.UNCATEGORIZED


class Setup:
  """Errors raised in setting up PKB."""

  class PythonPackageRequirementUnfulfilled(Error):
    """Error raised when a Python package requirement is unfulfilled."""

    pass

  class MissingExecutableError(Error):
    """Error raised when we cannot find an executable we need."""

    pass

  class NoRunURIError(Error):
    """Error raised when we were not given a run_uri and cannot infer it."""

    pass

  class BadRunURIError(Error):
    """Error raised when the given run_uri is invalid."""

    pass

  class BadPreprovisionedDataError(Error):
    """Error raised when the pre-provisioned data is invalid."""

    pass

  class InvalidSetupError(Error):
    """Error raised when SetUpPKB was not called correctly."""

    pass

  class InvalidFlagConfigurationError(Error):
    """Error raised when the set of command line flags is invalid."""

    pass

  class InvalidConfigurationError(Error):
    """Error raised when configuration is invalid."""

    pass


class VirtualMachine:
  """Errors raised by virtual_machine.py."""

  class RemoteCommandError(Error):
    """Error raised when a Remote Command or Remote Copy fails."""

    pass

  class RemoteExceptionError(Error):
    pass

  class AuthError(Error):
    """Error raised when one VM cannot access another VM."""

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
            info, error_message
        )
        return cls(info)
      raise TypeError(
          'The argument of FromDebugInfo should be an instance of dictionary.'
      )

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
            sep=sep, header=header, message=message
        )
        return error

      def AddKeyIfExists(result, header, key):
        if key in info:
          result = AddHeader(result, header, info[key])
          del info[key]
        return result

      result = (
          AddHeader('', 'error_message:', error_message)
          if error_message
          else ''
      )
      result = AddKeyIfExists(result, 'traceroute:', 'traceroute')
      return AddHeader(result, 'Debug Info:', pprint.pformat(info))

  class VmStateError(VirtualMachineError):
    pass


class VmUtil:
  """Errors raised by vm_util.py."""

  class RestConnectionError(Error):
    pass

  class IpParsingError(Error):
    pass

  class UserSetupError(Error):
    pass

  class ThreadException(Error):
    pass

  class CalledProcessException(Error):
    pass

  class IssueCommandError(Error):
    pass

  class IssueCommandTimeoutError(Error):
    pass


class Benchmarks:
  """Errors raised by individual benchmark."""

  class BucketCreationError(Error):
    pass

  class PrepareException(Error):
    pass

  class MissingObjectCredentialException(Error):
    pass

  class RunError(Error):
    pass

  class InsufficientCapacityCloudFailure(Error):
    STATUS = benchmark_status.FailedSubstatus.INSUFFICIENT_CAPACITY

  class QuotaFailure(Error):
    """Errors that are related to insufficient quota on cloud provider."""

    STATUS = benchmark_status.FailedSubstatus.QUOTA

    class RateLimitExceededError(Error):
      pass

  class KnownIntermittentError(Error):
    """Known intermittent failures of the benchmark.

    These are non-retryable, known failure modes of the benchmark.  It is
    recommended that the benchmark be completely re-run.
    """

    STATUS = benchmark_status.FailedSubstatus.KNOWN_INTERMITTENT

  class UnsupportedConfigError(Error):
    """Errors due to an unsupported configuration running.

    This is a retryable error when --zone=any.
    """

    STATUS = benchmark_status.FailedSubstatus.UNSUPPORTED


class Resource:
  """Errors related to resource creation and deletion."""

  class CreationError(Error):
    """An error on creation which is not retryable."""

    pass

  class CreationInternalError(Error):
    """An error on creation internal to the resource (cloud).

    These errors should not be retried, but instead cleanly retry creating the
    resource (probably by rerunning the benchmark).
    """

    STATUS = benchmark_status.FailedSubstatus.KNOWN_INTERMITTENT

  class CleanupError(Error):
    pass

  class RetryableCreationError(Error):
    pass

  class RetryableDeletionError(Error):
    pass

  class GetError(Error):
    """An error on get which is not retryable."""

    pass

  class RetryableGetError(Error):
    pass

  class UpdateError(Error):
    """An error on update."""

    pass

  class SubclassNotFoundError(Error):
    pass

  class RestoreError(Error):
    """Errors while restoring a resource."""

    STATUS = benchmark_status.FailedSubstatus.RESTORE_FAILED

  class FreezeError(Error):
    """Errors while freezing a resource."""

    STATUS = benchmark_status.FailedSubstatus.FREEZE_FAILED

  class ProvisionTimeoutError(Error):
    """Timeout errors while provisioning a resource."""

    STATUS = benchmark_status.FailedSubstatus.KNOWN_INTERMITTENT


class Config:
  """Errors related to configs."""

  class InvalidValue(Error):
    """User provided an invalid value for a config option."""

    STATUS = benchmark_status.FailedSubstatus.INVALID_VALUE

  class MissingOption(Error):
    """User did not provide a value for a required config option."""

    pass

  class ParseError(Error):
    """Error raised when a config can't be loaded properly."""

    pass

  class UnrecognizedOption(Error):
    """User provided a value for an unrecognized config option."""

    pass


class Juju:
  """Errors related to the Juju OS_TYPE."""

  class TimeoutException(Error):
    pass

  class UnitErrorException(Error):
    pass


def _GetAllErrorSubclasses(cls: Type[Error]) -> list[Type[Error]]:
  """Recursively finds all subclasses of Error."""
  subclasses = []
  for subclass in cls.__subclasses__():
    subclasses.append(subclass)
    subclasses.extend(_GetAllErrorSubclasses(subclass))
  return subclasses


def GetBenchmarkStatusFromException(
    e: BaseException,
) -> str:
  """Returns the benchmark status from the exception.

  When exceptions happen on on background theads (e.g. CreationInternalError on
  CreateAndBootVm) they are not propogated as exceptions to the caller, instead
  they are propagated as text inside a wrapper exception such as
  errors.VmUtil.ThreadException.

  Args:
    e: The exception instance to inspect.

  Returns:
    The benchmark status of the exception or its wrapped exception.
  """
  if isinstance(e, KeyboardInterrupt) or KeyboardInterrupt.__name__ in str(e):
    return benchmark_status.FailedSubstatus.PROCESS_KILLED

  if (
      isinstance(e, Error)
      and e.STATUS != benchmark_status.FailedSubstatus.UNCATEGORIZED
  ):
    return e.STATUS

  e_str = str(e)
  for cls in _GetAllErrorSubclasses(Error):
    if (
        cls.STATUS != benchmark_status.FailedSubstatus.UNCATEGORIZED
        and cls.__name__ in e_str
    ):
      return cls.STATUS

  return benchmark_status.FailedSubstatus.UNCATEGORIZED
