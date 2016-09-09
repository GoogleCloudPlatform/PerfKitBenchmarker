# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Utilities for working with DigitalOcean resources."""

import json
import logging

from perfkitbenchmarker import vm_util

# Default configuration for action status polling.
DEFAULT_ACTION_WAIT_SECONDS = 10
DEFAULT_ACTION_MAX_TRIES = 90


def DoctlAndParse(arg_list):
  """Run a doctl command and parse the output.

  Args:
    arg_list: a list of arguments for doctl. Will be formated with
      str() before being sent to the process.

  Returns:
    A tuple of
      - doctl's JSON output, pre-parsed, or None if output is empty.
      - doctl's return code

  Raises:
    errors.VmUtil.CalledProcessError if doctl fails.
  """

  stdout, _, retcode = vm_util.IssueCommand(
      ['doctl'] +
      [str(arg) for arg in arg_list] +
      ['--output=json'])

  # In case of error, doctl sometimes prints "null" before printing a
  # JSON error string to stdout. TODO(noahl): improve parsing of
  # error messages.
  if retcode and stdout.startswith('null'):
    output = stdout[4:]
  else:
    output = stdout

  if output:
    return json.loads(output), retcode
  else:
    return None, retcode


class ActionInProgressException(Exception):
  """Exception to indicate that a DigitalOcean action is in-progress."""
  pass


class ActionFailedError(Exception):
  """Exception raised when a DigitalOcean action fails."""
  pass


class UnknownStatusError(Exception):
  """Exception raised for an unknown status message."""
  pass


@vm_util.Retry(poll_interval=DEFAULT_ACTION_WAIT_SECONDS,
               max_retries=DEFAULT_ACTION_MAX_TRIES,
               retryable_exceptions=ActionInProgressException)
def WaitForAction(action_id):
  """Wait until a VM action completes."""
  response, retcode = DoctlAndParse(
      ['compute', 'action', 'get', action_id])
  if retcode:
    logging.warn('Unexpected action lookup failure.')
    raise ActionFailedError('Failed to get action %s' % action_id)

  status = response[0]['status']
  logging.debug('action %d: status is "%s".', action_id, status)
  if status == 'completed':
    return
  elif status == 'in-progress':
    raise ActionInProgressException()
  elif status == 'errored':
    raise ActionFailedError('Action %s errored' % action_id)
  else:
    raise UnknownStatusError('Action %s had unknown status "%s"' %
                             (action_id, status))
