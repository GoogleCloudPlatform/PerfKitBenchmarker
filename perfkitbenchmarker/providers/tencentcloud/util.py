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
"""Utilities for working with Tencent Cloud resources."""

import collections
import functools
import json
import logging
import re
from typing import Set
from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
import six

FLAGS = flags.FLAGS

def GetDefaultImage():
    pass

def GetRegionFromZone():
    pass

class TccliCommand(object):
  """A tccli command.

  Attributes:
    args: list of strings. Non-flag args to pass to gcloud, typically
        specifying an operation to perform (e.g. ['compute', 'images', 'list']
        to list available images).
    flags: OrderedDict mapping flag name string to flag value. Flags to pass to
        gcloud (e.g. {'project': 'my-project-id'}). If a provided value is
        True, the flag is passed to gcloud without a value. If a provided value
        is a list, the flag is passed to gcloud multiple times, once with each
        value in the list.
    additional_flags: list of strings. Additional flags to append unmodified to
        the end of the gcloud command (e.g. ['--metadata', 'color=red']).
  """

  def __init__(self, resource, *args):
    """Initializes a GcloudCommand with the provided args and common flags.

    Args:
      resource: A GCE resource of type BaseResource.
      *args: sequence of strings. Non-flag args to pass to gcloud, typically
          specifying an operation to perform (e.g. ['compute', 'images', 'list']
          to list available images).
    """
    self.args = list(args)
    self.flags = collections.OrderedDict()
    self.additional_flags = []
    self._AddCommonFlags(resource)

  def GetCommand(self):
    """Generates the gcloud command.

    Returns:
      list of strings. When joined by spaces, forms the gcloud shell command.
    """
    cmd = [FLAGS.gcloud_path]
    cmd.extend(self.args)
    for flag_name, values in sorted(self.flags.items()):
      flag_name_str = '--{0}'.format(flag_name)
      if values is True:
        cmd.append(flag_name_str)
      else:
        values_iterable = values if isinstance(values, list) else [values]
        for value in values_iterable:
          cmd.append(flag_name_str)
          cmd.append(str(value))
    cmd.extend(self.additional_flags)

    return cmd

  def __repr__(self):
    return '{0}({1})'.format(type(self).__name__, ' '.join(self.GetCommand()))

  @staticmethod
  def _IsIssueRateLimitMessage(text) -> bool:
    if RATE_LIMITED_MESSAGE in text:
      return True
    match = TAGGING_RATE_LIMITED_REGEX.search(text)
    if match:
      return True
    return False

  @vm_util.Retry(
      poll_interval=RATE_LIMITED_MAX_POLLING_INTERVAL,
      max_retries=RATE_LIMITED_MAX_RETRIES,
      fuzz=RATE_LIMITED_FUZZ,
      timeout=RATE_LIMITED_TIMEOUT,
      retryable_exceptions=(
          errors.Benchmarks.QuotaFailure.RateLimitExceededError,))
  def Issue(self, **kwargs):
    """Tries to run the gcloud command once, retrying if Rate Limited.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueCommand when
        issuing the gcloud command.

    Returns:
      A tuple of stdout, stderr, and retcode from running the gcloud command.
    Raises:
      RateLimitExceededError: if command fails with Rate Limit Exceeded.
      QuotaFailure: if command fails without Rate Limit Exceeded and
      retry_on_rate_limited is set to false
      IssueCommandError: if command fails without Rate Limit Exceeded.

    """
    try:
      stdout, stderr, retcode = _issue_command_function(self, **kwargs)
    except errors.VmUtil.IssueCommandError as error:
      error_message = str(error)
      if GcloudCommand._IsIssueRateLimitMessage(error_message):
        self._RaiseRateLimitedException(error_message)
      else:
        raise error
    if retcode and GcloudCommand._IsIssueRateLimitMessage(stderr):
      self._RaiseRateLimitedException(stderr)

    return stdout, stderr, retcode

  def _RaiseRateLimitedException(self, error):
    """Raise rate limited exception based on the retry_on_rate_limited flag.

    Args:
      error: Error message to raise

    Raises:
      RateLimitExceededError: if command fails with Rate Limit Exceeded and
      retry_on_rate_limited is set to true
      QuotaFailure: if command fails without Rate Limit Exceeded and
      retry_on_rate_limited is set to false
    """
    self.rate_limited = True
    if FLAGS.retry_on_rate_limited:
      raise errors.Benchmarks.QuotaFailure.RateLimitExceededError(error)
    raise errors.Benchmarks.QuotaFailure(error)

  def IssueRetryable(self, **kwargs):
    """Tries running the gcloud command until it succeeds or times out.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueRetryableCommand
        when issuing the gcloud command.

    Returns:
      (stdout, stderr) pair of strings from running the gcloud command.
    """
    return _issue_retryable_command_function(self, **kwargs)

  def _AddCommonFlags(self, resource):
    """Adds common flags to the command.

    Adds common gcloud flags derived from the PKB flags and provided resource.

    Args:
      resource: A GCE resource of type BaseResource.
    """
    self.flags['format'] = 'json'
    self.flags['quiet'] = True
    if resource:
      if resource.project is not None:
        self.flags['project'] = resource.project
      if hasattr(resource, 'zone') and resource.zone:
        self.flags['zone'] = resource.zone
    self.additional_flags.extend(FLAGS.additional_gcloud_flags or ())