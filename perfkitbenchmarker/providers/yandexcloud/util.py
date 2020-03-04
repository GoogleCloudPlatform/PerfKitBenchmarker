# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Utilities for working with Yandex.Cloud resources."""

import collections
import json
import yaml
import logging
import re
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.yandexcloud import \
    yc_machine_types
import six

if six.PY2:
  import functools32 as functools
else:
  import functools

FLAGS = flags.FLAGS
FLAVORS = yc_machine_types.FLAVORS


@functools.lru_cache()
def GetDefaultFolderId():
  """Get the default folder id."""
  conf = ParseConfig()
  if "folder-id" in conf:
      return conf["folder-id"]

  return None


def GetDefaultZone():
  """Get the default zone."""
  conf = ParseConfig()
  if "compute-default-zone" in conf:
      return conf["compute-default-zone"]

  return None


@functools.lru_cache()
def ParseConfig():
  """Parse yc config file"""
  cmd = [FLAGS.yc_path, 'config', 'list']
  stdout, _, _ = vm_util.IssueCommand(cmd)

  try:
    return yaml.safe_load(stdout)
  except yaml.YAMLError:
    return {}


def ReturnFlavor(machine_type):
    """Returns RAM and Core values based on machine_type selection."""

    logging.info('Fetching flavor specs for new VM.')
    for flavor in FLAVORS:
        if(machine_type == flavor['name']):
            return flavor['memory'], flavor['cores']


class YcCommand(object):
  """A yc command.

  Attributes:
    args: list of strings. Non-flag args to pass to yc, typically
        specifying an operation to perform (e.g. ['compute', 'images', 'list']
        to list available images).
    flags: OrderedDict mapping flag name string to flag value. Flags to pass to
        yc (e.g. {'folder-id': 'my-folder-id'}). If a provided value is
        True, the flag is passed to yc without a value. If a provided value
        is a list, the flag is passed to yc multiple times, once with each
        value in the list.
    additional_flags: list of strings. Additional flags to append unmodified to
        the end of the yc command (e.g. ['--metadata', 'color=red']).
  """

  def __init__(self, resource, *args):
    """Initializes a YcCommand with the provided args and common flags.

    Args:
      resource: A YC resource of type BaseResource.
      *args: sequence of strings. Non-flag args to pass to yc, typically
          specifying an operation to perform (e.g. ['compute', 'images', 'list']
          to list available images).
    """
    self.args = list(args)
    self.flags = collections.OrderedDict()
    self.additional_flags = []
    self._AddCommonFlags(resource)

  def GetCommand(self):
    """Generates the yc command.

    Returns:
      list of strings. When joined by spaces, forms the yc shell command.
    """
    cmd = [FLAGS.yc_path]
    cmd.extend(self.args)
    for flag_name, values in self.flags.items():
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

  def Issue(self, **kwargs):
    """Tries to run the yc command once.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueCommand when
        issuing the yc command.

    Returns:
      A tuple of stdout, stderr, and retcode from running the yc command.
    """
    return vm_util.IssueCommand(self.GetCommand(), **kwargs)

  def IssueRetryable(self, **kwargs):
    """Tries running the yc command until it succeeds or times out.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueRetryableCommand
          when issuing the yc command.

    Returns:
      (stdout, stderr) pair of strings from running the yc command.
    """
    return vm_util.IssueRetryableCommand(self.GetCommand(), **kwargs)

  def _AddCommonFlags(self, resource):
    """Adds common flags to the command.

    Adds common yc flags derived from the PKB flags and provided resource.

    Args:
      resource: A YC resource of type BaseResource.
    """
    self.flags['format'] = 'json'
    if resource:
      if resource.folder_id is not None:
        self.flags['folder-id'] = resource.folder_id
    self.additional_flags.extend(FLAGS.additional_yc_flags or ())


_LIMIT_EXCEEDED_REGEX = re.compile('The limit .* has exceeded.')


def CheckYcResponseKnownFailures(stderr, retcode):
  """Checks gcloud responses for quota exceeded errors.

  Args:
      stderr: The stderr from a gcloud command.
      retcode: The return code from a gcloud command.
  """
  if retcode and _LIMIT_EXCEEDED_REGEX.search(stderr):
    message = virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr
    logging.error(message)
    raise errors.Benchmarks.QuotaFailure(message)
  if retcode:
    raise errors.Benchmarks.RunError(stderr)


def FormatTags(tags_dict):
  """Format a dict of tags into arguments.

  Args:
    tags_dict: Tags to be formatted.

  Returns:
    A string contains formatted tags
  """
  return ','.join('{0}={1}'.format(k, v) for k, v in tags_dict.items())



def MakeFormattedDefaultTags(timeout_minutes=None):
  """Get the default tags formatted.

  Args:
    timeout_minutes: Timeout used for setting the timeout_utc tag.

  Returns:
    A string contains tags, contributed from the benchmark spec.
  """
  benchmark_spec = context.GetThreadBenchmarkSpec()
  if not benchmark_spec:
    return {}
  return FormatTags(benchmark_spec.GetResourceTags(timeout_minutes))
