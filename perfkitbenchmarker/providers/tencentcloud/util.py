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
  """A tccli command."""


  def __init__(self, *args):
    """Initializes a tccliCommand with the provided args and common flags.

      *args: sequence of strings. Non-flag args to pass to tccli, typically
          specifying an operation to perform (e.g. ['compute', 'images', 'list']
          to list available images).
    """
    self.args = list(args)
    self.flags = collections.OrderedDict()
    self.additional_flags = []
    self._AddCommonFlags()

  def GetCommand(self):
    """Generates the tccli command.

    Returns:
      list of strings. When joined by spaces, forms the tccli shell command.
    """
    cmd = [FLAGS.tencent_path, FLAGS.unfold]
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

  @vm_util.Retry()
  def Issue(self, **kwargs):
    """Tries to run the tccli command once, retrying if Rate Limited."""
    try:
      stdout, stderr, retcode = vm_util.IssueCommand(self.GetCommand(), **kwargs)
    except errors.VmUtil.IssueCommandError as error:
      raise error

    return stdout, stderr, retcode

  def IssueRetryable(self, **kwargs):
    """Tries running the tccli command until it succeeds or times out."""
    return vm_util.IssueRetryableCommand(self.GetCommand(), **kwargs)

  def _AddCommonFlags(self):
    """Adds common flags to the command.

    Adds common tccli flags derived from the PKB flags and provided resource.
    """
    self.flags['format'] = 'json'