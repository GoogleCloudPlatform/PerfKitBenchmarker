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
"""Utilities for working with Rackspace Cloud Platform resources."""

from collections import OrderedDict

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS


class RackCLICommand(object):
  """A rack cli command.

  Attributes:
    args: list of strings. Positional args to pass to rack cli, typically
        specifying an operation to perform (e.g. ['servers', 'images', 'list']
        to list available images).
    flags: OrderedDict mapping flag name string to flag value. Flags to pass to
        rack cli (e.g. {'image-id': 'some-image-id'}).
    additional_flags: list of strings. Additional flags to append unmodified to
        the end of the rack cli command (e.g. ['--metadata', 'owner=user']).
  """

  def __init__(self, resource, *args):
    """Initialize a RackCLICommand with the provided args and common flags.

    Args:
      resource: A Rackspace resource of type BaseResource.
      *args: sequence of strings. Positional args to pass to rack cli, typically
          specifying an operation to perform. (e.g. ['servers', 'image', 'list']
          to list available images).
    """
    self.resource = resource
    self.args = list(args)
    self.flags = OrderedDict()
    self.additional_flags = []

  def __repr__(self):
    return '{0}({1})'.format(type(self).__name__, ' '.join(self._GetCommand()))

  def Issue(self, **kwargs):
    """Tries running the rack cli command once.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueCommand when
          issuing the rack cli command.

    Returns:
      A tuple of stdout, stderr, and retcode from running the rack cli command.
    """
    return vm_util.IssueCommand(self._GetCommand(), **kwargs)

  def IssueRetryable(self, **kwargs):
    """Tries running the rack cli command until it succeeds or times out.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueRetryableCommand
          when issuing the rack cli command.

    Returns:
      (stdout, stderr) pair of strings from running the rack cli command.
    """
    return vm_util.IssueRetryableCommand(self._GetCommand(), **kwargs)

  def _AddCommonFlags(self, resource):
    """Adds common flags to the command.

    Adds common rack cli flags derived from the PKB flags and provided resource.

    Args:
      resource: A Rackspace resource of type BaseResource.
    """
    self.flags['output'] = 'json'
    if hasattr(resource, 'profile') and resource.profile is not None:
      self.flags['profile'] = resource.profile
    if hasattr(resource, 'region'):
      self.flags['region'] = resource.region
    self.additional_flags.extend(FLAGS.additional_rackspace_flags or ())

  def _GetCommand(self):
    """Generates the rack cli command.

    Returns:
        list of strings. When joined by spaces, form the rack cli command.
    """
    cmd = [FLAGS.rack_path]
    cmd.extend(self.args)
    self._AddCommonFlags(self.resource)
    for flag_name, value in self.flags.iteritems():
      flag_name_str = '--{0}'.format(flag_name)
      if value is True:
        cmd.append(flag_name_str)
      else:
        cmd.append(flag_name_str)
        cmd.append(str(value))
    cmd.extend(self.additional_flags)
    return cmd
