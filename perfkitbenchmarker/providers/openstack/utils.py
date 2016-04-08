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
"""Utilities for working with OpenStack Cloud resources."""

from collections import OrderedDict

import logging
import os
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

FIVE_COLUMNS_ROW_REGEX = (r'\|\s+(\S+)\s+\|\s+(\S+)\s+\|\s+(\S+)\s+'
                          r'\|\s+(\S+)\s+\|\s+(\S+)\s+\|')
FIVE_COLUMNS_PATTERN = re.compile(FIVE_COLUMNS_ROW_REGEX)


def ParseNovaTable(output, regex_pattern, key_names):
  stdout_lines = output.split('\n')
  groups = (regex_pattern.match(line) for line in stdout_lines)
  tuples = (g.groups() for g in groups if g)
  dict_list = []
  try:
    next(tuples)  # Skip table header
    dict_list = [dict(zip(key_names, t)) for t in tuples]
  except StopIteration:
    pass  # Empty list
  return dict_list


def ParseServerGroupTable(output):
  """Returns a dict with key/values returned from a Nova CLI formatted table.

  Returns:
    dict with key/value of the server-group.
  """
  keys = ('id', 'name', 'policies', 'members', 'metadata',)
  server_group_list = ParseNovaTable(output, FIVE_COLUMNS_PATTERN, keys)
  assert len(server_group_list) == 1, 'Server group is not unique.'
  return server_group_list[0]


def ParseFloatingIPTable(output):
  """Returns a list of dicts with floating IPs."""
  keys = ('id', 'ip', 'instance_id', 'fixed_ip', 'pool',)
  floating_ip_list = ParseNovaTable(output, FIVE_COLUMNS_PATTERN, keys)
  for floating_ip in floating_ip_list:
    if floating_ip['instance_id'] == '-':
      floating_ip['instance_id'] = None
    if floating_ip['fixed_ip'] == '-':
      floating_ip['fixed_ip'] = None
  return floating_ip_list


class OpenStackCLICommand(object):
  """An openstack cli command.

  Attributes:
    args: list of strings. Positional args to pass to openstack, typically
        specifying an operation to perform (e.g. ['image', 'list'] to list
        available images).
    flags: OrderedDict mapping flag name string to flag value. Flags to pass to
        openstack cli (e.g. {'os-compute-api-version': '2'}). If a provided
        value is True, the flag is passed to openstack cli without a value. If
        a provided value is a list, the flag is passed to openstack cli multiple
        times, once with each value in the list.
    additional_flags: list of strings. Additional flags to append unmodified to
        the end of the openstack cli command.
  """

  def __init__(self, resource, *args):
    """Initializes an OpenStackCLICommand with the provided args and common
    flags.

    Args:
      resource: An OpenStack resource of type BaseResource.
      *args: sequence of strings. Positional args to pass to openstack cli,
          typically specifying an operation to perform (e.g. ['image', 'list']
          to list available images).
    """
    self.args = list(args)
    self.flags = OrderedDict()
    self.additional_flags = []
    self._AddCommonFlags(resource)

  def __repr__(self):
    return '{0}({1})'.format(type(self).__name__, ' '.join(self._GetCommand()))

  def _GetCommand(self):
    """Generates the openstack cli command.

    Returns:
      list of strings. When joined by spaces, forms the openstack cli command.
    """
    cmd = [FLAGS.openstack_cli_path]
    cmd.extend(self.args)
    for flag_name, values in self.flags.iteritems():
      flag_name_str = '--%s' % flag_name
      if values is True:
        cmd.append(flag_name_str)
      else:
        values_iterable = values if isinstance(values, list) else [values]
        for value in values_iterable:
          cmd.append(flag_name_str)
          cmd.append(str(value))
    cmd.extend(self.additional_flags)
    return cmd

  def Issue(self, **kwargs):
    """Tries running the openstack cli command once.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueCommand when
          issuing the openstack cli command.

    Returns:
      A tuple of stdout, stderr, and retcode from running the openstack command.
    """
    return vm_util.IssueCommand(self._GetCommand(), **kwargs)

  def IssueRetryable(self, **kwargs):
    """Tries running the openstack cli command until it succeeds or times out.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueRetryableCommand
          when issuing the openstack cli command.

    Returns:
      (stdout, stderr) pair of strings from running the openstack command.
    """
    return vm_util.IssueRetryableCommand(self._GetCommand(), **kwargs)

  def _AddCommonFlags(self, resource):
    """Adds common flags to the command.

    Adds common openstack  flags derived from the PKB flags and provided
    resource.

    Args:
      resource: An OpenStack resource of type BaseResource.
    """
    self.flags['format'] = 'json'
    self.additional_flags.extend(FLAGS.openstack_additional_flags or ())


class NovaClient(object):

  def __getattribute__(self, item):
    try:
      return super(NovaClient, self).__getattribute__(item)
    except AttributeError:
      return self.__client.__getattribute__(item)

  def GetPassword(self):
    # For compatibility with Nova CLI, use 'OS'-prefixed environment value
    # if present. Also support reading the password from a file.

    error_msg = ('No OpenStack password specified. '
                 'Either set the environment variable OS_PASSWORD to the '
                 'admin password, or provide the name of a file '
                 'containing the password using the OPENSTACK_PASSWORD_FILE'
                 ' environment variable or --openstack_password_file flag.')

    password = os.getenv('OS_PASSWORD')
    if password is not None:
      return password
    try:
      password_file = os.path.expanduser(FLAGS.openstack_password_file)
      with open(password_file) as pwfile:
        password = pwfile.readline().rstrip()
        return password
    except IOError as e:
      raise Exception(error_msg + ' ' + str(e))

  def __init__(self):
    from keystoneclient import session as ksc_session
    from keystoneclient.auth.identity import v2
    from novaclient import client as nova

    self.url = FLAGS.openstack_auth_url
    self.user = FLAGS.openstack_username
    self.tenant = FLAGS.openstack_tenant
    self.endpoint_type = FLAGS.openstack_nova_endpoint_type
    self.http_log_debug = FLAGS.log_level == 'debug'
    self.password = self.GetPassword()

    self.__auth = v2.Password(auth_url=self.url,
                              username=self.user,
                              password=self.password,
                              tenant_name=self.tenant)
    self._session = ksc_session.Session(auth=self.__auth)
    self.__client = nova.Client(version='2', session=self._session,
                                http_log_debug=self.http_log_debug)
    # Set requests library logging level to WARNING
    # so it doesn't spam logs with unhelpful messages,
    # such as 'Starting new HTTP Connection'.
    rq_logger = logging.getLogger('requests')
    rq_logger.setLevel(logging.WARNING)
