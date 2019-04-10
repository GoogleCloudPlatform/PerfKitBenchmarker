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
"""Utilities for working with Google Cloud Platform resources."""

import collections
import json
import logging
import re
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
import six

if six.PY2:
  import functools32 as functools
else:
  import functools

FLAGS = flags.FLAGS


@functools.lru_cache()
def GetDefaultProject():
  """Get the default project."""
  cmd = [FLAGS.gcloud_path, 'config', 'list', '--format=json']
  stdout, _, _ = vm_util.IssueCommand(cmd)
  result = json.loads(stdout)
  return result['core']['project']


@functools.lru_cache()
def GetDefaultUser():
  """Get the default project."""
  cmd = [FLAGS.gcloud_path, 'config', 'list', '--format=json']
  stdout, _, _ = vm_util.IssueCommand(cmd)
  result = json.loads(stdout)
  return result['core']['account']


def GetRegionFromZone(zone):
  """Returns the region name from a fully-qualified zone name.

  Each fully-qualified GCP zone name is formatted as <region>-<zone> where, for
  example, each region looks like us-central1, europe-west1, or asia-east1.
  Therefore, we pull the first two parts the fully qualified zone name delimited
  by a dash and assume the rest is the name of the zone. See
  https://cloud.google.com/compute/docs/regions-zones for more information.

  Args:
    zone: The fully-qualified name of a GCP zone.
  """
  parts = zone.split('-')
  return '-'.join(parts[:2])


def GetMultiRegionFromRegion(region):
  """Gets the closest multi-region location to the region."""
  if (region.startswith('us') or
      region.startswith('northamerica') or
      region.startswith('southamerica')):
    return 'us'
  elif region.startswith('europe'):
    return 'eu'
  elif region.startswith('asia') or region.startswith('australia'):
    return 'asia'
  else:
    raise Exception('Unknown region "%s".' % region)


def IssueCommandFunction(cmd, **kwargs):
  """Use vm_util to issue the given command.

  Args:
    cmd: the gcloud command to run
    **kwargs: additional arguments for the gcloud command

  Returns:
    stdout, stderr, retcode tuple from running the command
  """
  return vm_util.IssueCommand(cmd.GetCommand(), **kwargs)


def IssueRetryableCommandFunction(cmd, **kwargs):
  """Use vm_util to issue the given retryable command.

  Args:
    cmd: the gcloud command to run
    **kwargs: additional arguments for the gcloud command

  Returns:
    stdout, stderr, tuple from running the command
  """
  return vm_util.IssueRetryableCommand(cmd.GetCommand(), **kwargs)


# The function that is used to issue a command, when given a GcloudCommand
# object and additional arguments. Can be overridden.
_issue_command_function = IssueCommandFunction

# The function that is used to issue a retryable command, when given a
# GcloudCommand object and additional arguments. Can be overridden.
_issue_retryable_command_function = IssueRetryableCommandFunction


def SetIssueCommandFunction(func):
  """Set the issue command function to be the given function.

  Args:
    func: the function to run when issuing a GcloudCommand.
  """
  global _issue_command_function
  _issue_command_function = func


def SetIssueRetryableCommandFunction(func):
  """Set the issue retryable command function to be the given function.

  Args:
    func: the function to run when issuing a GcloudCommand.
  """
  global _issue_retryable_command_function
  _issue_retryable_command_function = func


class GcloudCommand(object):
  """A gcloud command.

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
    """Tries to run the gcloud command once.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueCommand when
        issuing the gcloud command.

    Returns:
      A tuple of stdout, stderr, and retcode from running the gcloud command.
    """
    return _issue_command_function(self, **kwargs)

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


_QUOTA_EXCEEDED_REGEX = re.compile('Quota \'.*\' exceeded.')

_NOT_ENOUGH_RESOURCES_STDERR = ('does not have enough resources available to '
                                'fulfill the request.')
_NOT_ENOUGH_RESOURCES_MESSAGE = 'Creation failed due to not enough resources: '


def CheckGcloudResponseKnownFailures(stderr, retcode):
  """Checks gcloud responses for quota exceeded errors.

  Args:
      stderr: The stderr from a gcloud command.
      retcode: The return code from a gcloud command.
  """
  if retcode and _QUOTA_EXCEEDED_REGEX.search(stderr):
    message = virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr
    logging.error(message)
    raise errors.Benchmarks.QuotaFailure(message)
  if retcode and _NOT_ENOUGH_RESOURCES_STDERR in stderr:
    message = _NOT_ENOUGH_RESOURCES_MESSAGE + stderr
    logging.error(message)
    raise errors.Benchmarks.InsufficientCapacityCloudFailure(message)


def AuthenticateServiceAccount(vm, vm_gcloud_path='gcloud'):
  """Authorize gcloud to access Cloud Platform with a Google service account.

  If you want gcloud (and other tools in the Cloud SDK) to use service account
  credentials to make requests, use this method to authenticate.
  Account name is provided by FLAGS.gcp_service_account
  Credentials are fetched from a file whose local path is provided by
  FLAGS.gcp_service_account_key_filethat. It contains private authorization key.

  Args:
    vm: vm on which the gcloud library needs to be authenticated.
    vm_gcloud_path: Optional path to the gcloud binary on the vm.
  """
  vm.PushFile(FLAGS.gcp_service_account_key_file)
  activate_cmd = ('{} auth activate-service-account {} --key-file={}'
                  .format(vm_gcloud_path, FLAGS.gcp_service_account,
                          FLAGS.gcp_service_account_key_file.split('/')[-1]))
  vm.RemoteCommand(activate_cmd)


def InstallGcloudComponents(vm, vm_gcloud_path='gcloud', component='alpha'):
  """Install gcloud components on the target vm.

  Args:
    vm: vm on which the gcloud's alpha components need to be installed.
    vm_gcloud_path: Optional path to the gcloud binary on the vm.
    component: Gcloud component to install.
  """
  install_cmd = '{} components install {} --quiet'.format(vm_gcloud_path,
                                                          component)
  vm.RemoteCommand(install_cmd)


def FormatTags(tags_dict):
  """Format a dict of tags into arguments.

  Args:
    tags_dict: Tags to be formatted.

  Returns:
    A string contains formatted tags
  """
  return ','.join('{0}={1}'.format(k, v) for k, v in tags_dict.iteritems())


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
