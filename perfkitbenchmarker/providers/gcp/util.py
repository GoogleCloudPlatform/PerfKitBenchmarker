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

from collections import OrderedDict
import json
import logging
import re
import functools32

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS


@functools32.lru_cache()
def GetDefaultProject():
  """Get the default project."""
  cmd = [FLAGS.gcloud_path, 'config', 'list', '--format=json']
  stdout, _, _ = vm_util.IssueCommand(cmd)
  result = json.loads(stdout)
  return result['core']['project']


def GetRegionFromZone(zone):
  return zone[:-2]


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
    self.flags = OrderedDict()
    self.additional_flags = []
    self._AddCommonFlags(resource)

  def _GetCommand(self):
    """Generates the gcloud command.

    Returns:
      list of strings. When joined by spaces, forms the gcloud shell command.
    """
    cmd = [FLAGS.gcloud_path]
    cmd.extend(self.args)
    for flag_name, values in self.flags.iteritems():
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
    return '{0}({1})'.format(type(self).__name__, ' '.join(self._GetCommand()))

  def Issue(self, **kwargs):
    """Tries running the gcloud command once.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueCommand when
          issuing the gcloud command.

    Returns:
      A tuple of stdout, stderr, and retcode from running the gcloud command.
    """
    return vm_util.IssueCommand(self._GetCommand(), **kwargs)

  def IssueRetryable(self, **kwargs):
    """Tries running the gcloud command until it succeeds or times out.

    Args:
      **kwargs: Keyword arguments to forward to vm_util.IssueRetryableCommand
          when issuing the gcloud command.

    Returns:
      (stdout, stderr) pair of strings from running the gcloud command.
    """
    return vm_util.IssueRetryableCommand(self._GetCommand(), **kwargs)

  def _AddCommonFlags(self, resource):
    """Adds common flags to the command.

    Adds common gcloud flags derived from the PKB flags and provided resource.

    Args:
      resource: A GCE resource of type BaseResource.
    """
    self.flags['format'] = 'json'
    self.flags['quiet'] = True
    if resource.project is not None:
      self.flags['project'] = resource.project
    if hasattr(resource, 'zone') and resource.zone:
      self.flags['zone'] = resource.zone
    self.additional_flags.extend(FLAGS.additional_gcloud_flags or ())


_QUOTA_EXCEEDED_REGEX = re.compile('Quota \'.*\' exceeded.')
_QUOTA_EXCEEDED_MESSAGE = ('Creation failed due to quota exceeded: ')

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
    message = _QUOTA_EXCEEDED_MESSAGE + stderr
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
