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
import functools
import json
import logging
import re
from typing import Any, Set
from absl import flags
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

RATE_LIMITED_MESSAGE = 'Rate Limit Exceeded'

# When too many read reqeusts are issued, receive a message like:
# PERMISSION_DENIED: Quota exceeded for quota metric 'Read requests' and limit
# 'Read requests per minute' of service 'compute.googleapis.com'
# or
# Quota exceeded for quota metric 'Heavy-weight read requests' and limit
# 'Heavy-weight read requests per minute' of service 'compute.googleapis.com'
READ_LIMITED_MESSAGE_REGEX = re.compile(
    r'Quota exceeded .*[Rr]ead requests per minute'
)


# regex to check API limits when tagging resources
# matches a string like:
# ERROR: (gcloud.compute.disks.add-labels) PERMISSION_DENIED: Quota exceeded
# for quota group 'ReadGroup' and limit 'Read requests per 100 seconds' of
# service 'compute.googleapis.com' for consumer 'project_number:012345678901'.
TAGGING_RATE_LIMITED_REGEX = re.compile(
    "Quota exceeded for quota group '.*?' and limit "
    "'.*? per.*?seconds' of service 'compute.googleapis.com'"
)
RATE_LIMITED_MAX_RETRIES = 10
# 200s is chosen because 1) quota is measured in 100s intervals and 2) fuzzing
# causes a random number between 100 and this to be chosen.
RATE_LIMITED_MAX_POLLING_INTERVAL = 200
# This must be set. Otherwise, calling Issue() will fail in util_test.py.
RATE_LIMITED_FUZZ = 0.5
RATE_LIMITED_TIMEOUT = 1200
STOCKOUT_MESSAGE = (
    'Creation failed due to insufficient capacity indicating a '
    'potential stockout scenario.'
)


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


def GetProjectNumber(project_id: str | None = None) -> str:
  """Get the number of the default project."""
  # All GCP projects have both a project number & a project id. The project id
  # is the human-readable name of the project.
  if not project_id:
    project_id = GetDefaultProject()
  cmd = [
      FLAGS.gcloud_path,
      'projects',
      'list',
      f'--filter=name:{project_id}',
      '--format=json',
  ]
  stdout, _, _ = vm_util.IssueCommand(cmd)
  result = json.loads(stdout)
  return result[0]['projectNumber']


def GetRegionFromZone(zone) -> str:
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


def IsRegion(location: str) -> bool:
  """Determine if a zone or region is a region."""
  return bool(re.fullmatch(r'[a-z]+-[a-z]+[0-9]', location))


def GetAllZones() -> Set[str]:
  """Gets a list of valid zones."""
  cmd = GcloudCommand(None, 'compute', 'zones', 'list')
  cmd.flags['format'] = 'value(name)'
  stdout, _, _ = cmd.Issue()
  return set(stdout.splitlines())


def GetAllRegions() -> Set[str]:
  """Gets a list of valid regions."""
  cmd = GcloudCommand(None, 'compute', 'regions', 'list')
  cmd.flags['format'] = 'value(name)'
  stdout, _, _ = cmd.Issue()
  return set(stdout.splitlines())


def GetZonesInRegion(region) -> Set[str]:
  """Gets a list of zones for the given region."""
  cmd = GcloudCommand(None, 'compute', 'zones', 'list')
  cmd.flags.update({
      'filter': f"region='{region}'",
      'format': 'value(name)',
  })
  stdout, _, _ = cmd.Issue()
  return set(stdout.splitlines())


def GetZonesFromMachineType(machine_type: str) -> Set[str]:
  """Gets a list of zones for the given machine type."""
  cmd = GcloudCommand(None, 'compute', 'machine-types', 'list')
  cmd.flags.update({
      'filter': f"name='{machine_type}'",
      'format': 'value(zone)',
  })
  stdout, _, _ = cmd.Issue()
  zones_with_machine_type = set(stdout.splitlines())
  all_usable_zones = GetAllZones()
  if zones_with_machine_type:
    # Under some circumstances machine type lists can contain zones that we do
    # not have permission to use in the given project. Intersect with all usable
    # zones.
    return zones_with_machine_type & all_usable_zones
  return all_usable_zones


def GetGeoFromRegion(region: str) -> str:
  """Gets valid geo from the region, i.e. region us-central1 returns us."""
  return region.split('-')[0]


def GetRegionsInGeo(geo: str) -> Set[str]:
  """Gets valid regions in the geo."""
  return {region for region in GetAllRegions() if region.startswith(geo)}


def GetMultiRegionFromRegion(region: str):
  """Gets the closest multi-region location to the region."""
  if (
      region.startswith('us')
      or region.startswith('northamerica')
      or region.startswith('southamerica')
  ):
    return 'us'
  elif region.startswith('europe'):
    return 'eu'
  elif region.startswith('asia') or region.startswith('australia'):
    return 'asia'
  else:
    raise Exception('Unknown region "%s".' % region)


def IssueCommandFunction(cmd: 'GcloudCommand', **kwargs):
  """Use vm_util to issue the given command.

  Args:
    cmd: the gcloud command to run
    **kwargs: additional arguments for the gcloud command

  Returns:
    stdout, stderr, retcode tuple from running the command
  """
  kwargs['stack_level'] = kwargs.get('stack_level', 1) + 1
  return vm_util.IssueCommand(cmd.GetCommand(), **kwargs)


def IssueRetryableCommandFunction(cmd: 'GcloudCommand', **kwargs):
  """Use vm_util to issue the given retryable command.

  Args:
    cmd: the gcloud command to run
    **kwargs: additional arguments for the gcloud command

  Returns:
    stdout, stderr, tuple from running the command
  """
  kwargs['stack_level'] = kwargs.get('stack_level', 1) + 1
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


class GcloudCommand:
  """A gcloud command.

  Attributes:
    args: list of strings. Non-flag args to pass to gcloud, typically specifying
      an operation to perform (e.g. ['compute', 'images', 'list'] to list
      available images).
    flags: OrderedDict mapping flag name string to flag value. Flags to pass to
      gcloud (e.g. {'project': 'my-project-id'}). If a provided value is True,
      the flag is passed to gcloud without a value. If a provided value is a
      list, the flag is passed to gcloud multiple times, once with each value in
      the list.
    additional_flags: list of strings. Additional flags to append unmodified to
      the end of the gcloud command (e.g. ['--metadata', 'color=red']).
    rate_limited: boolean. True if rate limited, False otherwise.
    use_alpha_gcloud: boolean. Defaults to False.
    use_beta_gcloud: boolean. Defaults to False.
  """

  def __init__(self, common_resource: resource.BaseResource | None, *args):
    """Initializes a GcloudCommand with the provided args and common flags.

    Args:
      common_resource: A GCE resource of type BaseResource.
      *args: sequence of strings. Non-flag args to pass to gcloud, typically
        specifying an operation to perform (e.g. ['compute', 'images', 'list']
        to list available images).
    """
    self.args = list(args)
    self.flags = collections.OrderedDict()
    self.additional_flags = []
    self._AddCommonFlags(common_resource)
    self.rate_limited = False
    self.use_alpha_gcloud = False
    self.use_beta_gcloud = False

  def GetCommand(self):
    """Generates the gcloud command.

    Returns:
      list of strings. When joined by spaces, forms the gcloud shell command.

    Raises:
      ValueError: if passed a None value
    """
    cmd = [FLAGS.gcloud_path]
    cmd.extend(self.args)
    for flag_name, values in sorted(self.flags.items()):
      flag_name_str = '--{}'.format(flag_name)
      if values is True:
        cmd.append(flag_name_str)
      elif values is None:
        raise ValueError(f'Flag {flag_name} is None. Please filter out.')
      else:
        values_iterable = values if isinstance(values, list) else [values]
        for value in values_iterable:
          cmd.append(flag_name_str)
          cmd.append(str(value))
    cmd.extend(self.additional_flags)
    if self.use_alpha_gcloud and len(cmd) > 1 and cmd[1] != 'alpha':
      cmd.insert(1, 'alpha')

    if self.use_beta_gcloud and len(cmd) > 1 and cmd[1] != 'beta':
      cmd.insert(1, 'beta')
    return cmd

  def __repr__(self):
    return '{}({})'.format(type(self).__name__, ' '.join(self.GetCommand()))

  @staticmethod
  def _IsIssueRateLimitMessage(text: str) -> bool:
    return bool(
        RATE_LIMITED_MESSAGE in text
        or READ_LIMITED_MESSAGE_REGEX.search(text)
        or TAGGING_RATE_LIMITED_REGEX.search(text)
    )

  @vm_util.Retry(
      poll_interval=RATE_LIMITED_MAX_POLLING_INTERVAL,
      max_retries=RATE_LIMITED_MAX_RETRIES,
      fuzz=RATE_LIMITED_FUZZ,
      timeout=RATE_LIMITED_TIMEOUT,
      retryable_exceptions=(
          errors.Benchmarks.QuotaFailure.RateLimitExceededError,
      ),
  )
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
      # Increase stack_level by 2 to compensate for Retry.
      kwargs['stack_level'] = kwargs.get('stack_level', 1) + 2
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
    kwargs['stack_level'] = kwargs.get('stack_level', 1) + 1
    return _issue_retryable_command_function(self, **kwargs)

  def _AddCommonFlags(self, common_resource: resource.BaseResource | None):
    """Adds common flags to the command.

    Adds common gcloud flags derived from the PKB flags and provided resource.

    Args:
      common_resource: A GCE resource of type BaseResource.
    """
    self.flags['format'] = 'json'
    self.flags['quiet'] = True
    if common_resource:
      if (
          hasattr(common_resource, 'project')
          and common_resource.project is not None
      ):
        self.flags['project'] = common_resource.project
      if hasattr(common_resource, 'zone') and common_resource.zone:
        self.flags['zone'] = common_resource.zone
    if FLAGS.project:
      # If resource did not set the flag use the global default.
      self.flags.setdefault('project', FLAGS.project)
    self.additional_flags.extend(FLAGS.additional_gcloud_flags or ())


_QUOTA_EXCEEDED_REGEX = re.compile(
    r"(Quota '.*' exceeded|Insufficient .*\w+.* quota)"
)

# Resource availability errors documented at
# https://cloud.google.com/compute/docs/resource-error#resource_availability
_NOT_ENOUGH_RESOURCES_ERROR_SNIPPETS = (
    'does not have enough resources available to fulfill the request.',
    'ZONE_RESOURCE_POOL_EXHAUSTED',
)
_NOT_ENOUGH_RESOURCES_MESSAGE = 'Creation failed due to not enough resources: '

_INVALID_MACHINE_TYPE_REGEX = re.compile(
    r"Invalid value for field 'resource.machineType': '.*?'. "
    r"Machine type with name '.*?' does not exist in zone '.*?'."
)
_INVALID_MACHINE_TYPE_MESSAGE = 'Creation failed due to invalid machine type: '

_UNSUPPORTED_NODE_TYPE_REGEX = re.compile(
    "The resource 'projects/.*?/zones/.*?/nodeTypes/.*?' was not found",
)
_UNSUPPORTED_NODE_TYPE_MESSAGE = (
    'Creation failed due to unsupported node type: '
)


def CheckGcloudResponseKnownFailures(stderr: str, retcode: int):
  """Checks gcloud responses for quota exceeded errors.

  Args:
      stderr: The stderr from a gcloud command.
      retcode: The return code from a gcloud command.
  """
  if retcode:
    if _QUOTA_EXCEEDED_REGEX.search(stderr):
      message = virtual_machine.QUOTA_EXCEEDED_MESSAGE + stderr
      logging.error(message)
      raise errors.Benchmarks.QuotaFailure(message)
    for snippet in _NOT_ENOUGH_RESOURCES_ERROR_SNIPPETS:
      if snippet in stderr:
        message = _NOT_ENOUGH_RESOURCES_MESSAGE + stderr
        logging.error(message)
        raise errors.Benchmarks.InsufficientCapacityCloudFailure(message)
    if _INVALID_MACHINE_TYPE_REGEX.search(stderr):
      message = _INVALID_MACHINE_TYPE_MESSAGE + stderr
      raise errors.Benchmarks.UnsupportedConfigError(message)
    if _UNSUPPORTED_NODE_TYPE_REGEX.search(stderr):
      message = _UNSUPPORTED_NODE_TYPE_MESSAGE + stderr
      raise errors.Benchmarks.UnsupportedConfigError(message)


def AuthenticateServiceAccount(
    vm: virtual_machine.BaseVirtualMachine,
    vm_gcloud_path: str = 'gcloud',
    benchmark: Any = None,
):
  """Authorize gcloud to access Google Cloud Platform with a service account.

  If you want gcloud (and other tools in the Cloud SDK) to use service account
  credentials to make requests, use this method to authenticate.
  Account name is provided by FLAGS.gcp_service_account
  Credentials are fetched from a file whose local path is provided by
  FLAGS.gcp_service_account_key_file, which contains private authorization key.
  In the absence of a locally supplied credential file, the file is retrieved
  from pre-provisioned data bucket.

  Args:
    vm: vm on which the gcloud library needs to be authenticated.
    vm_gcloud_path: Optional path to the gcloud binary on the vm.
    benchmark: The module for retrieving the associated service account file.
  """
  if not FLAGS.gcp_service_account:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Authentication requires the service account name to be '
        'specified via --gcp_service_account.'
    )
  if not FLAGS.gcp_service_account_key_file:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Authentication requires the service account credential json to be '
        'specified via --gcp_service_account_key_file.'
    )
  if '/' in FLAGS.gcp_service_account_key_file:
    vm.PushFile(FLAGS.gcp_service_account_key_file, vm_util.VM_TMP_DIR)
    key_file_name = FLAGS.gcp_service_account_key_file.split('/')[-1]
  else:
    vm.InstallPreprovisionedBenchmarkData(
        benchmark, [FLAGS.gcp_service_account_key_file], vm_util.VM_TMP_DIR
    )
    key_file_name = FLAGS.gcp_service_account_key_file
  activate_cmd = '{} auth activate-service-account {} --key-file={}/{}'.format(
      vm_gcloud_path,
      FLAGS.gcp_service_account,
      vm_util.VM_TMP_DIR,
      key_file_name,
  )
  vm.RemoteCommand(activate_cmd)


def InstallGcloudComponents(
    vm: virtual_machine.BaseVirtualMachine,
    vm_gcloud_path: str = 'gcloud',
    component: str = 'alpha',
):
  """Install gcloud components on the target vm.

  Args:
    vm: vm on which the gcloud's alpha components need to be installed.
    vm_gcloud_path: Optional path to the gcloud binary on the vm.
    component: Gcloud component to install.
  """
  install_cmd = '{} components install {} --quiet'.format(
      vm_gcloud_path, component
  )
  vm.RemoteCommand(install_cmd)


def FormatTags(tags_dict: dict[str, str]):
  """Format a dict of tags into arguments.

  Args:
    tags_dict: Tags to be formatted.

  Returns:
    A string contains formatted tags
  """
  return ','.join('{}={}'.format(k, v) for k, v in sorted(tags_dict.items()))


def SplitTags(tags: str):
  """Formats a string of joined tags into a dictionary.

  Args:
    tags: A string containing tags formatted as key1=value1,key2=value2,...

  Returns:
    An OrderedDict mapping tag keys to values in the order the tags were given.
  """
  return collections.OrderedDict(
      tag_pair.split('=', 1) for tag_pair in tags.split(',')
  )


def GetDefaultTags(timeout_minutes: int | None = None):
  """Get the default tags in a dictionary.

  Args:
    timeout_minutes: Timeout used for setting the timeout_utc tag.

  Returns:
    A dict of tags, contributed from the benchmark spec.
  """
  benchmark_spec = context.GetThreadBenchmarkSpec()
  if not benchmark_spec:
    return {}
  return benchmark_spec.GetResourceTags(timeout_minutes)


def MakeFormattedDefaultTags(timeout_minutes: int | None = None):
  """Get the default tags formatted.

  Args:
    timeout_minutes: Timeout used for setting the timeout_utc tag.

  Returns:
    A string contains tags, contributed from the benchmark spec.
  """
  return FormatTags(GetDefaultTags(timeout_minutes))


def GetAccessToken(application_default: bool = True) -> str:
  """Gets the access token for the default project.

  Args:
    application_default: whether to use application-default in gcloud args.

  Returns:
    Text string of the access token.
  """
  cmd = [FLAGS.gcloud_path, 'auth']
  if application_default:
    cmd.append('application-default')
  cmd.append('print-access-token')
  stdout, _, _ = vm_util.IssueCommand(cmd)
  return stdout.strip()


def SetupPrivateServicesAccess(network: str, project: str) -> None:
  """Setup private services access for the network.

  Private services access is used by several GCP services and enables VMs to
  connect to service producers using internal IP addressses. See
  https://cloud.google.com/vpc/docs/configure-private-services-access for more
  info.

  Args:
    network: The network (VPC) name to setup private services access for.
    project: The project to setup private services access for.
  """
  cmd = GcloudCommand(
      None,
      'compute',
      'addresses',
      'create',
      'google-service-range',
  )
  cmd.flags['global'] = True
  cmd.flags['prefix-length'] = 16
  cmd.flags['purpose'] = 'VPC_PEERING'
  cmd.flags['network'] = network
  cmd.flags['project'] = project
  cmd.Issue(raise_on_failure=False)

  cmd = GcloudCommand(None, 'services', 'vpc-peerings', 'connect')
  cmd.flags['service'] = 'servicenetworking.googleapis.com'
  cmd.flags['network'] = network
  cmd.flags['ranges'] = 'google-service-range'
  cmd.flags['project'] = project
  _, stderr, _ = cmd.Issue(raise_on_failure=False)

  # There are create errors when creating the connection for a second time.
  # The workaround is to update the connection:
  if 'Please use UpdateConnection' in stderr:
    cmd = GcloudCommand(
        None,
        'services',
        'vpc-peerings',
        'update',
    )
    cmd.flags['network'] = network
    cmd.flags['project'] = project
    cmd.flags['ranges'] = 'google-service-range'
    cmd.flags['force'] = True
    cmd.Issue(raise_on_failure=False)
