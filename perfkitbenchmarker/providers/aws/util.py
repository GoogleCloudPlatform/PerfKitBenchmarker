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

"""Utilities for working with Amazon Web Services resources."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import json
import re
import string


from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
import six

AWS_PATH = 'aws'
AWS_PREFIX = [AWS_PATH, '--output', 'json']
FLAGS = flags.FLAGS


def IsRegion(zone_or_region):
  """Returns whether "zone_or_region" is a region."""
  if not re.match(r'[a-z]{2}-[a-z]+-[0-9][a-z]?$', zone_or_region):
    raise ValueError(
        '%s is not a valid AWS zone or region name' % zone_or_region)
  return zone_or_region[-1] in string.digits


def GetRegionFromZone(zone_or_region):
  """Returns the region a zone is in (or "zone_or_region" if it's a region)."""
  if IsRegion(zone_or_region):
    return zone_or_region
  return zone_or_region[:-1]


def GetRegionFromZones(zones):
  """Returns the region a set of zones are in.

  Args:
    zones: A set of zones.
  Raises:
    Exception: if the zones are in different regions.
  """
  region = None
  for zone in zones:
    current_region = GetRegionFromZone(zone)
    if region is None:
      region = current_region
    else:
      if region != current_region:
        raise Exception('Not All zones are in the same region %s not same as '
                        '%s. zones: %s' %
                        (region, current_region, ','.join(zones)))
  return region


def GetZonesInRegion(region):
  """Returns all available zones in a given region."""
  get_zones_cmd = AWS_PREFIX + [
      'ec2',
      'describe-availability-zones',
      '--region={0}'.format(region)
  ]
  stdout, _, _ = vm_util.IssueCommand(get_zones_cmd)
  response = json.loads(stdout)
  zones = [item['ZoneName'] for item in response['AvailabilityZones']
           if item['State'] == 'available']
  return zones


def GroupZonesIntoRegions(zones):
  """Returns a map of regions to zones."""
  regions_to_zones_map = collections.defaultdict(set)
  for zone in zones:
    region = GetRegionFromZone(zone)
    regions_to_zones_map[region].add(zone)
  return regions_to_zones_map


def EksZonesValidator(value):
  if len(value) < 2:
    return False
  if any(IsRegion(zone) for zone in value):
    return False
  region = GetRegionFromZone(value[0])
  if any(GetRegionFromZone(zone) != region for zone in value):
    return False
  return True


def FormatTags(tags_dict):
  """Format a dict of tags into arguments for 'tag' parameter.

  Args:
    tags_dict: Tags to be formatted.

  Returns:
    A list of tags formatted as arguments for 'tag' parameter.
  """
  return ['Key=%s,Value=%s' % (k, v) for k, v in six.iteritems(tags_dict)]


def FormatTagSpecifications(resource_type, tags_dict):
  """Format a dict of tags into arguments for 'tag-specifications' parameter.

  Args:
    resource_type: resource type to be tagged.
    tags_dict: Tags to be formatted.

  Returns:
    A list of tags formatted as arguments for 'tag-specifications' parameter.
  """
  tags = ','.join('{Key=%s,Value=%s}' %
                  (k, v) for k, v in six.iteritems(tags_dict))
  return 'ResourceType=%s,Tags=[%s]' % (resource_type, tags)


def AddTags(resource_id, region, **kwargs):
  """Adds tags to an AWS resource created by PerfKitBenchmarker.

  Args:
    resource_id: An extant AWS resource to operate on.
    region: The AWS region 'resource_id' was created in.
    **kwargs: dict. Key-value pairs to set on the instance.
  """
  if not kwargs:
    return

  tag_cmd = AWS_PREFIX + [
      'ec2',
      'create-tags',
      '--region=%s' % region,
      '--resources', resource_id,
      '--tags'] + FormatTags(kwargs)
  IssueRetryableCommand(tag_cmd)


def MakeDefaultTags(timeout_minutes=None):
  """Default tags for an AWS resource created by PerfKitBenchmarker.

  Args:
    timeout_minutes: Timeout used for setting the timeout_utc tag.

  Returns:
    Dict of default tags, contributed from the benchmark spec.
  """
  benchmark_spec = context.GetThreadBenchmarkSpec()
  if not benchmark_spec:
    return {}
  return benchmark_spec.GetResourceTags(timeout_minutes=timeout_minutes)


def MakeFormattedDefaultTags(timeout_minutes=None):
  """Get the default tags formatted correctly for --tags parameter."""
  return FormatTags(MakeDefaultTags(timeout_minutes=timeout_minutes))


def AddDefaultTags(resource_id, region):
  """Adds tags to an AWS resource created by PerfKitBenchmarker.

  By default, resources are tagged with "owner" and "perfkitbenchmarker-run"
  key-value
  pairs.

  Args:
    resource_id: An extant AWS resource to operate on.
    region: The AWS region 'resource_id' was created in.
  """
  tags = MakeDefaultTags()
  AddTags(resource_id, region, **tags)


def GetAccount():
  """Retrieve details about the current IAM identity.

  http://docs.aws.amazon.com/cli/latest/reference/sts/get-caller-identity.html

  Returns:
    A string of the AWS account ID number of the account that owns or contains
    the calling entity.
  """

  cmd = AWS_PREFIX + ['sts', 'get-caller-identity']
  stdout, _, _ = vm_util.IssueCommand(cmd)
  return json.loads(stdout)['Account']


@vm_util.Retry()
def IssueRetryableCommand(cmd, env=None, suppress_failure=None):
  """Tries running the provided command until it succeeds or times out.

  On Windows, the AWS CLI doesn't correctly set the return code when it
  has an error (at least on version 1.7.28). By retrying the command if
  we get output on stderr, we can work around this issue.

  Args:
    cmd: A list of strings such as is given to the subprocess.Popen()
        constructor.
    env: An alternate environment to pass to the Popen command.
    suppress_failure: A function to pass to vm_util.IssueCommand()

  Returns:
    A tuple of stdout and stderr from running the provided command.
  """
  stdout, stderr, retcode = vm_util.IssueCommand(
      cmd, env=env, raise_on_failure=False, suppress_failure=suppress_failure)
  if retcode:
    raise errors.VmUtil.CalledProcessException(
        'Command returned a non-zero exit code.\n')
  if stderr:
    raise errors.VmUtil.CalledProcessException(
        'The command had output on stderr:\n%s' % stderr)
  return stdout, stderr
