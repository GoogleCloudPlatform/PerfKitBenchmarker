# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Utilities for working with SoftLayer resources."""

import json

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

SoftLayer_PATH = 'slcli'
# CPOMRS Add variables for a temporary file location and the default DomainName
tmpfile='/tmp/tmp.txt'
defaultDomain = 'perfkit.org'
# CPOMRS
SoftLayer_PREFIX = [SoftLayer_PATH]
FLAGS = flags.FLAGS


def AddTags(resource_id, region, **kwargs):
  """Adds tags to an SoftLayer resource created by PerfKitBenchmarker.
     Read existing tags first and add existing tags with new ones
  """
  if not kwargs:
      return

  describe_cmd = SoftLayer_PREFIX + [
      '--format',
      'json',
      'vs',
      'detail',
      '%s' % resource_id]

  stdout, _ = IssueRetryableCommand(describe_cmd)
  response = json.loads(stdout)
  tags = response['tags']

  tag_cmd = SoftLayer_PREFIX + [
      'vs',
      'edit']

  if tags is not None:
    for tag in tags:
      tag_cmd = tag_cmd + ['--tag', '{0}'.format(tag)]

  for key, value in kwargs.iteritems():
      tag_cmd = tag_cmd + ['--tag', '{0}:{1}'.format(key, value)]

  tag_cmd = tag_cmd + ['{0}'.format(resource_id)]
  IssueRetryableCommand(tag_cmd)


def AddDefaultTags(resource_id, region):
  """Adds tags to an SoftLayer resource created by PerfKitBenchmarker.

  By default, resources are tagged with "owner" and "perfkitbenchmarker-run"
  key-value
  pairs.

  Args:
    resource_id: An extant SoftLayer resource to operate on.
    region: The SoftLayer region 'resource_id' was created in.
  """
  tags = {'owner': FLAGS.owner, 'perfkitbenchmarker-run': FLAGS.run_uri}
  AddTags(resource_id, region, **tags)


@vm_util.Retry()
def IssueRetryableCommand(cmd, env=None):
  """Tries running the provided command until it succeeds or times out.

  Args:
    cmd: A list of strings such as is given to the subprocess.Popen()
        constructor.
    env: An alternate environment to pass to the Popen command.

  Returns:
    A tuple of stdout and stderr from running the provided command.
  """
  stdout, stderr, retcode = vm_util.IssueCommand(cmd, env=env)
  if retcode:
      raise errors.VmUtil.CalledProcessException(
          'Command returned a non-zero exit code.\n')
  if stderr:
      raise errors.VmUtil.CalledProcessException(
          'The command had output on stderr:\n%s' % stderr)
  return stdout, stderr
