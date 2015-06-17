# Copyright 2014 Google Inc. All rights reserved.
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

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

AWS_PATH = 'aws'
AWS_PREFIX = [AWS_PATH, '--output', 'json']
FLAGS = flags.FLAGS


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
      '--tags']
  for key, value in kwargs.iteritems():
    tag_cmd.append('Key={0},Value={1}'.format(key, value))
  vm_util.IssueRetryableCommand(tag_cmd, retry_on_stderr=True)


def AddDefaultTags(resource_id, region):
  """Adds tags to an AWS resource created by PerfKitBenchmarker.

  By default, resources are tagged with "owner" and "perfkitbenchmarker-run"
  key-value
  pairs.

  Args:
    resource_id: An extant AWS resource to operate on.
    region: The AWS region 'resource_id' was created in.
  """
  tags = {'owner': FLAGS.owner, 'perfkitbenchmarker-run': FLAGS.run_uri}
  AddTags(resource_id, region, **tags)
