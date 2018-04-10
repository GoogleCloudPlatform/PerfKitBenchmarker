# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing classes related to AWS CloudWatch Logs."""

from __future__ import print_function
import json
import tempfile
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util


class LogGroup(resource.BaseResource):
  """Class representing a CloudWatch log group."""

  def __init__(self, region, name, retention_in_days=7):
    super(LogGroup, self).__init__()
    self.region = region
    self.name = name
    self.retention_in_days = retention_in_days

  def _Create(self):
    """Create the log group."""
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'logs', 'create-log-group',
        '--log-group-name', self.name
    ]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Delete the log group."""
    delete_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'logs', 'delete-log-group',
        '--log-group-name', self.name
    ]
    vm_util.IssueCommand(delete_cmd)

  def Exists(self):
    """Returns True if the log group exists."""
    describe_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'logs', 'describe-log-groups',
        '--log-group-name-prefix', self.name,
        '--no-paginate'
    ]
    stdout, _, _ = vm_util.IssueCommand(describe_cmd)
    log_groups = json.loads(stdout)['logGroups']
    group = next((group for group in log_groups
                  if group['logGroupName'] == self.name), None)
    return bool(group)

  def _PostCreate(self):
    """Set the retention policy."""
    put_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'logs', 'put-retention-policy',
        '--log-group-name', self.name,
        '--retention-in-days', str(self.retention_in_days)
    ]
    vm_util.IssueCommand(put_cmd)


def GetLogs(region, stream_name, group_name, token=None):
  """Fetches the JSON formatted log stream starting at the token."""
  get_cmd = util.AWS_PREFIX + [
      '--region', region,
      'logs', 'get-log-events',
      '--start-from-head',
      '--log-group-name', group_name,
      '--log-stream-name', stream_name,
  ]
  if token:
    get_cmd.extend(['--next-token', token])
  stdout, _, _ = vm_util.IssueCommand(get_cmd)
  return json.loads(stdout)


def GetLogStreamAsString(region, stream_name, log_group):
  """Returns the messages of the log stream as a string."""
  with tempfile.TemporaryFile() as tf:
    token = None
    events = []
    while token is None or events:
      response = GetLogs(region, stream_name, log_group, token)
      events = response['events']
      token = response['nextForwardToken']
      for event in events:
        print(event['message'], file=tf)
    tf.seek(0)
    return tf.read()
