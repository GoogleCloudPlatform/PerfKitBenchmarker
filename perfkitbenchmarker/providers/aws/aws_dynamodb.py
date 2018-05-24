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
"""Module containing class for AWS' dynamodb tables.

Tables can be created and deleted.
"""

import json
import logging

from perfkitbenchmarker import resource
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS
flags.DEFINE_enum('aws_dynamodb_attributetype',
                  'S', ['S', 'N', 'B'],
                  'The type of attribute, default to S (String).'
                  'Alternates are N (Number) and B (Binary).')
flags.DEFINE_string('aws_dynamodb_capacity',
                    'ReadCapacityUnits=5,WriteCapacityUnits=5',
                    'Set RCU/WCU for dynamodb table')


class AwsDynamoDBInstance(resource.BaseResource):

  def __init__(self, region, table_name, primary_key):
    super(AwsDynamoDBInstance, self).__init__()
    self.region = region
    self.attributes = 'AttributeName={0},AttributeType={1}'\
                      .format(primary_key, FLAGS.aws_dynamodb_attributetype)
    self.table_name = table_name
    self.primary_key = 'AttributeName={0},KeyType=HASH'.format(primary_key)
    self.throughput = FLAGS.aws_dynamodb_capacity

  def _Create(self):
    """Creates the dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'create-table',
        '--region', self.region,
        '--attribute-definitions', self.attributes,
        '--table-name', self.table_name,
        '--key-schema', self.primary_key,
        '--provisioned-throughput', self.throughput]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'delete-table',
        '--region', self.region,
        '--table-name', self.table_name]
    logging.info('Attempting deletion: ')
    vm_util.IssueCommand(cmd)

  def _IsReady(self):
    """Check if table is ready."""
    logging.info('Getting table ready status for {0}'.format(self.table_name))
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    stdout, _, retcode = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    return result['Table']['TableStatus'] == 'ACTIVE'

  def Exists(self):
    """Returns true if the table exists."""
    logging.info('Checking if table {0} exists'.format(self.table_name))
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    stdout, _, retcode = vm_util.IssueCommand(cmd)
    if retcode != 0:
      return False
    else:
      return True

  def _DescribeTable(self):
    """Calls describe on table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    if retcode != 0:
      logging.info('Could not find table {0}, {1}'.format(self.table_name, stderr))
      return {}
    for table_info in json.loads(stdout)['Table']:
      if table_info[3] == self.table_name:
        return table_info
    return {}
