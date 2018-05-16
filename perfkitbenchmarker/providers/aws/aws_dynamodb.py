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
TODO: derive region from endpoint.
"""

import json
import logging

from perfkitbenchmarker import resource, errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
flags.DEFINE_string('aws_dynamodb_region',
                    'us-west-1',
                    'The config for the DynamoDB instance.')

class AwsDynamoDBInstance(resource.BaseResource):

  def __init__(self, table_name, primary_key, throughput):
    super(AwsDynamoDBInstance, self).__init__()
    self.attributes = 'AttributeName=' + primary_key + ',AttributeType=S'
    self.table_name = table_name
    self.primary_key = 'AttributeName=' + primary_key + ',KeyType=HASH'
    self.throughput = throughput

  def _Create(self):
    """Creates the dynamodb table."""
    cmd = ['aws', 'dynamodb', 'create-table',
          '--region', FLAGS.aws_dynamodb_region,
          '--attribute-definitions', self.attributes,
          '--table-name', self.table_name,
          '--key-schema', self.primary_key,
          '--provisioned-throughput', self.throughput]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the table."""
    cmd = ['aws', 'dynamodb', 'delete-table',
           '--region', FLAGS.aws_dynamodb_region,
           '--table-name', self.table_name]
    vm_util.IssueCommand(cmd)

  def _IsReady(self):
    """Check if table is ready."""
    logging.info("Trying to get Table info for %s",
                 self.table_name)
    try:
      cmd = ['aws', 'dynamodb', 'describe-table',
             '--region', FLAGS.aws_dynamodb_region,
             '--table-name', self.table_name]
      stdout, stderr, retcode = vm_util.IssueCommand(cmd)
      result = json.loads(stdout)
      table_info = result['Table']
      logging.info("isready table_info:", table_info)
      table_status = table_info['TableStatus']
      if table_status == 'ACTIVE':
        logging.info("DynamoDB Table is up and running.")
        return table_info
    except errors.VirtualMachine.RemoteCommandError as e:
      raise errors.Resource.RetryableCreationError(
          "DynamoDB Table not up yet: %s." % str(e))
    else:
      raise errors.Resource.RetryableCreationError(
          "DynamoDB not up yet. Status: %s" %
          table_status)

  def _Exists(self):
    """Returns true if the table exists."""
    logging.info("Checking if table exists %s",
                 self.table_name)
    try:
      cmd = ['aws', 'dynamodb', 'describe-table',
             '--region', FLAGS.aws_dynamodb_region,
             '--table-name', self.table_name]
      stdout, stderr, retcode = vm_util.IssueCommand(cmd)
      result = json.loads(stdout)
      table_info = result['Table']
      table_status = table_info['TableStatus']
      if table_status == 'ACTIVE':
        logging.info("DynamoDB Table exists.")
        return True
      else:
        return False
    except errors.VirtualMachine.RemoteCommandError as e:
      raise errors.Resource.RetryableCreationError(
          "DynamoDB Table not up yet: %s." % str(e))

  def _DescribeTable(self):
    """Calls describe on table."""
    cmd = ['aws', 'dynamodb', 'describe-table',
            '--region', FLAGS.aws_dynamodb_region,
            '--table-name', self.table_name]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    if retcode != 0:
      logging.info('Could not find table %s, %s', self.table_name, stderr)
      return {}
    result = json.loads(stdout)
    for table_info in result['Table']:
      if table_info[3] == self.table_name:
        return table_info
    return {}
