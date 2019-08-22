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

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS
flags.DEFINE_string('aws_dynamodb_primarykey',
                    'primary_key',
                    'The primaryKey of dynamodb table.'
                    'This switches to sortkey if using sort.'
                    'If testing GSI/LSI, use the range keyname'
                    'of the index you want to test')
flags.DEFINE_boolean('aws_dynamodb_use_sort',
                     False,
                     'determine whether to use sort key or not')
flags.DEFINE_string('aws_dynamodb_sortkey',
                    'sort_key',
                    'The sortkey of dynamodb table.  '
                    'This switches to primarykey if using sort.'
                    'If testing GSI/LSI, use the primary keyname'
                    'of the index you want to test')
flags.DEFINE_enum('aws_dynamodb_attributetype',
                  'S', ['S', 'N', 'B'],
                  'The type of attribute, default to S (String).'
                  'Alternates are N (Number) and B (Binary).')
flags.DEFINE_integer('aws_dynamodb_read_capacity',
                     '5',
                     'Set RCU for dynamodb table')
flags.DEFINE_integer('aws_dynamodb_write_capacity',
                     '5',
                     'Set WCU for dynamodb table')
flags.DEFINE_integer('aws_dynamodb_lsi_count',
                     0, 'Set amount of Local Secondary Indexes. Only set 0-5')
flags.register_validator('aws_dynamodb_lsi_count',
                         lambda value: -1 < value < 6,
                         message='--count must be from 0-5')
flags.register_validator('aws_dynamodb_use_sort',
                         lambda sort: sort or not FLAGS.aws_dynamodb_lsi_count,
                         message='--aws_dynamodb_lsi_count requires sort key.')
flags.DEFINE_integer('aws_dynamodb_gsi_count',
                     0, 'Set amount of Global Secondary Indexes. Only set 0-5')
flags.register_validator('aws_dynamodb_gsi_count',
                         lambda value: -1 < value < 6,
                         message='--count must be from 0-5')
flags.DEFINE_boolean('aws_dynamodb_ycsb_consistentReads',
                     False,
                     "Consistent reads cost 2x eventual reads. "
                     "'false' is default which is eventual")
flags.DEFINE_integer('aws_dynamodb_connectMax', 50,
                     'Maximum number of concurrent dynamodb connections. '
                     'Defaults to 50.')


class _GetIndexes():
  """Used to create secondary indexes."""

  def __init__(self):
    self.lsi_count = FLAGS.aws_dynamodb_lsi_count
    self.gsi_count = FLAGS.aws_dynamodb_gsi_count

  def CreateLocalSecondaryIndex(self):
    """Used to create local secondary indexes."""
    lsi_items = []
    lsi_entry = []
    attr_list = []
    for lsi in range(0, self.lsi_count):
      lsi_item = ('{{"IndexName": "lsiidx{0}",'
                  '"KeySchema": [{{'
                  '"AttributeName": "{1}",'
                  '"KeyType": "HASH"}},{{'
                  '"AttributeName": "lattr{2}",'
                  '"KeyType": "RANGE"}}],'
                  '"Projection": {{'
                  '"ProjectionType": "KEYS_ONLY"}}}}'.format(
                      str(lsi),
                      FLAGS.aws_dynamodb_primarykey,
                      str(lsi)))
      lsi_entry.append(lsi_item)
      attr_list.append('{{"AttributeName": "lattr{0}","AttributeType": "{1}"}}'
                       .format(str(lsi), FLAGS.aws_dynamodb_attributetype))
    lsi_items.append('[' + ','.join(lsi_entry) + ']')
    lsi_items.append(','.join(attr_list))
    return lsi_items

  def CreateGlobalSecondaryIndex(self):
    """Used to create global secondary indexes."""
    gsi_items = []
    gsi_entry = []
    attr_list = []
    for gsi in range(0, self.gsi_count):
      gsi_item = ('{{"IndexName": "gsiidx{0}",'
                  '"KeySchema": [{{'
                  '"AttributeName": "gsikey{1}",'
                  '"KeyType": "HASH"}},{{'
                  '"AttributeName": "gattr{2}",'
                  '"KeyType": "RANGE"}}],'
                  '"Projection": {{'
                  '"ProjectionType": "KEYS_ONLY"}},'
                  '"ProvisionedThroughput": {{'
                  '"ReadCapacityUnits": {3},'
                  '"WriteCapacityUnits": {4}}}}}'.format(str(gsi),
                                                         str(gsi),
                                                         str(gsi),
                                                         5, 5))
      gsi_entry.append(gsi_item)
      attr_list.append('{{"AttributeName": "gattr{0}","AttributeType": "{1}"}}'
                       .format(str(gsi), FLAGS.aws_dynamodb_attributetype))
      attr_list.append('{{"AttributeName": "gsikey{0}","AttributeType": "{1}"}}'
                       .format(str(gsi), FLAGS.aws_dynamodb_attributetype))
    gsi_items.append('[' + ','.join(gsi_entry) + ']')
    gsi_items.append(','.join(attr_list))
    return gsi_items


class AwsDynamoDBInstance(resource.BaseResource):
  """Class for working with DynamoDB."""

  def __init__(self, table_name):
    super(AwsDynamoDBInstance, self).__init__()
    self.zone = FLAGS.zones[0]
    self.region = util.GetRegionFromZone(self.zone)
    self.primary_key = ('{{\"AttributeName\": \"{0}\",\"KeyType\": \"HASH\"}}'
                        .format(FLAGS.aws_dynamodb_primarykey))
    self.sort_key = ('{{\"AttributeName\": \"{0}\",\"KeyType\": \"RANGE\"}}'
                     .format(FLAGS.aws_dynamodb_sortkey))
    self.part_attributes = ('{{\"AttributeName\": \"{0}\",'
                            '\"AttributeType\": \"{1}\"}}'
                            .format(FLAGS.aws_dynamodb_primarykey,
                                    FLAGS.aws_dynamodb_attributetype))
    self.sort_attributes = ('{{\"AttributeName\": \"{0}\",'
                            '\"AttributeType\": \"{1}\"}}'
                            .format(FLAGS.aws_dynamodb_sortkey,
                                    FLAGS.aws_dynamodb_attributetype))
    self.table_name = table_name
    self.throughput = 'ReadCapacityUnits={read},WriteCapacityUnits={write}'.format(
        read=FLAGS.aws_dynamodb_read_capacity,
        write=FLAGS.aws_dynamodb_write_capacity)
    self.lsi_indexes = _GetIndexes().CreateLocalSecondaryIndex()
    self.gsi_indexes = _GetIndexes().CreateGlobalSecondaryIndex()

  def _Create(self):
    """Creates the dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'create-table',
        '--region', self.region,
        '--table-name', self.table_name,
        '--attribute-definitions', self.part_attributes,
        '--key-schema', self.primary_key,
        '--provisioned-throughput', self.throughput,
        '--tags'] + util.MakeFormattedDefaultTags()
    if FLAGS.aws_dynamodb_lsi_count > 0 and FLAGS.aws_dynamodb_use_sort:
      cmd[10] = (
          '[' + self.part_attributes + ', ' + self.sort_attributes + ', ' +
          self.lsi_indexes[1] + ']')
      logging.info('adding to --attribute definitions')
      cmd.append('--local-secondary-indexes')
      cmd.append(self.lsi_indexes[0])
      cmd[12] = ('[' + self.primary_key + ', ' + self.sort_key + ']')
      logging.info('adding to --key-schema')
    elif FLAGS.aws_dynamodb_use_sort:
      cmd[10] = ('[' + self.part_attributes + ', ' + self.sort_attributes + ']')
      logging.info('adding to --attribute definitions')
      cmd[12] = ('[' + self.primary_key + ', ' + self.sort_key + ']')
      logging.info('adding to --key-schema')
    if FLAGS.aws_dynamodb_gsi_count > 0:
      cmd[10] = cmd[10][:-1]
      cmd[10] += (', ' + self.gsi_indexes[1] + ']')
      logging.info('adding to --attribute definitions')
      cmd.append('--global-secondary-indexes')
      cmd.append(self.gsi_indexes[0])
    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warn('Failed to create table! {0}'.format(stderror))

  def _Delete(self):
    """Deletes the dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'delete-table',
        '--region', self.region,
        '--table-name', self.table_name]
    logging.info('Attempting deletion: ')
    vm_util.IssueCommand(cmd)

  def _IsReady(self):
    """Check if dynamodb table is ready."""
    logging.info('Getting table ready status for {0}'.format(self.table_name))
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    return result['Table']['TableStatus'] == 'ACTIVE'

  def Exists(self):
    """Returns true if the dynamodb table exists."""
    logging.info('Checking if table {0} exists'.format(self.table_name))
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    _, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return False
    else:
      return True

  def _DescribeTable(self):
    """Calls describe on dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.info(
          'Could not find table {0}, {1}'.format(self.table_name, stderr))
      return {}
    for table_info in json.loads(stdout)['Table']:
      if table_info[3] == self.table_name:
        return table_info
    return {}

  def GetEndPoint(self):
    ddbep = 'http://dynamodb.{0}.amazonaws.com'.format(self.region)
    return ddbep

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the dynamodb instance.

    Returns:
      dict mapping string property key to value.
    """
    return {
        'aws_dynamodb_primarykey': FLAGS.aws_dynamodb_primarykey,
        'aws_dynamodb_use_sort': FLAGS.aws_dynamodb_use_sort,
        'aws_dynamodb_sortkey': FLAGS.aws_dynamodb_sortkey,
        'aws_dynamodb_attributetype': FLAGS.aws_dynamodb_attributetype,
        'aws_dynamodb_read_capacity': FLAGS.aws_dynamodb_read_capacity,
        'aws_dynamodb_write_capacity': FLAGS.aws_dynamodb_write_capacity,
        'aws_dynamodb_lsi_count': FLAGS.aws_dynamodb_lsi_count,
        'aws_dynamodb_gsi_count': FLAGS.aws_dynamodb_gsi_count,
        'aws_dynamodb_consistentReads': FLAGS.aws_dynamodb_ycsb_consistentReads,
        'aws_dynamodb_connectMax': FLAGS.aws_dynamodb_connectMax,
    }
