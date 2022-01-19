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
from typing import Any, Collection, Dict, List, Optional, Tuple, Sequence

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import non_relational_db
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

# Throughput constants
_FREE_TIER_RCU = 25
_FREE_TIER_WCU = 25


class AwsDynamoDBInstance(non_relational_db.BaseNonRelationalDb):
  """Class for working with DynamoDB."""
  SERVICE_TYPE = non_relational_db.DYNAMODB

  def __init__(self, table_name: str, **kwargs):
    super(AwsDynamoDBInstance, self).__init__(**kwargs)
    self.table_name = table_name
    self.zone = FLAGS.zones[0] if FLAGS.zones else FLAGS.zone[0]
    self.region = util.GetRegionFromZone(self.zone)
    self.resource_arn: str = None  # Set during the _Exists() call.

    self.rcu = FLAGS.aws_dynamodb_read_capacity
    self.wcu = FLAGS.aws_dynamodb_write_capacity
    self.throughput = (
        f'ReadCapacityUnits={self.rcu},WriteCapacityUnits={self.wcu}')

    self.primary_key = FLAGS.aws_dynamodb_primarykey
    self.sort_key = FLAGS.aws_dynamodb_sortkey
    self.use_sort = FLAGS.aws_dynamodb_use_sort
    self.attribute_type = FLAGS.aws_dynamodb_attributetype

    self.lsi_count = FLAGS.aws_dynamodb_lsi_count
    self.lsi_indexes = self._CreateLocalSecondaryIndex()
    self.gsi_count = FLAGS.aws_dynamodb_gsi_count
    self.gsi_indexes = self._CreateGlobalSecondaryIndex()

  def _CreateLocalSecondaryIndex(self) -> List[str]:
    """Used to create local secondary indexes."""
    lsi_items = []
    lsi_entry = []
    attr_list = []
    for lsi in range(0, self.lsi_count):
      lsi_item = json.dumps({
          'IndexName': f'lsiidx{str(lsi)}',
          'KeySchema': [{
              'AttributeName': self.primary_key,
              'KeyType': 'HASH'
          }, {
              'AttributeName': f'lattr{str(lsi)}',
              'KeyType': 'RANGE'
          }],
          'Projection': {
              'ProjectionType': 'KEYS_ONLY'
          }
      })
      lsi_entry.append(lsi_item)
      attr_list.append(
          json.dumps({
              'AttributeName': f'lattr{str(lsi)}',
              'AttributeType': self.attribute_type
          }))
    lsi_items.append('[' + ','.join(lsi_entry) + ']')
    lsi_items.append(','.join(attr_list))
    return lsi_items

  def _CreateGlobalSecondaryIndex(self) -> List[str]:
    """Used to create global secondary indexes."""
    gsi_items = []
    gsi_entry = []
    attr_list = []
    for gsi in range(0, self.gsi_count):
      gsi_item = json.dumps({
          'IndexName': f'gsiidx{str(gsi)}',
          'KeySchema': [{
              'AttributeName': f'gsikey{str(gsi)}',
              'KeyType': 'HASH'
          }, {
              'AttributeName': f'gattr{str(gsi)}',
              'KeyType': 'RANGE'
          }],
          'Projection': {
              'ProjectionType': 'KEYS_ONLY'
          },
          'ProvisionedThroughput': {
              'ReadCapacityUnits': 5,
              'WriteCapacityUnits': 5
          }
      })
      gsi_entry.append(gsi_item)
      attr_list.append(
          json.dumps({
              'AttributeName': f'gattr{str(gsi)}',
              'AttributeType': self.attribute_type
          }))
      attr_list.append(
          json.dumps({
              'AttributeName': f'gsikey{str(gsi)}',
              'AttributeType': self.attribute_type
          }))
    gsi_items.append('[' + ','.join(gsi_entry) + ']')
    gsi_items.append(','.join(attr_list))
    return gsi_items

  def _SetAttrDefnArgs(self, cmd: List[str],
                       args: Sequence[str]) -> None:
    attr_def_args = _MakeArgs(args)
    cmd[10] = f'[{attr_def_args}]'
    logging.info('adding to --attribute-definitions')

  def _SetKeySchemaArgs(self, cmd: List[str],
                        args: Sequence[str]) -> None:
    key_schema_args = _MakeArgs(args)
    cmd[12] = f'[{key_schema_args}]'
    logging.info('adding to --key-schema')

  def _PrimaryKeyJson(self) -> str:
    return json.dumps({'AttributeName': self.primary_key, 'KeyType': 'HASH'})

  def _PrimaryAttrsJson(self) -> str:
    return json.dumps({
        'AttributeName': self.primary_key,
        'AttributeType': self.attribute_type
    })

  def _SortAttrsJson(self) -> str:
    return json.dumps({
        'AttributeName': self.sort_key,
        'AttributeType': self.attribute_type
    })

  def _SortKeyJson(self) -> str:
    return json.dumps({'AttributeName': self.sort_key, 'KeyType': 'RANGE'})

  def _Create(self) -> None:
    """Creates the dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'create-table',
        '--region', self.region,
        '--table-name', self.table_name,
        '--attribute-definitions', self._PrimaryAttrsJson(),
        '--key-schema', self._PrimaryKeyJson(),
        '--provisioned-throughput', self.throughput,
        '--tags'
    ] + util.MakeFormattedDefaultTags()
    if self.lsi_count > 0 and self.use_sort:
      self._SetAttrDefnArgs(cmd, [
          self._PrimaryAttrsJson(),
          self._SortAttrsJson(), self.lsi_indexes[1]
      ])
      cmd.append('--local-secondary-indexes')
      cmd.append(self.lsi_indexes[0])
      self._SetKeySchemaArgs(
          cmd, [self._PrimaryKeyJson(),
                self._SortKeyJson()])
    elif self.use_sort:
      self._SetAttrDefnArgs(
          cmd, [self._PrimaryAttrsJson(),
                self._SortAttrsJson()])
      self._SetKeySchemaArgs(
          cmd, [self._PrimaryKeyJson(),
                self._SortKeyJson()])
    if self.gsi_count > 0:
      self._SetAttrDefnArgs(
          cmd, cmd[10].strip('[]').split(',') + [self.gsi_indexes[1]])
      cmd.append('--global-secondary-indexes')
      cmd.append(self.gsi_indexes[0])
    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warning('Failed to create table! %s', stderror)

  def _Delete(self) -> None:
    """Deletes the dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'delete-table',
        '--region', self.region,
        '--table-name', self.table_name]
    logging.info('Attempting deletion: ')
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _IsReady(self) -> bool:
    """Check if dynamodb table is ready."""
    logging.info('Getting table ready status for %s', self.table_name)
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    return result['Table']['TableStatus'] == 'ACTIVE'

  def _Exists(self) -> bool:
    """Returns true if the dynamodb table exists."""
    logging.info('Checking if table %s exists', self.table_name)
    result = self._DescribeTable()
    if not result:
      return False
    if not self.resource_arn:
      self.resource_arn = result['TableArn']
    return True

  def _DescribeTable(self) -> Dict[Any, Any]:
    """Calls describe on dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region', self.region,
        '--table-name', self.table_name]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find table %s, %s', self.table_name, stderr)
      return {}
    return json.loads(stdout)['Table']

  def GetEndPoint(self) -> str:
    return f'http://dynamodb.{self.region}.amazonaws.com'

  def GetResourceMetadata(self) -> Dict[str, Any]:
    """Returns a dict containing metadata about the dynamodb instance.

    Returns:
      dict mapping string property key to value.
    """
    return {
        'aws_dynamodb_primarykey': self.primary_key,
        'aws_dynamodb_use_sort': self.use_sort,
        'aws_dynamodb_sortkey': self.sort_key,
        'aws_dynamodb_attributetype': self.attribute_type,
        'aws_dynamodb_read_capacity': self.rcu,
        'aws_dynamodb_write_capacity': self.wcu,
        'aws_dynamodb_lsi_count': self.lsi_count,
        'aws_dynamodb_gsi_count': self.gsi_count,
    }

  def SetThroughput(self,
                    rcu: Optional[int] = None,
                    wcu: Optional[int] = None) -> None:
    """Updates the table's rcu and wcu."""
    if not rcu:
      rcu = self.rcu
    if not wcu:
      wcu = self.wcu
    cmd = util.AWS_PREFIX + [
        'dynamodb', 'update-table',
        '--table-name', self.table_name,
        '--region', self.region,
        '--provisioned-throughput',
        f'ReadCapacityUnits={rcu},WriteCapacityUnits={wcu}',
    ]
    logging.info('Setting %s table provisioned throughput to %s rcu and %s wcu',
                 self.table_name, rcu, wcu)
    util.IssueRetryableCommand(cmd)
    while not self._IsReady():
      continue

  def _GetThroughput(self) -> Tuple[int, int]:
    """Returns the current (rcu, wcu) of the table."""
    output = self._DescribeTable()['ProvisionedThroughput']
    return output['ReadCapacityUnits'], output['WriteCapacityUnits']

  @vm_util.Retry(poll_interval=1, max_retries=3,
                 retryable_exceptions=(errors.Resource.CreationError))
  def _GetTagResourceCommand(self, tags: Sequence[str]) -> Sequence[str]:
    """Returns the tag-resource command with the provided tags.

    This function will retry up to max_retries to allow for instance creation to
    finish.

    Args:
      tags: List of formatted tags to append to the instance.

    Returns:
      A list of arguments for the 'tag-resource' command.

    Raises:
      errors.Resource.CreationError: If the current instance does not exist.
    """
    if not self._Exists():
      raise errors.Resource.CreationError(
          f'Cannot get resource arn of non-existent instance {self.table_name}')
    return util.AWS_PREFIX + [
        'dynamodb', 'tag-resource', '--resource-arn', self.resource_arn,
        '--region', self.region, '--tags'
    ] + list(tags)

  def UpdateWithDefaultTags(self) -> None:
    """Adds default tags to the table."""
    tags = util.MakeFormattedDefaultTags()
    cmd = self._GetTagResourceCommand(tags)
    logging.info('Setting default tags on table %s', self.table_name)
    util.IssueRetryableCommand(cmd)

  def UpdateTimeout(self, timeout_minutes: int) -> None:
    """Updates the timeout associated with the table."""
    tags = util.MakeFormattedDefaultTags(timeout_minutes)
    cmd = self._GetTagResourceCommand(tags)
    logging.info('Updating timeout tags on table %s with timeout minutes %s',
                 self.table_name, timeout_minutes)
    util.IssueRetryableCommand(cmd)

  def _Freeze(self) -> None:
    """See base class.

    Lowers provisioned throughput to free-tier levels. There is a limit to how
    many times throughput on a table may by lowered per day. See:
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughput.html.
    """
    # Check that we actually need to lower before issuing command.
    rcu, wcu = self._GetThroughput()
    if rcu > _FREE_TIER_RCU or wcu > _FREE_TIER_WCU:
      logging.info('(rcu=%s, wcu=%s) is higher than free tier.', rcu, wcu)
      self.SetThroughput(rcu=_FREE_TIER_RCU, wcu=_FREE_TIER_WCU)

  def _Restore(self) -> None:
    """See base class.

    Restores provisioned throughput back to benchmarking levels.
    """
    self.SetThroughput(self.rcu, self.wcu)


def _MakeArgs(args: Collection[str]) -> str:
  return ','.join(args)
