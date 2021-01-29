"""Tests for perfkitbenchmarker.providers.aws.aws_dynamodb."""

import json
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.aws import aws_dynamodb
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_DESCRIBE_TABLE_OUTPUT = """
{
    "Table": {
        "AttributeDefinitions": [
            {
                "AttributeName": "test",
                "AttributeType": "S"
            }
        ],
        "TableName": "test",
        "KeySchema": [
            {
                "AttributeName": "test",
                "KeyType": "HASH"
            }
        ],
        "TableStatus": "ACTIVE",
        "CreationDateTime": 1611605356.518,
        "ProvisionedThroughput": {
            "NumberOfDecreasesToday": 0,
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 0
        },
        "TableSizeBytes": 0,
        "ItemCount": 0,
        "TableArn": "arn:aws:dynamodb:us-east-2:835761027970:table/test",
        "TableId": "ecf0a60a-f18d-4666-affc-525ca6e1d207"
    }
}
"""


@flagsaver.flagsaver
def GetTestDynamoDBInstance(table_name='test_table'):
  FLAGS.zone = ['us-east-1a']
  return aws_dynamodb.AwsDynamoDBInstance(table_name)


class AwsDynamodbTest(pkb_common_test_case.PkbCommonTestCase):

  def assertArgumentInCommand(self, mock_cmd, arg):
    """Given an AWS command, checks that the argument is present."""
    command = ' '.join(mock_cmd.call_args[0][0])
    self.assertIn(arg, command)

  @flagsaver.flagsaver
  def testInitTableName(self):
    test_instance = GetTestDynamoDBInstance('dynamo_test_table')
    self.assertEqual(test_instance.table_name, 'dynamo_test_table')

  @flagsaver.flagsaver
  def testInitLocation(self):
    FLAGS.zone = ['us-east-1a']

    test_instance = aws_dynamodb.AwsDynamoDBInstance('test_table')

    self.assertEqual(test_instance.zone, 'us-east-1a')
    self.assertEqual(test_instance.region, 'us-east-1')

  @flagsaver.flagsaver
  def testInitKeysAndAttributes(self):
    FLAGS.aws_dynamodb_primarykey = 'test_primary_key'
    FLAGS.aws_dynamodb_sortkey = 'test_sort_key'
    FLAGS.aws_dynamodb_attributetype = 'test_attribute_type'

    test_instance = GetTestDynamoDBInstance()

    self.assertEqual(test_instance.primary_key,
                     '{"AttributeName": "test_primary_key","KeyType": "HASH"}')
    self.assertEqual(test_instance.sort_key,
                     '{"AttributeName": "test_sort_key","KeyType": "RANGE"}')
    self.assertEqual(
        test_instance.part_attributes,
        '{"AttributeName": "test_primary_key","AttributeType": "test_attribute_type"}'
    )
    self.assertEqual(
        test_instance.sort_attributes,
        '{"AttributeName": "test_sort_key","AttributeType": "test_attribute_type"}'
    )

  @flagsaver.flagsaver
  def testInitThroughput(self):
    FLAGS.aws_dynamodb_read_capacity = 1
    FLAGS.aws_dynamodb_write_capacity = 2

    test_instance = GetTestDynamoDBInstance()

    self.assertEqual(test_instance.throughput,
                     'ReadCapacityUnits=1,WriteCapacityUnits=2')

  @flagsaver.flagsaver
  def testGetResourceMetadata(self):
    FLAGS.zone = ['us-east-1a']
    FLAGS.aws_dynamodb_primarykey = 'test_primary_key'
    FLAGS.aws_dynamodb_use_sort = 'test_use_sort'
    FLAGS.aws_dynamodb_sortkey = 'test_sortkey'
    FLAGS.aws_dynamodb_attributetype = 'test_attribute_type'
    FLAGS.aws_dynamodb_read_capacity = 1
    FLAGS.aws_dynamodb_write_capacity = 2
    FLAGS.aws_dynamodb_lsi_count = 3
    FLAGS.aws_dynamodb_gsi_count = 4
    FLAGS.aws_dynamodb_ycsb_consistentReads = 5
    FLAGS.aws_dynamodb_connectMax = 6
    test_instance = aws_dynamodb.AwsDynamoDBInstance('test_table')

    actual_metadata = test_instance.GetResourceMetadata()

    expected_metadata = {
        'aws_dynamodb_primarykey': 'test_primary_key',
        'aws_dynamodb_use_sort': 'test_use_sort',
        'aws_dynamodb_sortkey': 'test_sortkey',
        'aws_dynamodb_attributetype': 'test_attribute_type',
        'aws_dynamodb_read_capacity': 1,
        'aws_dynamodb_write_capacity': 2,
        'aws_dynamodb_lsi_count': 3,
        'aws_dynamodb_gsi_count': 4,
        'aws_dynamodb_consistentReads': 5,
        'aws_dynamodb_connectMax': 6,
    }
    self.assertEqual(actual_metadata, expected_metadata)

  @parameterized.named_parameters({
      'testcase_name': 'ValidOutput',
      'output': json.loads(_DESCRIBE_TABLE_OUTPUT)['Table'],
      'expected': True
  }, {
      'testcase_name': 'EmptyOutput',
      'output': {},
      'expected': False
  })
  def testExists(self, output, expected):
    test_instance = GetTestDynamoDBInstance()
    self.enter_context(
        mock.patch.object(
            test_instance,
            '_DescribeTable',
            return_value=output))

    actual = test_instance._Exists()

    self.assertEqual(actual, expected)

  def testSetThroughput(self):
    test_instance = GetTestDynamoDBInstance(table_name='throughput_table')
    cmd = self.enter_context(
        mock.patch.object(
            util,
            'IssueRetryableCommand'))

    test_instance._SetThroughput(5, 5)

    self.assertArgumentInCommand(cmd, '--table-name throughput_table')
    self.assertArgumentInCommand(cmd, '--region us-east-1')
    self.assertArgumentInCommand(
        cmd,
        '--provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5')

  def testGetThroughput(self):
    test_instance = GetTestDynamoDBInstance()
    output = json.loads(_DESCRIBE_TABLE_OUTPUT)['Table']
    self.enter_context(
        mock.patch.object(
            test_instance,
            '_DescribeTable',
            return_value=output))

    actual_rcu, actual_wcu = test_instance._GetThroughput()

    self.assertEqual(actual_rcu, 5)
    self.assertEqual(actual_wcu, 0)

  def testTagResourceFailsWithNonExistentResource(self):
    test_instance = GetTestDynamoDBInstance()
    # Mark instance as non-existing.
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=False))

    with self.assertRaises(errors.Resource.CreationError):
      test_instance._GetTagResourceCommand(['test', 'tag'])

  def testUpdateWithDefaultTags(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.resource_arn = 'test_arn'
    cmd = self.enter_context(mock.patch.object(util, 'IssueRetryableCommand'))
    # Mark instance as existing.
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=True))

    test_instance.UpdateWithDefaultTags()

    self.assertArgumentInCommand(cmd, '--region us-east-1')
    self.assertArgumentInCommand(cmd, '--resource-arn test_arn')

  def testUpdateTimeout(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.resource_arn = 'test_arn'
    # Mock the aws util tags function.
    self.enter_context(
        mock.patch.object(
            util,
            'MakeDefaultTags',
            autospec=True,
            return_value={'timeout_utc': 60}))
    # Mock the actual call to the CLI
    cmd = self.enter_context(mock.patch.object(util, 'IssueRetryableCommand'))
    # Mark instance as existing.
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=True))

    test_instance.UpdateTimeout(timeout_minutes=60)

    self.assertArgumentInCommand(cmd, '--tags Key=timeout_utc,Value=60')


if __name__ == '__main__':
  unittest.main()
