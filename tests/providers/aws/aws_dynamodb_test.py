"""Tests for perfkitbenchmarker.providers.aws.aws_dynamodb."""

import unittest
from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker.providers.aws import aws_dynamodb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


@flagsaver.flagsaver
def GetTestDynamoDBInstance(table_name='test_table'):
  FLAGS.zone = ['us-east-1a']
  return aws_dynamodb.AwsDynamoDBInstance(table_name)


class AwsDynamodbTest(pkb_common_test_case.PkbCommonTestCase):

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


if __name__ == '__main__':
  unittest.main()
