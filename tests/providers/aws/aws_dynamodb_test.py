"""Tests for perfkitbenchmarker.providers.aws.aws_dynamodb."""

import json
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_dynamodb
from perfkitbenchmarker.providers.aws import util
from tests import matchers
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

_TEST_BENCHMARK_SPEC = """
aws_dynamodb_ycsb:
  description: >
      Run YCSB against AWS DynamoDB.
  non_relational_db:
    billing_mode: PROVISIONED
    service_type: dynamodb
    enable_freeze_restore: True
    table_name: test_instance
    zone: us-east-1a
    rcu: 5
    primary_key: test_primary_key
    sort_key: test_sort_key
    attribute_type: S
    lsi_count: 5
    gsi_count: 5
    use_sort: True
"""

_MINIMAL_TEST_BENCHMARK_SPEC = """
aws_dynamodb_ycsb:
  description: >
      Run YCSB against AWS DynamoDB.
  non_relational_db:
    service_type: dynamodb
"""


def GetTestDynamoDBInstance(minimal=False):
  spec = _MINIMAL_TEST_BENCHMARK_SPEC if minimal else _TEST_BENCHMARK_SPEC
  test_benchmark_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
      yaml_string=spec, benchmark_name='aws_dynamodb_ycsb'
  )
  test_benchmark_spec.ConstructNonRelationalDb()
  instance = test_benchmark_spec.non_relational_db
  if instance is None:
    raise ValueError(
        'BenchmarkSpec.ConstructNonRelationalDb() failed to create a '
        'non_relational_db instance for testing, tests cannot proceed.'
    )
  return instance


class AwsDynamodbTest(pkb_common_test_case.PkbCommonTestCase):

  def _MockHasAutoscalingPolicies(self, test_instance, policies_exist):
    self.enter_context(
        mock.patch.object(
            test_instance,
            '_HasAutoscalingPolicies',
            return_value=policies_exist,
        )
    )

  def assertArgumentInCommand(self, mock_cmd, arg):
    """Given an AWS command, checks that the argument is present."""
    command = ' '.join(mock_cmd.call_args[0][0])
    self.assertIn(arg, command)

  @flagsaver.flagsaver
  def testInitFromSpec(self):
    instance = GetTestDynamoDBInstance()

    with self.subTest('location'):
      self.assertEqual(instance.zone, 'us-east-1a')
      self.assertEqual(instance.region, 'us-east-1')
    with self.subTest('name'):
      self.assertEqual(instance.table_name, 'test_instance')
    with self.subTest('billing'):
      self.assertEqual(instance.billing_mode, 'PROVISIONED')
    with self.subTest('capacity'):
      self.assertEqual(instance.rcu, 5)
      self.assertEqual(instance.wcu, 25)
    with self.subTest('schema'):
      self.assertEqual(instance.primary_key, 'test_primary_key')
      self.assertEqual(instance.sort_key, 'test_sort_key')
      self.assertEqual(instance.attribute_type, 'S')
    with self.subTest('indexes'):
      self.assertEqual(instance.lsi_count, 5)
      self.assertEqual(instance.gsi_count, 5)
      self.assertEqual(instance.use_sort, True)

  @flagsaver.flagsaver
  def testInitLocation(self):
    FLAGS['zone'].parse(['us-east-1a'])

    test_instance = GetTestDynamoDBInstance(minimal=True)

    self.assertEqual(test_instance.zone, 'us-east-1a')
    self.assertEqual(test_instance.region, 'us-east-1')

  @flagsaver.flagsaver
  def testInitKeysAndAttributes(self):
    test_instance = GetTestDynamoDBInstance()

    self.assertEqual(
        test_instance._PrimaryKeyJson(),
        '{"AttributeName": "test_primary_key", "KeyType": "HASH"}',
    )
    self.assertEqual(
        test_instance._SortKeyJson(),
        '{"AttributeName": "test_sort_key", "KeyType": "RANGE"}',
    )
    self.assertEqual(
        test_instance._PrimaryAttrsJson(),
        '{"AttributeName": "test_primary_key", "AttributeType": "S"}',
    )
    self.assertEqual(
        test_instance._SortAttrsJson(),
        '{"AttributeName": "test_sort_key", "AttributeType": "S"}',
    )

  @flagsaver.flagsaver
  def testInitThroughput(self):
    FLAGS['aws_dynamodb_read_capacity'].parse(1)
    FLAGS['aws_dynamodb_write_capacity'].parse(2)

    test_instance = GetTestDynamoDBInstance()

    self.assertEqual(
        test_instance.throughput, 'ReadCapacityUnits=1,WriteCapacityUnits=2'
    )

  def testInitThroughputWithOnDemandRaises(self):
    FLAGS['aws_dynamodb_read_capacity'].parse(1)
    FLAGS['aws_dynamodb_write_capacity'].parse(2)
    FLAGS['aws_dynamodb_billing_mode'].parse(aws_dynamodb._ON_DEMAND)

    with self.assertRaises(errors.Config.InvalidValue):
      GetTestDynamoDBInstance()

  @flagsaver.flagsaver
  def testGetResourceMetadata(self):
    FLAGS['zone'].parse(['us-east-1a'])
    FLAGS['aws_dynamodb_primarykey'].parse('test_primary_key')
    FLAGS['aws_dynamodb_use_sort'].parse(True)
    FLAGS['aws_dynamodb_sortkey'].parse('test_sortkey')
    FLAGS['aws_dynamodb_attributetype'].parse('N')
    FLAGS['aws_dynamodb_read_capacity'].parse(1)
    FLAGS['aws_dynamodb_write_capacity'].parse(2)
    FLAGS['aws_dynamodb_lsi_count'].parse(3)
    FLAGS['aws_dynamodb_gsi_count'].parse(4)
    FLAGS['aws_dynamodb_billing_mode'].parse(aws_dynamodb._PROVISIONED)
    test_instance = GetTestDynamoDBInstance()

    actual_metadata = test_instance.GetResourceMetadata()

    expected_metadata = {
        'aws_dynamodb_primarykey': 'test_primary_key',
        'aws_dynamodb_use_sort': True,
        'aws_dynamodb_sortkey': 'test_sortkey',
        'aws_dynamodb_attributetype': 'N',
        'aws_dynamodb_read_capacity': 1,
        'aws_dynamodb_write_capacity': 2,
        'aws_dynamodb_lsi_count': 3,
        'aws_dynamodb_gsi_count': 4,
        'aws_dynamodb_billing_mode': aws_dynamodb._PROVISIONED,
    }
    self.assertEqual(actual_metadata, expected_metadata)

  def testCreateProvisionedThroughput(self):
    test_instance = GetTestDynamoDBInstance()
    mock_issue = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=['', '', 0])
    )

    test_instance._Create()

    mock_issue.assert_has_calls(
        [
            mock.call(
                matchers.HAS('--provisioned-throughput'),
                raise_on_failure=mock.ANY,
            )
        ],
    )

  def testCreateOnDemand(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.billing_mode = aws_dynamodb._ON_DEMAND
    mock_issue = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=['', '', 0])
    )

    test_instance._Create()

    self.assertIn('PAY_PER_REQUEST', mock_issue.call_args[0][0])
    self.assertNotIn('--provisioned-throughput', mock_issue.call_args[0][0])

  @flagsaver.flagsaver(aws_dynamodb_autoscaling_target=50)
  def testIsServerless(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.billing_mode = aws_dynamodb._ON_DEMAND
    self.assertTrue(test_instance.IsServerless())

  def testIsServerlessDefault(self):
    test_instance = GetTestDynamoDBInstance()
    self.assertFalse(test_instance.IsServerless())

  @parameterized.named_parameters(
      {
          'testcase_name': 'ValidOutput',
          'output': json.loads(_DESCRIBE_TABLE_OUTPUT)['Table'],
          'expected': True,
      },
      {'testcase_name': 'EmptyOutput', 'output': {}, 'expected': False},
  )
  def testExists(self, output, expected):
    test_instance = GetTestDynamoDBInstance()
    self.enter_context(
        mock.patch.object(test_instance, '_DescribeTable', return_value=output)
    )

    actual = test_instance._Exists()

    self.assertEqual(actual, expected)

  def testSetThroughputNotCalled(self):
    # Arrange
    test_instance = GetTestDynamoDBInstance()
    cmd = self.enter_context(mock.patch.object(util, 'IssueRetryableCommand'))
    self.enter_context(mock.patch.object(test_instance, '_IsReady'))
    self.enter_context(
        mock.patch.object(test_instance, '_GetThroughput', return_value=(5, 5))
    )

    # Act
    test_instance.SetThroughput(5, 5)

    # Assert
    cmd.assert_not_called()

  def testSetThroughput(self):
    # Arrange
    test_instance = GetTestDynamoDBInstance()
    cmd = self.enter_context(mock.patch.object(util, 'IssueRetryableCommand'))
    self.enter_context(mock.patch.object(test_instance, '_IsReady'))
    self.enter_context(
        mock.patch.object(test_instance, '_GetThroughput', return_value=(5, 5))
    )

    # Act
    test_instance.SetThroughput(10, 10)

    # Assert
    self.assertArgumentInCommand(cmd, '--table-name test_instance')
    self.assertArgumentInCommand(cmd, '--region us-east-1')
    self.assertArgumentInCommand(
        cmd,
        '--provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits=10',
    )

  def testGetThroughput(self):
    test_instance = GetTestDynamoDBInstance()
    output = json.loads(_DESCRIBE_TABLE_OUTPUT)['Table']
    self.enter_context(
        mock.patch.object(test_instance, '_DescribeTable', return_value=output)
    )

    actual_rcu, actual_wcu = test_instance._GetThroughput()

    self.assertEqual(actual_rcu, 5)
    self.assertEqual(actual_wcu, 0)

  def testTagResourceFailsWithNonExistentResource(self):
    test_instance = GetTestDynamoDBInstance()
    # Mark instance as non-existing.
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=False)
    )

    with self.assertRaises(vm_util.RetriesExceededRetryError) as e:
      test_instance._GetTagResourceCommand(['test', 'tag'])
    self.assertIs(type(e.exception.__cause__), errors.Resource.CreationError)

  def testUpdateWithDefaultTags(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.resource_arn = 'test_arn'
    cmd = self.enter_context(mock.patch.object(util, 'IssueRetryableCommand'))
    # Mark instance as existing.
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=True)
    )

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
            return_value={'timeout_utc': 60},
        )
    )
    # Mock the actual call to the CLI
    cmd = self.enter_context(mock.patch.object(util, 'IssueRetryableCommand'))
    # Mark instance as existing.
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=True)
    )

    test_instance.UpdateTimeout(timeout_minutes=60)

    self.assertArgumentInCommand(cmd, '--tags Key=timeout_utc,Value=60')

  @parameterized.named_parameters(
      {
          'testcase_name': 'OnlyRcu',
          'rcu': 5,
          'wcu': 500,
      },
      {
          'testcase_name': 'OnlyWcu',
          'rcu': 500,
          'wcu': 5,
      },
      {
          'testcase_name': 'Both',
          'rcu': 500,
          'wcu': 500,
      },
  )
  def testFreezeLowersThroughputToFreeTier(self, rcu, wcu):
    test_instance = GetTestDynamoDBInstance()
    self._MockHasAutoscalingPolicies(test_instance, False)
    self.enter_context(
        mock.patch.object(
            test_instance, '_GetThroughput', return_value=(rcu, wcu)
        )
    )
    mock_set_throughput = self.enter_context(
        mock.patch.object(test_instance, 'SetThroughput', autospec=True)
    )

    test_instance._Freeze()

    mock_set_throughput.assert_called_once_with(
        rcu=aws_dynamodb._FREE_TIER_RCU, wcu=aws_dynamodb._FREE_TIER_WCU
    )

  def testFreezeDoesNotLowerThroughputIfAlreadyAtFreeTier(self):
    test_instance = GetTestDynamoDBInstance()
    self._MockHasAutoscalingPolicies(test_instance, False)
    self.enter_context(
        mock.patch.object(test_instance, '_GetThroughput', return_value=(5, 5))
    )
    mock_set_throughput = self.enter_context(
        mock.patch.object(test_instance, 'SetThroughput', autospec=True)
    )

    test_instance._Freeze()

    mock_set_throughput.assert_not_called()

  @flagsaver.flagsaver(
      aws_dynamodb_autoscaling_target=50,
      aws_dynamodb_autoscaling_wcu_max=100,
      aws_dynamodb_autoscaling_rcu_max=200,
  )
  def testFreezeAutoscalingUsesFreeTierAsMin(self):
    test_instance = GetTestDynamoDBInstance()
    self._MockHasAutoscalingPolicies(test_instance, True)
    mock_autoscale = self.enter_context(
        mock.patch.object(test_instance, '_CreateScalableTarget')
    )

    test_instance._Freeze()

    mock_autoscale.assert_has_calls([
        mock.call(
            aws_dynamodb._RCU_SCALABLE_DIMENSION,
            aws_dynamodb._FREE_TIER_RCU,
            200,
        ),
        mock.call(
            aws_dynamodb._WCU_SCALABLE_DIMENSION,
            aws_dynamodb._FREE_TIER_WCU,
            100,
        ),
    ])

  def testRestoreSetsThroughputBackToOriginalLevels(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.rcu = 5000
    test_instance.wcu = 1000
    mock_set_throughput = self.enter_context(
        mock.patch.object(test_instance, 'SetThroughput', autospec=True)
    )
    self._MockHasAutoscalingPolicies(test_instance, False)

    test_instance._Restore()

    mock_set_throughput.assert_called_once_with(rcu=5000, wcu=1000)

  @flagsaver.flagsaver(
      aws_dynamodb_autoscaling_target=50,
      aws_dynamodb_autoscaling_wcu_max=100,
      aws_dynamodb_autoscaling_rcu_max=200,
  )
  def testRestoreAutoscalingUsesOriginalThroughputLevels(self):
    test_instance = GetTestDynamoDBInstance()

    mock_set_throughput = self.enter_context(
        mock.patch.object(test_instance, 'SetThroughput', autospec=True)
    )
    self._MockHasAutoscalingPolicies(test_instance, True)
    mock_autoscale = self.enter_context(
        mock.patch.object(test_instance, '_CreateScalableTarget')
    )

    test_instance._Restore()

    mock_autoscale.assert_has_calls([
        mock.call(aws_dynamodb._RCU_SCALABLE_DIMENSION, 5, 200),
        mock.call(aws_dynamodb._WCU_SCALABLE_DIMENSION, 25, 100),
    ])
    mock_set_throughput.assert_not_called()

  @flagsaver.flagsaver(
      aws_dynamodb_autoscaling_target=50,
      aws_dynamodb_autoscaling_wcu_max=100,
      aws_dynamodb_autoscaling_rcu_max=100,
  )
  def testPostCreateAutoscalingPoliciesCreatedCorrectly(self):
    test_instance = GetTestDynamoDBInstance()
    mock_create_policy = self.enter_context(
        mock.patch.object(test_instance, '_CreateAutoscalingPolicy')
    )

    test_instance._PostCreate()

    self.assertLen(mock_create_policy.mock_calls, 2)

  def testShouldNotAutoscale(self):
    test_instance = GetTestDynamoDBInstance()
    mock_create_policy = self.enter_context(
        mock.patch.object(test_instance, '_CreateAutoscalingPolicy')
    )

    test_instance._PostCreate()

    mock_create_policy.assert_not_called()

  def testFreezeOnDemandIsNoOp(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.billing_mode = aws_dynamodb._ON_DEMAND
    self._MockHasAutoscalingPolicies(test_instance, False)
    mock_set_throughput = self.enter_context(
        mock.patch.object(test_instance, 'SetThroughput')
    )

    test_instance._Freeze()

    mock_set_throughput.assert_not_called()

  def testRestoreOnDemandIsNoOp(self):
    test_instance = GetTestDynamoDBInstance()
    test_instance.billing_mode = aws_dynamodb._ON_DEMAND
    self._MockHasAutoscalingPolicies(test_instance, False)
    mock_set_throughput = self.enter_context(
        mock.patch.object(test_instance, 'SetThroughput')
    )

    test_instance._Restore()

    mock_set_throughput.assert_not_called()


if __name__ == '__main__':
  unittest.main()
