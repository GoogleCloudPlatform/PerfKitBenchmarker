import datetime
import inspect
import json
import time
from typing import Type, cast
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import types
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_spanner
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case
import requests


FLAGS = flags.FLAGS


def GetTestSpannerInstance(engine='spanner-googlesql'):
  spec_args = {'cloud': 'GCP', 'engine': engine}
  spanner_spec = gcp_spanner.SpannerSpec(
      'test_component', flag_values=FLAGS, **spec_args
  )
  spanner_spec.spanner_database_name = 'test_database'
  spanner_class = cast(
      Type[gcp_spanner.GcpSpannerInstance],
      relational_db.GetRelationalDbClass(
          cloud='GCP', is_managed_db=True, engine=engine
      ),
  )
  return spanner_class(spanner_spec)


class SpannerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    saved_flag_values = flagsaver.save_flag_values()
    FLAGS.run_uri = 'test_uri'
    self.addCleanup(flagsaver.restore_flag_values, saved_flag_values)

  def testFlagOverridesAutoScaler(self):
    FLAGS['cloud_spanner_autoscaler'].parse('True')

    test_instance = GetTestSpannerInstance()
    self.assertEqual(test_instance._autoscaler, True)
    self.assertEqual(test_instance._min_processing_units, 5000)
    self.assertEqual(test_instance._max_processing_units, 50000)
    self.assertEqual(test_instance._high_priority_cpu_target, 65)
    self.assertEqual(test_instance._storage_target, 95)

  def testFlagOverrides(self):
    FLAGS['cloud_spanner_config'].parse('regional-us-central1')
    FLAGS['cloud_spanner_nodes'].parse(5)
    FLAGS['cloud_spanner_project'].parse('test_project')

    test_instance = GetTestSpannerInstance()

    self.assertEqual(test_instance.nodes, 5)
    self.assertEqual(test_instance._config, 'regional-us-central1')
    self.assertEqual(test_instance.project, 'test_project')

  def testSetNodes(self):
    test_instance = GetTestSpannerInstance()
    # Don't actually issue a command.
    self.enter_context(
        mock.patch.object(test_instance, '_GetNodes', return_value=1)
    )
    self.enter_context(
        mock.patch.object(test_instance, '_WaitUntilInstanceReady')
    )
    cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=[None, None, 0])
    )

    test_instance._SetNodes(3)

    self.assertIn('--nodes 3', ' '.join(cmd.call_args[0][0]))

  def testSetNodesSkipsIfCountAlreadyCorrect(self):
    test_instance = GetTestSpannerInstance()
    self.enter_context(
        mock.patch.object(test_instance, '_GetNodes', return_value=1)
    )
    self.enter_context(
        mock.patch.object(test_instance, '_WaitUntilInstanceReady')
    )
    cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=[None, None, 0])
    )

    test_instance._SetNodes(1)

    cmd.assert_not_called()

  def testFreezeUsesCorrectNodeCount(self):
    instance = GetTestSpannerInstance()
    mock_set_nodes = self.enter_context(
        mock.patch.object(instance, '_SetNodes', autospec=True)
    )

    instance._Freeze()

    mock_set_nodes.assert_called_once_with(gcp_spanner._FROZEN_NODE_COUNT)

  def testRestoreUsesCorrectNodeCount(self):
    instance = GetTestSpannerInstance()
    instance.nodes = 5
    mock_set_nodes = self.enter_context(
        mock.patch.object(instance, '_SetNodes', autospec=True)
    )

    instance._Restore()

    mock_set_nodes.assert_called_once_with(5)

  @flagsaver.flagsaver(run_uri='test_uri')
  def testUpdateLabels(self):
    # Arrange
    instance = GetTestSpannerInstance()
    mock_endpoint_response = '"https://spanner.googleapis.com"'
    mock_labels_response = inspect.cleandoc("""
    {
      "config": "test_config",
      "displayName": "test_display_name",
      "labels": {
        "benchmark": "test_benchmark",
        "timeout_minutes": "10"
      },
      "name": "test_name"
    }
    """)
    self.enter_context(
        mock.patch.object(
            util.GcloudCommand,
            'Issue',
            side_effect=[
                (mock_endpoint_response, '', 0),
                (mock_labels_response, '', 0),
            ],
        )
    )
    self.enter_context(
        mock.patch.object(util, 'GetAccessToken', return_value='test_token')
    )
    mock_request = self.enter_context(
        mock.patch.object(
            requests, 'patch', return_value=mock.Mock(status_code=200)
        )
    )

    # Act
    new_labels = {
        'benchmark': 'test_benchmark_2',
        'metadata': 'test_metadata',
    }
    instance._UpdateLabels(new_labels)

    # Assert
    mock_request.assert_called_once_with(
        'https://spanner.googleapis.com/v1/projects/test_project/instances/pkb-instance-test_uri',
        headers={'Authorization': 'Bearer test_token'},
        json={
            'instance': {
                'labels': {
                    'benchmark': 'test_benchmark_2',
                    'timeout_minutes': '10',
                    'metadata': 'test_metadata',
                }
            },
            'fieldMask': 'labels',
        },
    )

  @parameterized.named_parameters([
      {
          'testcase_name': 'AllRead',
          'write_proportion': 0.0,
          'read_proportion': 1.0,
          'expected_qps': 30000,
      },
      {
          'testcase_name': 'AllWrite',
          'write_proportion': 1.0,
          'read_proportion': 0.0,
          'expected_qps': 6000,
      },
      {
          'testcase_name': 'ReadWrite',
          'write_proportion': 0.5,
          'read_proportion': 0.5,
          'expected_qps': 10000,
      },
  ])
  def testCalculateStartingThroughput(
      self, write_proportion, read_proportion, expected_qps
  ):
    # Arrange
    test_spanner = GetTestSpannerInstance()
    test_spanner.nodes = 3

    # Act
    actual_qps = test_spanner.CalculateTheoreticalMaxThroughput(
        read_proportion, write_proportion
    )

    # Assert
    self.assertEqual(expected_qps, actual_qps)

  @parameterized.named_parameters([
      {
          'testcase_name': 'AllRead',
          'write_proportion': 0.0,
          'read_proportion': 1.0,
          'expected_qps': 45000,
      },
      {
          'testcase_name': 'AllWrite',
          'write_proportion': 1.0,
          'read_proportion': 0.0,
          'expected_qps': 9000,
      },
      {
          'testcase_name': 'ReadWrite',
          'write_proportion': 0.5,
          'read_proportion': 0.5,
          'expected_qps': 15000,
      },
  ])
  def testCalculateAdjustedStartingThroughput(
      self, write_proportion, read_proportion, expected_qps
  ):
    # Arrange
    test_spanner = GetTestSpannerInstance()
    test_spanner._config = 'regional-us-east4'
    test_spanner.nodes = 3

    # Act
    actual_qps = test_spanner.CalculateTheoreticalMaxThroughput(
        read_proportion, write_proportion
    )

    # Assert
    self.assertEqual(expected_qps, actual_qps)

  def testCollectMetrics(self):
    test_instance = GetTestSpannerInstance()
    test_instance.project = 'test_project'
    test_instance.instance_id = 'pkb-instance-test_uri'

    mock_response = types.ListTimeSeriesResponse(
        time_series=[{
            'metric': {
                'type': 'spanner.googleapis.com/instance/storage/used_bytes'
            },
            'points': [
                {
                    'interval': {
                        'start_time': {'seconds': 1764103200, 'nanos': 0},
                        'end_time': {'seconds': 1764103200, 'nanos': 0},
                    },
                    'value': {'int64_value': 2 * 1024**3},
                },
                {
                    'interval': {
                        'start_time': {'seconds': 1764103260, 'nanos': 0},
                        'end_time': {'seconds': 1764103260, 'nanos': 0},
                    },
                    'value': {'int64_value': 4 * 1024**3},
                },
            ],
        }]
    )
    mock_client = mock.Mock()
    mock_client.list_time_series.return_value = mock_response.time_series

    self.enter_context(
        mock.patch.object(
            monitoring_v3,
            'MetricServiceClient',
            return_value=mock_client,
        )
    )

    start_time = datetime.datetime(2025, 11, 26, 10, 0, 0)
    end_time = datetime.datetime(2025, 11, 26, 10, 1, 0)
    samples = test_instance.CollectMetrics(start_time, end_time)

    size_avg = next(s for s in samples if s.metric == 'disk_bytes_used_average')
    size_min = next(s for s in samples if s.metric == 'disk_bytes_used_min')
    size_max = next(s for s in samples if s.metric == 'disk_bytes_used_max')

    self.assertEqual(size_avg.value, 3)
    self.assertEqual(size_avg.unit, 'GB')
    self.assertEqual(size_min.value, 2)
    self.assertEqual(size_max.value, 4)

  def testRunDDLQuery_WithTimeout(self):
    # Arrange
    test_instance = GetTestSpannerInstance()
    test_instance.instance_id = 'pkb-instance-test_uri'
    test_instance.database = 'pkb-database-test_uri'
    ddl = 'ALTER TABLE t ADD COLUMN c INT64'
    timeout = 60
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('', '', 0), autospec=True
    ) as issue:
      # Act
      test_instance.RunDDLQuery(ddl, timeout=timeout)

      # Assert
      gcloud_cmd = issue.call_args[0][0]
      self.assertEqual(gcloud_cmd.flags['instance'], test_instance.instance_id)
      self.assertEqual(gcloud_cmd.flags['ddl'], ddl)
      self.assertEqual(issue.call_args[1]['timeout'], timeout)

  def testRunDDLQuery_WithoutTimeout(self):
    # Arrange
    test_instance = GetTestSpannerInstance()
    test_instance.instance_id = 'pkb-instance-test_uri'
    test_instance.database = 'pkb-database-test_uri'
    ddl = 'ALTER TABLE t ADD COLUMN c INT64'
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('', '', 0), autospec=True
    ) as issue:
      # Act
      test_instance.RunDDLQuery(ddl)

      # Assert
      gcloud_cmd = issue.call_args[0][0]
      self.assertEqual(gcloud_cmd.flags['instance'], test_instance.instance_id)
      self.assertEqual(gcloud_cmd.flags['ddl'], ddl)
      self.assertEqual(issue.call_args[1]['timeout'], vm_util.DEFAULT_TIMEOUT)

  def testRunDDLQuery_Async(self):
    # Arrange
    test_instance = GetTestSpannerInstance()
    test_instance.instance_id = 'pkb-instance-test_uri'
    test_instance.database = 'pkb-database-test_uri'
    ddl = 'ALTER TABLE t ADD COLUMN c INT64'
    op_name = 'projects/p/instances/i/databases/d/operations/op1'
    with mock.patch.object(
        test_instance, '_WaitForSpannerAsyncOperation'
    ) as mock_wait:
      with mock.patch.object(
          util.GcloudCommand,
          'Issue',
          return_value=('[]', f'Operation name={op_name}', 0),
          autospec=True,
      ) as issue:
        # Act
        test_instance.RunDDLQuery(ddl, async_proc=True)

        # Assert
        with self.subTest('TestInitialDDLCommand'):
          issue.assert_called_once()
          (gcloud_cmd,), _ = issue.call_args
          self.assertEqual(
              gcloud_cmd.flags['instance'], test_instance.instance_id
          )
          self.assertEqual(gcloud_cmd.flags['ddl'], ddl)
          self.assertTrue(gcloud_cmd.flags['async'])

        with self.subTest('TestCallsWaitForOperation'):
          mock_wait.assert_called_once_with(
              op_name, timeout=vm_util.DEFAULT_TIMEOUT
          )

  def testRunDDLQuery_AsyncFailure(self):
    # Arrange
    test_instance = GetTestSpannerInstance()
    test_instance.instance_id = 'pkb-instance-test_uri'
    test_instance.database = 'pkb-database-test_uri'
    ddl = 'ALTER TABLE t ADD COLUMN c INT64'
    op_name = 'projects/p/instances/i/databases/d/operations/op1'
    with mock.patch.object(
        test_instance,
        '_WaitForSpannerAsyncOperation',
        side_effect=errors.Benchmarks.RunError('failed'),
    ):
      with mock.patch.object(
          util.GcloudCommand,
          'Issue',
          return_value=('[]', f'Operation name={op_name}', 0),
          autospec=True,
      ):
        # Act & Assert
        with self.assertRaisesRegex(errors.Benchmarks.RunError, 'failed'):
          test_instance.RunDDLQuery(ddl, async_proc=True)

  def testWaitForSpannerAsyncOperation_Success(self):
    # Arrange
    test_instance = GetTestSpannerInstance()
    test_instance.instance_id = 'pkb-instance-test_uri'
    test_instance.database = 'pkb-database-test_uri'
    op_name = 'projects/p/instances/i/databases/d/operations/op1'
    timeout = 60

    with mock.patch.object(
        util.GcloudCommand,
        'Issue',
        return_value=('{"done": true}', '', 0),
        autospec=True,
    ) as issue:
      # Act
      test_instance._WaitForSpannerAsyncOperation(op_name, timeout)

      # Assert
      issue.assert_called_once()
      (poll_cmd,), _ = issue.call_args
      self.assertIn('operations', poll_cmd.args)
      self.assertIn('describe', poll_cmd.args)
      self.assertIn(op_name, poll_cmd.args)
      self.assertEqual(poll_cmd.flags['instance'], test_instance.instance_id)
      self.assertEqual(poll_cmd.flags['database'], test_instance.database)

  def testWaitForSpannerAsyncOperation_RetryThenSuccess(self):
    # Arrange
    test_instance = GetTestSpannerInstance()
    test_instance.instance_id = 'pkb-instance-test_uri'
    test_instance.database = 'pkb-database-test_uri'
    op_name = 'projects/p/instances/i/databases/d/operations/op1'
    timeout = 60

    self.enter_context(mock.patch.object(vm_util.time, 'sleep'))

    with mock.patch.object(
        util.GcloudCommand,
        'Issue',
        side_effect=[
            ('{"done": false}', '', 0),
            ('{"done": true}', '', 0),
        ],
        autospec=True,
    ) as issue:
      # Act
      test_instance._WaitForSpannerAsyncOperation(op_name, timeout)

      # Assert
      self.assertEqual(issue.call_count, 2)

  def testWaitForSpannerAsyncOperation_Failure(self):
    # Arrange
    test_instance = GetTestSpannerInstance()
    test_instance.instance_id = 'pkb-instance-test_uri'
    test_instance.database = 'pkb-database-test_uri'
    op_name = 'projects/p/instances/i/databases/d/operations/op1'
    timeout = 60

    with mock.patch.object(
        util.GcloudCommand,
        'Issue',
        return_value=('{"done": true, "error": {"message": "failed"}}', '', 0),
        autospec=True,
    ):
      # Act & Assert
      with self.assertRaisesRegex(
          gcp_spanner.errors.Benchmarks.RunError, 'failed'
      ):
        test_instance._WaitForSpannerAsyncOperation(op_name, timeout)


class CreateTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    saved_flag_values = flagsaver.save_flag_values()
    FLAGS.run_uri = 'test_uri'
    self.addCleanup(flagsaver.restore_flag_values, saved_flag_values)

    self.test_instance = GetTestSpannerInstance()
    self.enter_context(mock.patch.object(self.test_instance, '_UpdateLabels'))

  def testCreateOnExistingInstance(self):
    self.assertFalse(self.test_instance.created)
    self.enter_context(
        mock.patch.object(self.test_instance, '_Exists', return_value=True)
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            side_effect=[('', 'error', -1), ('', 'success', 0)],
        )
    )

    self.test_instance.Create()

    self.assertTrue(self.test_instance.created)
    self.assertTrue(self.test_instance.user_managed)

  def testCreate(self):
    self.assertFalse(self.test_instance.created)
    self.enter_context(
        mock.patch.object(
            self.test_instance, '_Exists', side_effect=[False, True, True]
        )
    )
    self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=('', '', 0))
    )

    self.test_instance.Create()

    self.assertTrue(self.test_instance.created)
    self.assertFalse(self.test_instance.user_managed)


class RestoreTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(
      run_uri='test_uri',
      cloud_spanner_source_instance_id='source_instance',
      cloud_spanner_source_database_id='source_db',
  )
  def setUp(self):
    super().setUp()
    self.instance = GetTestSpannerInstance()
    self.instance.instance_id = 'target_instance'
    self.instance.database = 'target_db'

  def testRestoreDatabase(self):
    self.enter_context(
        mock.patch.object(
            self.instance,
            '_GetLatestBackup',
            return_value='backup_123',
            autospec=True,
        )
    )
    mock_cmd = mock.Mock()
    mock_cmd.flags = {}
    mock_cmd.Issue.side_effect = [
        ('', 'Restore database in progress. Operation name=operation_123', 0),
        (json.dumps({'done': True}), '', 0),
    ]
    mock_gcloud = self.enter_context(
        mock.patch.object(
            util, 'GcloudCommand', return_value=mock_cmd, autospec=True
        )
    )
    self.enter_context(mock.patch.object(time, 'sleep', autospec=True))
    self.enter_context(
        mock.patch.object(
            time, 'time', side_effect=range(0, 1000, 30), autospec=True
        )
    )

    self.instance.RestoreDatabase('target_db')

    with self.subTest('TestInstanceRestored'):
      self.assertTrue(self.instance.spanner_restored)

    with self.subTest('TestRestoreCommand'):
      mock_gcloud.assert_any_call(
          self.instance, 'spanner', 'databases', 'restore'
      )
      self.assertTrue(mock_cmd.flags['async'])

    with self.subTest('TestDescribeOperationCommand'):
      mock_gcloud.assert_any_call(
          self.instance, 'spanner', 'operations', 'describe', 'operation_123'
      )

  def testGetLatestBackup(self):
    mock_backups_list = [
        {
            'name': 'projects/p/instances/i/backups/backup_2023_10_26',
            'create_time': '2023-10-26T10:00:00Z',
        },
    ]
    mock_cmd = mock.Mock()
    mock_cmd.flags = {}
    mock_cmd.Issue.return_value = (json.dumps(mock_backups_list), '', 0)
    mock_gcloud = self.enter_context(
        mock.patch.object(
            util, 'GcloudCommand', return_value=mock_cmd, autospec=True
        )
    )

    backup_id = self.instance._GetLatestBackup()

    self.assertEqual(backup_id, 'backup_2023_10_26')
    mock_gcloud.assert_called_with(self.instance, 'spanner', 'backups', 'list')
    self.assertEqual(mock_cmd.flags['instance'], 'source_instance')
    self.assertIn('database:source_db', mock_cmd.flags['filter'])


class FlagValidationTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(run_uri='test_uri')
  def setUp(self):
    super().setUp()

  def testInvalidFlagCombination_OnlyInstance(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      with flagsaver.flagsaver(
          cloud_spanner_source_instance_id='source_instance',
          cloud_spanner_source_database_id=None,
      ):
        GetTestSpannerInstance()

  def testInvalidFlagCombination_OnlyDatabase(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      with flagsaver.flagsaver(
          cloud_spanner_source_instance_id=None,
          cloud_spanner_source_database_id='source_db',
      ):
        GetTestSpannerInstance()

  @flagsaver.flagsaver(
      cloud_spanner_source_instance_id='source_instance',
      cloud_spanner_source_database_id='source_db',
  )
  def testValidFlagCombination_Both(self):
    instance = GetTestSpannerInstance()
    self.assertEqual(instance.source_instance_id, 'source_instance')
    self.assertEqual(instance.source_database_id, 'source_db')

  @flagsaver.flagsaver(
      cloud_spanner_source_instance_id=None,
      cloud_spanner_source_database_id=None,
  )
  def testValidFlagCombination_None(self):
    instance = GetTestSpannerInstance()
    self.assertIsNone(instance.source_instance_id)
    self.assertIsNone(instance.source_database_id)


if __name__ == '__main__':
  unittest.main()
