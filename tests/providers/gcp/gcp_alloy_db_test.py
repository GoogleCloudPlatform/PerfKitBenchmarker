import datetime
import inspect
import unittest
from absl import flags
from absl.testing import flagsaver
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import types
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.gcp import gcp_alloy_db
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GcpAlloyDbTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'test_run'
    test_spec = inspect.cleandoc("""
    sysbench:
      relational_db:
        engine: alloydb-postgresql
        engine_version: '15'
        db_spec:
          GCP:
            machine_type: n1-standard-4
            zone: us-central1-c
        db_disk_spec:
          GCP:
            disk_size: 50
      vm_groups:
        default:
          vm_spec:
            GCP:
              machine_type: n1-standard-4
              zone: us-central1-c
    """)
    spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=test_spec, benchmark_name='sysbench'
    )
    spec.ConstructRelationalDb()
    self.db = spec.relational_db

    # Mock client_vm for network resource name
    self.db.client_vm = mock.Mock()
    self.db.client_vm.network.network_resource.name = 'network_name'

    # Default to no read replicas
    self.enter_context(flagsaver.flagsaver(alloydb_read_pool_node_count=0))

    self.mock_cmd = mock.Mock()
    self.mock_get_cmd = self.enter_context(
        mock.patch.object(
            gcp_alloy_db.GCPAlloyRelationalDb,
            '_GetAlloyDbCommand',
            return_value=self.mock_cmd,
        )
    )

  def testCollectMetrics(self):
    self.db.project = 'test_project'

    mock_response = types.ListTimeSeriesResponse(
        time_series=[{
            'metric': {'type': 'alloydb.googleapis.com/cluster/storage/usage'},
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
    mock_client = mock.MagicMock()
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
    samples = self.db.CollectMetrics(start_time, end_time)

    size_avg = next(s for s in samples if s.metric == 'disk_bytes_used_average')
    size_min = next(s for s in samples if s.metric == 'disk_bytes_used_min')
    size_max = next(s for s in samples if s.metric == 'disk_bytes_used_max')

    self.assertIsInstance(self.db, gcp_alloy_db.GCPAlloyRelationalDb)
    self.assertEqual(size_avg.value, 3.0)
    self.assertEqual(size_avg.unit, 'GB')
    self.assertEqual(size_min.value, 2.0)
    self.assertEqual(size_max.value, 4.0)

  def testCreateSuccess(self):
    # Cluster create, Instance create, Instance describe
    self.mock_cmd.Issue.side_effect = [
        ('', '', 0),
        ('', '', 0),
        ('{"ipAddress": "1.1.1.1"}', '', 0),
    ]

    self.db._Create()

    self.assertEqual(self.db.endpoint, '1.1.1.1')

  def testCreateInternalError(self):
    # Fail on first command (cluster create)
    self.mock_cmd.Issue.side_effect = [
        ('', 'an internal error has occurred', 1)
    ]

    with self.assertRaisesRegex(
        errors.Resource.RetryableCreationError,
        'Creation failed due to internal error',
    ):
      self.db._Create()

  def testCreateInsufficientCapacity(self):
    # Fail on first command (cluster create)
    self.mock_cmd.Issue.side_effect = [(
        '',
        'does not have enough resources available to fulfill the request.',
        1,
    )]

    with self.assertRaisesRegex(
        errors.Benchmarks.InsufficientCapacityCloudFailure,
        'Creation failed due to not enough resources',
    ):
      self.db._Create()

  def testCreateAlreadyExists(self):
    # Cluster exists, Instance exists, Instance describe success
    self.mock_cmd.Issue.side_effect = [
        ('', 'already exists', 1),
        ('', 'already exists', 1),
        ('{"ipAddress": "1.1.1.1"}', '', 0),
    ]

    self.db._Create()

    self.assertEqual(self.db.endpoint, '1.1.1.1')

  def testCreateOtherError(self):
    # Fail on first command
    self.mock_cmd.Issue.side_effect = [('', 'unknown error', 1)]

    with self.assertRaisesRegex(
        errors.VmUtil.IssueCommandError,
        'Command failed',
    ):
      self.db._Create()

  def testUpdateAlloyDBFlagsEnabledNoOtherFlags(self):
    self.mock_cmd.Issue.return_value = ('', '', 0)
    self.db.enable_columnar_engine = True

    self.db.UpdateAlloyDBFlags()

    # Assert _GetAlloyDbCommand was called with expected arguments
    self.mock_get_cmd.assert_called_once()
    cmd_args = self.mock_get_cmd.call_args[0][0]
    with self.subTest(msg='Command structure'):
      self.assertIn('instances', cmd_args)
      self.assertIn('update', cmd_args)

    # Find the --database-flags argument
    db_flags_arg = next(
        arg for arg in cmd_args if arg.startswith('--database-flags=')
    )
    with self.subTest(msg='google_columnar_engine.enabled'):
      self.assertIn('google_columnar_engine.enabled=on', db_flags_arg)
    with self.subTest(msg='google_columnar_engine.memory_size_in_mb'):
      self.assertNotIn(
          'google_columnar_engine.memory_size_in_mb=', db_flags_arg
      )
    with self.subTest(msg='google_columnar_engine.enable_auto_columnarization'):
      self.assertNotIn(
          'google_columnar_engine.enable_auto_columnarization=', db_flags_arg
      )
    with self.subTest(
        msg='google_columnar_engine.enable_columnar_recommendation'
    ):
      self.assertNotIn(
          'google_columnar_engine.enable_columnar_recommendation=',
          db_flags_arg,
      )
    with self.subTest(msg='google_columnar_engine.enable_index_caching'):
      self.assertNotIn(
          'google_columnar_engine.enable_index_caching=', db_flags_arg
      )
    with self.subTest(msg='google_columnar_engine.relations'):
      self.assertNotIn('google_columnar_engine.relations=', db_flags_arg)

  def testUpdateAlloyDBFlagsEnabledAllFlagsEnabled(self):
    self.mock_cmd.Issue.return_value = ('', '', 0)
    self.db.enable_columnar_engine = True

    self.db.UpdateAlloyDBFlags(
        columnar_engine_size=1024,
        enable_columnar_recommendation=True,
        enable_auto_columnarization=True,
        enable_index_caching=True,
        relation='relation_name',
    )

    # Assert _GetAlloyDbCommand was called with expected arguments
    self.mock_get_cmd.assert_called_once()
    cmd_args = self.mock_get_cmd.call_args[0][0]
    with self.subTest(msg='Command structure'):
      self.assertIn('instances', cmd_args)
      self.assertIn('update', cmd_args)

    # Find the --database-flags argument
    db_flags_arg = next(
        arg for arg in cmd_args if arg.startswith('--database-flags=')
    )
    with self.subTest(msg='google_columnar_engine.enabled'):
      self.assertIn('google_columnar_engine.enabled=on', db_flags_arg)
    with self.subTest(msg='google_columnar_engine.memory_size_in_mb'):
      self.assertIn(
          'google_columnar_engine.memory_size_in_mb=1024', db_flags_arg
      )
    with self.subTest(msg='google_columnar_engine.enable_auto_columnarization'):
      self.assertIn(
          'google_columnar_engine.enable_auto_columnarization=on', db_flags_arg
      )
    with self.subTest(
        msg='google_columnar_engine.enable_columnar_recommendation'
    ):
      self.assertIn(
          'google_columnar_engine.enable_columnar_recommendation=on',
          db_flags_arg,
      )
    with self.subTest(msg='google_columnar_engine.enable_index_caching'):
      self.assertIn(
          'google_columnar_engine.enable_index_caching=on', db_flags_arg
      )
    with self.subTest(msg='google_columnar_engine.relations'):
      self.assertIn(
          'google_columnar_engine.relations=relation_name', db_flags_arg
      )

  def testUpdateAlloyDBFlagsEnabledAllFlagsDisabled(self):
    self.mock_cmd.Issue.return_value = ('', '', 0)
    self.db.enable_columnar_engine = True

    self.db.UpdateAlloyDBFlags(
        columnar_engine_size=None,
        enable_columnar_recommendation=False,
        enable_auto_columnarization=False,
        enable_index_caching=False,
        relation=None,
    )

    # Assert _GetAlloyDbCommand was called with expected arguments
    self.mock_get_cmd.assert_called_once()
    cmd_args = self.mock_get_cmd.call_args[0][0]
    with self.subTest(msg='Command structure'):
      self.assertIn('instances', cmd_args)
      self.assertIn('update', cmd_args)

    # Find the --database-flags argument
    db_flags_arg = next(
        arg for arg in cmd_args if arg.startswith('--database-flags=')
    )
    with self.subTest(msg='google_columnar_engine.enabled'):
      self.assertIn('google_columnar_engine.enabled=on', db_flags_arg)
    with self.subTest(msg='google_columnar_engine.memory_size_in_mb'):
      self.assertNotIn(
          'google_columnar_engine.memory_size_in_mb=', db_flags_arg
      )
    with self.subTest(msg='google_columnar_engine.enable_auto_columnarization'):
      self.assertIn(
          'google_columnar_engine.enable_auto_columnarization=off', db_flags_arg
      )
    with self.subTest(
        msg='google_columnar_engine.enable_columnar_recommendation'
    ):
      self.assertIn(
          'google_columnar_engine.enable_columnar_recommendation=off',
          db_flags_arg,
      )
    with self.subTest(msg='google_columnar_engine.enable_index_caching'):
      self.assertIn(
          'google_columnar_engine.enable_index_caching=off', db_flags_arg
      )
    with self.subTest(msg='google_columnar_engine.relations'):
      self.assertNotIn('google_columnar_engine.relations=', db_flags_arg)

  def testUpdateAlloyDBFlagsDisabled(self):
    self.mock_cmd.Issue.return_value = ('', '', 0)
    self.db.enable_columnar_engine = False

    self.db.UpdateAlloyDBFlags(
        columnar_engine_size=1024,
        enable_columnar_recommendation=True,
        enable_auto_columnarization=True,
        enable_index_caching=True,
        relation='relation_name',
    )

    # Assert _GetAlloyDbCommand was not called as CE is disabled
    self.mock_get_cmd.assert_not_called()

  def testQueryPerfSnapReport(self):
    mock_query_tools = mock.Mock()
    mock_query_tools.IssueSqlCommand.side_effect = [
        ('1\n', ''),
        ('2\n', ''),
        ('Full report content\n', ''),
    ]
    with mock.patch.object(
        gcp_alloy_db.GCPAlloyRelationalDb,
        'client_vm_query_tools',
        new_callable=mock.PropertyMock,
        return_value=mock_query_tools,
    ):
      res1, _ = self.db.QueryPerfSnapReport()
      self.assertIn('Start snapshot taken: 1', res1)

      res2, _ = self.db.QueryPerfSnapReport()
      self.assertEqual(res2, 'Full report content\n')

      self.assertEqual(mock_query_tools.IssueSqlCommand.call_count, 3)
      mock_query_tools.IssueSqlCommand.assert_has_calls([
          mock.call('SELECT perfsnap.snap();', ignore_failure=True),
          mock.call('SELECT perfsnap.snap();', ignore_failure=True),
          mock.call('SELECT perfsnap.report(1, 2);', ignore_failure=True),
      ])


if __name__ == '__main__':
  unittest.main()
