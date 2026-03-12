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
    self.enter_context(
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


if __name__ == '__main__':
  unittest.main()
