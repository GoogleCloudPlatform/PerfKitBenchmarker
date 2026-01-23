import datetime
import inspect
import unittest
from absl import flags
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import types
import mock
from perfkitbenchmarker.providers.gcp import gcp_alloy_db
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GcpAlloyDbTest(pkb_common_test_case.PkbCommonTestCase):

  def testCollectMetrics(self):
    FLAGS.run_uri = '123456'
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
    db = spec.relational_db
    db.project = 'test_project'

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
    samples = db.CollectMetrics(start_time, end_time)

    size_avg = next(s for s in samples if s.metric == 'disk_bytes_used_average')
    size_min = next(s for s in samples if s.metric == 'disk_bytes_used_min')
    size_max = next(s for s in samples if s.metric == 'disk_bytes_used_max')

    self.assertIsInstance(db, gcp_alloy_db.GCPAlloyRelationalDb)
    self.assertEqual(size_avg.value, 3.0)
    self.assertEqual(size_avg.unit, 'GB')
    self.assertEqual(size_min.value, 2.0)
    self.assertEqual(size_max.value, 4.0)


if __name__ == '__main__':
  unittest.main()
