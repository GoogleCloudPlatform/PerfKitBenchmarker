import time
import unittest
from absl import flags
import mock
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from tests import pkb_common_test_case
from perfkitbenchmarker.windows_benchmarks import hammerdbcli_benchmark
from perfkitbenchmarker.windows_packages import hammerdb

FLAGS = flags.FLAGS


class HammerdbcliBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def testRunCallPerformanceCounters(self):
    benchmark_spec = mock.Mock()
    db_mock = mock.Mock(spec=relational_db.BaseRelationalDb)
    db_mock.engine_type = sql_engine_utils.SQLSERVER
    db_mock.is_managed_db = True
    db_mock.CollectMetrics.return_value = []
    benchmark_spec.relational_db = db_mock
    vm_mock = mock.Mock(spec=virtual_machine.BaseVirtualMachine)
    benchmark_spec.vm_groups = {'clients': [vm_mock]}

    # Mock hammerdb.Run to simulate a 5-second run.
    def mock_hammerdb_run(*args, **kwargs):
      del args, kwargs
      time.sleep(5)
      return []

    mock_run = self.enter_context(
        mock.patch.object(hammerdb, 'Run', side_effect=mock_hammerdb_run)
    )
    self.enter_context(mock.patch.object(hammerdb, '_COUNTER_QUERY_TIMEOUT', 1))

    hammerdbcli_benchmark.Run(benchmark_spec)

    self.assertGreaterEqual(db_mock.QueryPerformanceCounters.call_count, 2)
    mock_run.assert_called_once()


if __name__ == '__main__':
  unittest.main()
