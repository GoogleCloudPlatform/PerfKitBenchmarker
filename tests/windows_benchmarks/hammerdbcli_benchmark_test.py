# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

import textwrap
import time
import unittest
from absl import flags
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import hammerdbcli_benchmark as linux_hammerdb_benchmark
from perfkitbenchmarker.linux_packages import hammerdb as linux_hammerdb
from tests import pkb_common_test_case
from perfkitbenchmarker.windows_benchmarks import hammerdbcli_benchmark
from perfkitbenchmarker.windows_packages import hammerdb

FLAGS = flags.FLAGS

_WINDOWS_HAMMERDB_CONFIG = textwrap.dedent("""
hammerdbcli:
  relational_db:
    engine: sqlserver
    db_spec: *default_dual_core
    db_disk_spec: *default_500_gb
  vm_groups:
    clients:
      vm_spec: *default_dual_core
""")


def _MakeHammerdbRunSamples(tpm, nopm, num_virtual_users):
  """Builds a (TPM, NOPM) sample pair for one HammerDB run."""
  metadata = {'hammerdbcli_vu': num_virtual_users}
  return [
      sample.Sample(
          linux_hammerdb.TPM, tpm, linux_hammerdb.TPM, dict(metadata)
      ),
      sample.Sample(
          linux_hammerdb.NOPM, nopm, linux_hammerdb.NOPM, dict(metadata)
      ),
  ]


class HammerdbcliBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'test_uri'
    self.enter_context(mock.patch.object(linux_hammerdb_benchmark, 'PreRun'))
    self.enter_context(mock.patch.object(linux_hammerdb_benchmark, 'PostRun'))

  def testRunCallPerformanceCounters(self):
    benchmark_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        _WINDOWS_HAMMERDB_CONFIG, 'hammerdbcli'
    )
    benchmark_spec.ConstructRelationalDb()
    benchmark_spec.ConstructVirtualMachines()
    assert benchmark_spec.relational_db is not None
    benchmark_spec.relational_db.is_managed_db = True

    self.enter_context(
        mock.patch.object(
            benchmark_spec.relational_db, 'CollectMetrics', return_value=[]
        )
    )
    mock_query_perf = self.enter_context(
        mock.patch.object(
            benchmark_spec.relational_db, 'QueryPerformanceCounters'
        )
    )

    def MockHammerDbRun(*args, **kwargs):
      del args, kwargs
      time.sleep(2)
      return []

    mock_run = self.enter_context(
        mock.patch.object(hammerdb, 'Run', side_effect=MockHammerDbRun)
    )
    self.enter_context(
        mock.patch.object(hammerdb, '_COUNTER_QUERY_TIMEOUT', 0.5)
    )

    hammerdbcli_benchmark.Run(benchmark_spec)

    self.assertGreaterEqual(mock_query_perf.call_count, 2)
    mock_run.assert_called_once()

  def testRunSweepOutputsMaxTpmSamples(self):
    benchmark_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        _WINDOWS_HAMMERDB_CONFIG, 'hammerdbcli'
    )
    benchmark_spec.ConstructRelationalDb()
    benchmark_spec.ConstructVirtualMachines()
    assert benchmark_spec.relational_db is not None

    self.enter_context(
        mock.patch.object(
            benchmark_spec.relational_db, 'CollectMetrics', return_value=[]
        )
    )
    self.enter_context(mock.patch.object(hammerdb, 'ConfigureRunScript'))
    self.enter_context(
        mock.patch.object(
            linux_hammerdb, 'GetNumVirtualUsersList', return_value=[8, 16]
        )
    )

    run_8 = _MakeHammerdbRunSamples(
        tpm=50000.0, nopm=20000.0, num_virtual_users=8
    )
    run_16 = _MakeHammerdbRunSamples(
        tpm=80000.0, nopm=32000.0, num_virtual_users=16
    )
    self.enter_context(
        mock.patch.object(hammerdb, 'Run', side_effect=[run_8, run_16])
    )

    results = hammerdbcli_benchmark.Run(benchmark_spec)

    # Should contain 4 raw samples + 2 max_tpm duplicates from the 16 virtual
    # user run.
    metrics = sorted(s.metric for s in results)
    self.assertEqual(
        metrics,
        [
            'NOPM',
            'NOPM',
            'TPM',
            'TPM',
            'max_tpm_NOPM',
            'max_tpm_TPM',
        ],
    )
    max_tpm_sample = [s for s in results if s.metric == 'max_tpm_TPM'][0]
    self.assertEqual(max_tpm_sample.value, 80000.0)
    self.assertEqual(max_tpm_sample.metadata['hammerdbcli_vu'], 16)
    self.assertNotIn('max_tpm', max_tpm_sample.metadata)


if __name__ == '__main__':
  unittest.main()
