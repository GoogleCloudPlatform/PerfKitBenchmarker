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
import unittest
from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import hammerdbcli_benchmark
from perfkitbenchmarker.linux_packages import hammerdb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_HAMMERDB_CONFIG = textwrap.dedent("""
hammerdbcli:
  relational_db:
    engine: postgres
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
      sample.Sample(hammerdb.TPM, tpm, hammerdb.TPM, dict(metadata)),
      sample.Sample(hammerdb.NOPM, nopm, hammerdb.NOPM, dict(metadata)),
  ]


class HammerdbcliBenchmarkTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(run_uri='test_uri'))
    self.benchmark_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        _HAMMERDB_CONFIG, 'hammerdbcli'
    )
    self.benchmark_spec.ConstructRelationalDb()
    self.benchmark_spec.ConstructVirtualMachines()
    assert self.benchmark_spec.relational_db is not None
    self.relational_db: relational_db.BaseRelationalDb = (
        self.benchmark_spec.relational_db
    )
    self.enter_context(
        mock.patch.object(self.relational_db, 'CollectMetrics', return_value=[])
    )
    self.enter_context(
        mock.patch.object(
            hammerdbcli_benchmark, '_ReconfigureRunScriptForVirtualUsers'
        )
    )
    self.enter_context(
        mock.patch.object(
            hammerdb, 'GetNumVirtualUsersList', return_value=[8, 16]
        )
    )
    self.enter_context(
        mock.patch.object(hammerdb, 'Run', return_value='fake_stdout')
    )

  def testRunSweepOutputsMaxTpmSamples(self):
    # Return 50000 TPM for 8 VUs, and 80000 TPM for 16 VUs.
    run_8 = _MakeHammerdbRunSamples(
        tpm=50000.0, nopm=20000.0, num_virtual_users=8
    )
    run_16 = _MakeHammerdbRunSamples(
        tpm=80000.0, nopm=32000.0, num_virtual_users=16
    )
    self.enter_context(
        mock.patch.object(hammerdb, 'ParseResults', side_effect=[run_8, run_16])
    )

    results = hammerdbcli_benchmark.Run(self.benchmark_spec)

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

  @flagsaver.flagsaver(
      hammerdbcli_script='tpc_c',
      hammerdbcli_tpcc_num_warehouse=250,
  )
  def testPrepareSetsSimulatedDatasetSizeGb_Tpcc(self):
    self.enter_context(
        mock.patch.object(
            self.benchmark_spec.vm_groups['clients'][0], 'Install'
        )
    )
    self.enter_context(mock.patch.object(hammerdb, 'SetupConfig'))
    self.enter_context(
        mock.patch.object(hammerdbcli_benchmark, '_PrepareServer')
    )

    hammerdbcli_benchmark.Prepare(self.benchmark_spec)

    expected_gb = (250 * 100.0) / 1024.0
    self.assertAlmostEqual(
        self.relational_db.simulated_dataset_size_gb,
        expected_gb,
        places=4,
    )

  @flagsaver.flagsaver(
      hammerdbcli_script='tpc_h',
      hammerdbcli_tpch_scale_factor=15,
  )
  def testPrepareSetsSimulatedDatasetSizeGb_Tpch(self):
    self.enter_context(
        mock.patch.object(
            self.benchmark_spec.vm_groups['clients'][0], 'Install'
        )
    )
    self.enter_context(mock.patch.object(hammerdb, 'SetupConfig'))
    self.enter_context(
        mock.patch.object(hammerdbcli_benchmark, '_PrepareServer')
    )

    hammerdbcli_benchmark.Prepare(self.benchmark_spec)

    self.assertEqual(
        self.relational_db.simulated_dataset_size_gb,
        15.0,
    )


if __name__ == '__main__':
  unittest.main()
