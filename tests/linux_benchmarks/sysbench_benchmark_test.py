# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for sysbench."""

import os
import textwrap
import unittest
from unittest import mock
from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import sysbench_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class ScaleUpClientTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'sysbench-output-sample.txt'
    )
    with open(path) as fp:
      self.contents = fp.read()
    self.enter_context(
        mock.patch.object(
            sysbench_benchmark, '_GetCommonSysbenchOptions', return_value=[]
        )
    )
    FLAGS.sysbench_testname = sysbench_benchmark.SPANNER_TPCC

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  @flagsaver.flagsaver(
      sysbench_scaleup_clients_test_num_clients=2,
      sysbench_scale_up_max_cpu_utilization=0.5,
  )
  def testScaleUpClient(self):
    benchmark_spec = mock.Mock()
    benchmark_spec.relational_db = mock.Mock()
    benchmark_spec.relational_db.GetAverageCpuUsage = mock.Mock(
        return_value=0.5
    )
    clients = [mock.Mock(), mock.Mock()]
    for client in clients:
      client.RobustRemoteCommand = mock.Mock(return_value=[self.contents, ''])
    result = sysbench_benchmark._RunScaleUpClientsBenchmark(
        clients, 100, benchmark_spec, 10, {}
    )
    self.assertEqual(clients[0].RobustRemoteCommand.call_count, 2)
    self.assertEqual(clients[1].RobustRemoteCommand.call_count, 1)
    self.assertEqual(
        result,
        [
            sample.Sample(
                metric='total_tps',
                value=986.49,
                unit='tps',
                metadata={
                    'sysbench_scale_up_client_count': 1,
                    'cpu_utilization': 0.5,
                    'tps': [986.49],
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='total_qps',
                value=19730.45,
                unit='qps',
                metadata={
                    'sysbench_scale_up_client_count': 1,
                    'cpu_utilization': 0.5,
                    'qps': [19730.45],
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='min_latency',
                value=12.38,
                unit='ms',
                metadata={
                    'sysbench_scale_up_client_count': 1,
                    'cpu_utilization': 0.5,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='average_latency',
                value=16.21,
                unit='ms',
                metadata={
                    'sysbench_scale_up_client_count': 1,
                    'cpu_utilization': 0.5,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='max_latency',
                value=115.78,
                unit='ms',
                metadata={
                    'sysbench_scale_up_client_count': 1,
                    'cpu_utilization': 0.5,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='total_tps',
                value=1972.98,
                unit='tps',
                metadata={
                    'sysbench_scale_up_client_count': 2,
                    'cpu_utilization': 0.5,
                    'tps': [986.49, 986.49],
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='total_qps',
                value=39460.9,
                unit='qps',
                metadata={
                    'sysbench_scale_up_client_count': 2,
                    'cpu_utilization': 0.5,
                    'qps': [19730.45, 19730.45],
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='min_latency',
                value=12.38,
                unit='ms',
                metadata={
                    'sysbench_scale_up_client_count': 2,
                    'cpu_utilization': 0.5,
                    'latency_array': [12.38, 12.38],
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='average_latency',
                value=16.21,
                unit='ms',
                metadata={
                    'sysbench_scale_up_client_count': 2,
                    'cpu_utilization': 0.5,
                    'latency_array': [16.21, 16.21],
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='max_latency',
                value=115.78,
                unit='ms',
                metadata={
                    'sysbench_scale_up_client_count': 2,
                    'cpu_utilization': 0.5,
                    'latency_array': [115.78, 115.78],
                },
                timestamp=0,
            ),
        ],
    )


class SpannerBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            sysbench_benchmark, '_GetCommonSysbenchOptions', return_value=[]
        )
    )
    FLAGS.sysbench_testname = sysbench_benchmark.SPANNER_TPCC

  def testSpannerLoadCommandHasCorrectTest(self):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(mock.Mock(), 1, 0)
    self.assertIn('sysbench tpcc', command)

  @parameterized.named_parameters([
      {
          'testcase_name': 'SingleVM',
          'num_vms': 1,
          'expected_cluster': 0,
      },
      {
          'testcase_name': 'MultiVM',
          'num_vms': 2,
          'expected_cluster': 1,
      },
  ])
  def testSpannerLoadCommandHasCorrectClusterSetting(
      self, num_vms, expected_cluster
  ):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(
        mock.Mock(), num_vms, 0
    )
    self.assertIn(f'--enable_cluster={expected_cluster}', command)

  @flagsaver.flagsaver(sysbench_scale=1000)
  def testSpannerLoadCommandHasCorrectScales(self):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(mock.Mock(), 1, 0)
    with self.subTest('scale'):
      self.assertIn('--scale=1000', command)
    with self.subTest('start_scale'):
      self.assertIn('--start_scale=1', command)
    with self.subTest('end_scale'):
      self.assertIn('--end_scale=1000', command)

  @flagsaver.flagsaver(sysbench_scale=1000)
  def testSpannerLoadCommandHasCorrectScalesMultiVM(self):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(mock.Mock(), 5, 2)
    with self.subTest('scale'):
      self.assertIn('--scale=1000', command)
    with self.subTest('start_scale'):
      self.assertIn('--start_scale=401', command)
    with self.subTest('end_scale'):
      self.assertIn('--end_scale=600', command)

  def testSpannerRunCommandHasCorrectTest(self):
    command = sysbench_benchmark._GetSysbenchRunCommand(1, mock.Mock(), 1)
    self.assertIn('sysbench tpcc', command)

  def testSpannerMetadataHasForeignKey(self):
    metadata = sysbench_benchmark.CreateMetadataFromFlags()
    self.assertIn('sysbench_use_fk', metadata)

  class SysbenchBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

    def testLoadPhaseIncreasedCapacityAurora(self):
      mock_spec = textwrap.dedent("""
      sysbench:
        relational_db:
          engine: aurora-postgres
          db_spec:
            AWS:
              machine_type: db.m4.4xlarge
              load_machine_type: db.r7g.16xlarge
      """)
      mock_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
          mock_spec, 'sysbench'
      )
      mock_bm_spec.ConstructRelationalDb()
      mock_update_instance = self.enter_context(
          mock.patch.object(
              mock_bm_spec.relational_db, '_UpdateWriterInstanceClass'
          )
      )

      sysbench_benchmark._LoadDatabaseInParallel(mock_bm_spec.relational_db, [])

      mock_update_instance.assert_has_calls(
          [mock.call('db.r7g.16xlarge'), mock.call('db.m4.4xlarge')]
      )

    def testLoadPhaseIncreasedCapacitySpanner(self):
      mock_spec = textwrap.dedent("""
      sysbench:
        relational_db:
          engine: spanner-postgres
          spanner_nodes: 3
          spanner_load_nodes: 6
      """)
      mock_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
          mock_spec, 'sysbench'
      )
      mock_bm_spec.ConstructRelationalDb()
      mock_set_nodes = self.enter_context(
          mock.patch.object(mock_bm_spec.relational_db, '_SetNodes')
      )

      sysbench_benchmark._LoadDatabaseInParallel(mock_bm_spec.relational_db, [])

      mock_set_nodes.assert_has_calls([mock.call(6), mock.call(3)])

    def testIncreasedCapacityNotImplemented(self):
      mock_spec = textwrap.dedent("""
      sysbench:
        relational_db:
          engine: aurora-postgres
          load_machine_type: db.r7g.16xlarge
          db_spec:
            AWS:
              machine_type: db.m4.4xlarge
      """)
      mock_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
          mock_spec, 'sysbench'
      )
      mock_bm_spec.ConstructRelationalDb()
      mock_update_instance = self.enter_context(
          mock.patch.object(
              mock_bm_spec.relational_db, '_UpdateWriterInstanceClass'
          )
      )

      sysbench_benchmark._LoadDatabaseInParallel(mock_bm_spec.relational_db, [])

      mock_update_instance.assert_has_calls(
          [mock.call('db.r7g.16xlarge'), mock.call('db.m4.4xlarge')]
      )

  class SysbenchRunCommandTransactionLevelsTest(
      pkb_common_test_case.PkbCommonTestCase, parameterized.TestCase
  ):

    def setUp(self):
      super().setUp()
      self.mock_db = mock.MagicMock()
      self.mock_db.engine_type = sql_engine_utils.SPANNER_POSTGRES
      self.enter_context(
          mock.patch.object(
              sysbench_benchmark,
              '_GetCommonSysbenchOptions',
              return_value=['--mocked_db_options'],
          )
      )

    @parameterized.named_parameters(
        dict(
            testcase_name='_SER', level='SER', expected_flag='--trx_level=SER'
        ),
        dict(testcase_name='_RR', level='RR', expected_flag='--trx_level=RR'),
    )
    @flagsaver.flagsaver(sysbench_testname=sysbench_benchmark.SPANNER_TPCC)
    def test_spanner_tpcc_transaction_levels(self, level, expected_flag):
      FLAGS.sysbench_txn_isolation_level = level
      command = sysbench_benchmark._GetSysbenchRunCommand(
          duration=300, db=self.mock_db, sysbench_thread_count=64
      )
      self.assertIn(expected_flag, command)

    @parameterized.named_parameters(
        dict(
            testcase_name='_SER', level='SER', expected_flag='--trx_level=SER'
        ),
        dict(testcase_name='_RR', level='RR', expected_flag='--trx_level=RR'),
    )
    @flagsaver.flagsaver(sysbench_testname='oltp_read_write')
    def test_spanner_oltp_read_write_transaction_levels(
        self, level, expected_flag
    ):
      FLAGS.sysbench_txn_isolation_level = level
      command = sysbench_benchmark._GetSysbenchRunCommand(
          duration=60, db=self.mock_db, sysbench_thread_count=16
      )
      self.assertIn(expected_flag, command)

    @parameterized.named_parameters(
        dict(
            testcase_name='_SER', level='SER', expected_flag='--trx_level=SER'
        ),
        dict(testcase_name='_RR', level='RR', expected_flag='--trx_level=RR'),
    )
    @flagsaver.flagsaver(sysbench_testname='oltp_write_only')
    def test_spanner_oltp_write_only_transaction_levels(
        self, level, expected_flag
    ):
      FLAGS.sysbench_txn_isolation_level = level
      command = sysbench_benchmark._GetSysbenchRunCommand(
          duration=60, db=self.mock_db, sysbench_thread_count=16
      )
      self.assertIn(expected_flag, command)

    @parameterized.named_parameters(
        dict(
            testcase_name='_SER', level='SER', expected_flag='--trx_level=SER'
        ),
        dict(testcase_name='_RR', level='RR', expected_flag='--trx_level=RR'),
    )
    @flagsaver.flagsaver(sysbench_testname='oltp_read_only')
    def test_spanner_oltp_read_only_transaction_levels(
        self, level, expected_flag
    ):
      FLAGS.sysbench_txn_isolation_level = level
      command = sysbench_benchmark._GetSysbenchRunCommand(
          duration=60, db=self.mock_db, sysbench_thread_count=16
      )
      self.assertIn(expected_flag, command)

    @parameterized.named_parameters(
        dict(
            testcase_name='_oltp_read_write',
            sysbench_testname='oltp_read_write',
        ),
        dict(
            testcase_name='_oltp_write_only',
            sysbench_testname='oltp_write_only',
        ),
        dict(
            testcase_name='_oltp_read_only', sysbench_testname='oltp_read_only'
        ),
        dict(
            testcase_name='_tpcc',
            sysbench_testname=sysbench_benchmark.SPANNER_TPCC,
        ),
    )
    @flagsaver.flagsaver(sysbench_txn_isolation_level='RC')
    def test_spanner_transaction_level_rc_unsupported(self, sysbench_testname):
      FLAGS.sysbench_testname = sysbench_testname
      command = sysbench_benchmark._GetSysbenchRunCommand(
          duration=60, db=self.mock_db, sysbench_thread_count=16
      )
      # RC is not supported for Spanner, so no trx_level flag should be added.
      self.assertNotIn('--trx_level=', command)


if __name__ == '__main__':
  unittest.main()
