"""Tests for benchbase package."""

import json
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import data as pkb_data
from perfkitbenchmarker.linux_packages import benchbase
from tests import pkb_common_test_case


FLAGS = flags.FLAGS
# No need to mark as parsed, flagsaver handles a clean state.

RAW_TXN_CSV = """Transaction Type Index,Transaction Name,Start Time (microseconds),Latency (microseconds),Worker Id (start number),Phase Id (index in config file)
1,NewOrder,1770744128.464431,820732,63,1
5,StockLevel,1770744128.465026,685419,82,1
1,NewOrder,1770744128.466009,721878,2,1
2,Payment,1770744128.467030,179841,11,1
1,NewOrder,1770744128.468005,845845,0,1
3,OrderStatus,1770744128.469015,152001,6,1
1,NewOrder,1770744128.470019,718044,10,1
1,NewOrder,1770744128.471015,737124,7,1
1,NewOrder,1770744128.472016,774431,1,1
1,NewOrder,1770744128.473012,768836,12,1
4,Delivery,1770744128.474025,928432,5,1
4,Delivery,1770744128.475026,922045,9,1
4,Delivery,1770744128.476046,942821,13,1
5,StockLevel,1770744128.477003,148647,14,1
2,Payment,1770744128.478032,558904,15,1
1,NewOrder,1770744128.479061,765474,16,1
1,NewOrder,1770744128.480030,835730,17,1
1,NewOrder,1770744128.481036,830839,18,1
1,NewOrder,1770744128.482042,699926,19,1
2,Payment,1770744128.483053,175784,20,1
1,NewOrder,1770744128.484067,839271,21,1
2,Payment,1770744128.485063,533358,22,1
2,Payment,1770744128.486077,553753,23,1
1,NewOrder,1770744128.487052,903456,24,1
2,Payment,1770744128.488066,695271,25,1
1,NewOrder,1770744128.489044,719395,26,1
1,NewOrder,1770744128.490061,857368,27,1
1,NewOrder,1770744128.491062,802472,28,1
2,Payment,1770744128.492033,524618,29,1
2,Payment,1770744128.493053,671096,30,1
2,Payment,1770744128.494051,689684,32,1
1,NewOrder,1770744128.495045,718387,31,1
2,Payment,1770744128.496050,525300,33,1
2,Payment,1770744128.497045,532042,34,1
2,Payment,1770744128.498056,529186,35,1
4,Delivery,1770744128.499047,1351831,36,1
2,Payment,1770744128.500036,515341,37,1
2,Payment,1770744128.501034,628971,38,1
1,NewOrder,1770744128.502044,814048,39,1
1,NewOrder,1770744128.503069,663352,40,1
2,Payment,1770744128.504047,526334,41,1
2,Payment,1770744128.505054,135410,42,1
"""


class BenchbaseTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.Mock()

  def test_uninstall(self):
    benchbase.Uninstall(self.vm)
    self.vm.RemoteCommand.assert_called_once_with(
        f'sudo rm -rf {benchbase.BENCHBASE_DIR}'
    )

  @flagsaver.flagsaver(db_engine='spanner-postgres')
  @mock.patch.object(benchbase, '_InstallJDK23', autospec=True)
  def test_install_spanner(self, mock_install_jdk):
    benchbase.Install(self.vm)
    mock_install_jdk.assert_called_once_with(self.vm)
    self.vm.RemoteCommand.assert_has_calls([
        mock.call(f'sudo rm -rf {benchbase.BENCHBASE_DIR}'),
        mock.call(
            'git clone https://github.com/cmu-db/benchbase.git'
            f' {benchbase.BENCHBASE_DIR}'
        ),
    ])

  @flagsaver.flagsaver(
      db_engine='aurora-dsql-postgres',
      benchbase_repo_url='https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking.git',
  )
  @mock.patch.object(benchbase, '_InstallJDK23', autospec=True)
  def test_install_aurora_dsql(self, mock_install_jdk):
    benchbase.Install(self.vm)
    mock_install_jdk.assert_called_once_with(self.vm)
    self.vm.RemoteCommand.assert_has_calls([
        mock.call(f'sudo rm -rf {benchbase.BENCHBASE_DIR}'),
        mock.call(
            'git clone'
            ' https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking.git'
            f' {benchbase.BENCHBASE_DIR}'
        ),
    ])

  @flagsaver.flagsaver(
      db_engine='spanner-postgres',
      benchbase_rate='500',
  )
  @mock.patch.object(pkb_data, 'ResourcePath', return_value='dummy_template.j2')
  def test_create_config_file_spanner(self, _):
    benchbase.CreateConfigFile(self.vm)
    self.vm.RenderTemplate.assert_called_once()
    _, kwargs = self.vm.RenderTemplate.call_args

    self.assertEqual(kwargs['template_path'], 'dummy_template.j2')
    self.assertEqual(kwargs['remote_path'], benchbase.CONFIG_FILE_PATH)

    context = kwargs['context']
    self.assertEqual(context['db_type'], 'POSTGRES')
    self.assertEqual(context['username_element'], '<username>admin</username>')
    self.assertEqual(
        context['password_element'], '<password>password</password>'
    )
    self.assertEqual(context['rate_element'], '<rate>500</rate>')
    self.assertIn(
        'jdbc:postgresql://localhost:5432/benchbase', context['jdbc_url']
    )
    self.assertEqual(context['driver_class'], 'org.postgresql.Driver')
    self.assertEqual(context['isolation'], 'TRANSACTION_REPEATABLE_READ')
    self.assertEqual(context['scalefactor'], 10000)
    self.assertEqual(context['terminals'], 200)

  @flagsaver.flagsaver(
      db_engine='aurora-dsql-postgres',
      benchbase_rate='unlimited',
  )
  @mock.patch.object(pkb_data, 'ResourcePath', return_value='dummy_template.j2')
  def test_create_config_file_aurora_dsql(self, _):
    benchbase.CreateConfigFile(self.vm)
    self.vm.RenderTemplate.assert_called_once()
    _, kwargs = self.vm.RenderTemplate.call_args

    self.assertEqual(kwargs['template_path'], 'dummy_template.j2')
    self.assertEqual(kwargs['remote_path'], benchbase.CONFIG_FILE_PATH)

    context = kwargs['context']
    self.assertEqual(context['db_type'], 'AURORADSQL')
    self.assertEqual(
        context['username_element'], '<username>admin</username>'
    )
    self.assertEqual(
        context['password_element'], '<password></password>'
    )
    self.assertEqual(context['rate_element'], '<rate>unlimited</rate>')
    self.assertIn(
        'jdbc:postgresql://localhost:5432/postgres', context['jdbc_url']
    )

  def test_update_time(self):
    benchbase.UpdateTime(self.vm, 10)
    self.vm.RemoteCommand.assert_called_once_with(
        "sed -i 's|<time>.*</time>|<time>600</time>|' "
        f'{benchbase.CONFIG_FILE_PATH}'
    )

  def test_parse_results(self):
    self.vm.RemoteCommand.side_effect = [
        ('tpcc_2025-12-03_19-45-37.summary.json', ''),
        (
            json.dumps({
                'Latency Distribution': {
                    '95th Percentile Latency (microseconds)': 1555074,
                    'Maximum Latency (microseconds)': 5588384,
                    'Median Latency (microseconds)': 656210,
                    'Minimum Latency (microseconds)': 7632,
                    '25th Percentile Latency (microseconds)': 270068,
                    '90th Percentile Latency (microseconds)': 1442239,
                    '99th Percentile Latency (microseconds)': 2621825,
                    '75th Percentile Latency (microseconds)': 1121111,
                    'Average Latency (microseconds)': 732735,
                },
                'Throughput (requests/second)': 404.000,
            }),
            '',
        ),
        ('results.raw.csv', ''),  # ls for raw results
        (RAW_TXN_CSV, ''),  # cat for raw results
    ]
    results = benchbase.ParseResults(self.vm, {})
    # 5 txn types * 9 latency metrics + 9 aggregated latency metrics
    # + tps + tpmc = 56 metrics
    self.assertLen(results, 56)

    with self.subTest(name='aggregated_metrics'):
      actual_metrics = {
          s.metric: s.value for s in results if 'txn_type' not in s.metadata
      }
      expected_metrics = {
          '95th_percentile_latency': 1555.074,
          'maximum_latency': 5588.384,
          'median_latency': 656.210,
          'minimum_latency': 7.632,
          '25th_percentile_latency': 270.068,
          '90th_percentile_latency': 1442.239,
          '99th_percentile_latency': 2621.825,
          '75th_percentile_latency': 1121.111,
          'average_latency': 732.735,
          'tps': 404.000,
          'tpmc': 10908.0,
      }
      for metric, value in expected_metrics.items():
        self.assertIn(metric, actual_metrics)
        self.assertAlmostEqual(actual_metrics[metric], value, places=3)

    txn_metrics = [s for s in results if 'txn_type' in s.metadata]
    self.assertLen(txn_metrics, 45)
    self.assertEqual(
        txn_metrics[0].metadata['benchbase_use_foreign_key'], False
    )

    with self.subTest(name='new_order_metrics'):
      new_order_metrics = {
          s.metric: s.value
          for s in txn_metrics
          if s.metadata['txn_type'] == 'NewOrder'
      }
      expected_new_order_metrics = {
          '95th_percentile_latency': 861.977,
          'maximum_latency': 903.456,
          'median_latency': 774.431,
          'minimum_latency': 663.352,
          '25th_percentile_latency': 720.637,
          '90th_percentile_latency': 848.150,
          '99th_percentile_latency': 895.160,
          '75th_percentile_latency': 833.285,
          'average_latency': 780.874,
      }
      for metric, value in expected_new_order_metrics.items():
        self.assertIn(metric, new_order_metrics)
        self.assertAlmostEqual(new_order_metrics[metric], value, places=3)

    with self.subTest(name='payment_metrics'):
      payment_metrics = {
          s.metric: s.value
          for s in txn_metrics
          if s.metadata['txn_type'] == 'Payment'
      }
      expected_payment_metrics = {
          '95th_percentile_latency': 691.081,
          'maximum_latency': 695.271,
          'median_latency': 530.614,
          'minimum_latency': 135.410,
          '25th_percentile_latency': 522.299,
          '90th_percentile_latency': 680.390,
          '99th_percentile_latency': 694.433,
          '75th_percentile_latency': 576.421,
          'average_latency': 498.431,
      }
      for metric, value in expected_payment_metrics.items():
        self.assertIn(metric, payment_metrics)
        self.assertAlmostEqual(payment_metrics[metric], value, places=3)


if __name__ == '__main__':
  unittest.main()
