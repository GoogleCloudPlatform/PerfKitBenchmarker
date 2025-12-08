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
    ]
    results = benchbase.ParseResults(self.vm, {})
    self.assertLen(results, 11)
    actual_metrics = {s.metric: s.value for s in results}
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


if __name__ == '__main__':
  unittest.main()
