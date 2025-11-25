"""Tests for benchbase package."""

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
        context['username_element'], '<!--<username>admin</username>-->'
    )
    self.assertEqual(
        context['password_element'], '<!--<password>password</password>-->'
    )
    self.assertEqual(context['rate_element'], '<rate>unlimited</rate>')
    self.assertIn(
        'jdbc:postgresql://localhost:5432/postgres', context['jdbc_url']
    )


if __name__ == '__main__':
  unittest.main()
