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

import time
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import lssd_workloads_util
from perfkitbenchmarker.linux_benchmarks import unmanaged_mysql_sysbench_benchmark as base_benchmark
from perfkitbenchmarker.linux_benchmarks import unmanaged_mysql_sysbench_lssd_benchmark
from tests import pkb_common_test_case


class UnmanagedMysqlSysbenchLssdBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    self.server = mock.create_autospec(
        linux_virtual_machine.BaseLinuxVirtualMachine,
        instance=True,
    )
    self.server.OS_TYPE = 'ubuntu2404'
    self.server.RemoteCommand.return_value = ('/dev/md0 3.0T 1.2T 1.8T 40%', '')
    self.benchmark_spec = mock.create_autospec(
        bm_spec.BenchmarkSpec, instance=True
    )
    self.benchmark_spec.vm_groups = {'server': [self.server]}
    self.mock_prepare_system = self.enter_context(
        mock.patch.object(
            base_benchmark, 'PrepareSystem', autospec=True, spec_set=True
        )
    )
    self.mock_install_packages = self.enter_context(
        mock.patch.object(
            base_benchmark, 'InstallPackages', autospec=True, spec_set=True
        )
    )
    self.mock_start_services = self.enter_context(
        mock.patch.object(
            base_benchmark, 'StartServices', autospec=True, spec_set=True
        )
    )
    self.mock_precondition_disk = self.enter_context(
        mock.patch.object(
            lssd_workloads_util,
            'PreconditionDisk',
            autospec=True,
            spec_set=True,
        )
    )
    self.mock_create_fs = self.enter_context(
        mock.patch.object(
            lssd_workloads_util,
            'CreateFileSystemAndMount',
            autospec=True,
            spec_set=True,
        )
    )
    self.enter_context(
        mock.patch.object(time, 'sleep', autospec=True, spec_set=True)
    )

  def test_get_config_applies_lssd_defaults(self):
    config = unmanaged_mysql_sysbench_lssd_benchmark.GetConfig({})
    self.assertEqual(
        config['flags']['sysbench_run_threads'],
        256,
    )
    self.assertEqual(
        config['flags']['sysbench_run_seconds'],
        21600,
    )
    self.assertEqual(
        config['vm_groups']['server']['disk_spec']['GCP']['mount_point'],
        '/var/lib/mysql',
    )

  @flagsaver.flagsaver(
      run_uri='1cbe792f',
      mysql_disable_log_bin=False,
      lssd_workload_precondition_disk=True,
      disk_fs_type='xfs',
  )
  def test_prepare_preconditions_disk_toggles_binlog_and_restarts_mysql(self):
    # First pgrep returns 0 (mysqld still running), second returns 1 (stopped).
    # First mysqladmin ping returns 1 (not ready), second returns 0 (ready).
    self.server.RemoteCommandWithReturnCode.side_effect = [
        ('1234\n', '', 0),
        ('', '', 1),
        ('', 'connect failed', 1),
        ('mysqld is alive\n', '', 0),
    ]

    unmanaged_mysql_sysbench_lssd_benchmark.Prepare(self.benchmark_spec)

    self.mock_prepare_system.assert_called_once_with(self.benchmark_spec)
    self.server.InstallPackages.assert_called_once_with('fio')
    self.mock_precondition_disk.assert_called_once_with(self.server)
    self.mock_create_fs.assert_called_once_with(self.server, fs_type='xfs')
    self.mock_install_packages.assert_called_once_with(self.benchmark_spec)
    self.mock_start_services.assert_called_once_with(self.benchmark_spec)

    remote_cmds = [
        call.args[0] for call in self.server.RemoteCommand.call_args_list
    ]
    config_path = '/etc/mysql/mysql.conf.d/mysqld.cnf'
    self.assertIn('sudo mkdir -p /var/lib/mysql/tmp', remote_cmds)
    self.assertIn('sudo chown mysql:mysql /var/lib/mysql/tmp', remote_cmds)
    self.assertIn(
        "sudo sed -i 's|^enforce-gtid-consistency.*|"
        f"enforce-gtid-consistency\\ndisable_log_bin|' {config_path}",
        remote_cmds,
    )
    self.assertIn(
        f"sudo sed -i '/^disable_log_bin/d' {config_path}",
        remote_cmds,
    )
    self.assertIn(
        "sudo mysqladmin -u root -p'1cbe792f_P3rfk1tbenchm4rker#' "
        '-S /var/lib/mysql/mysql.sock shutdown',
        remote_cmds,
    )
    self.assertIn(
        "sudo sed -i 's/nofile.*/nofile 500000/' /etc/security/limits.conf",
        remote_cmds,
    )
    self.assertIn(
        'sudo -g mysql -u mysql nohup mysqld &> /dev/null &',
        remote_cmds,
    )
    self.assertIn('df -h /var/lib/mysql', remote_cmds)
    self.assertEqual(
        self.server.RemoteCommandWithReturnCode.call_args_list,
        [
            mock.call('pgrep -x mysqld', ignore_failure=True),
            mock.call('pgrep -x mysqld', ignore_failure=True),
            mock.call(
                "sudo mysqladmin -u root -p'1cbe792f_P3rfk1tbenchm4rker#' "
                '-S /var/lib/mysql/mysql.sock ping',
                ignore_failure=True,
            ),
            mock.call(
                "sudo mysqladmin -u root -p'1cbe792f_P3rfk1tbenchm4rker#' "
                '-S /var/lib/mysql/mysql.sock ping',
                ignore_failure=True,
            ),
        ],
    )

  @flagsaver.flagsaver(
      run_uri='1cbe792f',
      mysql_disable_log_bin=True,
      lssd_workload_precondition_disk=False,
  )
  def test_prepare_skips_precondition_and_preserves_disabled_binlog(self):
    self.server.RemoteCommandWithReturnCode.side_effect = [
        ('', '', 1),
        ('mysqld is alive\n', '', 0),
    ]

    unmanaged_mysql_sysbench_lssd_benchmark.Prepare(self.benchmark_spec)

    self.server.InstallPackages.assert_not_called()
    self.mock_precondition_disk.assert_not_called()
    self.mock_create_fs.assert_not_called()

    remote_cmds = [
        call.args[0] for call in self.server.RemoteCommand.call_args_list
    ]
    for cmd in remote_cmds:
      self.assertNotIn('disable_log_bin', cmd)
    self.assertIn(
        "sudo sed -i 's/nofile.*/nofile 500000/' /etc/security/limits.conf",
        remote_cmds,
    )

  def test_run_delegates_to_base_benchmark(self):
    expected_samples = [sample.Sample('tps', 3221.24, 'tps', {})]
    mock_run = self.enter_context(
        mock.patch.object(
            base_benchmark,
            'Run',
            autospec=True,
            spec_set=True,
            return_value=expected_samples,
        )
    )

    actual_samples = unmanaged_mysql_sysbench_lssd_benchmark.Run(
        self.benchmark_spec
    )

    self.assertEqual(actual_samples, expected_samples)
    mock_run.assert_called_once_with(self.benchmark_spec)

  def test_cleanup_delegates_to_base_benchmark(self):
    mock_cleanup = self.enter_context(
        mock.patch.object(
            base_benchmark, 'Cleanup', autospec=True, spec_set=True
        )
    )

    unmanaged_mysql_sysbench_lssd_benchmark.Cleanup(self.benchmark_spec)

    mock_cleanup.assert_called_once_with(self.benchmark_spec)


if __name__ == '__main__':
  unittest.main()
