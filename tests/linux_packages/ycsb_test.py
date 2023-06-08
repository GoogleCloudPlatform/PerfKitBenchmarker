# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.ycsb."""

import logging
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.linux_packages import ycsb_stats
from tests import matchers
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class PrerequisitesTestCase(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.named_parameters(
      {
          'testcase_name': 'SnapshotVersion',
          'url': 'https://storage.googleapis.com/externally_shared_files/ycsb-0.18.0-SNAPSHOT.tar.gz',
          'expected_version': 18,
      },
      {
          'testcase_name': 'StandardVersion',
          'url': 'https://storage.googleapis.com/ycsbclient/ycsb-0.17.0.tar.gz',
          'expected_version': 17,
      },
      {
          'testcase_name': 'GitHubVersion',
          'url': 'https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz',
          'expected_version': 17,
      },
  )
  def testGetVersionIndexFromUrl(self, url, expected_version):
    actual_version = ycsb._GetVersionFromUrl(url)
    self.assertEqual(actual_version, expected_version)

  @flagsaver.flagsaver
  def testBurstLoadCalledWithNoTargetRaises(self):
    # Arrange
    FLAGS.ycsb_burst_load = 1

    # Act & Assert
    with self.assertRaises(errors.Config.InvalidValue):
      ycsb.CheckPrerequisites()


class RunTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.ycsb_workload_files = ['workloadc']
    self.test_executor = ycsb.YCSBExecutor('test_database')
    # Result parsing is already handled elsewhere
    self.enter_context(mock.patch.object(ycsb_stats, 'ParseResults'))
    # Test VM with mocked command
    self.test_vm = mock.Mock()
    self.test_cmd = self.test_vm.RobustRemoteCommand
    self.test_cmd.return_value = ['', '']

  @flagsaver.flagsaver
  def testRunCalledWithCorrectTarget(self):
    # Act
    self.test_executor.Run([self.test_vm], run_kwargs={'target': 1000})

    # Assert
    self.assertIn('-target 1000', self.test_cmd.call_args[0][0])

  @flagsaver.flagsaver
  def testBurstLoadUnlimitedMultiplier(self):
    # Arrange
    FLAGS.ycsb_burst_load = -1
    FLAGS.ycsb_run_parameters = ['target=1000']

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    print(self.test_cmd.call_args_list)
    self.assertNotIn('target', self.test_cmd.call_args_list[1][0][0])

  @flagsaver.flagsaver
  def testBurstLoadCalledWithCorrectTarget(self):
    # Arrange
    FLAGS.ycsb_burst_load = 10
    FLAGS.ycsb_run_parameters = ['target=1000']

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertIn('-target 1000', self.test_cmd.call_args_list[0][0][0])
    self.assertIn('-target 10000', self.test_cmd.call_args_list[1][0][0])

  @flagsaver.flagsaver
  def testIncrementalLoadCalledWithCorrectTarget(self):
    # Arrange
    FLAGS.ycsb_incremental_load = 10000
    FLAGS.ycsb_client_vms = 1

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertSequenceEqual(
        [
            mock.call(matchers.HAS('-target 500')),
            mock.call(matchers.HAS('-target 750')),
            mock.call(matchers.HAS('-target 1125')),
            mock.call(matchers.HAS('-target 1687')),
            mock.call(matchers.HAS('-target 2531')),
            mock.call(matchers.HAS('-target 3796')),
            mock.call(matchers.HAS('-target 5695')),
            mock.call(matchers.HAS('-target 8542')),
            mock.call(matchers.HAS('-target 10000')),
        ],
        self.test_cmd.mock_calls,
    )

  @flagsaver.flagsaver
  def testIncrementalLoadUsesCorrectThreadCounts(self):
    # Arrange
    FLAGS.ycsb_incremental_load = 2500
    FLAGS.ycsb_client_vms = 1
    FLAGS['ycsb_threads_per_client'].parse(['1000'])
    mock_set_thread_count = self.enter_context(
        mock.patch.object(self.test_executor, '_SetClientThreadCount')
    )

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertSequenceEqual(
        [
            mock.call(500),
            mock.call(750),
            mock.call(1000),
            mock.call(1000),
            mock.call(1000),
        ],
        mock_set_thread_count.mock_calls,
    )

  @flagsaver.flagsaver
  def testIncrementalLoadCalledWithLowerTarget(self):
    # Arrange
    FLAGS.ycsb_incremental_load = 200  # Lower than 500, the default start
    FLAGS.ycsb_client_vms = 1

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertSequenceEqual(
        [mock.call(matchers.HAS('-target 200'))], self.test_cmd.mock_calls
    )


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
