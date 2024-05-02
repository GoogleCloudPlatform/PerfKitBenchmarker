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
from perfkitbenchmarker import sample
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
    # Second test VM with mocked command
    self.test_vm_2 = mock.Mock()
    self.test_cmd_2 = self.test_vm_2.RobustRemoteCommand
    self.test_cmd_2.return_value = ['', '']

  @flagsaver.flagsaver
  def testRunCalledWithCorrectTarget(self):
    # Act
    self.test_executor.Run([self.test_vm], run_kwargs={'target': 1000})

    # Assert
    self.assertIn('-target 1000', self.test_cmd.call_args[0][0])

  @flagsaver.flagsaver(ycsb_target_qps=250)
  def testRunCalledWithCorrectTargetUsingFlag(self):
    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertIn('-target 250', self.test_cmd.call_args[0][0])

  @flagsaver.flagsaver
  def testRunCalledWithCorrectTargetMultiVm(self):
    self.enter_context(
        mock.patch.object(
            ycsb_stats, 'CombineResults', return_value=ycsb_stats.YcsbResult()
        )
    )

    self.test_executor.Run(
        [self.test_vm, self.test_vm_2], run_kwargs={'target': 1000}
    )

    self.assertIn('-target 500', self.test_cmd.call_args[0][0])
    self.assertIn('-target 500', self.test_cmd_2.call_args[0][0])

  @flagsaver.flagsaver(ycsb_run_parameters=['target=100'])
  def testRunCalledWithCorrectTargetMultiVmRunParameters(self):
    self.enter_context(
        mock.patch.object(
            ycsb_stats, 'CombineResults', return_value=ycsb_stats.YcsbResult()
        )
    )

    self.test_executor.Run([self.test_vm, self.test_vm_2])

    self.assertIn('-target 100', self.test_cmd.call_args[0][0])
    self.assertIn('-target 100', self.test_cmd_2.call_args[0][0])

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

  @flagsaver.flagsaver(
      ycsb_cpu_optimization=True,
      ycsb_cpu_optimization_target_min=0.4,
      ycsb_cpu_optimization_target=0.5,
  )
  def testCpuMode(self):
    database = mock.Mock()
    database.CalculateTheoreticalMaxThroughput = mock.Mock(return_value=1000)
    database.GetAverageCpuUsage = mock.Mock(side_effect=[0.2, 0.7, 0.3, 0.45])
    self.enter_context(
        mock.patch.object(
            self.test_executor,
            'RunStaircaseLoads',
            side_effect=[
                [s]
                for s in _GetMockThroughputSamples([200, 100, 300, 150, 250])
            ],
        )
    )

    results = self.test_executor.Run([self.test_vm], database=database)

    self.assertEqual(results[0].metric, 'overall Throughput')
    self.assertEqual(results[0].value, 250)
    self.assertEqual(results[0].metadata['ycsb_cpu_utilization'], 0.45)

  @parameterized.parameters((300, 2.5, 8), (300, 1, 12), (300, 5, 12))
  @flagsaver.flagsaver(ycsb_lowest_latency_load=True)
  def testLowestLatencyMode(self, qps, read_latency, update_latency):
    self.enter_context(
        mock.patch.object(
            self.test_executor,
            'RunStaircaseLoads',
            side_effect=[
                _GetMockThroughputLatencySamples(100, 3, 10),
                _GetMockThroughputLatencySamples(150, 3, 9),
                _GetMockThroughputLatencySamples(200, 1, 9),
                _GetMockThroughputLatencySamples(
                    qps, read_latency, update_latency
                ),
            ],
        )
    )

    results = self.test_executor.Run([self.test_vm])

    self.assertEqual(results[0].value, 200)
    self.assertEqual(results[1].value, 1)
    self.assertEqual(results[2].value, 9)

  @flagsaver.flagsaver(
      ycsb_lowest_latency_load=True, ycsb_lowest_latency_target_qps=250
  )
  def testLowestLatencyModeWithFixedThroughput(self):
    self.enter_context(
        mock.patch.object(
            self.test_executor,
            'RunStaircaseLoads',
            side_effect=[
                _GetMockThroughputLatencySamples(100, 3, 10),
            ],
        )
    )
    result = self.test_executor.Run([self.test_vm])
    self.assertIn('ycsb_lowest_latency_fixed_qps', result[0].metadata)

  @flagsaver.flagsaver(
      ycsb_latency_threshold_mode=True,
      ycsb_latency_threshold_target=80,
      ycsb_latency_threshold_target_min=75,
      ycsb_latency_threshold_sleep_mins=0,
  )
  def testLatencyThresholdMode(self):
    self.enter_context(
        mock.patch.object(
            self.test_executor,
            'RunStaircaseLoads',
            side_effect=[
                _GetMockThroughputLatencySamples(200, 100, 30, 'p99'),
                _GetMockThroughputLatencySamples(150, 50, 15, 'p99'),
                _GetMockThroughputLatencySamples(175, 76, 25, 'p99'),
            ],
        )
    )

    results = self.test_executor.Run([self.test_vm])

    self.assertEqual(results[0].value, 175)
    self.assertEqual(results[1].value, 76)
    self.assertEqual(results[2].value, 25)


def _GetMockThroughputSamples(throughputs):
  result = []
  for throughput in throughputs:
    result.append(
        sample.Sample(
            metric='overall Throughput', value=throughput, unit='ops/sec'
        )
    )
  return result


def _GetMockThroughputLatencySamples(
    throughput, read_latency, update_latency, percentile='p95'
):
  return [
      sample.Sample('overall Throughput', value=throughput, unit='ops/sec'),
      sample.Sample(
          f'read {percentile} latency',
          value=read_latency,
          unit='ms',
      ),
      sample.Sample(
          f'update {percentile} latency',
          value=update_latency,
          unit='ms',
      ),
  ]


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
