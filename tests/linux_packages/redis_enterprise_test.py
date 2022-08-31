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

import os
import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import redis_enterprise
from tests import pkb_common_test_case

WRONG_RESULT = [sample.Sample('fake_metric', 0, 'fake_unit', {'threads': 10})]


def GetRedisEnterpriseOutput(file: str) -> str:
  path = os.path.join(os.path.dirname(__file__), '..', 'data', file)
  with open(path) as f:
    return f.read()


class RedisAdminTest(pkb_common_test_case.PkbCommonTestCase):

  def testMemorySize(self):
    output = GetRedisEnterpriseOutput('redis_enterprise_cluster_output.txt')
    mock_vm = mock.Mock()
    mock_vm.RemoteCommand.return_value = (output, None)
    actual = redis_enterprise.GetDatabaseMemorySize(mock_vm)
    self.assertEqual(51460000000, actual)


class ResultParserTest(pkb_common_test_case.PkbCommonTestCase):

  def testParseResults(self):
    output = GetRedisEnterpriseOutput('redis_enterprise_output.txt')
    actual = redis_enterprise.ParseResults(output)
    self.assertCountEqual([r.latency_usec for r in actual], [
        254.38488247733775,
        262.05762827864845,
        263.3261959721339,
    ])
    self.assertCountEqual([r.throughput for r in actual], [
        1420515,
        1413081,
        1419066,
    ])


class ThroughputOptimizerTest(pkb_common_test_case.PkbCommonTestCase):

  def _GetOptimizer(self, num_cpus):
    mock_server = mock.Mock(num_cpus=num_cpus)
    mock_client = mock.Mock()
    return redis_enterprise.ThroughputOptimizer([mock_server], [mock_client])

  def _CallList(self, calls):
    return [mock.call(*call) for call in calls]

  def testRun(self):
    # Arrange
    optimizer = self._GetOptimizer(16)
    self.enter_context(
        mock.patch.object(optimizer, '_CreateAndLoadDatabases'))
    self.enter_context(
        mock.patch.object(optimizer.client, 'GetDatabases', return_value={}))
    throughput_responses = [
        (60, WRONG_RESULT),
        (40, WRONG_RESULT),
        (70, WRONG_RESULT),
        (90, WRONG_RESULT),
        (80, WRONG_RESULT),
        (30, WRONG_RESULT),
        # This should be chosen as optimal throughput
        (100, [
            sample.Sample('max_throughput_under_1ms', 100, 'ops/sec',
                          {'threads': 100})
        ]),
        (20, WRONG_RESULT),
        (50, WRONG_RESULT),
        (40, WRONG_RESULT),
    ]
    self.enter_context(
        mock.patch.object(
            redis_enterprise, 'Run', side_effect=throughput_responses))

    # Act
    throughput, _ = optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    self.assertLen(optimizer.results, 32)
    self.assertEqual(optimizer.min_threads, 75)

  def testRunChecksCorrectNeighbors(self):
    # Arrange
    optimizer = self._GetOptimizer(16)
    throughput_responses = [
        (60, WRONG_RESULT),
        (40, WRONG_RESULT),
        (70, WRONG_RESULT),
        # This should be chosen as optimal throughput
        (100, [
            sample.Sample('max_throughput_under_1ms', 100, 'ops/sec',
                          {'threads': 100})
        ]),
        (20, WRONG_RESULT),
        (50, WRONG_RESULT),
        (40, WRONG_RESULT),
        (60, WRONG_RESULT),
    ]
    mock_run = self.enter_context(
        mock.patch.object(
            optimizer, '_FullRun', side_effect=throughput_responses))

    # Act
    throughput, _ = optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    mock_run.assert_has_calls(self._CallList([
        (3, 13),
        (3, 12),
        (3, 14),
        (2, 13),
        (4, 13),
        (2, 12),
        (2, 14),
        (1, 13),
    ]))

  @flagsaver.flagsaver(enterprise_redis_proxy_threads=3)
  def testRunChecksCorrectNeighborsVaryingShards(self):
    # Arrange
    optimizer = self._GetOptimizer(5)
    throughput_responses = [
        (70, WRONG_RESULT),
        (80, WRONG_RESULT),
        # This should be chosen as optimal throughput
        (100, [
            sample.Sample('max_throughput_under_1ms', 100, 'ops/sec',
                          {'threads': 100})
        ]),
        (20, WRONG_RESULT),
    ]
    mock_run = self.enter_context(
        mock.patch.object(
            optimizer, '_FullRun', side_effect=throughput_responses))

    # Act
    throughput, _ = optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    mock_run.assert_has_calls(self._CallList([
        (1, 3),
        (2, 3),
        (3, 3),
        (4, 3),
    ]))

  @flagsaver.flagsaver(enterprise_redis_shard_count=5)
  def testRunChecksCorrectNeighborsVaryingProxyThreads(self):
    # Arrange
    optimizer = self._GetOptimizer(5)
    throughput_responses = [
        (70, WRONG_RESULT),
        (80, WRONG_RESULT),
        # This should be chosen as optimal throughput
        (100, [
            sample.Sample('max_throughput_under_1ms', 100, 'ops/sec',
                          {'threads': 100})
        ]),
        (20, WRONG_RESULT),
    ]
    mock_run = self.enter_context(
        mock.patch.object(
            optimizer, '_FullRun', side_effect=throughput_responses))

    # Act
    throughput, _ = optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    mock_run.assert_has_calls(self._CallList([
        (5, 1),
        (5, 2),
        (5, 3),
        (5, 4),
    ]))

if __name__ == '__main__':
  unittest.main()
