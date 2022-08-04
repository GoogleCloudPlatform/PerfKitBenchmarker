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


class ResultParserTest(pkb_common_test_case.PkbCommonTestCase):

  def GetRedisEnterpriseOutput(self, file: str) -> str:
    path = os.path.join(os.path.dirname(__file__), '..', 'data', file)
    with open(path) as f:
      return f.read()

  def testParseResults(self):
    output = self.GetRedisEnterpriseOutput('redis_enterprise_output.txt')
    actual = redis_enterprise.ParseResult(output)
    # DB 4 has 5 elements in the time series so final length is 5 instead of 10.
    self.assertCountEqual(actual.latencies, [
        895.4008497116798,
        889.6032863841133,
        898.4151736901495,
        857.8283013278468,
        825.5712436699047,
    ])
    self.assertCountEqual(actual.throughputs, [
        423465.0,
        423435.0,
        426084.0,
        435096.0,
        441108.0,
    ])


class ThroughputOptimizerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(ThroughputOptimizerTest, self).setUp()
    mock_server = mock.Mock(num_cpus=16)
    mock_client = mock.Mock()
    mock_port = 1
    self.optimizer = redis_enterprise.ThroughputOptimizer([mock_server],
                                                          [mock_client],
                                                          mock_port)

  def _CallList(self, calls):
    return [mock.call(*call) for call in calls]

  def testRun(self):
    # Arrange
    self.enter_context(
        mock.patch.object(self.optimizer, '_CreateAndLoadDatabase'))
    self.enter_context(
        mock.patch.object(redis_enterprise, '_GetDatabase', return_value={}))
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
    throughput, _ = self.optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    self.assertLen(self.optimizer.results, 32)
    self.assertEqual(self.optimizer.min_threads, 75)

  def testRunChecksCorrectNeighbors(self):
    # Arrange
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
            self.optimizer, '_FullRun', side_effect=throughput_responses))

    # Act
    throughput, _ = self.optimizer.GetOptimalThroughput()

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
            self.optimizer, '_FullRun', side_effect=throughput_responses))

    # Act
    throughput, _ = self.optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    mock_run.assert_has_calls(self._CallList([
        (3, 3),
        (2, 3),
        (4, 3),
        (5, 3),
    ]))

  @flagsaver.flagsaver(enterprise_redis_shard_count=5)
  def testRunChecksCorrectNeighborsVaryingProxyThreads(self):
    # Arrange
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
            self.optimizer, '_FullRun', side_effect=throughput_responses))

    # Act
    throughput, _ = self.optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    mock_run.assert_has_calls(self._CallList([
        (5, 11),
        (5, 10),
        (5, 12),
        (5, 13),
    ]))

if __name__ == '__main__':
  unittest.main()
