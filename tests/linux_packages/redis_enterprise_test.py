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

import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import redis_enterprise
from tests import pkb_common_test_case


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

  def testRun(self):
    # Arrange
    mock_server = mock.Mock(num_cpus=5)
    mock_client = mock.Mock()
    mock_port = 1
    optimizer = redis_enterprise.ThroughputOptimizer([mock_server],
                                                     [mock_client], mock_port)
    self.enter_context(mock.patch.object(optimizer, '_CreateAndLoadDatabase'))
    self.enter_context(
        mock.patch.object(redis_enterprise, '_GetDatabase', return_value={}))
    wrong_result = [
        sample.Sample('fake_metric', 0, 'fake_unit', {'threads': 10})
    ]
    throughput_responses = [
        (60, wrong_result),
        (40, wrong_result),
        (70, wrong_result),
        (90, wrong_result),
        (80, wrong_result),
        (30, wrong_result),
        # This should be chosen as optimal throughput
        (100, [
            sample.Sample('max_throughput_under_1ms', 100, 'ops/sec',
                          {'threads': 100})
        ]),
        (20, wrong_result),
        (50, wrong_result),
        (40, wrong_result),
    ]
    self.enter_context(
        mock.patch.object(
            redis_enterprise, 'Run', side_effect=throughput_responses))

    # Act
    throughput, _ = optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    self.assertLen(optimizer.results, 5)
    self.assertEqual(optimizer.min_threads, 75)


if __name__ == '__main__':
  unittest.main()
