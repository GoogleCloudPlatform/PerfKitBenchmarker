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


import unittest
import mock
from perfkitbenchmarker.linux_packages import redis_enterprise
from tests import pkb_common_test_case


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
    throughput_responses = [
        (60, None),
        (40, None),
        (70, None),
        (90, None),
        (80, None),
        (30, None),
        (100, None),
        (20, None),
        (50, None),
        (40, None),
    ]
    self.enter_context(
        mock.patch.object(
            redis_enterprise, 'Run', side_effect=throughput_responses))

    # Act
    throughput, _ = optimizer.GetOptimalThroughput()

    # Assert
    self.assertEqual(throughput, 100)
    self.assertLen(optimizer.results, 5)


if __name__ == '__main__':
  unittest.main()
