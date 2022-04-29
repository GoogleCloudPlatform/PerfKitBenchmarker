# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for aws_dynamodb_ycsb_benchmark."""

import unittest

from absl import flags
import mock
from perfkitbenchmarker.linux_benchmarks import aws_dynamodb_ycsb_benchmark
from perfkitbenchmarker.providers.aws import aws_dynamodb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class RunTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(aws_dynamodb_ycsb_benchmark,
                          'GetRemoteVMCredentialsFullPath'))
    self.mock_spec = mock.MagicMock()
    self.mock_spec.executor = mock.MagicMock()

  def testRunThroughputIncreases(self):
    # Benchmark raises WCU to 10k during loading if WCU < 10k.
    # Arrange
    instance = aws_dynamodb.AwsDynamoDBInstance(rcu=1000, wcu=1000)
    mock_set_throughput = self.enter_context(
        mock.patch.object(instance, 'SetThroughput'))
    self.mock_spec.non_relational_db = instance

    # Act
    aws_dynamodb_ycsb_benchmark.Run(self.mock_spec)

    # Assert
    mock_set_throughput.assert_has_calls(
        [mock.call(wcu=10000), mock.call()])

  def testRunThroughputStaysSame(self):
    # WCU stays the same during loading if > 10k.
    # Arrange
    instance = aws_dynamodb.AwsDynamoDBInstance(rcu=1000, wcu=30000)
    mock_set_throughput = self.enter_context(
        mock.patch.object(instance, 'SetThroughput'))
    self.mock_spec.non_relational_db = instance

    # Act
    aws_dynamodb_ycsb_benchmark.Run(self.mock_spec)

    # Assert
    mock_set_throughput.assert_called_once_with()


if __name__ == '__main__':
  unittest.main()
