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
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import aws_dynamodb_ycsb_benchmark
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.aws import aws_dynamodb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class LoadStageTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(aws_dynamodb_ycsb_benchmark,
                          'GetRemoteVMCredentialsFullPath'))
    self.enter_context(
        mock.patch.object(vm_util, 'RunThreaded'))
    self.enter_context(mock.patch.object(ycsb.YCSBExecutor, 'Load'))
    self.mock_spec = mock.MagicMock()

  def testLoadThroughputIncreases(self):
    # Benchmark raises WCU to 10k during loading if WCU < 10k.
    # Arrange
    instance = aws_dynamodb.AwsDynamoDBInstance(rcu=1000, wcu=1000)
    mock_set_throughput = self.enter_context(
        mock.patch.object(instance, 'SetThroughput'))
    self.mock_spec.non_relational_db = instance

    # Act
    aws_dynamodb_ycsb_benchmark.Prepare(self.mock_spec)

    # Assert
    mock_set_throughput.assert_has_calls(
        [mock.call(wcu=10000), mock.call()])

  def testLoadThroughputStaysSame(self):
    # WCU stays the same during loading if > 10k.
    # Arrange
    instance = aws_dynamodb.AwsDynamoDBInstance(rcu=1000, wcu=30000)
    mock_set_throughput = self.enter_context(
        mock.patch.object(instance, 'SetThroughput'))
    self.mock_spec.non_relational_db = instance

    # Act
    aws_dynamodb_ycsb_benchmark.Prepare(self.mock_spec)

    # Assert
    mock_set_throughput.assert_called_once_with()

  @flagsaver.flagsaver(
      aws_dynamodb_autoscaling_target=50,
      aws_dynamodb_autoscaling_wcu_max=100,
      aws_dynamodb_autoscaling_rcu_max=200,
      aws_dynamodb_ycsb_provision_wcu=10000)
  def testLoadPhaseWithAutoscalingDoesNotSetThroughput(self):
    instance = aws_dynamodb.AwsDynamoDBInstance(rcu=1000, wcu=3000)
    mock_set_throughput = self.enter_context(
        mock.patch.object(instance, 'SetThroughput'))
    self.mock_spec.non_relational_db = instance

    aws_dynamodb_ycsb_benchmark.Prepare(self.mock_spec)

    mock_set_throughput.assert_not_called()

if __name__ == '__main__':
  unittest.main()
