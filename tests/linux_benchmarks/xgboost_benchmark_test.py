# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for nccl_benchmark."""

import os
import unittest
from absl import flags
import mock

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import xgboost_benchmark
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class XgboostBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                           test_util.SamplesTestMixin):

  def setUp(self) -> None:
    super(XgboostBenchmarkTest, self).setUp()
    self.enter_context(mock.patch.object(
        nvidia_driver, 'QueryNumberOfGpus', return_value=1))
    self.enter_context(mock.patch.object(
        cuda_toolkit, 'GetMetadata', return_value={}))

  def CudaOutput(self) -> str:
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'xgboost_output.txt')
    with open(path) as reader:
      return reader.read()

  def MockVm(self) -> mock.Mock:
    vm = mock.Mock()
    vm.RemoteCommandWithReturnCode.return_value = self.CudaOutput(), '', 0
    vm.RemoteCommand.return_value = '1.4.2', ''
    return vm

  def testCmd(self) -> None:
    vm = self.MockVm()
    xgboost_benchmark.Run(mock.Mock(vms=[vm]))
    vm.RemoteCommandWithReturnCode.assert_called_with(
        'PATH=/opt/conda/bin:$PATH python3 '
        '/opt/pkb/xgboost/tests/benchmark/benchmark_tree.py '
        '--tree_method=gpu_hist '
        '--sparsity=0.0 '
        '--rows=1000000 '
        '--columns=50 '
        '--iterations=500 '
        '--test_size=0.25', ignore_failure=True)

  def testSample(self) -> None:
    samples = xgboost_benchmark.Run(mock.Mock(vms=[self.MockVm()]))
    expected = sample.Sample(
        'training_time', 31.197044849395752, 'seconds',
        {
            'tree_method': 'gpu_hist',
            'sparsity': 0.0,
            'rows': 1000000,
            'columns': 50,
            'iterations': 500,
            'test_size': 0.25,
            'params': None,
            'xgboost_version': '1.4.2',
            'command':
                'PATH=/opt/conda/bin:$PATH python3 '
                '/opt/pkb/xgboost/tests/benchmark/benchmark_tree.py '
                '--tree_method=gpu_hist --sparsity=0.0 --rows=1000000 '
                '--columns=50 --iterations=500 --test_size=0.25'
        }
    )
    self.assertSamplesEqualUpToTimestamp(expected, samples[0])


if __name__ == '__main__':
  unittest.main()
