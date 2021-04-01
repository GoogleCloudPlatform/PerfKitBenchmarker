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
from perfkitbenchmarker.linux_benchmarks import cuda_memcpy_benchmark
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class CudaMemcpyBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                              test_util.SamplesTestMixin):

  def setUp(self) -> None:
    super(CudaMemcpyBenchmarkTest, self).setUp()
    self.enter_context(mock.patch.object(
        nvidia_driver, 'QueryNumberOfGpus', return_value=1))
    self.enter_context(mock.patch.object(
        cuda_toolkit, 'GetMetadata', return_value={}))

  def CudaOutput(self) -> str:
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'cuda_memcpy_output.txt')
    with open(path) as reader:
      return reader.read()

  def MockVm(self) -> mock.Mock:
    vm = mock.Mock()
    vm.RemoteCommandWithReturnCode.return_value = self.CudaOutput(), '', 0
    return vm

  @mock.patch.object(nvidia_driver, 'CheckNvidiaSmiExists', return_value=True)
  def testCmd(self, check_nvidia_smi_exists: mock.Mock) -> None:
    vm = self.MockVm()
    cuda_memcpy_benchmark.Run(mock.Mock(vms=[vm]))
    vm.RemoteCommandWithReturnCode.assert_called_with(
        '/usr/local/cuda/extras/demo_suite/bandwidthTest --csv --memory=pinned '
        '--mode=quick --htod --dtoh --dtod --device=0', ignore_failure=True)

  @mock.patch.object(nvidia_driver, 'CheckNvidiaSmiExists', return_value=True)
  def testSample(self, check_nvidia_smi_exists: mock.Mock) -> None:
    samples = cuda_memcpy_benchmark.Run(mock.Mock(vms=[self.MockVm()]))
    expected = sample.Sample(
        'H2D-Pinned', 8494.3, 'MB/s',
        {
            'time': 0.00377,
            'size': 33554432,
            'NumDevsUsed': '1',
            'device': 0,
            'command':
                '/usr/local/cuda/extras/demo_suite/bandwidthTest --csv '
                '--memory=pinned --mode=quick --htod --dtoh --dtod --device=0',
            'memory': 'pinned',
            'mode': 'quick',
            'htod': True,
            'dtoh': True,
            'dtod': True,
            'wc': False,
        }
    )
    self.assertSamplesEqualUpToTimestamp(expected, samples[0])

  @mock.patch.object(nvidia_driver, 'CheckNvidiaSmiExists', return_value=False)
  def testEmptySample(self, check_nvidia_smi_exists: mock.Mock) -> None:
    samples = cuda_memcpy_benchmark.Run(mock.Mock(vms=[self.MockVm()]))
    self.assertLen(samples, 0)


if __name__ == '__main__':
  unittest.main()
