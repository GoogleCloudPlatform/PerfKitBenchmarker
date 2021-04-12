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
"""Tests for GPU PingPong benchmark.

Test GPU Pingpong benchmark run phrase code.
"""

import os
import unittest
from absl import flags
import mock

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import gpu_pingpong_benchmark
from perfkitbenchmarker.linux_packages import cuda_toolkit
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


def MockVm() -> mock.Mock:
  """Mocks test VM and returns sample results."""
  vm = mock.Mock()
  vm.hostname = 'localhost'
  path = os.path.join(os.path.dirname(__file__), '..', 'data',
                      'gpu_pingpong_output.txt')
  with open(path) as reader:
    stderr = reader.read()
  vm.RemoteCommand.return_value = '', stderr
  return vm


class GpuPingpongBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                               test_util.SamplesTestMixin):

  def setUp(self) -> None:
    super().setUp()
    self.enter_context(mock.patch.object(
        cuda_toolkit, 'GetMetadata', return_value={}))

  def testCmd(self) -> None:
    """Tests GPU PingPong test command on the remote VM."""
    vm = MockVm()
    gpu_pingpong_benchmark.Run(mock.Mock(vms=[vm, MockVm()]))
    vm.RemoteCommand.assert_called_with(
        'PATH=/opt/conda/bin:$PATH python gpu_pingpong_test.py localhost:2000')

  def testSample(self) -> None:
    """Tests GPU PingPong test parsing code."""
    samples = gpu_pingpong_benchmark.Run(mock.Mock(vms=[MockVm(), MockVm()]))
    expected = sample.Sample(metric='latency', value=618.9995654384239,
                             unit='microseconds',
                             metadata={'ping': 305.00009531252977,
                                       'pong': 313.99947012589416})
    self.assertSamplesEqualUpToTimestamp(expected, samples[0])
    self.assertLen(samples, 19)


if __name__ == '__main__':
  unittest.main()
