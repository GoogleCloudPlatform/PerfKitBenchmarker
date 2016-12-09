# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.cuda_toolkit_8."""

import unittest

import mock

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_packages import cuda_toolkit_8


class CudaToolkit8TestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    pass

  def tearDown(self):
    pass

  def testQueryNumberOfGpus(self):
    vm = mock.MagicMock()
    vm.RemoteCommand = mock.MagicMock(return_value=("count\n8", None))
    self.assertEqual(8, cuda_toolkit_8.QueryNumberOfGpus(vm))

  def testQueryGpuClockSpeed(self):
    vm = mock.MagicMock()
    vm.RemoteCommand = mock.MagicMock(
        return_value=("clocks.applications.graphics [MHz], "
                      "clocks.applications.memory [Mhz]\n"
                      "324 MHz, 527 MHz", None))
    self.assertEqual((324, 527), cuda_toolkit_8.QueryGpuClockSpeed(vm, 3))
    vm.RemoteCommand.assert_called_with(
        "sudo nvidia-smi "
        "--query-gpu=clocks.applications.memory,"
        "clocks.applications.graphics --format=csv --id=3", should_log=True)


if __name__ == '__main__':
  unittest.main()
