# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for ping_benchmark."""

import mock
import unittest

from perfkitbenchmarker.linux_benchmarks import cluster_boot_benchmark
from perfkitbenchmarker import benchmark_spec


class TestGenerateJobFileString(unittest.TestCase):

  def testRunCountTest(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm0 = mock.MagicMock()
    vm_spec.vms = [vm0]
    vm0.bootable_time = 3
    vm0.create_start_time = 2
    vm_spec.vm_boot_starttime = 1
    vm_spec.vm_boot_endtime = 4
    cluster_boot_benchmark.Prepare(vm_spec)
    samples = cluster_boot_benchmark.Run(vm_spec)
    cluster_boot_benchmark.Cleanup(vm_spec)
    self.assertEquals(len(samples), 2)

if __name__ == '__main__':
  unittest.main()
