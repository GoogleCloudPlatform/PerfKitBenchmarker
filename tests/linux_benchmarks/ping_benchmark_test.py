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

import os
import unittest
from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker.linux_benchmarks import ping_benchmark

flags.FLAGS.mark_as_parsed()


class TestGenerateJobFileString(unittest.TestCase):

  def testRunCountTest(self):
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm0 = mock.MagicMock()
    vm1 = mock.MagicMock()
    vm_spec.vms = [vm0, vm1]
    path = os.path.join(os.path.dirname(__file__), '..', 'data', 'ping.out')
    outfile = open(path, 'r')
    pingstdout = outfile.read()
    for vm in vm_spec.vms:
      vm.RemoteCommand.side_effect = [(pingstdout, ' '), (pingstdout, ' ')]
    ping_benchmark.Prepare(vm_spec)
    samples = ping_benchmark.Run(vm_spec)
    ping_benchmark.Cleanup(vm_spec)

    self.assertEqual(vm_spec.vms[0].RemoteCommand.call_count, 2)
    self.assertEqual(vm_spec.vms[1].RemoteCommand.call_count, 2)
    self.assertEqual(len(samples), 16)


if __name__ == '__main__':
  unittest.main()
