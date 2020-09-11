# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for netperf_benchmark."""

import json
import os
import unittest
from absl import flags
import mock
import itertools

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import iperf_benchmark

FLAGS = flags.FLAGS

class IperfBenchmarkTestCase(unittest.TestCase):


  def testIperfParseResults(self):

    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm0 = mock.MagicMock()
    vm1 = mock.MagicMock()
    vm_spec.vms = [vm0, vm1]
    path = os.path.join(os.path.dirname(__file__), '..', 'data', 'iperf_results.txt')
    outfile = open(path, 'r')
    iperf_stdout_list = outfile.read().split('#')

    for vm in vm_spec.vms:
      vm.RemoteCommand.side_effect = [(i, '') for i in iperf_stdout_list]

    results1 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 1, 'INTERNAL', 'UDP')
    results2 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 2, 'INTERNAL', 'UDP')
    results3 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 1, 'INTERNAL', 'TCP')
    results4 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 2, 'INTERNAL', 'TCP')

    self.assertEqual(results1.value, 1.05)
    self.assertEqual(results1.metadata['buffer_size'], 0.20)
    self.assertEqual(results1.metadata['datagram_size_bytes'], 1470)
    self.assertEqual(results1.metadata['write'], 5350)
    self.assertEqual(results1.metadata['err'], 0)
    self.assertEqual(results1.metadata['pps'], 89)
    self.assertEqual(results1.metadata['ipg_target'], 11215.21)
    self.assertEqual(results1.metadata['ipg_target_unit'], 'us')
    self.assertEqual(results1.metadata['jitter'], 0.017)
    self.assertEqual(results1.metadata['jitter_unit'], 'ms')
    self.assertEqual(results1.metadata['lost_datagrams'], 0)
    self.assertEqual(results1.metadata['total_datagrams'], 5350)
    self.assertEqual(results1.metadata['out_of_order_datagrams'], 0)

    self.assertEqual(results2.value, 2.10)
    self.assertEqual(results2.metadata['buffer_size'], 0.20)
    self.assertEqual(results2.metadata['datagram_size_bytes'], 1470)
    self.assertEqual(results2.metadata['write'], 10700)
    self.assertEqual(results2.metadata['err'], 0)
    self.assertEqual(results2.metadata['pps'], 178)
    self.assertEqual(results2.metadata['ipg_target'], 11215.21)
    self.assertEqual(results2.metadata['ipg_target_unit'], 'us')
    self.assertEqual(results2.metadata['jitter'], 0.0325)
    self.assertEqual(results2.metadata['jitter_unit'], 'ms')
    self.assertEqual(results2.metadata['lost_datagrams'], 1)
    self.assertEqual(results2.metadata['total_datagrams'], 10700)
    self.assertEqual(results2.metadata['out_of_order_datagrams'], 1)

    self.assertEqual(results3.value, 1966.0)
    self.assertEqual(results3.metadata['buffer_size'], 0.12)
    self.assertEqual(results3.metadata['tcp_window_size'], 1.67)
    self.assertEqual(results3.metadata['write'], 112505)
    self.assertEqual(results3.metadata['err'], 0)
    self.assertEqual(results3.metadata['retry'], 0)
    self.assertEqual(results3.metadata['cwnd'], -1)
    self.assertEqual(results3.metadata['rtt'], 1346)
    self.assertEqual(results3.metadata['rtt_unit'], 'us')
    self.assertEqual(results3.metadata['netpwr'], 182579.69)

    self.assertEqual(results4.value, 1970.0)
    self.assertEqual(results4.metadata['buffer_size'], 0.12)
    self.assertEqual(results4.metadata['tcp_window_size'], 0.17)
    self.assertEqual(results4.metadata['write'], 112760)
    self.assertEqual(results4.metadata['err'], 0)
    self.assertEqual(results4.metadata['retry'], 0)
    self.assertEqual(results4.metadata['cwnd'], -1)
    self.assertEqual(results4.metadata['rtt'], 183.5)
    self.assertEqual(results4.metadata['rtt_unit'], 'us')
    self.assertEqual(results4.metadata['netpwr'], 736102.93)

if __name__ == '__main__':
  unittest.main()
