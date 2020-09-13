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
      vm.machine_type = 'mock_machine_1'
      vm.zone = 'antarctica-1a'

    results1 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 1, 'INTERNAL', 'UDP')
    results2 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 2, 'INTERNAL', 'UDP')
    results3 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 1, 'INTERNAL', 'TCP')
    results4 = iperf_benchmark._RunIperf(vm0, vm1, '10.128.0.2', 2, 'INTERNAL', 'TCP')

    expected_results1 = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 1,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.20,
        'datagram_size_bytes': 1470,
        'write': 5350,
        'err': 0,
        'pps': 89,
        'ipg_target': 11215.21,
        'ipg_target_unit': 'us',
        'jitter': 0.017,
        'jitter_unit': 'ms',
        'lost_datagrams': 0,
        'total_datagrams': 5350,
        'out_of_order_datagrams': 0
    }

    expected_results2 = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 2,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.20,
        'datagram_size_bytes': 1470,
        'write': 10700,
        'err': 0,
        'pps': 178,
        'ipg_target': 11215.21,
        'ipg_target_unit': 'us',
        'jitter': 0.0325,
        'jitter_unit': 'ms',
        'lost_datagrams': 1,
        'total_datagrams': 10700,
        'out_of_order_datagrams': 1
    }

    expected_results3 = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 1,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.12,
        'tcp_window_size': 1.67,
        'write': 112505,
        'err': 0,
        'retry': 0,
        'cwnd': -1,
        'rtt': 1346,
        'rtt_unit': 'us',
        'netpwr': 182579.69
    }

    expected_results4 = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 2,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.12,
        'tcp_window_size': 0.17,
        'write': 112760,
        'err': 0,
        'retry': 0,
        'cwnd': -1,
        'rtt': 183.5,
        'rtt_unit': 'us',
        'netpwr': 736102.93
    }

    self.assertEqual(expected_results1, results1.metadata)
    self.assertEqual(expected_results2, results2.metadata)
    self.assertEqual(expected_results3, results3.metadata)
    self.assertEqual(expected_results4, results4.metadata)

    self.assertEqual(results1.value, 1.05)
    self.assertEqual(results2.value, 2.10)
    self.assertEqual(results3.value, 1966.0)
    self.assertEqual(results4.value, 1970.0)

if __name__ == '__main__':
  unittest.main()
