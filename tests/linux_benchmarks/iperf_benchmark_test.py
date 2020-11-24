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
"""Tests for iperf_benchmark."""

import unittest
from absl import flags
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker.linux_benchmarks import iperf_benchmark

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class IperfBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    super(IperfBenchmarkTestCase, self).setUp()

    self.vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm0 = mock.MagicMock(
        internal_ip='10.128.0.1',
        machine_type='mock_machine_1',
        zone='antarctica-1a')
    vm1 = mock.MagicMock(
        internal_ip='10.128.0.2',
        machine_type='mock_machine_1',
        zone='antarctica-1a')
    self.vm_spec.vms = [vm0, vm1]

  def testIperfParseResultsUDPSingleThread(self):

    iperf_output = """
      Client connecting to 10.128.0.2, UDP port 25000 with pid 10159
      Sending 1470 byte datagrams, IPG target: 11215.21 us (kalman adjust)
      UDP buffer size: 0.20 MByte (default)
      ------------------------------------------------------------
      [  3] local 10.128.0.3 port 37350 connected with 10.128.0.2 port 25000
      [ ID] Interval        Transfer     Bandwidth      Write/Err  PPS
      [  3] 0.00-60.00 sec  7.50 MBytes  1.05 Mbits/sec  5350/0       89 pps
      [  3] Sent 5350 datagrams
      [  3] Server Report:
      [  3]  0.0-60.0 sec  7.50 MBytes  1.05 Mbits/sec   0.017 ms    0/ 5350 (0%)
      """

    self.vm_spec.vms[0].RemoteCommand.side_effect = [(iperf_output, '')]
    results = iperf_benchmark._RunIperf(self.vm_spec.vms[0],
                                        self.vm_spec.vms[1], '10.128.0.2', 1,
                                        'INTERNAL', 'UDP')

    expected_results = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 1,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.20,
        'datagram_size_bytes': 1470,
        'write_packet_count': 5350,
        'err_packet_count': 0,
        'pps': 89,
        'ipg_target': 11215.21,
        'ipg_target_unit': 'us',
        'jitter': 0.017,
        'jitter_unit': 'ms',
        'lost_datagrams': 0,
        'total_datagrams': 5350,
        'out_of_order_datagrams': 0
    }

    self.assertEqual(results.value, 1.05)
    self.assertEqual(expected_results, results.metadata)

  def testIperfParseResultsUDPMultiThread(self):

    iperf_output = """
      Client connecting to 10.128.0.2, UDP port 25000 with pid 10188
      Sending 1470 byte datagrams, IPG target: 11215.21 us (kalman adjust)
      UDP buffer size: 0.20 MByte (default)
      ------------------------------------------------------------
      [  4] local 10.128.0.3 port 51681 connected with 10.128.0.2 port 25000
      [  3] local 10.128.0.3 port 58632 connected with 10.128.0.2 port 25000
      [ ID] Interval        Transfer     Bandwidth      Write/Err  PPS
      [  3] 0.00-60.00 sec  7.50 MBytes  1.05 Mbits/sec  5350/0       89 pps
      [  3] Sent 5350 datagrams
      [  3] Server Report:
      [  3]  0.0-60.0 sec  7.50 MBytes  1.05 Mbits/sec   0.026 ms    1/ 5350 (0.019%)
      [  4] 0.00-60.00 sec  7.50 MBytes  1.05 Mbits/sec  5350/0       89 pps
      [  4] Sent 5350 datagrams
      [SUM] 0.00-60.00 sec  15.0 MBytes  2.10 Mbits/sec  10700/0      178 pps
      [SUM] Sent 10700 datagrams
      [  4] Server Report:
      [  4]  0.0-60.0 sec  7.50 MBytes  1.05 Mbits/sec   0.039 ms    0/ 5350 (0%)
      [  4] 0.00-60.00 sec  1 datagrams received out-of-order
      """

    self.vm_spec.vms[0].RemoteCommand.side_effect = [(iperf_output, '')]
    results = iperf_benchmark._RunIperf(self.vm_spec.vms[0],
                                        self.vm_spec.vms[1], '10.128.0.2', 2,
                                        'INTERNAL', 'UDP')

    expected_results = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 2,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.20,
        'datagram_size_bytes': 1470,
        'write_packet_count': 10700,
        'err_packet_count': 0,
        'pps': 178,
        'ipg_target': 11215.21,
        'ipg_target_unit': 'us',
        'jitter': 0.0325,
        'jitter_unit': 'ms',
        'lost_datagrams': 1,
        'total_datagrams': 10700,
        'out_of_order_datagrams': 1
    }

    self.assertEqual(expected_results, results.metadata)
    self.assertEqual(results.value, 2.10)

  def testIperfParseResultsTCPSingleThread(self):

    iperf_output = """
      Client connecting to 10.128.0.2, TCP port 20000 with pid 10208
      Write buffer size: 0.12 MByte
      TCP window size: 1.67 MByte (default)
      ------------------------------------------------------------
      [  3] local 10.128.0.3 port 33738 connected with 10.128.0.2 port 20000 (ct=1.62 ms)
      [ ID] Interval        Transfer    Bandwidth       Write/Err  Rtry     Cwnd/RTT        NetPwr
      [  3] 0.00-60.00 sec  14063 MBytes  1966 Mbits/sec  112505/0          0       -1K/1346 us  182579.69
      """

    self.vm_spec.vms[0].RemoteCommand.side_effect = [(iperf_output, '')]
    results = iperf_benchmark._RunIperf(self.vm_spec.vms[0],
                                        self.vm_spec.vms[1], '10.128.0.2', 1,
                                        'INTERNAL', 'TCP')

    expected_results = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 1,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.12,
        'tcp_window_size': 1.67,
        'write_packet_count': 112505,
        'err_packet_count': 0,
        'retry_packet_count': 0,
        'congestion_window': -1,
        'rtt': 1346,
        'rtt_unit': 'us',
        'netpwr': 182579.69
    }

    self.assertEqual(expected_results, results.metadata)
    self.assertEqual(results.value, 1966.0)

  def testIperfParseResultsTCPMultiThread(self):

    iperf_output = """
      Client connecting to 10.128.0.2, TCP port 20000 with pid 10561
      Write buffer size: 0.12 MByte
      TCP window size: 0.17 MByte (default)
      ------------------------------------------------------------
      [  4] local 10.128.0.2 port 54718 connected with 10.128.0.3 port 20000 (ct=0.16 ms)
      [  3] local 10.128.0.2 port 54716 connected with 10.128.0.3 port 20000 (ct=0.30 ms)
      [ ID] Interval        Transfer    Bandwidth       Write/Err  Rtry     Cwnd/RTT        NetPwr
      [  4] 0.00-60.01 sec  7047 MBytes   985 Mbits/sec  56373/0          0       -1K/238 us  517366.48
      [  3] 0.00-60.00 sec  7048 MBytes   985 Mbits/sec  56387/0          0       -1K/129 us  954839.38
      [SUM] 0.00-60.01 sec  14095 MBytes  1970 Mbits/sec  112760/0         0
      """

    self.vm_spec.vms[0].RemoteCommand.side_effect = [(iperf_output, '')]
    results = iperf_benchmark._RunIperf(self.vm_spec.vms[0],
                                        self.vm_spec.vms[1], '10.128.0.2', 2,
                                        'INTERNAL', 'TCP')

    expected_results = {
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 2,
        'sending_zone': 'antarctica-1a',
        'runtime_in_seconds': 60,
        'ip_type': 'INTERNAL',
        'buffer_size': 0.12,
        'tcp_window_size': 0.17,
        'write_packet_count': 112760,
        'err_packet_count': 0,
        'retry_packet_count': 0,
        'congestion_window': -1,
        'rtt': 183.5,
        'rtt_unit': 'us',
        'netpwr': 736102.93
    }

    self.assertEqual(expected_results, results.metadata)
    self.assertEqual(results.value, 1970.0)


if __name__ == '__main__':
  unittest.main()
