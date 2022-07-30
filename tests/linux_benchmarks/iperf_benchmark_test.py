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

  def testIperfParseResultsTCPMultiThreadWithIntervalReports(self):

    iperf_output = """
        Client connecting to 10.128.0.2, TCP port 20000 with pid 10054
        Write buffer size: 0.12 MByte
        TCP window size: 0.08 MByte (default)
        ------------------------------------------------------------
        [  3] local 10.160.0.98 port 59524 connected with 10.128.0.68 port 20000 (ct=261.36 ms)
        [  4] local 10.160.0.98 port 59526 connected with 10.128.0.68 port 20000 (ct=260.81 ms)
        [  5] local 10.160.0.98 port 59528 connected with 10.128.0.68 port 20000 (ct=268.94 ms)
        [ ID] Interval        Transfer    Bandwidth       Write/Err  Rtry     Cwnd/RTT        NetPwr
        [  3] 0.00-1.00 sec  0.62 MBytes  5.21 Mbits/sec  5/0          0       28K/261265 us  2.49
        [  4] 0.00-1.00 sec  0.75 MBytes  6.29 Mbits/sec  6/0          0       28K/260765 us  3.02
        [  5] 0.00-1.00 sec  0.75 MBytes  6.29 Mbits/sec  6/0          0       28K/268926 us  2.92
        [SUM] 0.00-1.00 sec  2.12 MBytes  17.8 Mbits/sec  17/0         0
        [  3] 1.00-2.00 sec  7.88 MBytes  66.1 Mbits/sec  63/0          0      226K/261302 us  31.60
        [  4] 1.00-2.00 sec  11.2 MBytes  94.4 Mbits/sec  90/0          0      226K/260657 us  45.26
        [  5] 1.00-2.00 sec  10.0 MBytes  83.9 Mbits/sec  80/0          0      226K/268935 us  38.99
        [SUM] 1.00-2.00 sec  29.1 MBytes   244 Mbits/sec  233/0         0
        [  5] 2.00-3.00 sec  45.2 MBytes   379 Mbits/sec  362/0          0     3011K/268952 us  176.18
        [  3] 2.00-3.00 sec   106 MBytes   892 Mbits/sec  851/0          0     3620K/261276 us  426.91
        [  4] 2.00-3.00 sec   125 MBytes  1045 Mbits/sec  997/0          0     3597K/261199 us  499.98
        [SUM] 2.00-3.00 sec   276 MBytes  2316 Mbits/sec  2210/0         0
        [  5] 3.00-4.00 sec  71.0 MBytes   596 Mbits/sec  568/0          0    22316K/269496 us  276.25
        [  3] 3.00-4.00 sec  66.5 MBytes   558 Mbits/sec  532/0          0    49005K/264374 us  263.76
        [  4] 3.00-4.00 sec  77.8 MBytes   652 Mbits/sec  622/0          0    51703K/264568 us  308.15
        [SUM] 3.00-4.00 sec   215 MBytes  1806 Mbits/sec  1722/0         0
        [  5] 4.00-5.00 sec  71.2 MBytes   598 Mbits/sec  570/0          0    22316K/270509 us  276.19
        [  5] 0.00-5.04 sec   198 MBytes   330 Mbits/sec  1586/0          0       -1K/158775 us  259.61
        [  3] 4.00-5.00 sec  66.5 MBytes   558 Mbits/sec  532/0          0    49005K/264387 us  263.74
        [  3] 0.00-5.07 sec   248 MBytes   410 Mbits/sec  1983/0          0       -1K/261758 us  195.93
        [  4] 4.00-5.00 sec  78.8 MBytes   661 Mbits/sec  630/0          0    51703K/263757 us  313.07
        [SUM] 4.00-5.00 sec   216 MBytes  1816 Mbits/sec  1732/0         0
        [  4] 0.00-5.24 sec   293 MBytes   469 Mbits/sec  2345/0          0       -1K/183205 us  320.04
        [SUM] 0.00-5.24 sec   739 MBytes  1183 Mbits/sec  5914/0         0
      """

    self.vm_spec.vms[0].RemoteCommand.side_effect = [(iperf_output, '')]
    results = iperf_benchmark._RunIperf(self.vm_spec.vms[0],
                                        self.vm_spec.vms[1], '10.128.0.2', 2,
                                        'INTERNAL', 'TCP')

    expected_results = {'buffer_size': 0.12,
        'congestion_window': 17135.86666666667,
        'congestion_window_scale': 'K',
        'err_packet_count': '0',
        'interval_congestion_window_list': [28.0,
                                            226.0,
                                            3409.3333333333335,
                                            41008.0,
                                            41008.0],
        'interval_length_seconds': 1.0,
        'interval_netpwr_list': [8.43,
                                 115.85,
                                 1103.0700000000002,
                                 848.16,
                                 853.0],
        'interval_retry_list': [0, 0, 0, 0, 0],
        'interval_rtt_list': [263652.0,
                              263631.3333333333,
                              263809.0,
                              266146.0,
                              266217.6666666667],
        'interval_start_time_list': [0.0, 1.0, 2.0, 3.0, 4.0],
        'interval_throughput_list': [17.8, 244.0, 2316.0, 1806.0, 1816.0],
        'ip_type': 'internal',
        'netpwr': 2928.51,
        'receiving_machine_type': 'mock_machine_1',
        'receiving_zone': 'antarctica-1a',
        'retry_packet_count': '0',
        'rtt': 264691.2,
        'rtt_unit': 'us',
        'run_number': 0,
        'runtime_in_seconds': 5,
        'sending_machine_type': 'mock_machine_1',
        'sending_thread_count': 3,
        'sending_zone': 'antarctica-1a',
        'tcp_window_size': 0.08,
        'transfer_mbytes': '739',
        'write_packet_count': '5914'},


    self.assertEqual(expected_results, results.metadata)
    self.assertEqual(results.value, 1183.0)


if __name__ == '__main__':
  unittest.main()
