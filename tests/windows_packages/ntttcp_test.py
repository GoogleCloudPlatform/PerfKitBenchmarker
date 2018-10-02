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

"""Tests for ntttcp_benchmark."""

import os
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.windows_packages import ntttcp


class NtttcpBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def getDataContents(self, file_name):
    path = os.path.join(os.path.dirname(__file__), '..', 'data', file_name)
    with open(path) as fp:
      contents = fp.read()
    return contents

  def setUp(self):
    self.xml_tcp_send_results = self.getDataContents('ntttcp_tcp_sender.xml')
    self.xml_tcp_rec_results = self.getDataContents('ntttcp_tcp_receiver.xml')
    self.xml_udp_send_results = self.getDataContents('ntttcp_udp_sender.xml')
    self.xml_udp_rec_results = self.getDataContents('ntttcp_udp_receiver.xml')

  def testNtttcpTcpParsing(self):
    samples = ntttcp.ParseNtttcpResults(self.xml_tcp_send_results,
                                        self.xml_tcp_rec_results, {})

    expected_metadata = {
        'async': 'False',
        'bind_sender': 'False',
        'cooldown_time': '30000',
        'dash_n_timeout': '10800000',
        'max_active_threads': '2',
        'no_sync': 'False',
        'port': '5003',
        'receiver avg_bytes_per_compl': '149.998',
        'receiver avg_frame_size': '1266.217',
        'receiver avg_packets_per_dpc': '0.598',
        'receiver avg_packets_per_interrupt': '0.379',
        'receiver bufferCount': '9223372036854775807',
        'receiver bufferLen': '150',
        'receiver cpu': '36.872',
        'receiver cycles': '89.055',
        'receiver dpcs': '48156.278',
        'receiver errors': '1',
        'receiver interrupts': '75870.499',
        'receiver io': '2',
        'receiver packets_received': '1726938',
        'receiver packets_retransmitted': '4',
        'receiver packets_sent': '1092640',
        'receiver realtime': '60.015000',
        'receiver rb': -1,
        'receiver sb': -1,
        'receiver threads_avg_bytes_per_compl': '149.998',
        'receiver throughput': '291.484',
        'receiver total_buffers': '14577858.000',
        'receiver total_bytes': '2085.379314',
        'recv_socket_buff': '-1',
        'run_time': '60000',
        'sender avg_bytes_per_compl': '150.000',
        'sender avg_frame_size': '751.222',
        'sender avg_packets_per_dpc': '1.064',
        'sender avg_packets_per_interrupt': '0.516',
        'sender bufferCount': '9223372036854775807',
        'sender bufferLen': '150',
        'sender cpu': '36.234',
        'sender cycles': '87.514',
        'sender dpcs': '17108.590',
        'sender errors': '0',
        'sender interrupts': '35302.624',
        'sender io': '2',
        'sender_name': None,
        'sender packets_received': '1092639',
        'sender packets_retransmitted': '10',
        'sender packets_sent': '2910833',
        'sender realtime': '60.015000',
        'sender rb': -1,
        'sender sb': -1,
        'sender threads_avg_bytes_per_compl': '150.000',
        'sender total_buffers': '14577884.000',
        'sender total_bytes': '2085.383034',
        'send_socket_buff': '8192',
        'sync_port': 'False',
        'udp': 'False',
        'use_ipv6': 'False',
        'verbose': 'False',
        'verify_data': 'False',
        'wait_all': 'False',
        'wait_timeout_milliseconds': '600000',
        'warmup_time': '30000',
        'wsa': 'False',
    }

    expected_thread_0_metadata = expected_metadata.copy()
    expected_thread_0_metadata['thread_index'] = '0'

    expected_thread_1_metadata = expected_metadata.copy()
    expected_thread_1_metadata['thread_index'] = '1'

    expected_samples = [
        sample.Sample('Total Throughput', 291.485, 'Mbps', expected_metadata),
        sample.Sample('Thread Throughput', 147.105, 'Mbps',
                      expected_thread_0_metadata),
        sample.Sample('Thread Throughput', 144.379, 'Mbps',
                      expected_thread_1_metadata)
    ]

    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)

  def testNtttcpUdpParsing(self):
    samples = ntttcp.ParseNtttcpResults(self.xml_udp_send_results,
                                        self.xml_udp_rec_results, {})

    expected_metadata = {
        'async': 'False',
        'bind_sender': 'False',
        'cooldown_time': '30000',
        'dash_n_timeout': '10800000',
        'max_active_threads': '2',
        'no_sync': 'False',
        'port': '5003',
        'receiver avg_bytes_per_compl': '128.000',
        'receiver avg_frame_size': '99.200',
        'receiver avg_packets_per_dpc': '6.147',
        'receiver avg_packets_per_interrupt': '3.838',
        'receiver bufferCount': '9223372036854775807',
        'receiver bufferLen': '128',
        'receiver cpu': '51.120',
        'receiver cycles': '189.967',
        'receiver dpcs': '38835.774',
        'receiver errors': '0',
        'receiver interrupts': '62200.183',
        'receiver io': '2',
        'receiver packets_received': '14326674',
        'receiver packets_retransmitted': '0',
        'receiver packets_sent': '0',
        'receiver realtime': '60.015000',
        'receiver rb': -1,
        'receiver sb': -1,
        'receiver threads_avg_bytes_per_compl': '128.000',
        'receiver throughput': '189.447',
        'receiver total_buffers': '11103157.000',
        'receiver total_bytes': '1355.365845',
        'recv_socket_buff': '-1',
        'run_time': '60000',
        'sender avg_bytes_per_compl': '128.000',
        'sender avg_frame_size': '128.000',
        'sender avg_packets_per_dpc': '0.000',
        'sender avg_packets_per_interrupt': '0.000',
        'sender bufferCount': '9223372036854775807',
        'sender bufferLen': '128',
        'sender cpu': '68.290',
        'sender cycles': '196.108',
        'sender dpcs': '250.737',
        'sender errors': '0',
        'sender interrupts': '1669.516',
        'sender io': '2',
        'sender_name': None,
        'sender packets_received': '0',
        'sender packets_retransmitted': '0',
        'sender packets_sent': '14368008',
        'sender realtime': '60.015000',
        'sender rb': -1,
        'sender sb': -1,
        'sender threads_avg_bytes_per_compl': '128.000',
        'sender total_buffers': '14368009.000',
        'sender total_bytes': '1753.907349',
        'send_socket_buff': '8192',
        'sync_port': 'False',
        'udp': 'True',
        'use_ipv6': 'False',
        'verbose': 'False',
        'verify_data': 'False',
        'wait_all': 'False',
        'wait_timeout_milliseconds': '600000',
        'warmup_time': '30000',
        'wsa': 'False',
    }

    expected_thread_0_metadata = expected_metadata.copy()
    expected_thread_0_metadata['thread_index'] = '0'

    expected_thread_1_metadata = expected_metadata.copy()
    expected_thread_1_metadata['thread_index'] = '1'

    expected_samples = [
        sample.Sample('Total Throughput', 245.153, 'Mbps', expected_metadata),
        sample.Sample('Thread Throughput', 121.160, 'Mbps',
                      expected_thread_0_metadata),
        sample.Sample('Thread Throughput', 123.993, 'Mbps',
                      expected_thread_1_metadata)
    ]

    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)
