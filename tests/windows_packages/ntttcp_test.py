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

  def setUp(self):
    self.maxDiff = None
    # Load data
    path = os.path.join(os.path.dirname(__file__),
                        '..', 'data',
                        'ntttcp_results.xml')
    with open(path) as fp:
      self.ntttcp_xml_results = fp.read()

  def testNtttcpParsing(self):
    extra_metadata = {}
    samples = ntttcp.ParseNtttcpResults(self.ntttcp_xml_results,
                                        extra_metadata)

    expected_metadata = {
        'async': 'False', 'bind_sender': 'False', 'cooldown_time': '15000',
        'dash_n_timeout': '10800000', 'max_active_threads': '2',
        'port': '5003', 'recv_socket_buff': '-1', 'run_time': '30000',
        'send_socket_buff': '8192', 'sync_port': 'False', 'udp': 'False',
        'use_ipv6': 'False', 'verbose': 'False', 'verify_data': 'False',
        'wait_all': 'False', 'warmup_time': '15000', 'wsa': 'False'
    }

    expected_thread_0_metadata = expected_metadata.copy()
    expected_thread_0_metadata['thread_index'] = '0'

    expected_thread_1_metadata = expected_metadata.copy()
    expected_thread_1_metadata['thread_index'] = '1'

    expected_samples = [
        sample.Sample('Total Throughput', 1990.541, 'Mbps',
                      expected_metadata),
        sample.Sample('Thread Throughput', 975.871, 'Mbps',
                      expected_thread_0_metadata),
        sample.Sample('Thread Throughput', 1014.669, 'Mbps',
                      expected_thread_1_metadata)]

    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)
