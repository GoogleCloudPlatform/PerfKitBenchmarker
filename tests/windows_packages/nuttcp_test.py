# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

import collections
import unittest
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.windows_packages import nuttcp

# 1416.3418 MB /  10.00 sec = 1188.1121 Mbps 85 %TX 26 %RX 104429 / 1554763
#  drop/pkt 6.72 %loss
nuttcp_results = """1416.3418 MB /  10.00 sec = 1188.1121 Mbps 85 %TX 26 \
%RX 104429 / 1554763 drop/pkt 6.72 %loss
"""


class NuttcpTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def testNuttcpUDPStreamSamples(self):
    bandwidth = 1188.1121
    packet_loss = '6.72'

    machine = collections.namedtuple('machine', 'zone machine_type')

    client = machine(machine_type='cA', zone='cZ')
    server = machine(machine_type='sB', zone='sZ')

    result_sample = [
        nuttcp.GetUDPStreamSample(nuttcp_results, client, server, bandwidth,
                                  'external')
    ]

    expected_metadata = {
        'receiving_machine_type': 'sB',
        'receiving_zone': 'sZ',
        'sending_machine_type': 'cA',
        'sending_zone': 'cZ',
        'packet_loss': packet_loss,
        'bandwidth_requested': bandwidth,
        'network_type': 'external'
    }

    expected_sample = [
        sample.Sample('bandwidth', 1188.1121, 'Mbps', expected_metadata)
    ]

    self.assertSampleListsEqualUpToTimestamp(result_sample, expected_sample)
