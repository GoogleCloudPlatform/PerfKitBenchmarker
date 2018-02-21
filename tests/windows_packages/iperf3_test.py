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

import unittest
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.windows_packages import iperf3

# Command generating these results:
# ./iperf3.exe --client 10.129.0.3 --port 5201 --udp -t 3 -b 5G
iperf3_results = """
[  4] local 10.129.0.4 port 49526 connected to 10.129.0.3 port 5201
[ ID] Interval           Transfer     Bandwidth       Total Datagrams
[  4]   0.00-1.00   sec   159 MBytes  1.34 Gbits/sec  20398
[  4]   1.00-2.00   sec   166 MBytes  1.40 Gbits/sec  21292
[  4]   2.00-3.00   sec   167 MBytes  1.40 Gbits/sec  21323
- - - - - - - - - - - - - - - - - - - - - - - - -
[ ID] Interval           Transfer     Bandwidth       Jitter    Lost/Total Datagrams
[  4]   0.00-3.00   sec   492 MBytes  1.38 Gbits/sec  0.072 ms  35148/62949 (56%)
[  4] Sent 62949 datagrams

iperf Done.

"""


class Iperf3TestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def testIperfUDPStreamSamples(self):
    lost = 35148
    sent = 62949
    bandwidth = 5000
    internal_ip_used = True

    samples = iperf3.GetUDPStreamSamples(iperf3_results, bandwidth,
                                         internal_ip_used)

    expected_metadata = {
        'total_lost': lost,
        'total_sent': sent,
        'bandwidth': bandwidth,
        'internal_ip_used': internal_ip_used
    }

    expected_samples = [
        sample.Sample('Loss Rate', 55.836, 'Percent',
                      expected_metadata),
        sample.Sample('Bandwidth Achieved', 1380, 'Mbits/sec',
                      expected_metadata),
        sample.Sample('Jitter', 0.072, 'ms',
                      expected_metadata),
    ]

    self.assertSampleListsEqualUpToTimestamp(samples, expected_samples)
