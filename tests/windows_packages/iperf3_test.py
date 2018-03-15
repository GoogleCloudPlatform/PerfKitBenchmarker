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

from collections import namedtuple
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

# ./iperf3.exe --client 127.0.0.1 --port 5201 -t 3 -f M -P 5
iperf3_tcp_results = """
Connecting to host 127.0.0.1, port 5201
[  4] local 127.0.0.1 port 53966 connected to 127.0.0.1 port 5201
[  6] local 127.0.0.1 port 53967 connected to 127.0.0.1 port 5201
[  8] local 127.0.0.1 port 53968 connected to 127.0.0.1 port 5201
[ 10] local 127.0.0.1 port 53969 connected to 127.0.0.1 port 5201
[ 12] local 127.0.0.1 port 53970 connected to 127.0.0.1 port 5201
[ ID] Interval           Transfer     Bandwidth
[  4]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
[  6]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
[  8]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
[ 10]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
[ 12]   0.00-1.01   sec   102 MBytes   854 Mbits/sec
[SUM]   0.00-1.01   sec   512 MBytes  4.27 Gbits/sec
- - - - - - - - - - - - - - - - - - - - - - - - -
[  4]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
[  6]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
[  8]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
[ 10]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
[ 12]   1.01-2.00   sec   106 MBytes   895 Mbits/sec
[SUM]   1.01-2.00   sec   531 MBytes  4.48 Gbits/sec
- - - - - - - - - - - - - - - - - - - - - - - - -
[  4]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
[  6]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
[  8]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
[ 10]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
[ 12]   2.00-3.01   sec   126 MBytes  1.05 Gbits/sec
[SUM]   2.00-3.01   sec   631 MBytes  5.27 Gbits/sec
- - - - - - - - - - - - - - - - - - - - - - - - -
[ ID] Interval           Transfer     Bandwidth
[  4]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
[  4]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
[  6]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
[  6]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
[  8]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
[  8]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
[ 10]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
[ 10]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
[ 12]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  sender
[ 12]   0.00-3.01   sec   335 MBytes   935 Mbits/sec                  receiver
[SUM]   0.00-3.01   sec  1.64 GBytes  4670 Mbits/sec                  sender
[SUM]   0.00-3.01   sec  1.64 GBytes  4670 Mbits/sec                  receiver

iperf Done.
"""


class Iperf3TestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def testIperfUDPStreamSamples(self):
    lost = 35148
    sent = 62949
    bandwidth = 5000
    internal_ip_used = True

    fake_vm = namedtuple('fake_vm', 'machine_type zone')

    sending_vm = fake_vm(machine_type='A', zone='B')
    receiving_vm = fake_vm(machine_type='A', zone='B')

    samples = iperf3.GetUDPStreamSamples(
        sending_vm, receiving_vm, iperf3_results, bandwidth, internal_ip_used)

    expected_metadata = {
        'protocol': 'UDP',
        'total_lost': lost,
        'total_sent': sent,
        'bandwidth': bandwidth,
        'receiving_machine_type': receiving_vm.machine_type,
        'receiving_zone': receiving_vm.zone,
        'sending_machine_type': sending_vm.machine_type,
        'sending_zone': sending_vm.zone,
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

  def testIperfTCPMultiStream(self):

    tcp_number_of_streams = 10

    fake_vm = namedtuple('fake_vm', 'machine_type zone')

    sending_vm = fake_vm(machine_type='A', zone='B')
    receiving_vm = fake_vm(machine_type='A', zone='B')

    def _Metadata(thread_id):
      return {
          'protocol': 'TCP',
          'num_threads': tcp_number_of_streams,
          'receiving_machine_type': receiving_vm.machine_type,
          'receiving_zone': receiving_vm.zone,
          'sending_machine_type': sending_vm.machine_type,
          'sending_zone': sending_vm.zone,
          'thread_id': thread_id,
          'internal_ip_used': True
      }

    expected_samples = [
        sample.Sample('Bandwidth', 935.0, 'Mbits/sec', _Metadata('4')),
        sample.Sample('Bandwidth', 935.0, 'Mbits/sec', _Metadata('6')),
        sample.Sample('Bandwidth', 935.0, 'Mbits/sec', _Metadata('8')),
        sample.Sample('Bandwidth', 935.0, 'Mbits/sec', _Metadata('10')),
        sample.Sample('Bandwidth', 935.0, 'Mbits/sec', _Metadata('12')),
        sample.Sample('Bandwidth', 4670.0, 'Mbits/sec', _Metadata('SUM')),
    ]

    samples = iperf3.ParseTCPMultiStreamOutput(iperf3_tcp_results, sending_vm,
                                               receiving_vm,
                                               tcp_number_of_streams, True)

    self.assertSampleListsEqualUpToTimestamp(samples, expected_samples)
