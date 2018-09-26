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

"""Tests for psping_benchmark."""

import collections
import json
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.windows_packages import psping


psping_results = """
PsPing v2.10 - PsPing - ping, latency, bandwidth measurement utility
Copyright (C) 2012-2016 Mark Russinovich
Sysinternals - www.sysinternals.com

TCP latency test connecting to 10.138.0.2:47001: Connected
15 iterations (warmup 5) sending 8192 bytes TCP latency test:   0%
Connected
15 iterations (warmup 5) sending 8192 bytes TCP latency test: 100%

TCP roundtrip latency statistics (post warmup):
  Sent = 10, Size = 8192, Total Bytes: 81920,
  Minimum = 0.19ms, Maxiumum = 0.58ms, Average = 0.27ms

Latency Count
0.30\t688
0.51\t292
0.71\t15
0.92\t2
1.13\t0
"""


class PspingBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def testPspingParsing(self):

    minimum = 0.19
    maximum = 0.58
    average = 0.27
    use_internal_ip = True

    machine = collections.namedtuple('machine', 'zone machine_type')

    client = machine(machine_type='cA', zone='cZ')
    server = machine(machine_type='sB', zone='sZ')

    samples = psping.ParsePspingResults(psping_results, client, server,
                                        use_internal_ip)

    expected_metadata = {
        'internal_ip_used': use_internal_ip,
        'sending_zone': client.zone,
        'sending_machine_type': client.machine_type,
        'receiving_zone': server.zone,
        'receiving_machine_type': server.machine_type,
    }

    histogram = json.dumps([
        {'latency': 0.3, 'count': 688, 'bucket_number': 1},
        {'latency': 0.51, 'count': 292, 'bucket_number': 2},
        {'latency': 0.71, 'count': 15, 'bucket_number': 3},
        {'latency': 0.92, 'count': 2, 'bucket_number': 4},
        {'latency': 1.13, 'count': 0, 'bucket_number': 5},
    ])

    expected_samples = [
        sample.Sample('latency', average, 'ms', expected_metadata),
        sample.Sample('latency:maximum', maximum, 'ms', expected_metadata),
        sample.Sample('latency:minimum', minimum, 'ms', expected_metadata),
    ]

    expected_histogram_metadata = expected_metadata.copy()
    expected_histogram_metadata['histogram'] = histogram

    expected_samples.append(sample.Sample('latency:histogram', 0, 'ms',
                                          expected_histogram_metadata))

    self.assertSampleListsEqualUpToTimestamp(expected_samples, samples)
