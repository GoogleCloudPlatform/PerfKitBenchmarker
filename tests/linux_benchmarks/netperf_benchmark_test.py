# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import netperf_benchmark


class NetperfBenchmarkTestCase(unittest.TestCase):

  maxDiff = None

  def setUp(self):
    # Load data
    path = os.path.join(os.path.dirname(__file__),
                        '..', 'data',
                        'netperf_results.json')

    with open(path) as fp:
      self.expected_stdout = ['\n'.join(i) for i in json.load(fp)]

    p = mock.patch(vm_util.__name__ + '.ShouldRunOnExternalIpAddress')
    self.should_run_external = p.start()
    self.addCleanup(p.stop)

    p = mock.patch(vm_util.__name__ + '.ShouldRunOnInternalIpAddress')
    self.should_run_internal = p.start()
    self.addCleanup(p.stop)
    netperf_benchmark.FLAGS.netperf_enable_histograms = False

  def _ConfigureIpTypes(self, run_external=True, run_internal=True):
    self.should_run_external.return_value = run_external
    self.should_run_internal.return_value = run_internal

  def testExternalAndInternal(self):
    self._ConfigureIpTypes()
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm_spec.vms = [mock.MagicMock(), mock.MagicMock()]
    vm_spec.vms[0].RemoteCommand.side_effect = [
        (i, '') for i in self.expected_stdout]

    result = netperf_benchmark.Run(vm_spec)

    tps = 'transactions_per_second'
    mbps = 'Mbits/sec'
    self.assertListEqual(
        [('TCP_RR_Transaction_Rate', 1405.5, tps),
         ('TCP_RR_Latency_p50', 683.0, 'us'),
         ('TCP_RR_Latency_p90', 735.0, 'us'),
         ('TCP_RR_Latency_p99', 841.0, 'us'),
         ('TCP_RR_Latency_min', 600.0, 'us'),
         ('TCP_RR_Latency_max', 900.0, 'us'),
         ('TCP_RR_Latency_stddev', 783.80, 'us'),
         ('TCP_RR_Transaction_Rate', 3545.77, tps),
         ('TCP_RR_Latency_p50', 274.0, 'us'),
         ('TCP_RR_Latency_p90', 309.0, 'us'),
         ('TCP_RR_Latency_p99', 371.0, 'us'),
         ('TCP_RR_Latency_min', 200.0, 'us'),
         ('TCP_RR_Latency_max', 400.0, 'us'),
         ('TCP_RR_Latency_stddev', 189.82, 'us'),
         ('TCP_CRR_Transaction_Rate', 343.35, tps),
         ('TCP_CRR_Latency_p50', 2048.0, 'us'),
         ('TCP_CRR_Latency_p90', 2372.0, 'us'),
         ('TCP_CRR_Latency_p99', 30029.0, 'us'),
         ('TCP_CRR_Latency_min', 2000.0, 'us'),
         ('TCP_CRR_Latency_max', 35000.0, 'us'),
         ('TCP_CRR_Latency_stddev', 8147.88, 'us'),
         ('TCP_CRR_Transaction_Rate', 1078.07, tps),
         ('TCP_CRR_Latency_p50', 871.0, 'us'),
         ('TCP_CRR_Latency_p90', 996.0, 'us'),
         ('TCP_CRR_Latency_p99', 2224.0, 'us'),
         ('TCP_CRR_Latency_min', 800.0, 'us'),
         ('TCP_CRR_Latency_max', 2500.0, 'us'),
         ('TCP_CRR_Latency_stddev', 551.07, 'us'),
         ('TCP_STREAM_Throughput', 1187.94, mbps),
         ('TCP_STREAM_Throughput', 1973.37, mbps),
         ('UDP_RR_Transaction_Rate', 1359.71, tps),
         ('UDP_RR_Latency_p50', 700.0, 'us'),
         ('UDP_RR_Latency_p90', 757.0, 'us'),
         ('UDP_RR_Latency_p99', 891.0, 'us'),
         ('UDP_RR_Latency_min', 600.0, 'us'),
         ('UDP_RR_Latency_max', 1000.0, 'us'),
         ('UDP_RR_Latency_stddev', 808.44, 'us'),
         ('UDP_RR_Transaction_Rate', 3313.49, tps),
         ('UDP_RR_Latency_p50', 295.0, 'us'),
         ('UDP_RR_Latency_p90', 330.0, 'us'),
         ('UDP_RR_Latency_p99', 406.0, 'us'),
         ('UDP_RR_Latency_min', 200.0, 'us'),
         ('UDP_RR_Latency_max', 500.0, 'us'),
         ('UDP_RR_Latency_stddev', 214.64, 'us')],
        [i[:3] for i in result])

    external_meta = {'ip_type': 'external'}
    internal_meta = {'ip_type': 'internal'}
    expected_meta = (([external_meta] * 7 + [internal_meta] * 7) * 2 +
                     [external_meta, internal_meta] +
                     [external_meta] * 7 +
                     [internal_meta] * 7)

    for i, meta in enumerate(expected_meta):
      self.assertIsInstance(result[i][3], dict)
      self.assertDictContainsSubset(meta, result[i][3])
