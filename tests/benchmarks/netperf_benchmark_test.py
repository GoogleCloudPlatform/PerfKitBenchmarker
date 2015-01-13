# Copyright 2014 Google Inc. All rights reserved.
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

import unittest

import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.benchmarks import netperf_benchmark


class NetperfBenchmarkTestCase(unittest.TestCase):

  maxDiff = None

  def setUp(self):
    p = mock.patch(vm_util.__name__ + '.ShouldRunOnExternalIpAddress')
    self.should_run_external = p.start()
    self.addCleanup(p.stop)

    p = mock.patch(vm_util.__name__ + '.ShouldRunOnInternalIpAddress')
    self.should_run_internal = p.start()
    self.addCleanup(p.stop)

  def _ConfigureIpTypes(self, run_external=True, run_internal=True):
    self.should_run_external.return_value = run_external
    self.should_run_internal.return_value = run_internal

  def testExternalAndInternal(self):
    self._ConfigureIpTypes()
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm_spec.vms = [mock.MagicMock(), mock.MagicMock()]
    vm_spec.vms[0].RemoteCommand.side_effect = [('14.1 \n', ''),
                                                ('12.4 \n', ''),
                                                ('146.82 \n', ''),
                                                ('0.1623\t \n', ''),
                                                ('10.1 \n', ''),
                                                ('11.2 \n', ''),
                                                ('14.82 \n', ''),
                                                ('2.4\t \n', '')]

    result = netperf_benchmark.Run(vm_spec)

    self.assertEqual(8, len(result))
    tps = 'transactions_per_second'
    mbps = 'Mbits/sec'
    self.assertListEqual(
        [('TCP_RR_Transaction_Rate', 14.1, tps),
         ('TCP_RR_Transaction_Rate', 12.4, tps),
         ('TCP_CRR_Transaction_Rate', 146.82, tps),
         ('TCP_CRR_Transaction_Rate', 0.1623, tps),
         ('TCP_STREAM_Throughput', 10.1, mbps),
         ('TCP_STREAM_Throughput', 11.2, mbps),
         ('UDP_RR_Transaction_Rate', 14.82, tps),
         ('UDP_RR_Transaction_Rate', 2.4, tps)],
        [i[:3] for i in result])

    external_meta = {'ip_type': 'external'}
    internal_meta = {'ip_type': 'internal'}
    expected_meta = [external_meta, internal_meta] * 4

    for i, meta in enumerate(expected_meta):
      self.assertIsInstance(result[i][3], dict)
      self.assertDictContainsSubset(meta, result[i][3])
