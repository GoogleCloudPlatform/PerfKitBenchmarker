# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for bidirectional_network_benchmark."""

import json
import os
import re
import threading
import unittest
import mock
import numpy
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker.linux_benchmarks import bidirectional_network_benchmark
from tests import mock_flags

MBPS = 'Mbits/sec'
TOLERANCE = 0.000001


class BidirectionalNetworkBenchmarkTestCase(unittest.TestCase):

  maxDiff = None

  def setUp(self):
    # Load netperf stdout data
    path = os.path.join(os.path.dirname(__file__),
                        '..', 'data',
                        'bidirectional_network_results.json')
    with open(path) as fp:
      remote_stdouts = json.load(fp)
      self.expected_stdout = [json.dumps(stdout)
                              for stdout in remote_stdouts]

  def _AssertSamples(self, results, delta, values):
    """"Asserts that the samples matching delta correspond to values."""

    delta_metric = [r for r in results if r[1] == delta]
    self.assertEqual(1, len(delta_metric), delta)

    # The bidirectional_network_tests run in parallel and are grouped by
    # <test number>_<test name>.  This can vary due to the parallel nature of
    # the tests, so we grab all samples with the header that matches the header
    # of the delta sample.
    delta_header = delta_metric[0][0][:len('0_TCP_STREAM_')]
    netperf_run = [r for r in results
                   if r[0][:len(delta_header)] == delta_header]

    header = delta_header + 'Throughput_'

    # assert we have 8 values because percentiles are manually picked
    self.assertEqual(8, len(values))
    values.sort()
    self._AssertSample(netperf_run, header + 'max', values[7], MBPS)
    self._AssertSample(netperf_run, header + 'min', values[0], MBPS)
    self._AssertSample(netperf_run, header + 'stddev',
                       numpy.std(values, ddof=1), MBPS)
    self._AssertSample(netperf_run, header + 'p90', values[7], MBPS)
    self._AssertSample(netperf_run, header + 'average', numpy.mean(values),
                       MBPS)
    self._AssertSample(netperf_run, header + 'p50', values[4], MBPS)
    self._AssertSample(netperf_run, header + 'total', sum(values), MBPS)
    self._AssertSample(netperf_run, header + 'p99', values[7], MBPS)

  def _AssertSample(self, results, metric, value, unit):
    """Asserts results contains a sample matching metric/value/unit."""
    match = [r for r in results if (r.metric == metric and r.unit == unit and
                                    abs(r.value - value) < TOLERANCE)]
    print metric + ',' + str(value) + ',' + unit
    self.assertEqual(1, len(match))

  def testRun(self):
    mocked_flags = mock_flags.PatchTestCaseFlags(self)
    mocked_flags.bidirectional_network_tests = ['TCP_STREAM', 'TCP_MAERTS',
                                                'TCP_MAERTS', 'TCP_MAERTS']
    mocked_flags.bidirectional_network_test_length = 60
    mocked_flags.bidirectional_stream_num_streams = 8

    # Helper for GetNetperfStdOut whichs holds the last returned index for the
    # given test. Used to ensure the mocked stdout is returned matching the
    # requested netperf test and each is returned exactly once.
    last_returned = {
        'TCP_STREAM': -1,
        'TCP_MAERTS': -1
    }

    stdout_lock = threading.Lock()

    def GetNetperfStdOut(remote_cmd, timeout):
      """Mock returning Netperf stdout."""
      del timeout  # unused by mock
      with stdout_lock:
        match = re.search('-t (.*?) ', remote_cmd)
        netperf_test = match.group(1)
        i = last_returned[netperf_test]
        while True:
          i += 1
          if mocked_flags.bidirectional_network_tests[i] == netperf_test:
            last_returned[netperf_test] = i
            return (self.expected_stdout[i], '')

    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm_spec.vms = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock(),
                   mock.MagicMock(), mock.MagicMock()]
    vm_spec.vms[0].RemoteCommand.side_effect = GetNetperfStdOut

    results = bidirectional_network_benchmark.Run(vm_spec)

    # deltas calculated by
    #   cat bidirectional_network_results.json | jq '.[] | .[4] - .[3]'
    # samples extracted with
    #   egrep -o "[0-9]*\.[0-9]*,10\^6bits/s" bidirectional_network_results.json

    # TCP STREAM from bidirectional_network_results.json
    delta0 = 0.009217977523803711
    samples0 = [
        1114.84,
        2866.35,
        4488.67,
        1626.20,
        675.19,
        1223.60,
        944.58,
        2987.61,
    ]

    # TCP MEARTS 0 from bidirectional_network_results.json
    delta1 = 0.010135173797607422
    samples1 = [
        436.87,
        433.89,
        487.15,
        1030.73,
        1501.02,
        415.35,
        524.82,
        587.19,
    ]

    # TCP MEARTS 1 from bidirectional_network_results.json
    delta2 = 0.009433984756469727
    samples2 = [
        89.63,
        540.79,
        1124.56,
        672.74,
        578.30,
        561.74,
        658.57,
        525.62,
    ]

    # TCP MEARTS 2 from bidirectional_network_results.json
    delta3 = 0.010863065719604492
    samples3 = [
        608.63,
        521.83,
        382.78,
        513.72,
        607.00,
        235.67,
        653.73,
        550.04,
    ]

    # per test metrics
    self._AssertSamples(results, delta0, samples0)
    self._AssertSamples(results, delta1, samples1)
    self._AssertSamples(results, delta2, samples2)
    self._AssertSamples(results, delta3, samples3)

    # summary metrics
    self._AssertSample(results, 'outbound_network_total', sum(samples0), MBPS)
    self._AssertSample(results, 'inbound_network_total',
                       sum(samples1 + samples2 + samples3), MBPS)
    self._AssertSample(results, 'all_streams_start_delta',
                       1508187617.100678 - 1508187614.93243, 'seconds')

    metrics_per_test = 9  # 8 throughput samples, 1 delta
    num_tests = 4
    summary_metrics = 3
    total_metrics = num_tests * metrics_per_test + summary_metrics

    self.assertEqual(total_metrics, len(results))
