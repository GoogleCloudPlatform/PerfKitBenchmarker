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
"""Tests for netperf_benchmark."""

import json
import os
import unittest
from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import netperf_benchmark

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class NetperfBenchmarkTestCase(parameterized.TestCase, unittest.TestCase):
  maxDiff = None

  def setUp(self):
    super().setUp()
    # Load data
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'netperf_results.json'
    )

    with open(path) as fp:
      stdouts = ['\n'.join(i) for i in json.load(fp)]
      self.expected_stdout = [
          json.dumps(([stdout], [''], [0])) for stdout in stdouts
      ]

    p = mock.patch(vm_util.__name__ + '.ShouldRunOnExternalIpAddress')
    self.should_run_external = p.start()
    self.addCleanup(p.stop)

    p = mock.patch(vm_util.__name__ + '.ShouldRunOnInternalIpAddress')
    self.should_run_internal = p.start()
    self.addCleanup(p.stop)
    FLAGS.netperf_enable_histograms = False

  def _ConfigureIpTypes(self, run_external=True, run_internal=True):
    self.should_run_external.return_value = run_external
    self.should_run_internal.return_value = run_internal

  def testHistogramStatsCalculator(self):
    FLAGS.netperf_histogram_percentiles = (
        [0.0, 20.0, 30.0, 74.0, 80.0, 100.0])
    histogram = {1: 5, 2: 10, 5: 5}
    stats = netperf_benchmark._HistogramStatsCalculator(
        histogram
    )
    self.assertEqual(stats['p0'], 1)
    self.assertEqual(stats['p20'], 1)
    self.assertEqual(stats['p30'], 2)
    self.assertEqual(stats['p74'], 2)
    self.assertEqual(stats['p80'], 5)
    self.assertEqual(stats['p100'], 5)
    self.assertLessEqual(abs(stats['stddev'] - 1.538), 0.001)

  @flagsaver.flagsaver(netperf_benchmarks=netperf_benchmark.ALL_BENCHMARKS)
  @flagsaver.flagsaver(netperf_num_streams=[1])
  def testExternalAndInternal(self):
    self._ConfigureIpTypes()
    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm_spec.vms = [mock.MagicMock(), mock.MagicMock()]
    vm_spec.vms[0].RobustRemoteCommand.side_effect = [
        (i, '') for i in self.expected_stdout
    ]
    vm_spec.vms[1].GetInternalIPs.return_value = ['test_ip']
    vm_spec.vms[0].GetInternalIPs.return_value = ['test_ip']
    run_result = netperf_benchmark.Run(vm_spec)
    result = []
    for sample in run_result:
      if sample[0] not in ['start_time', 'end_time']:
        result.append(sample)

    tps = 'transactions_per_second'
    mbps = 'Mbits/sec'
    self.assertListEqual(
        [
            ('TCP_RR_Transaction_Rate', 1405.5, tps),
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
            ('TCP_STREAM_Throughput_1stream', 1187.94, mbps),
            ('TCP_STREAM_Throughput', 1973.37, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_1stream', 1973.37, 'Mbits/sec'),
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
            ('UDP_RR_Latency_stddev', 214.64, 'us'),
            ('UDP_STREAM_Throughput', 1102.42, mbps),
            ('UDP_STREAM_Throughput', 1802.72, 'Mbits/sec'),
        ],
        [i[:3] for i in result],
    )

    external_meta = {'ip_type': 'external'}
    internal_meta = {'ip_type': 'internal'}
    expected_meta = (
        ([external_meta] * 7 + [internal_meta] * 7) * 2
        + [external_meta, external_meta, internal_meta, internal_meta]
        + [external_meta] * 7
        + [internal_meta] * 7
    )

    for i, meta in enumerate(expected_meta):
      self.assertIsInstance(result[i][3], dict)
      self.assertEqual(result[i][3], {**result[i][3], **meta})

  @parameterized.named_parameters(
      (
          'no_times_up',
          (
              'MIGRATED TCP STREAM TEST from 0.0.0.0 (0.0.0.0) port 0 AF_INET'
              ' to 10.0.0.137 () port 20157 AF_INET :'
              ' histogram\nrecv_response_timed_n: no response received. errno'
              ' 110 counter 0\n'
          ),
      ),
      (
          'has_times_up',
          (
              'MIGRATED TCP STREAM TEST from 0.0.0.0 (0.0.0.0) port 0 AF_INET'
              ' to 10.0.0.172 () port 20169 AF_INET : histogram\ncatcher: timer'
              ' popped with times_up != 0\nrecv_response_timed_n: no response'
              ' received. errno 4 counter -1\n'
          ),
      ),
  )
  def testParseNetperfOutputError(self, output):
    with self.assertRaises(errors.Benchmarks.KnownIntermittentError) as e:
      netperf_benchmark.ParseNetperfOutput(
          output, {}, 'fake_benchmark_name', False
      )
    self.assertIn('Failed to parse stdout', str(e.exception))

  @flagsaver.flagsaver(netperf_benchmarks=[netperf_benchmark.TCP_STREAM])
  def testMultiStreams(self):
    self._ConfigureIpTypes()
    num_streams = 4
    FLAGS.netperf_num_streams = flag_util.IntegerList([num_streams])
    self.should_run_external.return_value = True
    self.should_run_internal.return_value = False
    # Read netperf mock results for multiple streams
    path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'data',
        'netperf_results_multistreams.json',
    )
    with open(path) as fp:
      stdouts = ['\n'.join(i) for i in json.load(fp)]
      self.expected_stdout = []
      for i in range(0, len(stdouts), num_streams):
        self.expected_stdout.append(
            json.dumps((stdouts[i : i + num_streams], [''], [0]))
        )

    vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    vm_spec.vms = [mock.MagicMock(), mock.MagicMock()]
    vm_spec.vms[0].RobustRemoteCommand.side_effect = [
        (i, '') for i in self.expected_stdout
    ]
    vm_spec.vms[1].GetInternalIPs.return_value = ['test_ip']
    run_result = netperf_benchmark.Run(vm_spec)
    result = []
    for sample in run_result:
      if sample[0] not in ['start_time', 'end_time']:
        result.append(sample)

    self.assertListEqual(
        [
            ('TCP_STREAM_Throughput_p50', 3000.0, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_p90', 4000.0, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_p99', 4000.0, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_average', 2500.0, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_stddev', 1290.9944487358057, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_min', 1000.0, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_max', 4000.0, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_total', 10000.0, 'Mbits/sec'),
            ('TCP_STREAM_Throughput_4streams', 10000.0, 'Mbits/sec'),
        ],
        [i[:3] for i in result],
    )


if __name__ == '__main__':
  unittest.main()
