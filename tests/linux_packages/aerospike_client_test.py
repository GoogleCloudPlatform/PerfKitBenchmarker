# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.aerospike_client."""

import collections
import os
import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import aerospike_client
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


def _ReadFileToString(filename):
  """Helper function to read a file into a string."""
  with open(filename) as f:
    return f.read()


TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'asbench')

ASBENCH_HISTOGRAM_DATA = _ReadFileToString(
    os.path.join(TEST_DATA_DIR, 'histogram_data').strip()
)

ASBENCH_OUTPUT_HEADER = """
hosts:                  10.128.0.99
port:                   3000
user:                   (null)
services-alternate:     false
...more header...
2022-07-27 18:19:13.834 INFO Start 128 transaction threads
Stage 1: default config (specify your own with --workload-stages)"""
ASBENCH_OUTPUT_RESULT_1 = """
2022-07-27 18:19:14.834 INFO write(tps=15694 timeouts=0 errors=0) read(tps=141748 timeouts=0 errors=0) total(tps=157442 timeouts=0 errors=0)
hdr: write 2022-07-27T18:19:14Z 1, 15697, 70, 16735, 726, 1248, 2008, 3577, 13183
hdr: read  2022-07-27T18:19:14Z 1, 141792, 61, 68863, 685, 1201, 2008, 3315, 45471
2022-07-27 18:19:15.834 INFO write(tps=15470 timeouts=0 errors=0) read(tps=140415 timeouts=0 errors=0) total(tps=155885 timeouts=0 errors=0)
hdr: write 2022-07-27T18:19:15Z 2, 31167, 70, 16735, 750, 1278, 2071, 3137, 8575
hdr: read  2022-07-27T18:19:15Z 2, 282209, 61, 68863, 712, 1230, 2026, 3095, 18607
2022-07-27 18:19:16.834 INFO write(tps=15780 timeouts=0 errors=0) read(tps=141700 timeouts=0 errors=0) total(tps=157480 timeouts=0 errors=0)
hdr: write 2022-07-27T18:19:16Z 3, 46947, 70, 16735, 755, 1280, 2063, 3013, 8511
hdr: read  2022-07-27T18:19:16Z 3, 423901, 61, 68863, 718, 1236, 2031, 2981, 10303
"""
ASBENCH_OUTPUT_RESULT_2 = """
2022-07-27 18:19:14.834 INFO write(tps=15780 timeouts=0 errors=0) read(tps=141700 timeouts=0 errors=0) total(tps=157480 timeouts=2 errors=1)
hdr: write 2022-07-27T18:19:14Z 1, 46947, 70, 16735, 755, 1280, 2063, 3013, 8511
hdr: read  2022-07-27T18:19:14Z 1, 423901, 61, 68863, 718, 1236, 2031, 2981, 10303
2022-07-27 18:19:15.834 INFO write(tps=15694 timeouts=0 errors=0) read(tps=141748 timeouts=0 errors=0) total(tps=157442 timeouts=0 errors=0)
hdr: write 2022-07-27T18:19:15Z 2, 15697, 70, 16735, 726, 1248, 2008, 3577, 13183
hdr: read  2022-07-27T18:19:15Z 2, 141792, 61, 68863, 685, 1201, 2008, 3315, 45471
2022-07-27 18:19:16.834 INFO write(tps=15470 timeouts=0 errors=0) read(tps=140415 timeouts=0 errors=0) total(tps=155885 timeouts=0 errors=0)
hdr: write 2022-07-27T18:19:16Z 3, 31167, 70, 16735, 750, 1278, 2071, 3137, 8575
hdr: read  2022-07-27T18:19:16Z 3, 282209, 61, 68863, 712, 1230, 2026, 3095, 18607
"""
HISTOGRAM_OUTPUT_1 = """header lines...
Stage 1: default config (specify your own with --workload-stages)
write_hist 2022-07-27T18:19:13Z, 1.00011s, 15702, 0:2755
read_hist 2022-07-27T18:19:13Z, 1.00011s, 141825, 0:3000
"""
HISTOGRAM_OUTPUT_2 = """header lines...
Stage 1: default config (specify your own with --workload-stages)
write_hist 2022-07-27T18:19:14Z, 1.00011s, 15702, 0:75, 100:739, 200:253
read_hist 2022-07-27T18:19:14Z, 1.00011s, 141825, 0:1152, 100:6850, 200:2373
write_hist 2022-07-27T18:19:15Z, 0.999992s, 15468, 100:3, 200:9, 300:176
read_hist 2022-07-27T18:19:15Z, 0.999992s, 140414, 0:7, 100:18, 200:195, 300:2681
write_hist 2022-07-27T18:19:16Z, 0.999966s, 15783, 0:1, 100:2, 200:10, 300:186, 400:1357
read_hist 2022-07-27T18:19:16Z, 0.999966s, 141688, 0:8, 100:21, 200:162, 300:2857
"""


class AerospikeClientTestCase(pkb_common_test_case.PkbCommonTestCase):
  maxDiff = None  # pylint: disable=invalid-name

  def setUp(self):
    super().setUp()
    FLAGS['aerospike_instances'].present = 1
    FLAGS['aerospike_instances'].value = 1

  def testParseAsbenchStdout(self):
    actual_samples = aerospike_client.ParseAsbenchStdout(
        ASBENCH_OUTPUT_HEADER + ASBENCH_OUTPUT_RESULT_1
    )

    self.assertEqual(
        actual_samples,
        [
            sample.Sample(
                metric='throughput',
                value=157442.0,
                unit='transaction_per_second',
                metadata={
                    'tps': 157442.0,
                    'timeouts': 0.0,
                    'errors': 0.0,
                    'start_timestamp': 1658945954.0,
                    'window': 1,
                    'write_min': 70.0,
                    'write_max': 16735.0,
                    'write_p50': 726.0,
                    'write_p90': 1248.0,
                    'write_p99': 2008.0,
                    'write_p99.9': 3577.0,
                    'write_p99.99': 13183.0,
                    'read_min': 61.0,
                    'read_max': 68863.0,
                    'read_p50': 685.0,
                    'read_p90': 1201.0,
                    'read_p99': 2008.0,
                    'read_p99.9': 3315.0,
                    'read_p99.99': 45471.0,
                },
                timestamp=actual_samples[0].timestamp,
            ),
            sample.Sample(
                metric='throughput',
                value=155885.0,
                unit='transaction_per_second',
                metadata={
                    'tps': 155885.0,
                    'timeouts': 0.0,
                    'errors': 0.0,
                    'start_timestamp': 1658945954.0,
                    'window': 2,
                    'write_min': 70.0,
                    'write_max': 16735.0,
                    'write_p50': 750.0,
                    'write_p90': 1278.0,
                    'write_p99': 2071.0,
                    'write_p99.9': 3137.0,
                    'write_p99.99': 8575.0,
                    'read_min': 61.0,
                    'read_max': 68863.0,
                    'read_p50': 712.0,
                    'read_p90': 1230.0,
                    'read_p99': 2026.0,
                    'read_p99.9': 3095.0,
                    'read_p99.99': 18607.0,
                },
                timestamp=actual_samples[1].timestamp,
            ),
            sample.Sample(
                metric='throughput',
                value=157480.0,
                unit='transaction_per_second',
                metadata={
                    'tps': 157480.0,
                    'timeouts': 0.0,
                    'errors': 0.0,
                    'start_timestamp': 1658945954.0,
                    'window': 3,
                    'write_min': 70.0,
                    'write_max': 16735.0,
                    'write_p50': 755.0,
                    'write_p90': 1280.0,
                    'write_p99': 2063.0,
                    'write_p99.9': 3013.0,
                    'write_p99.99': 8511.0,
                    'read_min': 61.0,
                    'read_max': 68863.0,
                    'read_p50': 718.0,
                    'read_p90': 1236.0,
                    'read_p99': 2031.0,
                    'read_p99.9': 2981.0,
                    'read_p99.99': 10303.0,
                },
                timestamp=actual_samples[2].timestamp,
            ),
        ],
    )

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAggregateAsbenchSamples(self):
    agg_samples = aerospike_client.AggregateAsbenchSamples(
        aerospike_client.ParseAsbenchStdout(
            ASBENCH_OUTPUT_HEADER + ASBENCH_OUTPUT_RESULT_1
        )
        + aerospike_client.ParseAsbenchStdout(
            ASBENCH_OUTPUT_HEADER + ASBENCH_OUTPUT_RESULT_2
        )
    )
    self.assertEqual(
        agg_samples,
        [
            sample.Sample(
                metric='throughput',
                value=314922.0,
                unit='transaction_per_second',
                metadata={
                    'start_timestamp': 1658945954.0,
                    'tps': 314922.0,
                    'timeouts': 2.0,
                    'errors': 1.0,
                    'window': 1,
                    'write_min': 70.0,
                    'write_max': 16735.0,
                    'read_min': 61.0,
                    'read_max': 68863.0,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='throughput',
                value=313327.0,
                unit='transaction_per_second',
                metadata={
                    'start_timestamp': 1658945954.0,
                    'tps': 313327.0,
                    'timeouts': 0.0,
                    'errors': 0.0,
                    'window': 2,
                    'write_min': 70.0,
                    'write_max': 16735.0,
                    'read_min': 61.0,
                    'read_max': 68863.0,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='throughput',
                value=313365.0,
                unit='transaction_per_second',
                metadata={
                    'start_timestamp': 1658945954.0,
                    'tps': 313365.0,
                    'timeouts': 0.0,
                    'errors': 0.0,
                    'window': 3,
                    'write_min': 70.0,
                    'write_max': 16735.0,
                    'read_min': 61.0,
                    'read_max': 68863.0,
                },
                timestamp=0,
            ),
        ],
    )

  def testCalculatePercentileFromHistogram(self):
    histogram = collections.OrderedDict({0: 10, 100: 20, 200: 30, 400: 40})
    percentiles = [5, 10, 50, 90, 99, 99.9, 100]
    expected_results = [100, 100, 300, 500, 500, 500, 500]
    for i in range(len(percentiles)):
      latency = aerospike_client.CalculatePercentileFromHistogram(
          histogram, percentiles[i]
      )
      self.assertEqual(latency, expected_results[i])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testPublishPercentileSamplesFromRun(self):
    histograms = collections.OrderedDict({})
    timestamps = {}
    percentiles = ['99.9']
    aerospike_client.ParseHistogramFile(
        ASBENCH_HISTOGRAM_DATA, histograms, timestamps
    )

    samples = aerospike_client.GeneratePercentileTimeSeriesSamples(
        histograms, percentiles, timestamps
    )
    self.assertListEqual(
        samples,
        [
            sample.Sample(
                metric='write_99.9_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [2.0, 0.9, 2.5, 0.9],
                    'timestamps': [
                        1679618806000.0,
                        1679618807000.0,
                        1679618808000.0,
                        1679618809000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='read_99.9_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [1.4, 0.9, 5.0, 2.6],
                    'timestamps': [
                        1679618806000.0,
                        1679618807000.0,
                        1679618808000.0,
                        1679618809000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
        ],
    )

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testPublishPercentileSamples(self):
    histograms = collections.OrderedDict({})
    timestamps = {}
    percentiles = ['50', '90', '99', '99.9']
    aerospike_client.ParseHistogramFile(
        HISTOGRAM_OUTPUT_1, histograms, timestamps
    )
    aerospike_client.ParseHistogramFile(
        HISTOGRAM_OUTPUT_2, histograms, timestamps
    )
    samples = aerospike_client.GeneratePercentileTimeSeriesSamples(
        histograms, percentiles, timestamps
    )
    self.assertListEqual(
        samples,
        [
            sample.Sample(
                metric='write_50_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.1, 0.4, 0.5],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='write_90_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.2, 0.4, 0.5],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='write_99_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.3, 0.4, 0.5],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='write_99.9_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.3, 0.4, 0.5],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='read_50_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.2, 0.4, 0.4],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='read_90_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.3, 0.4, 0.4],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='read_99_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.3, 0.4, 0.4],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
            sample.Sample(
                metric='read_99.9_percentile_latency_time_series',
                value=0.0,
                unit='ms',
                metadata={
                    'values': [0.3, 0.4, 0.4],
                    'timestamps': [
                        1658945953000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                },
                timestamp=0,
            ),
        ],
    )

  def testParseHistogramFile(self):
    histograms = collections.OrderedDict({})
    timestamps = {}
    aerospike_client.ParseHistogramFile(
        HISTOGRAM_OUTPUT_1, histograms, timestamps
    )
    aerospike_client.ParseHistogramFile(
        HISTOGRAM_OUTPUT_2, histograms, timestamps
    )
    self.assertEqual(
        histograms,
        {
            (1, 'read'): collections.OrderedDict(
                [(0, 4152), (100, 6850), (200, 2373)]
            ),
            (1, 'write'): collections.OrderedDict(
                [(0, 2830), (100, 739), (200, 253)]
            ),
            (2, 'read'): collections.OrderedDict(
                [(0, 7), (100, 18), (200, 195), (300, 2681)]
            ),
            (2, 'write'): collections.OrderedDict(
                [(100, 3), (200, 9), (300, 176)]
            ),
            (3, 'read'): collections.OrderedDict(
                [(0, 8), (100, 21), (200, 162), (300, 2857)]
            ),
            (3, 'write'): collections.OrderedDict(
                [(0, 1), (100, 2), (200, 10), (300, 186), (400, 1357)]
            ),
        },
    )

  def testCreateTimeSeriesSample(self):
    raw_samples = aerospike_client.ParseAsbenchStdout(
        ASBENCH_OUTPUT_HEADER + ASBENCH_OUTPUT_RESULT_1
    )
    ts_samples = aerospike_client.CreateTimeSeriesSample(raw_samples)
    self.assertEqual(
        ts_samples,
        [
            sample.Sample(
                metric='OPS_time_series',
                value=0.0,
                unit='ops',
                metadata={
                    'values': [157442.0, 155885.0, 157480.0],
                    'timestamps': [
                        1658945954000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                    'ramp_up_ends': 1658946014000.0,
                },
                timestamp=ts_samples[0].timestamp,
            ),
            sample.Sample(
                metric='Read_Min_Latency_time_series',
                value=0.0,
                unit='us',
                metadata={
                    'values': [61.0, 61.0, 61.0],
                    'timestamps': [
                        1658945954000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                    'ramp_up_ends': 1658946014000.0,
                },
                timestamp=ts_samples[1].timestamp,
            ),
            sample.Sample(
                metric='Read_Max_Latency_time_series',
                value=0.0,
                unit='us',
                metadata={
                    'values': [68863.0, 68863.0, 68863.0],
                    'timestamps': [
                        1658945954000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                    'ramp_up_ends': 1658946014000.0,
                },
                timestamp=ts_samples[2].timestamp,
            ),
            sample.Sample(
                metric='Write_Min_Latency_time_series',
                value=0.0,
                unit='us',
                metadata={
                    'values': [70.0, 70.0, 70.0],
                    'timestamps': [
                        1658945954000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                    'ramp_up_ends': 1658946014000.0,
                },
                timestamp=ts_samples[3].timestamp,
            ),
            sample.Sample(
                metric='Write_Max_Latency_time_series',
                value=0.0,
                unit='us',
                metadata={
                    'values': [16735.0, 16735.0, 16735.0],
                    'timestamps': [
                        1658945954000.0,
                        1658945955000.0,
                        1658945956000.0,
                    ],
                    'interval': 1,
                    'ramp_up_ends': 1658946014000.0,
                },
                timestamp=ts_samples[4].timestamp,
            ),
            sample.Sample(
                metric='total_ops',
                value=156935.66666666666,
                unit='ops',
                metadata={},
                timestamp=ts_samples[5].timestamp,
            ),
        ],
    )


if __name__ == '__main__':
  unittest.main()
