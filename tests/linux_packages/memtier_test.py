"""Tests for perfkitbenchmarker.linux_packages.memtier."""


import json
import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_packages import memtier
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

TEST_OUTPUT = """
  4         Threads
  50        Connections per thread
  20        Seconds
  Type        Ops/sec     Hits/sec   Misses/sec    Avg. Latency      p50 Latency     p90 Latency     p95 Latency     p99 Latency   p99.9 Latency   KB/sec
  -------------------------------------------------------------------------------------------------------------------------------------------------------
  Sets        4005.50          ---          ---         1.50600         1.21500         2.29500         2.31900         2.39900         3.93500    308.00
  Gets       40001.05     40001.05         0.00         1.54300         1.21500         2.28700         2.31900         2.39100         3.84700   1519.00
  Waits          0.00          ---          ---             ---             ---             ---             ---             ---             ---       ---
  Totals     44006.55     40001.05         0.00         1.54000         1.21500         2.29500         2.31900         2.39900         3.87100   1828.00

  Request Latency Distribution
Type        <= msec      Percent
------------------------------------------------------------------------
SET               0         5.00
SET               1        10.00
SET               2        15.00
SET               3        30.00
SET               4        50.00
SET               5        70.00
SET               6        90.00
SET               7        95.00
SET               8        99.00
SET               9       100.00
---
GET               0         50.0
GET               2       100.00
"""

METADATA = {
    'test': 'foobar',
    'p90_latency': 2.295,
    'p95_latency': 2.319,
    'p99_latency': 2.399,
    'avg_latency': 1.54,
}

TIME_SERIES_JSON = """
  {
    "ALL STATS":
    {
      "Totals":
      {
        "Time-Serie":
        {
          "0": {"Count": 3, "Max Latency": 1},
          "1": {"Count": 4, "Max Latency": 2.1}
        }
      },
      "Runtime":
      {
        "Start time": 1657947420452,
        "Finish time": 1657947420454,
        "Total duration": 2,
        "Time unit": "MILLISECONDS"
      }
    }
  }
"""


def GetMemtierResult(ops_per_sec, p95_latency):
  return memtier.MemtierResult(
      ops_per_sec, 0, 0, 0, p95_latency, 0, [], [], [], [], [], {})


class MemtierTestCase(pkb_common_test_case.PkbCommonTestCase,
                      test_util.SamplesTestMixin):

  def testParseResults(self):
    get_metadata = {
        'histogram': json.dumps([
            {'microsec': 0.0, 'count': 4500},
            {'microsec': 2000.0, 'count': 4500}])
    }
    get_metadata.update(METADATA)
    set_metadata = {
        'histogram': json.dumps([
            {'microsec': 0.0, 'count': 50},
            {'microsec': 1000.0, 'count': 50},
            {'microsec': 2000.0, 'count': 50},
            {'microsec': 3000.0, 'count': 150},
            {'microsec': 4000.0, 'count': 200},
            {'microsec': 5000.0, 'count': 200},
            {'microsec': 6000.0, 'count': 200},
            {'microsec': 7000.0, 'count': 50},
            {'microsec': 8000.0, 'count': 40},
            {'microsec': 9000.0, 'count': 10}])
    }
    set_metadata.update(METADATA)

    time_series_metadata = {'time_series': {'0': 3, '1': 4}}
    time_series_metadata.update(METADATA)
    latency_series_metadata = {'time_series': {'0': 1, '1': 2.1}}
    latency_series_metadata.update(METADATA)
    runtime_info_metadata = {
        'Start_time': 1657947420452,
        'Finish_time': 1657947420454,
        'Total_duration': 2,
        'Time_unit': 'MILLISECONDS'
    }

    expected_result = [
        sample.Sample(
            metric='Ops Throughput',
            value=44006.55,
            unit='ops/s',
            metadata=METADATA),
        sample.Sample(
            metric='KB Throughput',
            value=1828.0,
            unit='KB/s',
            metadata=METADATA),
        sample.Sample(
            metric='Latency', value=1.54, unit='ms', metadata=METADATA),
        sample.Sample(
            metric='get latency histogram',
            value=0,
            unit='',
            metadata=get_metadata),
        sample.Sample(
            metric='set latency histogram',
            value=0,
            unit='',
            metadata=set_metadata),
        sample.Sample(
            metric='Memtier Duration',
            value=2,
            unit='ms',
            metadata=runtime_info_metadata),
    ]
    samples = []
    results = memtier.MemtierResult.Parse(TEST_OUTPUT, TIME_SERIES_JSON)
    samples.extend(results.GetSamples(METADATA))
    self.assertSampleListsEqualUpToTimestamp(samples, expected_result)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAggregateMemtierWithOneResult(self):
    FLAGS.memtier_time_series = True
    timestamps = [0, 1000, 2000, 3000, 4000]
    ops_values = [1, 1, 1, 1, 1]
    latency = [1, 2, 3, 4, 5]
    results = [
        memtier.MemtierResult(1, 2, 0, 0, 0, 0, [], [], timestamps, ops_values,
                              latency, {})
    ]
    samples = memtier.AggregateMemtierResults(results, {})
    expected_result = [
        sample.Sample(
            metric='Total Ops Throughput',
            value=1.0,
            unit='ops/s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='Total KB Throughput',
            value=2.0,
            unit='KB/s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='OPS_time_series',
            value=0.0,
            unit='ops',
            metadata={
                'values': [1, 1, 1, 1, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1
            },
            timestamp=0),
        sample.Sample(
            metric='Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1
            },
            timestamp=0)
    ]
    self.assertEqual(samples, expected_result)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAggregateMemtierResultsWithMultipleResults(self):
    FLAGS.memtier_time_series = True
    timestamps = [0, 1000, 2000, 3000, 4000]
    ops_values_1 = [1, 1, 1, 1, 1]
    latency_1 = [1, 2, 3, 4, 5]
    ops_values_2 = [1, 1, 1, 1, 1]
    latency_2 = [5, 4, 3, 2, 1]
    results = [
        memtier.MemtierResult(2, 4, 0, 0, 0, 0, [], [], timestamps,
                              ops_values_1, latency_1, {}),
        memtier.MemtierResult(2, 4, 0, 0, 0, 0, [], [], timestamps,
                              ops_values_2, latency_2, {})
    ]
    samples = memtier.AggregateMemtierResults(results, {})
    expected_result = [
        sample.Sample(
            metric='Total Ops Throughput',
            value=4,
            unit='ops/s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='Total KB Throughput',
            value=8,
            unit='KB/s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='OPS_time_series',
            value=0.0,
            unit='ops',
            metadata={
                'values': [2, 2, 2, 2, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1
            },
            timestamp=0),
        sample.Sample(
            metric='Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1
            },
            timestamp=0)
    ]
    self.assertEqual(samples, expected_result)

  def testParseResults_no_time_series(self):
    get_metadata = {
        'histogram': json.dumps([
            {'microsec': 0.0, 'count': 4500},
            {'microsec': 2000.0, 'count': 4500}])
    }
    get_metadata.update(METADATA)
    set_metadata = {
        'histogram': json.dumps([
            {'microsec': 0.0, 'count': 50},
            {'microsec': 1000.0, 'count': 50},
            {'microsec': 2000.0, 'count': 50},
            {'microsec': 3000.0, 'count': 150},
            {'microsec': 4000.0, 'count': 200},
            {'microsec': 5000.0, 'count': 200},
            {'microsec': 6000.0, 'count': 200},
            {'microsec': 7000.0, 'count': 50},
            {'microsec': 8000.0, 'count': 40},
            {'microsec': 9000.0, 'count': 10}])
    }
    set_metadata.update(METADATA)

    time_series_metadata = {'time_series': {'0': 3, '1': 4}}
    time_series_metadata.update(METADATA)
    latency_series_metadata = {'time_series': {'0': 1, '1': 2.1}}
    latency_series_metadata.update(METADATA)

    expected_result = [
        sample.Sample(
            metric='Ops Throughput',
            value=44006.55,
            unit='ops/s',
            metadata=METADATA),
        sample.Sample(
            metric='KB Throughput',
            value=1828.0,
            unit='KB/s',
            metadata=METADATA),
        sample.Sample(
            metric='Latency', value=1.54, unit='ms', metadata=METADATA),
        sample.Sample(
            metric='get latency histogram',
            value=0,
            unit='',
            metadata=get_metadata),
        sample.Sample(
            metric='set latency histogram',
            value=0,
            unit='',
            metadata=set_metadata),
    ]
    samples = []
    results = memtier.MemtierResult.Parse(TEST_OUTPUT, None)
    samples.extend(results.GetSamples(METADATA))
    self.assertSampleListsEqualUpToTimestamp(samples, expected_result)

  def testMeasureLatencyCappedThroughput(self):
    mock_run_results = [
        # Multi-pipeline
        GetMemtierResult(7270, 0.175),
        GetMemtierResult(386941, 6.751),
        GetMemtierResult(424626, 3.247),
        GetMemtierResult(408957, 1.591),
        GetMemtierResult(398920, 0.839),
        GetMemtierResult(408290, 1.207),
        GetMemtierResult(405672, 1.015),
        GetMemtierResult(408808, 0.951),
        GetMemtierResult(405209, 0.967),
        GetMemtierResult(398249, 1.015),
        GetMemtierResult(409221, 0.967),
        GetMemtierResult(413240, 0.975),
        GetMemtierResult(412573, 0.975),
        # Multi-client
        GetMemtierResult(7433, 0.159),
        GetMemtierResult(218505, 2.975),
        GetMemtierResult(79875, 4.447),
        GetMemtierResult(323469, 0.519),
        GetMemtierResult(321503, 0.743),
        GetMemtierResult(324469, 0.855),
        GetMemtierResult(308853, 1.007),
        GetMemtierResult(322717, 0.903),
        GetMemtierResult(321258, 0.919),
        GetMemtierResult(323695, 0.927),
        GetMemtierResult(310044, 0.983),
    ]
    self.enter_context(
        mock.patch.object(memtier, '_Run', side_effect=mock_run_results))

    results = memtier.MeasureLatencyCappedThroughput(None, 'unused', 0)

    actual_throughputs = []
    for s in results:
      if s.metric == 'Ops Throughput':
        actual_throughputs.append(s.value)
    self.assertEqual(actual_throughputs, [413240, 324469])


if __name__ == '__main__':
  unittest.main()
