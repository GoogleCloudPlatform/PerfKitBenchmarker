"""Tests for perfkitbenchmarker.linux_packages.memtier."""

import json
import os
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_packages import memtier
from tests import matchers
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

TEST_OUTPUT = """
  4         Threads
  50        Connections per thread
  20        Seconds
  Type        Ops/sec     Hits/sec   Misses/sec    Avg. Latency      p50 Latency     p90 Latency     p95 Latency     p99 Latency   p99.5 Latency p99.9 Latency p99.950 Latency p99.990 Latency   KB/sec
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Sets        4005.50          ---          ---         1.50600         1.21500         2.29500         2.31900         2.39900         3.93500       3.93600         3.93700         3.93800    308.00
  Gets       40001.05     40001.05         0.00         1.54300         1.21500         2.28700         2.31900         2.39100         3.84700       3.84800         3.84900         3.85000   1519.00
  Waits          0.00          ---          ---             ---             ---             ---             ---             ---             ---           ---             ---           ---       ---
  Totals     44006.55     40001.05         0.00         1.54000         1.21500         2.29500         2.31900         2.39900         3.87100       3.87200         3.87300         3.87400   1828.00

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
GET
"""

METADATA = {
    'test': 'foobar',
    'p50_latency': 1.215,
    'p90_latency': 2.295,
    'p95_latency': 2.319,
    'p99_latency': 2.399,
    'p99.5_latency': 3.871,
    'p99.9_latency': 3.872,
    'p99.950_latency': 3.873,
    'p99.990_latency': 3.874,
    'avg_latency': 1.54,
}

TIME_SERIES_JSON = json.loads("""
  {
    "ALL STATS":
    {
      "Totals":
      {
        "Time-Serie":
        {
          "0": {"Count": 3, "Average Latency": 1, "Max Latency": 1, "Min Latency": 1, "p50.00": 1, "p90.00": 1, "p95.00": 1, "p99.00": 1, "p99.90": 1},
          "1": {"Count": 4, "Average Latency": 2.1, "Max Latency": 2.1, "Min Latency": 2.1, "p50.00": 2.1, "p90.00": 2.1, "p95.00": 2.1, "p99.00": 2.1, "p99.90": 2.1}
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
""")


def GetMemtierResult(ops_per_sec, p95_latency):
  return memtier.MemtierResult(
      ops_per_sec,
      0,
      0,
      {'90': 0, '95': p95_latency, '99': 0},
      [],
      [],
      [],
      [],
      {},
      {},
  )


class MemtierTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'redis_memtier.json'
    )
    with open(path) as fp:
      self.time_series = json.load(fp)

  def testParseResults(self):
    get_metadata = {
        'histogram': json.dumps([
            {'microsec': 0.0, 'count': 4500},
            {'microsec': 2000.0, 'count': 4500},
        ])
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
            {'microsec': 9000.0, 'count': 10},
        ])
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
        'Time_unit': 'MILLISECONDS',
    }

    expected_result = [
        sample.Sample(
            metric='Ops Throughput',
            value=44006.55,
            unit='ops/s',
            metadata=METADATA,
        ),
        sample.Sample(
            metric='KB Throughput', value=1828.0, unit='KB/s', metadata=METADATA
        ),
        sample.Sample(
            metric='Latency', value=1.54, unit='ms', metadata=METADATA
        ),
        sample.Sample(
            metric='get latency histogram',
            value=0,
            unit='',
            metadata=get_metadata,
        ),
        sample.Sample(
            metric='set latency histogram',
            value=0,
            unit='',
            metadata=set_metadata,
        ),
        sample.Sample(
            metric='Memtier Duration',
            value=2,
            unit='ms',
            metadata=runtime_info_metadata,
        ),
    ]
    samples = []
    results = memtier.MemtierResult.Parse(TEST_OUTPUT, TIME_SERIES_JSON)
    samples.extend(results.GetSamples(METADATA))
    self.assertSampleListsEqualUpToTimestamp(samples, expected_result)

  def testParseRealResults(self):
    expected_result = [
        sample.Sample(
            metric='Ops Throughput',
            value=44006.55,
            unit='ops/s',
            metadata={
                'test': 'foobar',
                'p50_latency': 1.215,
                'p90_latency': 2.295,
                'p95_latency': 2.319,
                'p99.5_latency': 3.871,
                'p99.950_latency': 3.873,
                'p99.990_latency': 3.874,
                'p99.9_latency': 3.872,
                'p99_latency': 2.399,
                'avg_latency': 1.54,
            },
            timestamp=1681957774.583395,
        ),
        sample.Sample(
            metric='KB Throughput',
            value=1828.0,
            unit='KB/s',
            metadata={
                'test': 'foobar',
                'p50_latency': 1.215,
                'p90_latency': 2.295,
                'p95_latency': 2.319,
                'p99.5_latency': 3.871,
                'p99.950_latency': 3.873,
                'p99.990_latency': 3.874,
                'p99.9_latency': 3.872,
                'p99_latency': 2.399,
                'avg_latency': 1.54,
            },
            timestamp=1681957774.5834072,
        ),
        sample.Sample(
            metric='Latency',
            value=1.54,
            unit='ms',
            metadata={
                'test': 'foobar',
                'p50_latency': 1.215,
                'p90_latency': 2.295,
                'p95_latency': 2.319,
                'p99.5_latency': 3.871,
                'p99.950_latency': 3.873,
                'p99.990_latency': 3.874,
                'p99.9_latency': 3.872,
                'p99_latency': 2.399,
                'avg_latency': 1.54,
            },
            timestamp=1681957774.5834093,
        ),
        sample.Sample(
            metric='get latency histogram',
            value=0.0,
            unit='',
            metadata={
                'test': 'foobar',
                'p50_latency': 1.215,
                'p90_latency': 2.295,
                'p95_latency': 2.319,
                'p99.5_latency': 3.871,
                'p99.950_latency': 3.873,
                'p99.990_latency': 3.874,
                'p99.9_latency': 3.872,
                'p99_latency': 2.399,
                'avg_latency': 1.54,
                'histogram': (
                    '[{"microsec": 0.0, "count": 4500}, {"microsec": 2000.0,'
                    ' "count": 4500}]'
                ),
            },
            timestamp=1681957774.583477,
        ),
        sample.Sample(
            metric='set latency histogram',
            value=0.0,
            unit='',
            metadata={
                'test': 'foobar',
                'p50_latency': 1.215,
                'p90_latency': 2.295,
                'p95_latency': 2.319,
                'p99.5_latency': 3.871,
                'p99.950_latency': 3.873,
                'p99.990_latency': 3.874,
                'p99.9_latency': 3.872,
                'p99_latency': 2.399,
                'avg_latency': 1.54,
                'histogram': (
                    '[{"microsec": 0.0, "count": 50}, {"microsec": 1000.0,'
                    ' "count": 50}, {"microsec": 2000.0, "count": 50},'
                    ' {"microsec": 3000.0, "count": 150}, {"microsec": 4000.0,'
                    ' "count": 200}, {"microsec": 5000.0, "count": 200},'
                    ' {"microsec": 6000.0, "count": 200}, {"microsec": 7000.0,'
                    ' "count": 50}, {"microsec": 8000.0, "count": 40},'
                    ' {"microsec": 9000.0, "count": 10}]'
                ),
            },
            timestamp=1681957774.58352,
        ),
        sample.Sample(
            metric='Memtier Duration',
            value=799002.0,
            unit='ms',
            metadata={
                'Start_time': 1681939139082,
                'Finish_time': 1681939938084,
                'Total_duration': 799002,
                'Time_unit': 'MILLISECONDS',
            },
            timestamp=1681957774.5835233,
        ),
    ]
    results = memtier.MemtierResult.Parse(TEST_OUTPUT, self.time_series)
    samples = results.GetSamples(METADATA)
    self.assertSampleListsEqualUpToTimestamp(samples, expected_result)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAggregateMemtierWithOneResult(self):
    FLAGS.memtier_time_series = True
    timestamps = [0, 1000, 2000, 3000, 4000]
    ops_values = [1, 1, 1, 1, 1]
    latency = {
        'Average Latency': [1, 2, 3, 4, 5],
        'Max Latency': [1, 2, 3, 4, 5],
        'Min Latency': [1, 2, 3, 4, 5],
        'p50.00': [1, 2, 3, 4, 5],
        'p90.00': [1, 2, 3, 4, 5],
        'p95.00': [1, 2, 3, 4, 5],
        'p99.00': [1, 2, 3, 4, 5],
        'p99.90': [1, 2, 3, 4, 5],
    }
    results = [
        memtier.MemtierResult(
            1,
            2,
            0,
            {'90': 0, '95': 0, '99': 0},
            [],
            [],
            timestamps,
            ops_values,
            latency,
            {},
        )
    ]
    samples = memtier.AggregateMemtierResults(results, {})
    expected_result = [
        sample.Sample(
            metric='Total Ops Throughput',
            value=1.0,
            unit='ops/s',
            metadata={},
            timestamp=0,
        ),
        sample.Sample(
            metric='Total KB Throughput',
            value=2.0,
            unit='KB/s',
            metadata={},
            timestamp=0,
        ),
        sample.Sample(
            metric='OPS_time_series',
            value=0.0,
            unit='ops',
            metadata={
                'values': [1, 1, 1, 1, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'ramp_down_starts': 4000,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
    ]
    self.assertEqual(samples, expected_result)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  @flagsaver.flagsaver(memtier_time_series=True)
  def testAggregateMemtierResultsWithMultipleResultsDifferentStartTime(self):
    timestamps_1 = [0, 1000, 2000, 3000, 4000]
    ops_values_1 = [1, 1, 1, 1, 1]
    latency_1 = {
        'Average Latency': [1, 2, 3, 4, 5],
        'Max Latency': [1, 2, 3, 4, 5],
        'Min Latency': [1, 2, 3, 4, 5],
        'p50.00': [1, 2, 3, 4, 5],
        'p90.00': [1, 2, 3, 4, 5],
        'p95.00': [1, 2, 3, 4, 5],
        'p99.00': [1, 2, 3, 4, 5],
        'p99.90': [1, 2, 3, 4, 5],
    }
    timestamps_2 = [1000, 2000, 3000, 4000, 5000]
    ops_values_2 = [1, 1, 1, 1, 1]
    latency_2 = {
        'Average Latency': [5, 4, 3, 2, 1],
        'Max Latency': [5, 4, 3, 2, 1],
        'Min Latency': [5, 4, 3, 2, 1],
        'p50.00': [5, 4, 3, 2, 1],
        'p90.00': [5, 4, 3, 2, 1],
        'p95.00': [5, 4, 3, 2, 1],
        'p99.00': [5, 4, 3, 2, 1],
        'p99.90': [5, 4, 3, 2, 1],
    }
    timestamps_3 = [2000, 3000, 4000, 5000, 6000]
    ops_values_3 = [1, 1, 1, 1, 1]
    latency_3 = {
        'Average Latency': [5, 4, 3, 1000, 1000],
        'Max Latency': [5, 4, 3, 1000, 1000],
        'Min Latency': [5, 4, 3, 1000, 1000],
        'p50.00': [5, 4, 3, 1000, 1000],
        'p90.00': [5, 4, 3, 1000, 1000],
        'p95.00': [5, 4, 3, 1000, 1000],
        'p99.00': [5, 4, 3, 1000, 1000],
        'p99.90': [5, 4, 3, 1000, 1000],
    }
    results = [
        memtier.MemtierResult(
            2,
            4,
            0,
            {'90': 0, '95': 0, '99': 0},
            [],
            [],
            timestamps_1,
            ops_values_1,
            latency_1,
            {},
        ),
        memtier.MemtierResult(
            2,
            4,
            0,
            {'90': 0, '95': 0, '99': 0},
            [],
            [],
            timestamps_2,
            ops_values_2,
            latency_2,
            {},
        ),
        memtier.MemtierResult(
            2,
            4,
            0,
            {'90': 0, '95': 0, '99': 0},
            [],
            [],
            timestamps_3,
            ops_values_3,
            latency_3,
            {},
        ),
    ]
    samples = memtier.AggregateMemtierResults(results, {})
    expected_result = [
        sample.Sample(
            metric='Total Ops Throughput',
            value=6.0,
            unit='ops/s',
            metadata={},
            timestamp=0,
        ),
        sample.Sample(
            metric='Total KB Throughput',
            value=12.0,
            unit='KB/s',
            metadata={},
            timestamp=0,
        ),
        sample.Sample(
            metric='OPS_time_series',
            value=0.0,
            unit='ops',
            metadata={
                'values': [1, 2, 3, 3, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'ramp_down_starts': 4000,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 5, 5, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 5, 4, 3, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [0, 0, 5, 4, 3],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 2,
            },
            timestamp=0,
        ),
    ]
    self.assertEqual(samples, expected_result)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testAggregateMemtierResultsWithMultipleResults(self):
    FLAGS.memtier_time_series = True
    timestamps = [0, 1000, 2000, 3000, 4000]
    ops_values_1 = [1, 1, 1, 1, 1]
    latency_1 = {
        'Average Latency': [1, 2, 3, 4, 5],
        'Max Latency': [1, 2, 3, 4, 5],
        'Min Latency': [1, 2, 3, 4, 5],
        'p50.00': [1, 2, 3, 4, 5],
        'p90.00': [1, 2, 3, 4, 5],
        'p95.00': [1, 2, 3, 4, 5],
        'p99.00': [1, 2, 3, 4, 5],
        'p99.90': [1, 2, 3, 4, 5],
    }
    ops_values_2 = [1, 1, 1, 1, 1]
    latency_2 = {
        'Average Latency': [5, 4, 3, 2, 1],
        'Max Latency': [5, 4, 3, 2, 1],
        'Min Latency': [5, 4, 3, 2, 1],
        'p50.00': [5, 4, 3, 2, 1],
        'p90.00': [5, 4, 3, 2, 1],
        'p95.00': [5, 4, 3, 2, 1],
        'p99.00': [5, 4, 3, 2, 1],
        'p99.90': [5, 4, 3, 2, 1],
    }
    results = [
        memtier.MemtierResult(
            2,
            4,
            0,
            {'90': 0, '95': 0, '99': 0},
            [],
            [],
            timestamps,
            ops_values_1,
            latency_1,
            {},
        ),
        memtier.MemtierResult(
            2,
            4,
            0,
            {'90': 0, '95': 0, '99': 0},
            [],
            [],
            timestamps,
            ops_values_2,
            latency_2,
            {},
        ),
    ]
    samples = memtier.AggregateMemtierResults(results, {})
    expected_result = [
        sample.Sample(
            metric='Total Ops Throughput',
            value=4.0,
            unit='ops/s',
            metadata={},
            timestamp=0,
        ),
        sample.Sample(
            metric='Total KB Throughput',
            value=8.0,
            unit='KB/s',
            metadata={},
            timestamp=0,
        ),
        sample.Sample(
            metric='OPS_time_series',
            value=0.0,
            unit='ops',
            metadata={
                'values': [2, 2, 2, 2, 2],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'ramp_down_starts': 4000,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Average Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Max Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='Min Latency_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p50.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p90.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p95.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.00_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [1, 2, 3, 4, 5],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 0,
            },
            timestamp=0,
        ),
        sample.Sample(
            metric='p99.90_time_series',
            value=0.0,
            unit='ms',
            metadata={
                'values': [5, 4, 3, 2, 1],
                'timestamps': [0, 1000, 2000, 3000, 4000],
                'interval': 1,
                'client': 1,
            },
            timestamp=0,
        ),
    ]
    print(samples)
    self.assertEqual(samples, expected_result)

  def testParseResults_no_time_series(self):
    get_metadata = {
        'histogram': json.dumps([
            {'microsec': 0.0, 'count': 4500},
            {'microsec': 2000.0, 'count': 4500},
        ])
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
            {'microsec': 9000.0, 'count': 10},
        ])
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
            metadata=METADATA,
        ),
        sample.Sample(
            metric='KB Throughput', value=1828.0, unit='KB/s', metadata=METADATA
        ),
        sample.Sample(
            metric='Latency', value=1.54, unit='ms', metadata=METADATA
        ),
        sample.Sample(
            metric='get latency histogram',
            value=0,
            unit='',
            metadata=get_metadata,
        ),
        sample.Sample(
            metric='set latency histogram',
            value=0,
            unit='',
            metadata=set_metadata,
        ),
    ]
    samples = []
    results = memtier.MemtierResult.Parse(TEST_OUTPUT, None)
    samples.extend(results.GetSamples(METADATA))
    self.assertSampleListsEqualUpToTimestamp(samples, expected_result)

  @flagsaver.flagsaver(num_cpus_override=16)
  def testMeasureLatencyCappedThroughput(self):
    mock_run_results = [
        # Multi-pipeline
        GetMemtierResult(10, 10.0),
        GetMemtierResult(20, 5.0),
        GetMemtierResult(30, 2.0),
        GetMemtierResult(8, 1.5),
        GetMemtierResult(9, 0.7),
        GetMemtierResult(3, 1.4),
        GetMemtierResult(2, 0.8),
        GetMemtierResult(4, 1.3),
        GetMemtierResult(15, 0.9),
        GetMemtierResult(7, 1.2),
        GetMemtierResult(10, 0.9),
        GetMemtierResult(1, 1.1),
        GetMemtierResult(9, 0.9),
        GetMemtierResult(30, 1.2),
        # Multi-client
        GetMemtierResult(10, 10.0),
        GetMemtierResult(20, 5.0),
        GetMemtierResult(30, 2.0),
        GetMemtierResult(8, 1.5),
        GetMemtierResult(9, 0.7),
        GetMemtierResult(3, 1.4),
    ]
    self.enter_context(
        mock.patch.object(memtier, '_Run', side_effect=mock_run_results)
    )

    mock_vm = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    results = memtier.MeasureLatencyCappedThroughput(mock_vm, 1, 'unused', 0)

    actual_throughputs = []
    for s in results:
      if s.metric == 'Ops Throughput':
        actual_throughputs.append(s.value)
    self.assertEqual(actual_throughputs, [15.0, 9.0])

  def testRunParallelSingleVm(self):
    vm1 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    connections = [
        memtier.MemtierConnection(vm1, '10.0.1.117', 6379),
    ]
    mock_run_threaded = self.enter_context(
        mock.patch.object(background_tasks, 'RunThreaded')
    )

    memtier._RunParallelConnections(connections, '0.0.0.0', 1234, 1, 2, 3)

    mock_run_threaded.assert_called_once_with(
        memtier._Run,
        [
            (
                (),
                {
                    'vm': vm1,
                    'server_ip': '0.0.0.0',
                    'server_port': 1234,
                    'threads': 1,
                    'clients': 2,
                    'pipeline': 3,
                    'password': None,
                    'unique_id': vm1.ip_address,
                },
            ),
        ],
    )

  def testRunParallelMultipleVms(self):
    vm1 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    vm1.ip_address = 'vm1'
    vm2 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    vm2.ip_address = 'vm2'
    connections = [
        memtier.MemtierConnection(vm1, '10.0.1.117', 6379),
        memtier.MemtierConnection(vm1, '10.0.2.104', 6379),
        memtier.MemtierConnection(vm1, '10.0.3.217', 6379),
        memtier.MemtierConnection(vm2, '10.0.2.177', 6379),
        memtier.MemtierConnection(vm2, '10.0.1.174', 6379),
        memtier.MemtierConnection(vm2, '10.0.3.6', 6379),
    ]
    mock_run_threaded = self.enter_context(
        mock.patch.object(background_tasks, 'RunThreaded')
    )

    memtier._RunParallelConnections(connections, '0.0.0.0', 1234, 1, 2, 3)

    mock_run_threaded.assert_called_once_with(
        memtier._Run,
        [
            (
                (),
                {
                    'vm': vm1,
                    'server_ip': '0.0.0.0',
                    'server_port': 1234,
                    'threads': 1,
                    'clients': 2,
                    'pipeline': 3,
                    'password': None,
                    'shard_addresses': (
                        '10.0.1.117:6379,10.0.2.104:6379,10.0.3.217:6379'
                    ),
                    'unique_id': 'vm1',
                },
            ),
            (
                (),
                {
                    'vm': vm2,
                    'server_ip': '0.0.0.0',
                    'server_port': 1234,
                    'threads': 1,
                    'clients': 2,
                    'pipeline': 3,
                    'password': None,
                    'shard_addresses': (
                        '10.0.2.177:6379,10.0.1.174:6379,10.0.3.6:6379'
                    ),
                    'unique_id': 'vm2',
                },
            ),
        ],
    )

  @flagsaver.flagsaver(memtier_distribution_iterations=1, num_cpus_override=16)
  def testMeasureLatencyCappedThroughputDistribution(self):
    vm1 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    vm1.ip_address = 'vm1'
    vm2 = pkb_common_test_case.TestLinuxVirtualMachine(
        pkb_common_test_case.CreateTestVmSpec()
    )
    vm2.ip_address = 'vm2'
    connections = [
        memtier.MemtierConnection(vm1, '10.0.1.117', 6379),
        memtier.MemtierConnection(vm1, '10.0.2.104', 6379),
        memtier.MemtierConnection(vm1, '10.0.3.217', 6379),
        memtier.MemtierConnection(vm2, '10.0.2.177', 6379),
        memtier.MemtierConnection(vm2, '10.0.1.174', 6379),
        memtier.MemtierConnection(vm2, '10.0.3.6', 6379),
    ]

    mock_binary_search = self.enter_context(
        mock.patch.object(
            memtier,
            '_BinarySearchForLatencyCappedThroughput',
            return_value=[
                memtier.MemtierResult(
                    parameters=memtier.MemtierBinarySearchParameters(
                        pipelines=1, threads=2, clients=3
                    )
                )
            ],
        )
    )
    mock_results = [
        memtier.MemtierResult(
            ops_per_sec=0,
            kb_per_sec=0,
            latency_ms=0,
            latency_dic={'90': 0, '95': 50, '99': 1.0},
        ),
        memtier.MemtierResult(
            ops_per_sec=200,
            kb_per_sec=2,
            latency_ms=0.2,
            latency_dic={'90': 10, '95': 40, '99': 0.8},
        ),
        memtier.MemtierResult(
            ops_per_sec=400,
            kb_per_sec=4,
            latency_ms=0.4,
            latency_dic={'90': 20, '95': 30, '99': 0.6},
        ),
        memtier.MemtierResult(
            ops_per_sec=600,
            kb_per_sec=6,
            latency_ms=0.6,
            latency_dic={'90': 30, '95': 20, '99': 0.4},
        ),
        memtier.MemtierResult(
            ops_per_sec=800,
            kb_per_sec=8,
            latency_ms=0.8,
            latency_dic={'90': 40, '95': 10, '99': 0.2},
        ),
        memtier.MemtierResult(
            ops_per_sec=1000,
            kb_per_sec=10,
            latency_ms=1.0,
            latency_dic={'90': 50, '95': 0, '99': 0.0},
        ),
    ]
    mock_run = self.enter_context(
        mock.patch.object(
            memtier,
            '_RunParallelConnections',
            return_value=mock_results,
        )
    )

    results = memtier.MeasureLatencyCappedThroughputDistribution(
        connections, '0.0.0.0', 1234, [vm1, vm2], 6
    )

    expected_metadata = {
        'distribution_iterations': 1,
        'threads': 2,
        'clients': 3,
        'pipelines': 1,
    }

    with self.subTest('SamplesAreCorrect'):
      # self.assertSampleListsEqualUpToTimestamp(results, expected_samples)
      self.assertSampleInList(
          sample.Sample(
              metric='Mean ops_per_sec',
              value=500.0,
              unit='ops/s',
              metadata=expected_metadata,
          ),
          results,
      )
      self.assertSampleInList(
          sample.Sample(
              metric='Stdev kb_per_sec',
              value=3.7416573867739413,
              unit='KB/s',
              metadata=expected_metadata,
          ),
          results,
      )
    with self.subTest('BinarySearchHasCorrectArgs'):
      mock_binary_search.assert_called_once_with(
          connections, [memtier._ClientModifier(10, 16)], '0.0.0.0', 1234, None
      )
    with self.subTest('RunHasCorrectArgs'):
      mock_run.assert_has_calls(
          [mock.call(connections, '0.0.0.0', 1234, 2, 3, 1, None)]
      )

  def testCombineResults(self):
    result1 = memtier.MemtierResult(
        ops_per_sec=800,
        kb_per_sec=8,
        latency_ms=0.8,
        latency_dic={'90': 40, '95': 10, '99': 0.2},
        metadata={'test_metadata': True},
        parameters=memtier.MemtierBinarySearchParameters(lower_bound=1),
    )
    result2 = memtier.MemtierResult(
        ops_per_sec=1000,
        kb_per_sec=10,
        latency_ms=1.0,
        latency_dic={'90': 50, '95': 0, '99': 0.0},
    )
    expected_result = memtier.MemtierResult(
        ops_per_sec=1800,
        kb_per_sec=18,
        latency_ms=0.9,
        latency_dic={'90': 45, '95': 5, '99': 0.1},
        metadata={'test_metadata': True},
        parameters=memtier.MemtierBinarySearchParameters(lower_bound=1),
    )
    self.assertEqual(
        expected_result, memtier._CombineResults([result1, result2])
    )

  @flagsaver.flagsaver(
      memtier_key_maximum=1000, memtier_data_size_list='1024:1,32:1'
  )
  def testLoad(self):
    vm1 = mock.Mock()
    vm2 = mock.Mock()
    test_vms = [vm1, vm2]

    memtier.Load(test_vms, 'test_ip', 9999)

    vm1.RemoteCommand.assert_called_once_with(
        matchers.HAS('--key-minimum 1 --key-maximum 500')
    )
    vm2.RemoteCommand.assert_called_once_with(
        matchers.HAS('--key-minimum 500 --key-maximum 1000')
    )
    vm1.RemoteCommand.assert_called_once_with(
        matchers.HAS('--data-size-list 1024:1,32:1')
    )
    vm2.RemoteCommand.assert_called_once_with(
        matchers.HAS('--data-size-list 1024:1,32:1')
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'WithDataSizeArg',
          'input_args': {'data_size': 1024},
          'expected_cmd_regex': '--data-size 1024',
      },
      {
          'testcase_name': 'WithDataSizeListArg',
          'input_args': {'data_size_list': '1024:1,32:2'},
          'expected_cmd_regex': '--data-size-list 1024:1,32:2',
      },
      {
          'testcase_name': 'WithBothDataSizeAndListArgs',
          'input_args': {'data_size': 1024, 'data_size_list': '1024:1,32:2'},
          'expected_cmd_regex': '--data-size-list 1024:1,32:2',
      },
  )
  def testBuildMemtierCommand(self, input_args, expected_cmd_regex):
    cmd = memtier.BuildMemtierCommand(**input_args)
    self.assertRegex(cmd, expected_cmd_regex)

  def testGetMetadataDefault(self):
    meta = memtier.GetMetadata(clients=100, threads=4, pipeline=1)
    self.assertEqual(
        meta,
        {
            'memtier_clients': 100,
            'memtier_cluster_mode': False,
            'memtier_data_size': 32,
            'memtier_key_maximum': 10000000,
            'memtier_key_pattern': 'R:R',
            'memtier_pipeline': 1,
            'memtier_protocol': 'memcache_binary',
            'memtier_ratio': '1:9',
            'memtier_requests': 10000,
            'memtier_expiry_range': None,
            'memtier_run_count': 1,
            'memtier_run_mode': 'NORMAL_RUN',
            'memtier_threads': 4,
            'memtier_version': '2.1.1',
            'memtier_tls': False,
        },
    )

  @flagsaver.flagsaver(memtier_data_size_list='1024:1,32:2')
  def testGetMetadataWithDataSizeList(self):
    meta = memtier.GetMetadata(clients=100, threads=4, pipeline=1)
    self.assertNotIn('memtier_data_size', meta)
    self.assertEqual(meta['memtier_data_size_list'], '1024:1,32:2')


if __name__ == '__main__':
  unittest.main()
