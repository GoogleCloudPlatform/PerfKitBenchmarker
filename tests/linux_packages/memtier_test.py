"""Tests for perfkitbenchmarker.linux_packages.memtier."""


import json
import unittest
from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_packages import memtier

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

METADATA = {'test': 'foobar'}


class MemtierTestCase(unittest.TestCase, test_util.SamplesTestMixin):

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
    expected_result = [
        sample.Sample(
            metric='Ops Throughput',
            value=44006.55, unit='ops/s',
            metadata=METADATA),
        sample.Sample(
            metric='KB Throughput',
            value=1828.0,
            unit='KB/s',
            metadata=METADATA),
        sample.Sample(
            metric='Latency',
            value=1.54,
            unit='ms',
            metadata=METADATA),
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
    results = memtier.MemtierResult.Parse(TEST_OUTPUT)
    samples.extend(results.GetSamples(METADATA))
    self.assertSampleListsEqualUpToTimestamp(samples, expected_result)


if __name__ == '__main__':
  unittest.main()
