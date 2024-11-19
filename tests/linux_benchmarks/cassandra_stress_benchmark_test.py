import unittest
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import cassandra_stress_benchmark
from tests import pkb_common_test_case


class CassandraStressBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def testSarRequestCount(self):
    count = cassandra_stress_benchmark.CalculateNumberOfSarRequestsFromDuration(
        '10m', 10
    )
    self.assertEqual(count, 60)
    count = cassandra_stress_benchmark.CalculateNumberOfSarRequestsFromDuration(
        '10s', 10
    )
    self.assertEqual(count, 1)
    count = cassandra_stress_benchmark.CalculateNumberOfSarRequestsFromDuration(
        '10h', 10
    )
    self.assertEqual(count, 3600)
    count = cassandra_stress_benchmark.CalculateNumberOfSarRequestsFromDuration(
        None, 10
    )
    self.assertEqual(count, 0)

  def testCassandraStressResponseParsing(self):
    sample_results = """
      Results:
      Op rate                   :  255,886 op/s  [WRITE: 255,886 op/s]
      Partition rate            :  255,886 pk/s  [WRITE: 255,886 pk/s]
      Row rate                  :  255,886 row/s [WRITE: 255,886 row/s]
      Latency mean              :    0.6 ms [WRITE: 0.6 ms]
      Latency median            :    0.4 ms [WRITE: 0.4 ms]
      Latency 95th percentile   :    0.6 ms [WRITE: 0.6 ms]
      Latency 99th percentile   :    0.9 ms [WRITE: 0.9 ms]
      Latency 99.9th percentile :   68.4 ms [WRITE: 68.4 ms]
      Latency max               :  258.7 ms [WRITE: 258.7 ms]
      Total partitions          : 200,000,000 [WRITE: 200,000,000]
      Total errors              :          0 [WRITE: 0]
      Total GC count            : 0
      Total GC memory           : 0 B
      Total GC time             :    0.0 seconds
      Avg GC time               :    NaN ms
      StdDev GC time            :    0.0 ms
      Total operation time      : 00:13:01"""
    expected_response = {
        'op rate': 255886.0,
        'partition rate': 255886.0,
        'row rate': 255886.0,
        'latency mean': 0.6,
        'latency median': 0.4,
        'latency 95th percentile': 0.6,
        'latency 99th percentile': 0.9,
        'latency 99.9th percentile': 68.4,
        'latency max': 258.7,
        'total partitions': 200000000.0,
        'total errors': 0.0,
        'total operation time': 781,
    }
    response = cassandra_stress_benchmark.ParseResp(sample_results)
    self.assertEqual(response, expected_response)


if __name__ == '__main__':
  unittest.main()
