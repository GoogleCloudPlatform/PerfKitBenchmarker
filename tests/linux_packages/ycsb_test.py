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
"""Tests for perfkitbenchmarker.packages.ycsb."""

import copy
import logging
import os
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import ycsb
from tests import matchers
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


def open_data_file(filename):
  path = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
  with open(path) as fp:
    return fp.read()


def _parse_and_return_time_series(filename):
  content = open_data_file(filename)
  return ycsb.ParseResults(content, 'timeseries')


class SimpleResultParserTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(SimpleResultParserTestCase, self).setUp()
    self.contents = open_data_file('ycsb-test-run.dat')
    self.results = ycsb.ParseResults(self.contents, 'histogram')

  def testCommandLineSet(self):
    self.assertEqual(
        'Command line: -db com.yahoo.ycsb.BasicDB -P workloads/workloada -t',
        self.results.command_line,
    )

  def testClientSet(self):
    self.assertEqual('YCSB Client 0.1', self.results.client)

  def testUpdateStatisticsParsed(self):
    self.assertEqual(
        ycsb._OpResult(
            group='update',
            statistics={
                'Operations': 531,
                'Return=0': 531,
                'AverageLatency(ms)': 0.0659774011299435,
                'MinLatency(ms)': 0.042,
                'MaxLatency(ms)': 0.345,
                '95thPercentileLatency(ms)': 0,
                '99thPercentileLatency(ms)': 0,
            },
            data_type=ycsb.HISTOGRAM,
            data=[(0, 530), (19, 1)],
        ),
        self.results.groups['update'],
    )

  def testReadStatisticsParsed(self):
    self.assertEqual(
        ycsb._OpResult(
            group='read',
            statistics={
                'Operations': 469,
                'Return=0': 469,
                'AverageLatency(ms)': 0.03847761194029851,
                'MinLatency(ms)': 0.034,
                'MaxLatency(ms)': 0.102,
                '95thPercentileLatency(ms)': 0,
                '99thPercentileLatency(ms)': 0,
            },
            data_type=ycsb.HISTOGRAM,
            data=[(0, 469)],
        ),
        self.results.groups['read'],
    )

  def testOverallStatisticsParsed(self):
    self.assertEqual(
        ycsb._OpResult(
            group='overall',
            statistics={'RunTime(ms)': 80.0, 'Throughput(ops/sec)': 12500.0},
            data_type='histogram',
            data=[],
        ),
        self.results.groups['overall'],
    )


class DetailedResultParserTestCase(unittest.TestCase):

  def setUp(self):
    super(DetailedResultParserTestCase, self).setUp()
    self.contents = open_data_file('ycsb-test-run-2.dat')
    self.results = ycsb.ParseResults(self.contents, 'histogram')

  def testPercentilesFromHistogram_read(self):
    hist = self.results.groups['read'].data
    percentiles = ycsb._PercentilesFromHistogram(hist)
    self.assertEqual(1, percentiles['p50'])
    self.assertEqual(7, percentiles['p99'])

  def testPercentilesFromHistogram_update(self):
    hist = self.results.groups['update'].data
    percentiles = ycsb._PercentilesFromHistogram(hist)
    self.assertEqual(1, percentiles['p50'])
    self.assertEqual(7, percentiles['p99'])


class ThroughputTimeSeriesParserTestCase(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    self.results_1 = _parse_and_return_time_series('ycsb-time-series.dat')
    self.results_2 = _parse_and_return_time_series('ycsb-time-series-2.dat')

  def testParsedThroughputTimeSeriesIsCorrect(self):
    results = _parse_and_return_time_series('ycsb-time-series.dat')
    expected = {
        10: 2102.9,
        20: 2494.5,
        30: 2496.8,
        40: 2509.6,
        50: 2487.2,
        60: 2513.2,
    }
    self.assertEqual(results.throughput_time_series, expected)

  @flagsaver.flagsaver(ycsb_throughput_time_series=True)
  def testCombinedThroughputTimeSeriesIsCorrect(self):
    results_1 = _parse_and_return_time_series('ycsb-time-series.dat')
    results_2 = _parse_and_return_time_series('ycsb-time-series-2.dat')

    combined = ycsb._CombineResults(
        result_list=[results_1, results_2],
        measurement_type=ycsb.TIMESERIES,
        combined_hdr={},
    )

    expected = {
        10: 4187.5,
        20: 4990.2,
        30: 4994.0,
        40: 5018.2,
        50: 4976.5,
        60: 5023.2,
    }
    self.assertEqual(combined.throughput_time_series, expected)


class BadResultParserTestCase(unittest.TestCase):

  def testBadTestRun(self):
    contents = open_data_file('ycsb-test-run-3.dat')
    self.assertRaises(
        errors.Benchmarks.KnownIntermittentError,
        ycsb.ParseResults,
        contents,
        'histogram',
    )

  @flagsaver.flagsaver(ycsb_max_error_rate=0.95)
  def testErrorRate(self):
    contents = open_data_file('ycsb-test-run-4.dat')
    self.assertRaises(
        errors.Benchmarks.RunError, ycsb.ParseResults, contents, 'hdrhistogram'
    )


class WeightedQuantileTestCase(unittest.TestCase):

  def testEvenlyWeightedSamples(self):
    x = list(range(1, 101))  # 1-100
    weights = [1 for _ in x]
    self.assertEqual(50, ycsb._WeightedQuantile(x, weights, 0.50))
    self.assertEqual(75, ycsb._WeightedQuantile(x, weights, 0.75))
    self.assertEqual(90, ycsb._WeightedQuantile(x, weights, 0.90))
    self.assertEqual(95, ycsb._WeightedQuantile(x, weights, 0.95))
    self.assertEqual(99, ycsb._WeightedQuantile(x, weights, 0.99))
    self.assertEqual(100, ycsb._WeightedQuantile(x, weights, 1))

  def testLowWeight(self):
    x = [1, 4]
    weights = [99, 1]
    for i in range(100):
      self.assertEqual(1, ycsb._WeightedQuantile(x, weights, i / 100.0))
    self.assertEqual(4, ycsb._WeightedQuantile(x, weights, 0.995))

  def testMidWeight(self):
    x = [0, 1.2, 4]
    weights = [1, 98, 1]
    for i in range(2, 99):
      self.assertAlmostEqual(1.2, ycsb._WeightedQuantile(x, weights, i / 100.0))
    self.assertEqual(4, ycsb._WeightedQuantile(x, weights, 0.995))


class ParseWorkloadTestCase(unittest.TestCase):

  def testParsesEmptyString(self):
    self.assertDictEqual({}, ycsb.ParseWorkload(''))

  def testIgnoresComment(self):
    self.assertDictEqual({}, ycsb.ParseWorkload('#\n'))
    self.assertDictEqual(
        {}, ycsb.ParseWorkload('#recordcount = 10\n# columnfamily=cf')
    )
    self.assertDictEqual(
        {'recordcount': '10'}, ycsb.ParseWorkload('#Sample!\nrecordcount = 10')
    )

  def testParsesSampleWorkload(self):
    contents = open_data_file('ycsb_workloada')
    actual = ycsb.ParseWorkload(contents)

    expected = {
        'recordcount': '1000',
        'operationcount': '1000',
        'workload': 'com.yahoo.ycsb.workloads.CoreWorkload',
        'readallfields': 'true',
        'readproportion': '0.5',
        'updateproportion': '0.5',
        'scanproportion': '0',
        'insertproportion': '0',
        'requestdistribution': 'zipfian',
    }

    self.assertDictEqual(expected, actual)


class CombineResultsTestCase(unittest.TestCase):

  def testGroupMissing(self):
    r1 = ycsb.YcsbResult(
        groups={
            'read': ycsb._OpResult(
                group='read',
                statistics={'Operations': 100, 'Return=0': 100},
                data_type=ycsb.HISTOGRAM,
            )
        }
    )
    r2 = ycsb.YcsbResult(
        groups={
            'read': ycsb._OpResult(
                group='read',
                statistics={'Operations': 96, 'Return=0': 94, 'Return=-1': 2},
                data_type=ycsb.HISTOGRAM,
            ),
            'update': ycsb._OpResult(
                group='update',
                statistics={'Operations': 100, 'AverageLatency(ms)': 25},
                data_type=ycsb.HISTOGRAM,
            ),
        }
    )
    combined = ycsb._CombineResults([r1, r2], 'histogram', {})
    self.assertCountEqual(['read', 'update'], combined.groups)
    self.assertCountEqual(
        ['Operations', 'Return=0', 'Return=-1'],
        combined.groups['read'].statistics,
    )
    read_stats = combined.groups['read'].statistics
    self.assertEqual(
        {'Operations': 196, 'Return=0': 194, 'Return=-1': 2}, read_stats
    )

  def testDropUnaggregatedFromSingleResult(self):
    r = ycsb.YcsbResult(
        client='',
        command_line='',
        groups={
            'read': ycsb._OpResult(
                group='read',
                statistics={'AverageLatency(ms)': 21},
                data_type=ycsb.HISTOGRAM,
            )
        },
    )

    r_copy = copy.deepcopy(r)
    self.assertEqual(r, r_copy)
    combined = ycsb._CombineResults([r], 'histogram', {})
    self.assertEqual(r, r_copy)
    r.groups['read'].statistics = {}
    self.assertEqual(r, combined)


class HdrLogsParserTestCase(unittest.TestCase):

  def testParseHdrLogFile(self):
    rawlog = """
      #[StartTime: 1523565997 (seconds since epoch), Thu Apr 12 20:46:37 UTC 2018]
         Value     Percentile TotalCount 1/(1-Percentile)

         314.000 0.000000000000          2           1.00
         853.000 0.100000000000      49955           1.11
         949.000 0.200000000000     100351           1.25
         949.000 0.210000000000     100351           1.27
         1033.000 0.300000000000     150110           1.43
      #[Mean    =     1651.145, StdDeviation   =      851.707]
      #[Max     =   203903.000, Total count    =       499019]
      #[Buckets =            8, SubBuckets     =         2048]
    """
    actual = ycsb.ParseHdrLogFile(rawlog)
    expected = [
        (0.0, 0.314, 2),
        (10.0, 0.853, 49953),
        (20.0, 0.949, 50396),
        (30.0, 1.033, 49759),
    ]
    self.assertEqual(actual, expected)


class PrerequisitesTestCase(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.named_parameters(
      {
          'testcase_name': 'SnapshotVersion',
          'url': 'https://storage.googleapis.com/externally_shared_files/ycsb-0.18.0-SNAPSHOT.tar.gz',
          'expected_version': 18,
      },
      {
          'testcase_name': 'StandardVersion',
          'url': 'https://storage.googleapis.com/ycsbclient/ycsb-0.17.0.tar.gz',
          'expected_version': 17,
      },
      {
          'testcase_name': 'GitHubVersion',
          'url': 'https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz',
          'expected_version': 17,
      },
  )
  def testGetVersionIndexFromUrl(self, url, expected_version):
    actual_version = ycsb._GetVersionFromUrl(url)
    self.assertEqual(actual_version, expected_version)

  @flagsaver.flagsaver
  def testBurstLoadCalledWithNoTargetRaises(self):
    # Arrange
    FLAGS.ycsb_burst_load = 1

    # Act & Assert
    with self.assertRaises(errors.Config.InvalidValue):
      ycsb.CheckPrerequisites()


class RunTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.ycsb_workload_files = ['workloadc']
    self.test_executor = ycsb.YCSBExecutor('test_database')
    # Result parsing is already handled elsewhere
    self.enter_context(mock.patch.object(ycsb, 'ParseResults'))
    # Test VM with mocked command
    self.test_vm = mock.Mock()
    self.test_cmd = self.test_vm.RobustRemoteCommand
    self.test_cmd.return_value = ['', '']

  @flagsaver.flagsaver
  def testRunCalledWithCorrectTarget(self):
    # Act
    self.test_executor.Run([self.test_vm], run_kwargs={'target': 1000})

    # Assert
    self.assertIn('-target 1000', self.test_cmd.call_args[0][0])

  @flagsaver.flagsaver
  def testBurstLoadUnlimitedMultiplier(self):
    # Arrange
    FLAGS.ycsb_burst_load = -1
    FLAGS.ycsb_run_parameters = ['target=1000']

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    print(self.test_cmd.call_args_list)
    self.assertNotIn('target', self.test_cmd.call_args_list[1][0][0])

  @flagsaver.flagsaver
  def testBurstLoadCalledWithCorrectTarget(self):
    # Arrange
    FLAGS.ycsb_burst_load = 10
    FLAGS.ycsb_run_parameters = ['target=1000']

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertIn('-target 1000', self.test_cmd.call_args_list[0][0][0])
    self.assertIn('-target 10000', self.test_cmd.call_args_list[1][0][0])

  @flagsaver.flagsaver
  def testIncrementalLoadCalledWithCorrectTarget(self):
    # Arrange
    FLAGS.ycsb_incremental_load = 10000
    FLAGS.ycsb_client_vms = 1

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertSequenceEqual(
        [
            mock.call(matchers.HAS('-target 500')),
            mock.call(matchers.HAS('-target 750')),
            mock.call(matchers.HAS('-target 1125')),
            mock.call(matchers.HAS('-target 1687')),
            mock.call(matchers.HAS('-target 2531')),
            mock.call(matchers.HAS('-target 3796')),
            mock.call(matchers.HAS('-target 5695')),
            mock.call(matchers.HAS('-target 8542')),
            mock.call(matchers.HAS('-target 10000')),
        ],
        self.test_cmd.mock_calls,
    )

  @flagsaver.flagsaver
  def testIncrementalLoadUsesCorrectThreadCounts(self):
    # Arrange
    FLAGS.ycsb_incremental_load = 2500
    FLAGS.ycsb_client_vms = 1
    FLAGS['ycsb_threads_per_client'].parse(['1000'])
    mock_set_thread_count = self.enter_context(
        mock.patch.object(self.test_executor, '_SetClientThreadCount')
    )

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertSequenceEqual(
        [
            mock.call(500),
            mock.call(750),
            mock.call(1000),
            mock.call(1000),
            mock.call(1000),
        ],
        mock_set_thread_count.mock_calls,
    )

  @flagsaver.flagsaver
  def testIncrementalLoadCalledWithLowerTarget(self):
    # Arrange
    FLAGS.ycsb_incremental_load = 200  # Lower than 500, the default start
    FLAGS.ycsb_client_vms = 1

    # Act
    self.test_executor.Run([self.test_vm])

    # Assert
    self.assertSequenceEqual(
        [mock.call(matchers.HAS('-target 200'))], self.test_cmd.mock_calls
    )


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
