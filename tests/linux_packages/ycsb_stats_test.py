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
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.linux_packages import ycsb_stats
from tests import pkb_common_test_case
from pyfakefs import fake_filesystem


FLAGS = flags.FLAGS

TEST_DIR = '/mock_dir'


def open_data_file(filename):
  path = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
  with open(path) as fp:
    return fp.read()


def _parse_and_return_time_series(filename):
  content = open_data_file(filename)
  return ycsb_stats.ParseResults(content, ycsb_stats.HDRHISTOGRAM)


class SimpleResultParserTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(SimpleResultParserTestCase, self).setUp()
    self.contents = open_data_file('ycsb-test-run.dat')
    self.results = ycsb_stats.ParseResults(self.contents, 'histogram')

  def testCommandLineSet(self):
    self.assertEqual(
        'Command line: -db com.yahoo.ycsb.BasicDB -P workloads/workloada -t',
        self.results.command_line,
    )

  def testClientSet(self):
    self.assertEqual('YCSB Client 0.1', self.results.client)

  def testUpdateStatisticsParsed(self):
    self.assertEqual(
        ycsb_stats._OpResult(
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
            data_type=ycsb_stats.HISTOGRAM,
            data=[(0, 530), (19, 1)],
        ),
        self.results.groups['update'],
    )

  def testReadStatisticsParsed(self):
    self.assertEqual(
        ycsb_stats._OpResult(
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
            data_type=ycsb_stats.HISTOGRAM,
            data=[(0, 469)],
        ),
        self.results.groups['read'],
    )

  def testOverallStatisticsParsed(self):
    self.assertEqual(
        ycsb_stats._OpResult(
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
    self.results = ycsb_stats.ParseResults(self.contents, 'histogram')

  def testPercentilesFromHistogram_read(self):
    hist = self.results.groups['read'].data
    percentiles = ycsb_stats._PercentilesFromHistogram(hist)
    self.assertEqual(1, percentiles['p50'])
    self.assertEqual(7, percentiles['p99'])

  def testPercentilesFromHistogram_update(self):
    hist = self.results.groups['update'].data
    percentiles = ycsb_stats._PercentilesFromHistogram(hist)
    self.assertEqual(1, percentiles['p50'])
    self.assertEqual(7, percentiles['p99'])


class StatusTimeSeriesParserTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.results_1 = _parse_and_return_time_series('ycsb-time-series.dat')
    self.results_2 = _parse_and_return_time_series('ycsb-time-series-2.dat')

  def testParsedStatusTimeSeriesIsCorrect(self):
    results = _parse_and_return_time_series('ycsb-time-series.dat')
    expected = ycsb_stats._StatusResult(
        timestamp=10,
        overall_throughput=2316.0,
        op_results=[
            ycsb_stats._OpResult(
                group='read',
                statistics={
                    'Count': 2315.0,
                    'Max': 40.511,
                    'Min': 2.350,
                    'Avg': 3.45271,
                    '90': 4.131,
                    '99': 7.239,
                    '99.9': 10.175,
                    '99.99': 40.511,
                },
            )
        ],
    )

    self.assertEqual(results.status_time_series[10], expected)
    self.assertLen(results.status_time_series, 9)

  @flagsaver.flagsaver(ycsb_status=True)
  def testCombinedStatusTimeSeriesIsCorrect(self):
    results_1 = _parse_and_return_time_series('ycsb-time-series.dat')
    results_2 = _parse_and_return_time_series('ycsb-time-series-2.dat')

    combined = ycsb_stats.CombineResults(
        result_list=[results_1, results_2],
        measurement_type=ycsb_stats.TIMESERIES,
        combined_hdr={},
    )

    actual = combined.status_time_series[10]
    with self.subTest('Attributes'):
      self.assertEqual(actual.timestamp, 10)
      self.assertEqual(actual.overall_throughput, 4432.0)
    with self.subTest('Statistics'):
      self.assertEqual(actual.op_results[0].group, 'read')
      expected_statistics = {
          'Count': 4430.0,
          'Max': 52.767,
          'Min': 2.178,
          'Avg': 3.60740577,
          '90': 4.347,
          '99': 7.429,
          '99.9': 23.631,
          '99.99': 46.639,
      }
      for stat, value in expected_statistics.items():
        self.assertAlmostEqual(actual.op_results[0].statistics[stat], value)
    with self.subTest('NumItems'):
      self.assertLen(combined.status_time_series, 9)


class BadResultParserTestCase(unittest.TestCase):

  def testBadTestRun(self):
    contents = open_data_file('ycsb-test-run-3.dat')
    self.assertRaises(
        errors.Benchmarks.KnownIntermittentError,
        ycsb_stats.ParseResults,
        contents,
        'histogram',
    )

  def testErrorRate(self):
    contents = open_data_file('ycsb-test-run-4.dat')
    self.assertRaises(
        errors.Benchmarks.RunError,
        ycsb_stats.ParseResults,
        contents,
        'hdrhistogram',
        0.95,
    )


class WeightedQuantileTestCase(unittest.TestCase):

  def testEvenlyWeightedSamples(self):
    x = list(range(1, 101))  # 1-100
    weights = [1 for _ in x]
    self.assertEqual(50, ycsb_stats._WeightedQuantile(x, weights, 0.50))
    self.assertEqual(75, ycsb_stats._WeightedQuantile(x, weights, 0.75))
    self.assertEqual(90, ycsb_stats._WeightedQuantile(x, weights, 0.90))
    self.assertEqual(95, ycsb_stats._WeightedQuantile(x, weights, 0.95))
    self.assertEqual(99, ycsb_stats._WeightedQuantile(x, weights, 0.99))
    self.assertEqual(100, ycsb_stats._WeightedQuantile(x, weights, 1))

  def testLowWeight(self):
    x = [1, 4]
    weights = [99, 1]
    for i in range(100):
      self.assertEqual(1, ycsb_stats._WeightedQuantile(x, weights, i / 100.0))
    self.assertEqual(4, ycsb_stats._WeightedQuantile(x, weights, 0.995))

  def testMidWeight(self):
    x = [0, 1.2, 4]
    weights = [1, 98, 1]
    for i in range(2, 99):
      self.assertAlmostEqual(
          1.2, ycsb_stats._WeightedQuantile(x, weights, i / 100.0)
      )
    self.assertEqual(4, ycsb_stats._WeightedQuantile(x, weights, 0.995))


class ParseWorkloadTestCase(unittest.TestCase):

  def testParsesEmptyString(self):
    self.assertDictEqual({}, ycsb.ParseWorkload(''))

  def testIgnoresComment(self):
    self.assertDictEqual({}, ycsb.ParseWorkload('#\n'))
    self.assertDictEqual(
        {}, ycsb.ParseWorkload('#recordcount = 10\n# columnfamily=cf')
    )
    self.assertDictEqual(
        {'recordcount': '10'},
        ycsb.ParseWorkload('#Sample!\nrecordcount = 10'),
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
    r1 = ycsb_stats.YcsbResult(
        groups={
            'read': ycsb_stats._OpResult(
                group='read',
                statistics={'Operations': 100, 'Return=0': 100},
                data_type=ycsb_stats.HISTOGRAM,
            )
        }
    )
    r2 = ycsb_stats.YcsbResult(
        groups={
            'read': ycsb_stats._OpResult(
                group='read',
                statistics={'Operations': 96, 'Return=0': 94, 'Return=-1': 2},
                data_type=ycsb_stats.HISTOGRAM,
            ),
            'update': ycsb_stats._OpResult(
                group='update',
                statistics={'Operations': 100, 'AverageLatency(ms)': 25},
                data_type=ycsb_stats.HISTOGRAM,
            ),
        }
    )
    combined = ycsb_stats.CombineResults([r1, r2], 'histogram', {})
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
    r = ycsb_stats.YcsbResult(
        client='',
        command_line='',
        groups={
            'read': ycsb_stats._OpResult(
                group='read',
                statistics={'AverageLatency(ms)': 21},
                data_type=ycsb_stats.HISTOGRAM,
            )
        },
    )

    r_copy = copy.deepcopy(r)
    self.assertEqual(r, r_copy)
    combined = ycsb_stats.CombineResults([r], 'histogram', {})
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
    actual = ycsb_stats.ParseHdrLogFile(rawlog)
    expected = [
        (0.0, 0.314, 2),
        (10.0, 0.853, 49953),
        (20.0, 0.949, 50396),
        (30.0, 1.033, 49759),
    ]
    self.assertEqual(actual, expected)


class YcsbResultTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.fs = fake_filesystem.FakeFilesystem()
    self.fs.create_dir(TEST_DIR)
    self.fake_open = fake_filesystem.FakeFileOpen(self.fs)

  def testWriteStatusTimeSeriesToFile(self):
    result = ycsb_stats.YcsbResult(
        status_time_series={
            2: ycsb_stats._StatusResult(
                timestamp=2,
                overall_throughput=2920.0,
                op_results=[
                    ycsb_stats._OpResult(
                        group='read',
                        data_type='',
                        data=[],
                        statistics={
                            'Count': 2993.0,
                            'Max': 346111.0,
                            'Min': 5804.0,
                            'Avg': 96.36988477781489,
                            '90': 244.555,
                            '99': 273.94100000000003,
                            '99.9': 288.693,
                            '99.99': 308.661,
                        },
                    )
                ],
            ),
            3: ycsb_stats._StatusResult(
                timestamp=3,
                overall_throughput=24279.0,
                op_results=[
                    ycsb_stats._OpResult(
                        group='read',
                        data_type='',
                        data=[],
                        statistics={
                            'Count': 24211.0,
                            'Max': 463359.0,
                            'Min': 2824.0,
                            'Avg': 23.845614725951016,
                            '90': 72.162,
                            '99': 148.974,
                            '99.9': 193.701,
                            '99.99': 263.192,
                        },
                    )
                ],
            ),
        }
    )
    self.enter_context(
        mock.patch.object(vm_util, 'GetTempDir', return_value=TEST_DIR)
    )
    self.enter_context(
        mock.patch.object(ycsb_stats, 'open', side_effect=self.fake_open)
    )

    result.WriteStatusTimeSeriesToFile()

    expected_content = (
        'time,90,99,99.9,99.99,Avg,Count,Max,Min\r\n'
        '2,244.555,273.94100000000003,288.693,308.661,96.36988477781489,2993.0,346111.0,5804.0\r\n'
        '3,72.162,148.974,193.701,263.192,23.845614725951016,24211.0,463359.0,2824.0\r\n'
    )
    self.assertEqual(
        self.fs.get_object(TEST_DIR + '/ycsb_status_output_read.csv').contents,
        expected_content,
    )


class OpResultTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testFromStatusLine(self):
    sample_line = (
        '2023-06-05 16:15:54:654 10 sec: 15407 operations; 666.67 current'
        ' ops/sec; est completion in 18 hours 3 minutes [READ: Count=12,'
        ' Max=5607, Min=2880, Avg=4079.83, 90=5291, 99=5607, 99.9=5607,'
        ' 99.99=5607] [CLEANUP: Count=16, Max=12823, Min=0, Avg=987.19, 90=916,'
        ' 99=12823, 99.9=12823, 99.99=12823]'
    )

    actual = list(ycsb_stats._ParseStatusLine(sample_line))
    expected = [
        ycsb_stats._OpResult(
            group='read',
            statistics={
                '90': 5.291,
                '99': 5.607,
                '99.9': 5.607,
                '99.99': 5.607,
                'Avg': 4.07983,
                'Count': 12.0,
                'Max': 5.607,
                'Min': 2.880,
            },
        ),
        ycsb_stats._OpResult(
            group='cleanup',
            statistics={
                '90': 0.916,
                '99': 12.823,
                '99.9': 12.823,
                '99.99': 12.823,
                'Avg': 0.98719,
                'Count': 16.0,
                'Max': 12.823,
                'Min': 0.0,
            },
        ),
    ]

    self.assertEqual(actual, expected)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
