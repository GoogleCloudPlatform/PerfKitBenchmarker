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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import os
import unittest


from perfkitbenchmarker.linux_packages import ycsb
import six
from six.moves import range


def open_data_file(filename):
  path = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
  with open(path) as fp:
    return fp.read()


class SimpleResultParserTestCase(unittest.TestCase):
  maxDiff = None

  def setUp(self):
    super(SimpleResultParserTestCase, self).setUp()
    self.contents = open_data_file('ycsb-test-run.dat')
    self.results = ycsb.ParseResults(self.contents, 'histogram')

  def testCommandLineSet(self):
    self.assertEqual('Command line: -db com.yahoo.ycsb.BasicDB '
                     '-P workloads/workloada -t', self.results['command_line'])

  def testClientSet(self):
    self.assertEqual('YCSB Client 0.1', self.results['client'])

  def testUpdateStatisticsParsed(self):
    self.assertDictEqual(
        {
            'group': 'update',
            'statistics': {
                'Operations': 531,
                'Return=0': 531,
                'AverageLatency(ms)': .0659774011299435,
                'MinLatency(ms)': 0.042,
                'MaxLatency(ms)': .345,
                '95thPercentileLatency(ms)': 0,
                '99thPercentileLatency(ms)': 0
            },
            'histogram': [(0, 530), (19, 1)],
        },
        dict(self.results['groups']['update']))

  def testReadStatisticsParsed(self):
    self.assertDictEqual(
        {
            'group': 'read',
            'statistics': {
                'Operations': 469,
                'Return=0': 469,
                'AverageLatency(ms)': 0.03847761194029851,
                'MinLatency(ms)': 0.034,
                'MaxLatency(ms)': 0.102,
                '95thPercentileLatency(ms)': 0,
                '99thPercentileLatency(ms)': 0
            },
            'histogram': [(0, 469)],
        },
        dict(self.results['groups']['read']))

  def testOverallStatisticsParsed(self):
    self.assertDictEqual(
        {
            'statistics': {
                'RunTime(ms)': 80.0,
                'Throughput(ops/sec)': 12500.0
            },
            'group': 'overall',
            'histogram': []
        },
        self.results['groups']['overall'])


class DetailedResultParserTestCase(unittest.TestCase):

  def setUp(self):
    super(DetailedResultParserTestCase, self).setUp()
    self.contents = open_data_file('ycsb-test-run-2.dat')
    self.results = ycsb.ParseResults(self.contents, 'histogram')

  def testPercentilesFromHistogram_read(self):
    hist = self.results['groups']['read']['histogram']
    percentiles = ycsb._PercentilesFromHistogram(hist)
    self.assertEqual(1, percentiles['p50'])
    self.assertEqual(7, percentiles['p99'])

  def testPercentilesFromHistogram_update(self):
    hist = self.results['groups']['update']['histogram']
    percentiles = ycsb._PercentilesFromHistogram(hist)
    self.assertEqual(1, percentiles['p50'])
    self.assertEqual(7, percentiles['p99'])


class BadResultParserTestCase(unittest.TestCase):

  def testBadTestRun(self):
    contents = open_data_file('ycsb-test-run-3.dat')
    self.assertRaises(IOError, ycsb.ParseResults, contents, 'histogram')


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
    self.assertDictEqual({}, ycsb._ParseWorkload(''))

  def testIgnoresComment(self):
    self.assertDictEqual({}, ycsb._ParseWorkload('#\n'))
    self.assertDictEqual({},
                         ycsb._ParseWorkload('#recordcount = 10\n'
                                             '# columnfamily=cf'))
    self.assertDictEqual({'recordcount': '10'},
                         ycsb._ParseWorkload('#Sample!\nrecordcount = 10'))

  def testParsesSampleWorkload(self):
    contents = open_data_file('ycsb_workloada')
    actual = ycsb._ParseWorkload(contents)

    expected = {
        'recordcount': '1000',
        'operationcount': '1000',
        'workload': 'com.yahoo.ycsb.workloads.CoreWorkload',
        'readallfields': 'true',
        'readproportion': '0.5',
        'updateproportion': '0.5',
        'scanproportion': '0',
        'insertproportion': '0',
        'requestdistribution': 'zipfian'
    }

    self.assertDictEqual(expected, actual)


class CombineResultsTestCase(unittest.TestCase):

  def testGroupMissing(self):
    r1 = {
        'client': '',
        'command_line': '',
        'groups': {
            'read': {
                'group': 'read',
                'statistics': {'Operations': 100,
                               'Return=0': 100},
                'histogram': []
            }
        }
    }
    r2 = {
        'client': '',
        'command_line': '',
        'groups': {
            'read': {
                'group': 'read',
                'statistics': {'Operations': 96, 'Return=0': 94,
                               'Return=-1': 2},
                'histogram': []
            },
            'update': {
                'group': 'update',
                'statistics': {'Operations': 100,
                               'AverageLatency(ms)': 25},
                'histogram': []
            }
        }
    }
    combined = ycsb._CombineResults([r1, r2], 'histogram', {})
    six.assertCountEqual(self, ['read', 'update'], combined['groups'])
    six.assertCountEqual(self, ['Operations', 'Return=0', 'Return=-1'],
                         combined['groups']['read']['statistics'])
    read_stats = combined['groups']['read']['statistics']
    self.assertEqual({'Operations': 196, 'Return=0': 194, 'Return=-1': 2},
                     read_stats)

  def testDropUnaggregatedFromSingleResult(self):
    r = {
        'client': '',
        'command_line': '',
        'groups': {
            'read': {
                'group': 'read',
                'statistics': {'AverageLatency(ms)': 21},
                'histogram': []
            }
        }
    }

    r_copy = copy.deepcopy(r)
    self.assertEqual(r, r_copy)
    combined = ycsb._CombineResults([r], 'histogram', {})
    self.assertEqual(r, r_copy)
    r['groups']['read']['statistics'] = {}
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
    expected = [(0.0, 0.314, 2), (10.0, 0.853, 49953),
                (20.0, 0.949, 50396), (30.0, 1.033, 49759)]
    self.assertEqual(actual, expected)


if __name__ == '__main__':
  unittest.main()
