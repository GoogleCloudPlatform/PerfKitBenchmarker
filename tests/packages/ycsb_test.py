# Copyright 2014 Google Inc. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.ycsb"""

import os
import unittest


from perfkitbenchmarker.packages import ycsb


class SimpleResultParserTestCase(unittest.TestCase):
  maxDiff = None

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'ycsb-test-run.dat')
    with open(path) as fp:
      self.contents = fp.read()
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
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'ycsb-test-run-2.dat')
    with open(path) as fp:
      self.contents = fp.read()
    self.results = ycsb.ParseResults(self.contents)

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

  def testPercentilesFromHistogram_cleanup(self):
    hist = self.results['groups']['cleanup']['histogram']
    percentiles = ycsb._PercentilesFromHistogram(hist)
    self.assertEqual(0, percentiles['p50'])
    self.assertEqual(385, percentiles['p99'])


class WeightedQuantileTestCase(unittest.TestCase):

  def testEvenlyWeightedSamples(self):
    x = range(1, 101)  # 1-100
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
    for i in xrange(100):
      self.assertEqual(1, ycsb._WeightedQuantile(x, weights, i / 100.0))
    self.assertEqual(4, ycsb._WeightedQuantile(x, weights, 0.995))

  def testMidWeight(self):
    x = [0, 1.2, 4]
    weights = [1, 98, 1]
    for i in xrange(2, 99):
      self.assertAlmostEqual(1.2, ycsb._WeightedQuantile(x, weights, i / 100.0))
    self.assertEqual(4, ycsb._WeightedQuantile(x, weights, 0.995))


class _ParseWorkloadTestCase(unittest.TestCase):

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
    test_file_path = os.path.join(os.path.dirname(__file__), '..', 'data',
                                  'ycsb_workloada')

    with open(test_file_path) as fp:
      contents = fp.read()

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
