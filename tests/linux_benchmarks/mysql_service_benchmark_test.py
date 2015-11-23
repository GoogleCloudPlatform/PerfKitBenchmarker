# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for mysql_service_benchmark."""
import logging
import os
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mysql_service_benchmark


class MySQLServiceBenchmarkTestCase(unittest.TestCase,
                                    test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'sysbench-output-sample.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseSysbenchResult(self):
    results = []
    metadata = {}
    mysql_service_benchmark.ParseSysbenchOutput(
        self.contents, results, metadata)
    logging.info('results are, %s', results)
    expected_results = [
        sample.Sample('sysbench tps p1', 526.38, 'NA', {}),
        sample.Sample('sysbench tps p5', 526.38, 'NA', {}),
        sample.Sample('sysbench tps p50', 579.5, 'NA', {}),
        sample.Sample('sysbench tps p90', 636.0, 'NA', {}),
        sample.Sample('sysbench tps p99', 636.0, 'NA', {}),
        sample.Sample('sysbench tps p99.9', 636.0, 'NA', {}),
        sample.Sample('sysbench tps average', 583.61, 'NA', {}),
        sample.Sample('sysbench tps stddev', 33.639045340624214, 'NA', {}),
        sample.Sample('sysbench tps cv', 0.05763959723209714, 'NA', {}),
        sample.Sample('sysbench latency min', 18.31, 'milliseconds', {}),
        sample.Sample('sysbench latency avg', 27.26, 'milliseconds', {}),
        sample.Sample('sysbench latency max', 313.5, 'milliseconds', {}),
        sample.Sample(
            'sysbench latency percentile 99', 57.15,
            'milliseconds', {})]
    self.assertSampleListsEqualUpToTimestamp(results, expected_results)


if __name__ == '__main__':
  unittest.main()
