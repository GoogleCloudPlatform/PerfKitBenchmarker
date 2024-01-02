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
"""Tests for sysbench_memory."""
import logging
import os
import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import sysbench_memory_benchmark


class SysBenchMemoryBenchmarkTestCase(
    unittest.TestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super(SysBenchMemoryBenchmarkTestCase, self).setUp()
    path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'data',
        'sysbench-memory-output-sample.txt',
    )
    with open(path) as fp:
      self.contents = fp.read()

  def testParseSysbenchMemoryResult(self):
    metadata = {}
    results = sysbench_memory_benchmark.GenerateMetricsForSysbenchMemoryOutput(
        self.contents, metadata
    )
    logging.info('results are, %s', results)
    expected_results = [
        sample.Sample('Minimum_throughput', 3803.18, 'MiB/sec', {}),
        sample.Sample('Maximum_throughput', 5353.06, 'MiB/sec', {}),
        sample.Sample('Average_throughput', 4577.78, 'MiB/sec', {}),
        sample.Sample('vcpu_variation', 774.26, 'MiB/sec', {}),
        sample.Sample('Range_throughput', 1549.88, 'MiB/sec', {}),
        sample.Sample('Difference_from_all_max', 3101.12, 'MiB/sec', {}),
        sample.Sample('Difference_from_all_median', -1.36, 'MiB/sec', {}),
    ]
    self.assertSampleListsEqualUpToTimestamp(results, expected_results)


if __name__ == '__main__':
  unittest.main()
