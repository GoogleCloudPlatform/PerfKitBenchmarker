# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for sysbench."""
import logging
import os
import unittest
from unittest import mock
from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import sysbench_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class MySQLServiceBenchmarkTestCase(unittest.TestCase,
                                    test_util.SamplesTestMixin):

  def setUp(self):
    super().setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'sysbench-output-sample.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testParseSysbenchResult(self):
    results = []
    metadata = {}
    results = sysbench_benchmark._ParseSysbenchTimeSeries(
        self.contents, metadata)
    logging.info('results are, %s', results)
    expected_results = [
        sample.Sample('tps_array', -1, 'tps', {'tps': [
            1012.86, 1006.64, 1022.3, 1016.16, 1009.03, 1016.99, 1010.0, 1018.0,
            1002.01, 998.49, 959.52, 913.49, 936.98, 916.01, 957.96]}),
        sample.Sample('latency_array', -1, 'ms', {'latency': [
            28.67, 64.47, 38.94, 44.98, 89.16, 29.72, 106.75, 46.63, 116.8,
            41.85, 27.17, 104.84, 58.92, 75.82, 73.13]}),
        sample.Sample('qps_array', -1, 'qps', {'qps': [
            20333.18, 20156.38, 20448.49, 20334.15, 20194.07, 20331.31,
            20207.00, 20348.96, 20047.11, 19972.86, 19203.97, 18221.83,
            18689.14, 18409.68, 19155.63]})]
    self.assertSampleListsEqualUpToTimestamp(results, expected_results)


class SpannerBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            sysbench_benchmark, '_GetCommonSysbenchOptions', return_value=[]
        )
    )
    FLAGS.sysbench_testname = sysbench_benchmark.SPANNER_TPCC

  def testSpannerLoadCommandHasCorrectTest(self):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(mock.Mock(), 1, 0)
    self.assertIn('sysbench tpcc', command)

  @parameterized.named_parameters([
      {
          'testcase_name': 'SingleVM',
          'num_vms': 1,
          'expected_cluster': 0,
      },
      {
          'testcase_name': 'MultiVM',
          'num_vms': 2,
          'expected_cluster': 1,
      },
  ])
  def testSpannerLoadCommandHasCorrectClusterSetting(
      self, num_vms, expected_cluster
  ):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(
        mock.Mock(), num_vms, 0
    )
    self.assertIn(f'--enable_cluster={expected_cluster}', command)

  @flagsaver.flagsaver(sysbench_scale=1000)
  def testSpannerLoadCommandHasCorrectScales(self):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(mock.Mock(), 1, 0)
    with self.subTest('scale'):
      self.assertIn('--scale=1000', command)
    with self.subTest('start_scale'):
      self.assertIn('--start_scale=1', command)
    with self.subTest('end_scale'):
      self.assertIn('--end_scale=1000', command)

  @flagsaver.flagsaver(sysbench_scale=1000)
  def testSpannerLoadCommandHasCorrectScalesMultiVM(self):
    command = sysbench_benchmark._GetSysbenchPrepareCommand(mock.Mock(), 5, 2)
    with self.subTest('scale'):
      self.assertIn('--scale=1000', command)
    with self.subTest('start_scale'):
      self.assertIn('--start_scale=401', command)
    with self.subTest('end_scale'):
      self.assertIn('--end_scale=600', command)

  def testSpannerRunCommandHasCorrectTest(self):
    command = sysbench_benchmark._GetSysbenchRunCommand(1, mock.Mock(), 1)
    self.assertIn('sysbench tpcc', command)

  def testSpannerMetadataHasForeignKey(self):
    metadata = sysbench_benchmark.CreateMetadataFromFlags()
    self.assertIn('sysbench_use_fk', metadata)


if __name__ == '__main__':
  unittest.main()
