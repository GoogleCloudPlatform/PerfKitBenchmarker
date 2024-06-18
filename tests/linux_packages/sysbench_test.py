"""Tests for sysbench."""

import os
import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_packages import sysbench
from tests import pkb_common_test_case


class SysbenchTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(sysbench_ignore_concurrent_modification=True)
  def testInstallIgnoresConcurrentMods(self):
    vm = mock.Mock()
    sysbench._Install(vm)
    self.assertIn('P0001', vm.RemoteCommand.call_args_list[2][0][0])


class MySQLServiceBenchmarkTestCase(
    unittest.TestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'sysbench-output-sample.txt'
    )
    with open(path) as fp:
      self.contents = fp.read()

  def testParseSysbenchResult(self):
    metadata = {}
    results = sysbench.ParseSysbenchTimeSeries(
        self.contents, metadata
    )
    expected_results = [
        sample.Sample(
            'tps_array',
            -1,
            'tps',
            {
                'tps': [
                    1012.86,
                    1006.64,
                    1022.3,
                    1016.16,
                    1009.03,
                    1016.99,
                    1010.0,
                    1018.0,
                    1002.01,
                    998.49,
                    959.52,
                    913.49,
                    936.98,
                    916.01,
                    957.96,
                ]
            },
        ),
        sample.Sample(
            'latency_array',
            -1,
            'ms',
            {
                'latency': [
                    28.67,
                    64.47,
                    38.94,
                    44.98,
                    89.16,
                    29.72,
                    106.75,
                    46.63,
                    116.8,
                    41.85,
                    27.17,
                    104.84,
                    58.92,
                    75.82,
                    73.13,
                ]
            },
        ),
        sample.Sample(
            'qps_array',
            -1,
            'qps',
            {
                'qps': [
                    20333.18,
                    20156.38,
                    20448.49,
                    20334.15,
                    20194.07,
                    20331.31,
                    20207.00,
                    20348.96,
                    20047.11,
                    19972.86,
                    19203.97,
                    18221.83,
                    18689.14,
                    18409.68,
                    19155.63,
                ]
            },
        ),
    ]
    self.assertSampleListsEqualUpToTimestamp(results, expected_results)

if __name__ == '__main__':
  unittest.main()
