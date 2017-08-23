# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for GPU PCIe bandwidth benchmark."""
import os
import unittest

import mock

from perfkitbenchmarker.linux_benchmarks import gpu_pcie_bandwidth_benchmark


class GpuBandwidthTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(gpu_pcie_bandwidth_benchmark.__name__ + '.FLAGS')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__), '../data',
                        'cuda_bandwidth_test_results.txt')
    with open(path) as fp:
      self.test_output = fp.read()

    path = os.path.join(os.path.dirname(__file__), '../data',
                        'cuda_bandwidth_test_range_results.txt')
    with open(path) as fp:
      self.range_test_output = fp.read()

  def testParseDeviceMetadata(self):
    actual = gpu_pcie_bandwidth_benchmark.\
        _ParseDeviceInfo(self.test_output)
    expected = {'0': 'Tesla K80', '1': 'Tesla K80'}
    self.assertEqual(expected, actual)

  def testParseCudaBandwidthTestResults(self):
    results = gpu_pcie_bandwidth_benchmark.\
        _ParseOutputFromSingleIteration(self.test_output)
    self.assertEqual(3, len(results))
    self.assertAlmostEqual(9254.7, results['Host to device bandwidth'])
    self.assertAlmostEqual(9686.1, results['Device to host bandwidth'])
    self.assertAlmostEqual(155985.8, results['Device to device bandwidth'])

  def testParseCudaBandwidthTestRangeResults(self):
    results = gpu_pcie_bandwidth_benchmark.\
        _ParseOutputFromSingleIteration(self.range_test_output)
    self.assertEqual(3, len(results))
    self.assertAlmostEqual(8063.3666667,
                           results['Host to device bandwidth'])
    self.assertAlmostEqual(10518.7666667,
                           results['Device to host bandwidth'])
    self.assertAlmostEqual(157524.4333333,
                           results['Device to device bandwidth'])

  def testCalculateMetrics(self):
    raw_results = [{
        'Host to device bandwidth': 9250,
        'Device to host bandwidth': 9000,
        'Device to device bandwidth': 155000
    }, {
        'Host to device bandwidth': 8000,
        'Device to host bandwidth': 8500,
        'Device to device bandwidth': 152000
    }]
    samples = gpu_pcie_bandwidth_benchmark.\
        _CalculateMetricsOverAllIterations(raw_results)
    metrics = {i[0]: i[1] for i in samples}

    sample = next(x for x in samples if x.metadata == {'iteration': 0}
                  and x.metric == 'Host to device bandwidth')
    self.assertAlmostEqual(9250, sample.value)

    sample = next(x for x in samples if x.metadata == {'iteration': 0}
                  and x.metric == 'Device to host bandwidth')
    self.assertAlmostEqual(9000, sample.value)

    sample = next(x for x in samples if x.metadata == {'iteration': 0}
                  and x.metric == 'Device to device bandwidth')
    self.assertAlmostEqual(155000, sample.value)

    sample = next(x for x in samples if x.metadata == {'iteration': 1}
                  and x.metric == 'Host to device bandwidth')
    self.assertAlmostEqual(8000, sample.value)

    sample = next(x for x in samples if x.metadata == {'iteration': 1}
                  and x.metric == 'Device to host bandwidth')
    self.assertAlmostEqual(8500, sample.value)

    sample = next(x for x in samples if x.metadata == {'iteration': 1}
                  and x.metric == 'Device to device bandwidth')
    self.assertAlmostEqual(152000, sample.value)

    self.assertAlmostEqual(8000, metrics['Host to device bandwidth, min'])
    self.assertAlmostEqual(9250, metrics['Host to device bandwidth, max'])
    self.assertAlmostEqual(8625.0, metrics['Host to device bandwidth, mean'])
    self.assertAlmostEqual(625.0, metrics['Host to device bandwidth, stddev'])

    self.assertAlmostEqual(8500, metrics['Device to host bandwidth, min'])
    self.assertAlmostEqual(9000, metrics['Device to host bandwidth, max'])
    self.assertAlmostEqual(8750.0, metrics['Device to host bandwidth, mean'])
    self.assertAlmostEqual(250.0, metrics['Device to host bandwidth, stddev'])

    self.assertAlmostEqual(152000, metrics['Device to device bandwidth, min'])
    self.assertAlmostEqual(155000, metrics['Device to device bandwidth, max'])
    self.assertAlmostEqual(153500, metrics['Device to device bandwidth, mean'])
    self.assertAlmostEqual(1500, metrics['Device to device bandwidth, stddev'])


if __name__ == '__main__':
  unittest.main()
