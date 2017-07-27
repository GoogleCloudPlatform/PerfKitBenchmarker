# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for Stencil2D benchmark."""
import os
import unittest

import mock
from mock import ANY
from mock import call

from perfkitbenchmarker import flag_util
from perfkitbenchmarker.linux_benchmarks import stencil2d_benchmark


class Stencil2DBenchmarkTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(stencil2d_benchmark.__name__ + '.FLAGS')
    p.start()
    self.addCleanup(p.stop)

    path = os.path.join(os.path.dirname(__file__), '../data',
                        'stencil2d_output.txt')
    with open(path) as fp:
      self.test_output = fp.read()

  def testMakeSampleFromOutput(self):
    testMetadata = {'foo': 'bar'}
    actual = stencil2d_benchmark._MakeSamplesFromStencilOutput(
        self.test_output, testMetadata)
    results_dict = {x.metric: x for x in actual}

    stencil_dp_median_results = results_dict['Stencil2D DP median']
    self.assertEqual('Stencil2D DP median', stencil_dp_median_results.metric)
    self.assertEqual(474.761, stencil_dp_median_results.value)
    self.assertEqual('GFLOPS', stencil_dp_median_results.unit)
    self.assertEqual(testMetadata, stencil_dp_median_results.metadata)

    stencil_sp_median_results = results_dict['Stencil2D SP median']
    self.assertEqual('Stencil2D SP median', stencil_sp_median_results.metric)
    self.assertEqual(753.795, stencil_sp_median_results.value)
    self.assertEqual('GFLOPS', stencil_sp_median_results.unit)
    self.assertEqual(testMetadata, stencil_sp_median_results.metadata)

    stencil_dp_stddev_results = results_dict['Stencil2D DP stddev']
    self.assertEqual('Stencil2D DP stddev', stencil_dp_stddev_results.metric)
    self.assertEqual(2.51807, stencil_dp_stddev_results.value)
    self.assertEqual('GFLOPS', stencil_dp_stddev_results.unit)
    self.assertEqual(testMetadata, stencil_dp_stddev_results.metadata)

    stencil_sp_stddev_results = results_dict['Stencil2D SP stddev']
    self.assertEqual('Stencil2D SP stddev', stencil_sp_stddev_results.metric)
    self.assertEqual(9.13922, stencil_sp_stddev_results.value)
    self.assertEqual('GFLOPS', stencil_sp_stddev_results.unit)
    self.assertEqual(testMetadata, stencil_sp_stddev_results.metadata)

  @mock.patch(('perfkitbenchmarker.linux_packages.'
               'cuda_toolkit_8.GetGpuType'))
  @mock.patch(('perfkitbenchmarker.linux_packages.'
               'cuda_toolkit_8.GetDriverVersion'))
  @mock.patch(('perfkitbenchmarker.linux_packages.'
               'cuda_toolkit_8.QueryNumberOfGpus'))
  @mock.patch(('perfkitbenchmarker.linux_packages.'
               'cuda_toolkit_8.QueryGpuClockSpeed'))
  @mock.patch(('perfkitbenchmarker.linux_packages.'
               'cuda_toolkit_8.QueryAutoboostPolicy'))
  @mock.patch(('perfkitbenchmarker.linux_benchmarks.'
               'stencil2d_benchmark._RunSingleIteration'))
  def testRun(self,
              run_single_iteration_mock,
              query_autoboost_policy_mock,
              query_gpu_clock_speed_mock,
              query_number_of_gpus_mock,
              get_driver_version_mock,
              get_gpu_type):
    get_gpu_type.return_value = 'k80'
    get_driver_version_mock.return_value = '123.45'
    query_number_of_gpus_mock.return_value = 8
    query_gpu_clock_speed_mock.return_value = [100, 200]
    query_autoboost_policy_mock.return_value = {
        'autoboost': True,
        'autoboost_default': True,
    }
    benchmark_spec = mock.MagicMock()
    problem_sizes = [2, 3, 4]
    stencil2d_benchmark.FLAGS.stencil2d_problem_sizes = (
        flag_util.IntegerList(problem_sizes))

    expected_calls = [call(ANY, size, ANY, ANY, ANY) for size in problem_sizes]

    stencil2d_benchmark.Run(benchmark_spec)
    run_single_iteration_mock.assert_has_calls(expected_calls, any_order=True)


if __name__ == '__main__':
  unittest.main()
