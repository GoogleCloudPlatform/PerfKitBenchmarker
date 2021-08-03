# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for mlperf_benchmark."""
import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mlperf_benchmark
from perfkitbenchmarker.sample import Sample
from tests import pkb_common_test_case


class MlperfBenchmarkTestCase(pkb_common_test_case.PkbCommonTestCase,
                              test_util.SamplesTestMixin):

  def setUp(self):
    super(MlperfBenchmarkTestCase, self).setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'mlperf_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testTrainResults(self):
    samples = mlperf_benchmark.MakeSamplesFromOutput({'version': 'v1.0'},
                                                     self.contents,
                                                     use_tpu=False,
                                                     model='resnet')
    golden = Sample(metric='speed', value=17651.66, unit='samples/sec',
                    metadata={'version': 'v1.0'})
    self.assertSamplesEqualUpToTimestamp(golden, samples[0])


if __name__ == '__main__':
  unittest.main()
