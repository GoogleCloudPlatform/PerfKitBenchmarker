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
"""Tests for multiworker mlperf_benchmark."""
import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mlperf_benchmark
from perfkitbenchmarker.linux_benchmarks import mlperf_multiworkers_benchmark
from perfkitbenchmarker.sample import Sample
from tests import pkb_common_test_case


class MlperfMultiworkersBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super().setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'mlperf_multiworkers_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testTrainResults(self):
    samples = mlperf_multiworkers_benchmark.MakeSamplesFromOutput(
        {'version': mlperf_benchmark.MLPERF_VERSION},
        self.contents, model='transformer')
    golden = [
        Sample('speed', 196673.0, 'samples/sec', {'version': 'v1.0'}),
        Sample('speed', 203225.0, 'samples/sec', {'version': 'v1.0'}),
        Sample('speed', 198987.0, 'samples/sec', {'version': 'v1.0'}),
        Sample('Time', 2087, 'seconds', {'version': 'v1.0'})
    ]
    print(samples)
    self.assertSampleListsEqualUpToTimestamp(golden, samples)


if __name__ == '__main__':
  unittest.main()
