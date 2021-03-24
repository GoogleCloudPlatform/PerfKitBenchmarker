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
import mock

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mlperf_multiworkers_benchmark
from perfkitbenchmarker.sample import Sample


class MlperfMultiworkersBenchmarkTestCase(
    unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super(MlperfMultiworkersBenchmarkTestCase, self).setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'mlperf_multiworkers_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  @mock.patch('time.time', mock.MagicMock(return_value=1550279509.59))
  def testTrainResults(self):
    samples = mlperf_multiworkers_benchmark.MakeSamplesFromOutput(
        {'version': 'v0.6.0'}, self.contents, model='transformer')
    golden = [
        Sample('Eval Accuracy', 2150.2649784088135, '%',
               {'epoch': 1, 'times': 0.0, 'version': 'v0.6.0'}),
        Sample('Eval Accuracy', 2388.354390859604, '%',
               {'epoch': 2, 'times': 674.6299998760223, 'version': 'v0.6.0'}),
        Sample('Eval Accuracy', 2573.164999485016, '%',
               {'epoch': 3, 'times': 1362.2619998455048, 'version': 'v0.6.0'}),
        Sample('Time', 2087, 'seconds', {'version': 'v0.6.0'})
    ]
    self.assertEqual(samples, golden)


if __name__ == '__main__':
  unittest.main()
