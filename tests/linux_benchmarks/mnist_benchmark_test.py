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
"""Tests for mnist_benchmark."""
import os
import unittest
import mock

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark
from perfkitbenchmarker.sample import Sample


class MnistBenchmarkTestCase(unittest.TestCase,
                             test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'mnist_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

    self.metadata_input = {'num_examples_per_epoch': 1251.1}
    self.metadata_output = {'num_examples_per_epoch': 1251.1, 'step': 2000,
                            'elapsed_seconds': 0, 'epoch': 1.5985932379506036}

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testTrainResults(self):
    samples = mnist_benchmark.MakeSamplesFromTrainOutput(
        self.metadata_input, self.contents, 0)
    golden = [
        Sample('Loss', 0.09562386, '', self.metadata_output),
        Sample('Global Steps Per Second', 217.69966666666664,
               'global_steps/sec', self.metadata_output),
        Sample('Examples Per Second', 222924.33333333334,
               'examples/sec', self.metadata_output)
    ]
    self.assertEqual(samples, golden)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testEvalResults(self):
    samples = mnist_benchmark.MakeSamplesFromEvalOutput(
        self.metadata_input, self.contents, 0)
    golden = [
        Sample('Eval Loss', 0.03615343, '', self.metadata_output),
        Sample('Accuracy', 98.77387, '%', self.metadata_output)
    ]
    self.assertEqual(samples, golden)

if __name__ == '__main__':
  unittest.main()
