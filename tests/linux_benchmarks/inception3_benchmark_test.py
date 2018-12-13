# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for inception3_benchmark."""
import os
import unittest
import mock

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark
from perfkitbenchmarker.sample import Sample


class Inception3BenchmarkTestCase(unittest.TestCase,
                                  test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'inception3_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

    self.metadata_input = {'num_examples_per_epoch': 1251.1,
                           'train_batch_size': 1024}
    self.metadata_output = {'num_examples_per_epoch': 1251.1,
                            'train_batch_size': 1024, 'step': 4000,
                            'epoch': 3.197186475901207, 'elapsed_seconds': 0}

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testTrainResults(self):
    samples = mnist_benchmark.MakeSamplesFromTrainOutput(
        self.metadata_input, self.contents, 0)
    golden = [
        Sample('Loss', 5.7193503, '', self.metadata_output),
        Sample('Global Steps Per Second', 1.4384171428571428,
               'global_steps/sec', self.metadata_output),
        Sample('Examples Per Second', 1472.9414285714283,
               'examples/sec', self.metadata_output)
    ]
    self.assertEqual(samples, golden)


if __name__ == '__main__':
  unittest.main()
