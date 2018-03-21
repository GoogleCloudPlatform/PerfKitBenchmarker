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

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mnist_benchmark


class MnistBenchmarkTestCase(unittest.TestCase,
                             test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'mnist_output.txt')
    with open(path) as fp:
      self.contents = fp.read()
    self.metadata = {}

  def testExtractThroughput(self):
    metric = 'metric'
    unit = 'unit'
    with mock.patch('time.time') as foo:
      regex = r'global_step/sec: (\S+)'
      foo.return_value = 0
      samples = mnist_benchmark._ExtractThroughput(regex, self.contents,
                                                   self.metadata, metric, unit)
      golden = [sample.Sample(metric, 74.8278, unit, {'index': 0}),
                sample.Sample(metric, 28.9749, unit, {'index': 1}),
                sample.Sample(metric, 20.8336, unit, {'index': 2}),
                sample.Sample(metric, 31.2365, unit, {'index': 3}),
                sample.Sample(metric, 31.6943, unit, {'index': 4}),
                sample.Sample(metric, 39.1528, unit, {'index': 5})]
    self.assertEqual(samples, golden)


if __name__ == '__main__':
  unittest.main()
