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
"""Tests for resnet_benchmark."""
import os
import unittest
import mock

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import resnet_benchmark


class ResNetBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'resnet_output.txt')
    with open(path) as fp:
      self.contents = fp.read()
    self.metadata = {}

  def testExtractThroughput(self):
    with mock.patch('time.time') as foo:
      foo.return_value = 0
      samples = resnet_benchmark._MakeSamplesFromOutput(self.metadata,
                                                        self.contents)
      golden = [
          sample.Sample('Loss', 6.673467, '',
                        {'duration': 0.0, 'step': 0}),
          sample.Sample('Global Steps Per Second', 0.0, 'global_steps/sec',
                        {'duration': 0.0, 'step': 0}),
          sample.Sample('Loss', 4.0741725, '',
                        {'duration': 794.456, 'step': 2502}),
          sample.Sample('Global Steps Per Second', 3.14932, 'global_steps/sec',
                        {'duration': 794.456, 'step': 2502}),
          sample.Sample('Examples Per Second', 3224.91, 'examples/sec',
                        {'duration': 794.456, 'step': 2502}),
          sample.Sample('Final Loss', 3.6904116, '', {}),
          sample.Sample('Eval Loss', 3.7262352, '', {}),
          sample.Sample('Top 1 Accuracy', 34.114584, '%', {}),
          sample.Sample('Top 5 Accuracy', 61.56616, '%', {}),
          sample.Sample('Elapsed Seconds', 1695, 'seconds', {}),
      ]

    self.assertEqual(samples, golden)


if __name__ == '__main__':
  unittest.main()
