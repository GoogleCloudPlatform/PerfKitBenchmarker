# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for roberta_mmlm_benchmark."""
import os
import unittest
import mock

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import roberta_mmlm_benchmark
from perfkitbenchmarker.sample import Sample


class RobertaMmlmBenchmarkTestCase(unittest.TestCase,
                                   test_util.SamplesTestMixin):

  def setUp(self):
    super(RobertaMmlmBenchmarkTestCase, self).setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'roberta_mmlm_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  @mock.patch('time.time', mock.MagicMock(return_value=1550279509.59))
  def testTrainResults(self):
    samples = roberta_mmlm_benchmark.MakeSamplesFromOutput(
        {'num_accelerators': 16}, self.contents)
    self.assertEqual(436, len(samples))
    golden = Sample(
        metric='wps',
        value=26259.0,
        unit='wps',
        metadata={
            'num_accelerators': 16,
            'epoch': '001',
            'step': '10',
            'steps per epoch': '2183',
            'loss': '18.137',
            'nll_loss': '18.137',
            'ppl': '288277',
            'wps': '26259',
            'ups': '0',
            'wpb': '60162.909',
            'bsz': '128.000',
            'num_updates': '11',
            'lr': '2.93333e-07',
            'gnorm': '8.833',
            'clip': '1.000',
            'oom': '0.000',
            'loss_scale': '128.000',
            'wall': '28',
            'train_wall': '27',
        },
        timestamp=1550279509.59)
    print(samples[0])
    print(golden)
    self.assertEqual(golden, samples[0])


if __name__ == '__main__':
  unittest.main()
