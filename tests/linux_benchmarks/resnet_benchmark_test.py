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

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import resnet_benchmark
from perfkitbenchmarker.sample import Sample


class ResNetBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'resnet_tpu_output.txt')
    with open(path) as fp:
      self.tpu_contents = fp.read()

    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'resnet_gpu_output.txt')
    with open(path) as fp:
      self.gpu_contents = fp.read()

    self.metadata = {'num_examples_per_epoch': 1251.1}

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testTpuResults(self):
    samples = resnet_benchmark._MakeSamplesFromOutput(self.metadata,
                                                      self.tpu_contents)
    golden = [
        Sample('Loss', 6.3166966, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.9999200703381026,
                'duration': 423, 'step': 1251}),
        Sample('Loss', 5.30481, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 1.9998401406762052,
                'duration': 783, 'step': 2502}),
        Sample('Global Steps Per Second', 3.47162, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 1.9998401406762052,
                'duration': 783, 'step': 2502}),
        Sample('Examples Per Second', 3554.94, 'examples/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 1.9998401406762052,
                'duration': 783, 'step': 2502}),
        Sample('Loss', 4.3771253, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 2.9997602110143076,
                'duration': 1139, 'step': 3753}),
        Sample('Global Steps Per Second', 3.51319, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 2.9997602110143076,
                'duration': 1139, 'step': 3753}),
        Sample('Examples Per Second', 3597.51, 'examples/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 2.9997602110143076,
                'duration': 1139, 'step': 3753}),
        Sample('Loss', 3.9155605, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 3.996483094876509,
                'duration': 1486, 'step': 5000}),
        Sample('Global Steps Per Second', 3.60089, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 3.996483094876509,
                'duration': 1486, 'step': 5000}),
        Sample('Examples Per Second', 3687.31, 'examples/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 3.996483094876509,
                'duration': 1486, 'step': 5000}),
        Sample('Loss', 3.774139, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 4.996403165214612,
                'duration': 1968, 'step': 6251}),
        Sample('Loss', 3.2543745, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 5.996323235552714,
                'duration': 2327, 'step': 7502}),
        Sample('Global Steps Per Second', 3.48231, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 5.996323235552714,
                'duration': 2327, 'step': 7502}),
        Sample('Examples Per Second', 3565.89, 'examples/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 5.996323235552714,
                'duration': 2327, 'step': 7502}),
        Sample('Loss', 3.1598916, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 6.996243305890816,
                'duration': 2685, 'step': 8753}),
        Sample('Global Steps Per Second', 3.49526, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 6.996243305890816,
                'duration': 2685, 'step': 8753}),
        Sample('Examples Per Second', 3579.15, 'examples/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 6.996243305890816,
                'duration': 2685, 'step': 8753}),
        Sample('Loss', 3.054053, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 7.992966189753018,
                'duration': 3031, 'step': 10000}),
        Sample('Global Steps Per Second', 3.60296, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 7.992966189753018,
                'duration': 3031, 'step': 10000}),
        Sample('Examples Per Second', 3689.43, 'examples/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 7.992966189753018,
                'duration': 3031, 'step': 10000}),
        Sample('Eval Loss', 3.636791, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 3.996483094876509,
                'duration': 1539, 'step': 5000}),
        Sample('Top 1 Accuracy', 35.95581, '%',
               {'num_examples_per_epoch': 1251.1, 'epoch': 3.996483094876509,
                'duration': 1539, 'step': 5000}),
        Sample('Top 5 Accuracy', 63.112384, '%',
               {'num_examples_per_epoch': 1251.1, 'epoch': 3.996483094876509,
                'duration': 1539, 'step': 5000}),
        Sample('Eval Loss', 3.0327156, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 7.992966189753018,
                'duration': 3082, 'step': 10000}),
        Sample('Top 1 Accuracy', 49.57479, '%',
               {'num_examples_per_epoch': 1251.1, 'epoch': 7.992966189753018,
                'duration': 3082, 'step': 10000}),
        Sample('Top 5 Accuracy', 75.47607400000001, '%',
               {'num_examples_per_epoch': 1251.1, 'epoch': 7.992966189753018,
                'duration': 3082, 'step': 10000}),
        Sample('Elapsed Seconds', 34890, 'seconds',
               {'num_examples_per_epoch': 1251.1})
    ]
    self.assertEqual(samples, golden)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testGpuResults(self):
    samples = resnet_benchmark._MakeSamplesFromOutput(self.metadata,
                                                      self.gpu_contents)
    golden = [
        Sample('Loss', 7.98753, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0,
                'step': 0, 'duration': 35}),
        Sample('Global Steps Per Second', 2.52565, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0,
                'step': 0, 'duration': 35}),
        Sample('Loss', 7.9780626, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.07992966189753017,
                'step': 100, 'duration': 75}),
        Sample('Global Steps Per Second', 2.75627, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.07992966189753017,
                'step': 100, 'duration': 75}),
        Sample('Loss', 7.9498286, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.15985932379506035,
                'step': 200, 'duration': 111}),
        Sample('Global Steps Per Second', 2.72345, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.15985932379506035,
                'step': 200, 'duration': 111}),
        Sample('Loss', 7.9504285, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.23978898569259055,
                'step': 300, 'duration': 148}),
        Sample('Global Steps Per Second', 2.74449, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.23978898569259055,
                'step': 300, 'duration': 148}),
        Sample('Loss', 7.9720306, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.3197186475901207,
                'step': 400, 'duration': 184}),
        Sample('Global Steps Per Second', 2.68677, 'global_steps/sec',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.3197186475901207,
                'step': 400, 'duration': 184}),
        Sample('Loss', 7.9649105, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 0.3996483094876509,
                'step': 500, 'duration': 221}),
        Sample('Eval Loss', 7.8702474, '',
               {'num_examples_per_epoch': 1251.1, 'epoch': 1.5985932379506036,
                'step': 2000, 'duration': 920}),
        Sample('Top 1 Accuracy', 0.5941901399999999, '%',
               {'num_examples_per_epoch': 1251.1, 'epoch': 1.5985932379506036,
                'step': 2000, 'duration': 920}),
        Sample('Top 5 Accuracy', 2.1947023, '%',
               {'num_examples_per_epoch': 1251.1, 'epoch': 1.5985932379506036, 
                'step': 2000, 'duration': 920}),
        Sample('Elapsed Seconds', 920, 'seconds',
               {'num_examples_per_epoch': 1251.1})
    ]
    self.assertEqual(samples, golden)

if __name__ == '__main__':
  unittest.main()
