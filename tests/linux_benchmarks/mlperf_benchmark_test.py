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
from perfkitbenchmarker.linux_benchmarks import mlperf_benchmark
from perfkitbenchmarker.sample import Sample


class MlperfBenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super(MlperfBenchmarkTestCase, self).setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'mlperf_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  @mock.patch('time.time', mock.MagicMock(return_value=1550279509.59))
  def testTrainResults(self):
    samples = mlperf_benchmark.MakeSamplesFromOutput({},
                                                     self.contents,
                                                     use_tpu=True,
                                                     model='resnet')
    golden = [
        Sample('Eval Accuracy', 32.322001457214355, '%', {
            'epoch': 4,
            'times': 0.0,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 40.342000126838684, '%', {
            'epoch': 8,
            'times': 164.16299986839294,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 48.21600019931793, '%', {
            'epoch': 12,
            'times': 328.239000082016,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 51.749998331069946, '%', {
            'epoch': 16,
            'times': 492.335000038147,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 52.851998805999756, '%', {
            'epoch': 20,
            'times': 656.4279999732971,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 52.99599766731262, '%', {
            'epoch': 24,
            'times': 820.5209999084473,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 60.44999957084656, '%', {
            'epoch': 28,
            'times': 984.6259999275208,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 62.775999307632446, '%', {
            'epoch': 32,
            'times': 1148.7119998931885,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 66.22400283813477, '%', {
            'epoch': 36,
            'times': 1312.8050000667572,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 67.34600067138672, '%', {
            'epoch': 40,
            'times': 1476.9070000648499,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 70.77400088310242, '%', {
            'epoch': 44,
            'times': 1640.994999885559,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 72.40599989891052, '%', {
            'epoch': 48,
            'times': 1805.085000038147,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 73.85799884796143, '%', {
            'epoch': 52,
            'times': 1969.1849999427795,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 75.26000142097473, '%', {
            'epoch': 56,
            'times': 2133.2750000953674,
            'version': 'v0.6.0'
        }),
        Sample('Eval Accuracy', 76.0420024394989, '%', {
            'epoch': 60,
            'times': 2297.3669998645782,
            'version': 'v0.6.0'
        })
    ]
    self.assertEqual(samples, golden)


if __name__ == '__main__':
  unittest.main()
