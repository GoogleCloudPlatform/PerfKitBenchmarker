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
    samples = mlperf_benchmark.MakeSamplesFromOutput({}, self.contents)
    golden = [
        Sample('Eval Accuracy', 5.96720390021801, '%',
               {'epoch': 0, 'times': 0.0, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 36.89168393611908, '%',
               {'epoch': 4, 'times': 1164.691000699997, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 49.114990234375, '%',
               {'epoch': 8, 'times': 2329.8028297424316, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 53.01310420036316, '%',
               {'epoch': 12, 'times': 3498.9867885112762, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 53.55224609375, '%',
               {'epoch': 16, 'times': 4667.747241735458, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 54.87263798713684, '%',
               {'epoch': 20, 'times': 5831.299504995346, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 54.70173954963684, '%',
               {'epoch': 24, 'times': 6996.661015510559, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 56.72810673713684, '%',
               {'epoch': 28, 'times': 8160.468462944031, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 70.751953125, '%',
               {'epoch': 32, 'times': 9329.49914598465, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 71.368408203125, '%',
               {'epoch': 36, 'times': 10494.261439800262, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 71.49454951286316, '%',
               {'epoch': 40, 'times': 11657.773159980774, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 70.70515751838684, '%',
               {'epoch': 44, 'times': 12823.00942158699, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 70.65632939338684, '%',
               {'epoch': 48, 'times': 13988.791482448578, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 70.562744140625, '%',
               {'epoch': 52, 'times': 15154.056546211243, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 70.88623046875, '%',
               {'epoch': 56, 'times': 16318.724472999573, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 74.67244267463684, '%',
               {'epoch': 60, 'times': 17482.81353545189, 'version': '0.5.0'}),
        Sample('Eval Accuracy', 75.00407099723816, '%',
               {'epoch': 61, 'times': 17788.61406970024, 'version': '0.5.0'}),
        Sample('Times', 18183, 'seconds', {})
    ]
    self.assertEqual(samples, golden)


if __name__ == '__main__':
  unittest.main()
