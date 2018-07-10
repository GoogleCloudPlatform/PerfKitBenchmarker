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

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import resnet_benchmark


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

    self.metadata = {}

  def testTpuResults(self):
    samples = resnet_benchmark._MakeSamplesFromOutput(self.metadata,
                                                      self.tpu_contents)
    golden = [
        sample.Sample(metric='Loss', value=6.580669, unit='',
                      metadata={'duration': 454, 'step': 1251},
                      timestamp=1530051477.0),
        sample.Sample(metric='Loss', value=3.5848448, unit='',
                      metadata={'duration': 2153, 'step': 6251},
                      timestamp=1530053176.0),
        sample.Sample(metric='Loss', value=3.312076, unit='',
                      metadata={'duration': 3850, 'step': 11251},
                      timestamp=1530054873.0),
        sample.Sample(metric='Loss', value=3.014329, unit='',
                      metadata={'duration': 5547, 'step': 16251},
                      timestamp=1530056570.0),
        sample.Sample(metric='Loss', value=3.7149916, unit='',
                      metadata={'duration': 1652, 'step': 5000},
                      timestamp=1530052675.0),
        sample.Sample(metric='Loss', value=3.8248343, unit='',
                      metadata={'duration': 3353, 'step': 10000},
                      timestamp=1530054376.0),
        sample.Sample(metric='Loss', value=3.5890362, unit='',
                      metadata={'duration': 5047, 'step': 15000},
                      timestamp=1530056070.0),
        sample.Sample(metric='Loss', value=3.4957042, unit='',
                      metadata={'duration': 6741, 'step': 20000},
                      timestamp=1530057764.0),
        sample.Sample(metric='Global Steps Per Second', value=3.13751,
                      unit='global_steps/sec',
                      metadata={'duration': 1254, 'step': 3753},
                      timestamp=1530052277.0),
        sample.Sample(metric='Examples Per Second', value=3212.81,
                      unit='examples/sec',
                      metadata={'duration': 1254, 'step': 3753},
                      timestamp=1530052277.0),
        sample.Sample(metric='Global Steps Per Second', value=3.12108,
                      unit='global_steps/sec',
                      metadata={'duration': 2953, 'step': 8753},
                      timestamp=1530053976.0),
        sample.Sample(metric='Examples Per Second', value=3195.99,
                      unit='examples/sec',
                      metadata={'duration': 2953, 'step': 8753},
                      timestamp=1530053976.0),
        sample.Sample(metric='Global Steps Per Second', value=3.13681,
                      unit='global_steps/sec',
                      metadata={'duration': 4649, 'step': 13753},
                      timestamp=1530055672.0),
        sample.Sample(metric='Examples Per Second', value=3212.1,
                      unit='examples/sec',
                      metadata={'duration': 4649, 'step': 13753},
                      timestamp=1530055672.0),
        sample.Sample(metric='Global Steps Per Second', value=3.14282,
                      unit='global_steps/sec',
                      metadata={'duration': 6344, 'step': 18753},
                      timestamp=1530057367.0),
        sample.Sample(metric='Examples Per Second', value=3218.25,
                      unit='examples/sec',
                      metadata={'duration': 6344, 'step': 18753},
                      timestamp=1530057367.0),
        sample.Sample(metric='Eval Loss', value=3.7454875, unit='',
                      metadata={'duration': 1700, 'step': 5000},
                      timestamp=1530052723.0),
        sample.Sample(metric='Top 1 Accuracy', value=35.49601, unit='%',
                      metadata={'duration': 1700, 'step': 5000},
                      timestamp=1530052723.0),
        sample.Sample(metric='Top 5 Accuracy', value=61.68213, unit='%',
                      metadata={'duration': 1700, 'step': 5000},
                      timestamp=1530052723.0),
        sample.Sample(metric='Eval Loss', value=3.1737862, unit='',
                      metadata={'duration': 3400, 'step': 10000},
                      timestamp=1530054423.0),
        sample.Sample(metric='Top 1 Accuracy', value=46.836343, unit='%',
                      metadata={'duration': 3400, 'step': 10000},
                      timestamp=1530054423.0),
        sample.Sample(metric='Top 5 Accuracy', value=72.786456, unit='%',
                      metadata={'duration': 3400, 'step': 10000},
                      timestamp=1530054423.0),
        sample.Sample(metric='Eval Loss', value=3.0474436, unit='',
                      metadata={'duration': 5094, 'step': 15000},
                      timestamp=1530056117.0),
        sample.Sample(metric='Top 1 Accuracy', value=50.83008, unit='%',
                      metadata={'duration': 5094, 'step': 15000},
                      timestamp=1530056117.0),
        sample.Sample(metric='Top 5 Accuracy', value=76.75781, unit='%',
                      metadata={'duration': 5094, 'step': 15000},
                      timestamp=1530056117.0),
        sample.Sample(metric='Eval Loss', value=2.8425567, unit='',
                      metadata={'duration': 6788, 'step': 20000},
                      timestamp=1530057811.0),
        sample.Sample(metric='Top 1 Accuracy', value=55.65999, unit='%',
                      metadata={'duration': 6788, 'step': 20000},
                      timestamp=1530057811.0),
        sample.Sample(metric='Top 5 Accuracy', value=80.440265, unit='%',
                      metadata={'duration': 6788, 'step': 20000},
                      timestamp=1530057811.0),
        sample.Sample(metric='Elapsed Seconds', value=6788, unit='seconds',
                      metadata={}, timestamp=1530057812.0)
    ]
    self.assertEqual(samples, golden)

  def testGpuResults(self):
    samples = resnet_benchmark._MakeSamplesFromOutput(self.metadata,
                                                      self.gpu_contents)
    golden = [
        sample.Sample(metric='Loss', value=7.98753, unit='',
                      metadata={'duration': 34, 'step': 1},
                      timestamp=1529467163.0),
        sample.Sample(metric='Loss', value=7.8747644, unit='',
                      metadata={'duration': 776, 'step': 2000},
                      timestamp=1529467905.0),
        sample.Sample(metric='Global Steps Per Second', value=2.70357,
                      unit='global_steps/sec',
                      metadata={'duration': 258, 'step': 601},
                      timestamp=1529467387.0),
        sample.Sample(metric='Global Steps Per Second', value=2.60443,
                      unit='global_steps/sec',
                      metadata={'duration': 480, 'step': 1201},
                      timestamp=1529467609.0),
        sample.Sample(metric='Global Steps Per Second', value=2.65584,
                      unit='global_steps/sec',
                      metadata={'duration': 703, 'step': 1801},
                      timestamp=1529467832.0),
        sample.Sample(metric='Eval Loss', value=7.8702474, unit='',
                      metadata={'duration': 920, 'step': 2000},
                      timestamp=1529468049.0),
        sample.Sample(metric='Top 1 Accuracy', value=0.5941901399999999,
                      unit='%', metadata={'duration': 920, 'step': 2000},
                      timestamp=1529468049.0),
        sample.Sample(metric='Top 5 Accuracy', value=2.1947023, unit='%',
                      metadata={'duration': 920, 'step': 2000},
                      timestamp=1529468049.0),
        sample.Sample(metric='Elapsed Seconds', value=920, unit='seconds',
                      metadata={}, timestamp=1529468049.0)
    ]
    self.assertEqual(samples, golden)

if __name__ == '__main__':
  unittest.main()
