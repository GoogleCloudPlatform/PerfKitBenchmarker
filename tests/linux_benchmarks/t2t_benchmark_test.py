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
"""Tests for t2t_benchmark."""
import os
import unittest
import mock
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import t2t_benchmark
from perfkitbenchmarker.sample import Sample


class Tensor2TensorBenchmarkTestCase(unittest.TestCase,
                                     test_util.SamplesTestMixin):

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testT2TTpuOutput(self):
    self.maxDiff = None
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 't2t_tpu_output.txt')
    with open(path) as fp:
      t2t_contents = fp.read()

    samples = t2t_benchmark._MakeSamplesFromOutput({
        'use_tpu': True
    }, t2t_contents)

    golden = [
        Sample(
            metric='Global Steps Per Second',
            value=1.85777,
            unit='global_steps/sec',
            metadata={
                'use_tpu': True,
                'index': 0
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=5.06989,
            unit='global_steps/sec',
            metadata={
                'use_tpu': True,
                'index': 1
            },
            timestamp=0),
        Sample(
            metric='Examples Per Second',
            value=118.897,
            unit='examples/sec',
            metadata={
                'use_tpu': True,
                'index': 0
            },
            timestamp=0),
        Sample(
            metric='Examples Per Second',
            value=324.473,
            unit='examples/sec',
            metadata={
                'use_tpu': True,
                'index': 1
            },
            timestamp=0),
        Sample(
            metric='Eval Loss',
            value=3.9047337,
            unit='',
            metadata={
                'use_tpu': True,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Accuracy',
            value=32.064167,
            unit='%',
            metadata={
                'use_tpu': True,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Accuracy Per Sequence',
            value=0.0,
            unit='%',
            metadata={
                'use_tpu': True,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Negative Log Perplexity',
            value=-4.501835,
            unit='perplexity',
            metadata={
                'use_tpu': True,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Top 5 Accuracy',
            value=50.96436,
            unit='%',
            metadata={
                'use_tpu': True,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Eval Loss',
            value=3.7047337,
            unit='',
            metadata={
                'use_tpu': True,
                'step': 1200
            },
            timestamp=0),
        Sample(
            metric='Accuracy',
            value=33.064167,
            unit='%',
            metadata={
                'use_tpu': True,
                'step': 1200
            },
            timestamp=0),
        Sample(
            metric='Accuracy Per Sequence',
            value=0.0,
            unit='%',
            metadata={
                'use_tpu': True,
                'step': 1200
            },
            timestamp=0),
        Sample(
            metric='Negative Log Perplexity',
            value=-4.101835,
            unit='perplexity',
            metadata={
                'use_tpu': True,
                'step': 1200
            },
            timestamp=0),
        Sample(
            metric='Top 5 Accuracy',
            value=55.96436,
            unit='%',
            metadata={
                'use_tpu': True,
                'step': 1200
            },
            timestamp=0)
    ]
    self.assertEqual(samples, golden)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testT2TGpuOutput(self):
    self.maxDiff = None
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 't2t_gpu_output.txt')
    with open(path) as fp:
      t2t_contents = fp.read()

    samples = t2t_benchmark._MakeSamplesFromOutput({
        'use_tpu': False
    }, t2t_contents)

    golden = [
        Sample(
            metric='Global Steps Per Second',
            value=3.04983,
            unit='global_steps/sec',
            metadata={
                'index': 0,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.12771,
            unit='global_steps/sec',
            metadata={
                'index': 1,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.11027,
            unit='global_steps/sec',
            metadata={
                'index': 2,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.10924,
            unit='global_steps/sec',
            metadata={
                'index': 3,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.12186,
            unit='global_steps/sec',
            metadata={
                'index': 4,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.08434,
            unit='global_steps/sec',
            metadata={
                'index': 5,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.10174,
            unit='global_steps/sec',
            metadata={
                'index': 6,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.11809,
            unit='global_steps/sec',
            metadata={
                'index': 7,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Global Steps Per Second',
            value=4.10496,
            unit='global_steps/sec',
            metadata={
                'index': 8,
                'use_tpu': False
            },
            timestamp=0),
        Sample(
            metric='Eval Loss',
            value=7.2263174,
            unit='',
            metadata={
                'use_tpu': False,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Accuracy',
            value=13.972055999999998,
            unit='%',
            metadata={
                'use_tpu': False,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Accuracy Per Sequence',
            value=0.0,
            unit='%',
            metadata={
                'use_tpu': False,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Negative Log Perplexity',
            value=-7.2263174,
            unit='perplexity',
            metadata={
                'use_tpu': False,
                'step': 1000
            },
            timestamp=0),
        Sample(
            metric='Top 5 Accuracy',
            value=24.800399000000002,
            unit='%',
            metadata={
                'use_tpu': False,
                'step': 1000
            },
            timestamp=0)
    ]
    self.assertEqual(samples, golden)


if __name__ == '__main__':
  unittest.main()
