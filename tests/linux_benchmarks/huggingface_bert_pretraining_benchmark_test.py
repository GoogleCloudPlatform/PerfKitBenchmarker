# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for huggingface_bert_pretraining_benchmark."""
import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import huggingface_bert_pretraining_benchmark
from tests import pkb_common_test_case


class HuggingfaceBertPretrainingTestCase(pkb_common_test_case.PkbCommonTestCase,
                                         test_util.SamplesTestMixin):

  def setUp(self):
    super(HuggingfaceBertPretrainingTestCase, self).setUp()
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data',
        'huggingface_bert_pretraining_output.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def testTrainResults(self):
    samples = huggingface_bert_pretraining_benchmark.MakeSamplesFromOutput(
        self.contents)
    self.assertLen(samples, 24)
    self.assertEqual(samples[0].metric, 'throughput')
    self.assertAlmostEqual(samples[0].value, 1.78)
    self.assertEqual(samples[0].unit, 'i/s')
    self.assertEqual(samples[0].metadata['epoch'], 0)
    self.assertEqual(samples[0].metadata['step'], 1)
    self.assertAlmostEqual(samples[0].metadata['step_loss'], 4.8438)
    self.assertAlmostEqual(samples[0].metadata['learning_rate'], 3.59e-07)
    self.assertEqual(samples[-1].metric, 'throughput')
    self.assertAlmostEqual(samples[-1].value, 32.8)
    self.assertEqual(samples[-1].unit, 'i/s')
    self.assertEqual(samples[-1].metadata['epoch'], 0)
    self.assertEqual(samples[-1].metadata['step'], 24)
    self.assertAlmostEqual(samples[-1].metadata['step_loss'], 4.1250)
    self.assertAlmostEqual(samples[-1].metadata['learning_rate'], 8.60e-06)


if __name__ == '__main__':
  unittest.main()
