# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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


import unittest
from absl import flags
from absl.testing import flagsaver
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import test_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ai_model_create_benchmark
from tests import pkb_common_test_case
from tests.resources import fake_managed_ai_model

FLAGS = flags.FLAGS


class AiModelCreateBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(zone=['us-west-1a']))
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        ai_model_create_benchmark.BENCHMARK_NAME, flag_values=FLAGS
    )
    self.bm_spec = benchmark_spec.BenchmarkSpec(
        ai_model_create_benchmark, config_spec, 'benchmark_uid'
    )
    self.bm_spec.ai_model = fake_managed_ai_model.FakeManagedAiModel()
    self.bm_spec.resources.append(self.bm_spec.ai_model)

  def testBenchmarkPassesForOneModel(self):
    self.bm_spec.ai_model.existing_endpoints = [
        'model1',
    ]
    ai_model_create_benchmark.Run(self.bm_spec)

  def testBenchmarkFailsIfMoreModelsFound(self):
    self.bm_spec.ai_model.existing_endpoints = [
        'model1',
        'model2',
    ]
    with self.assertRaises(errors.Benchmarks.PrepareException):
      ai_model_create_benchmark.Run(self.bm_spec)

  def testBenchmarkRunGivesCorrectSamplesForOneModel(self):
    self.enter_context(flagsaver.flagsaver(create_second_model=False))
    self.bm_spec.ai_model.existing_endpoints = [
        'model1',
    ]
    self.bm_spec.ai_model.Create()
    ai_model_create_benchmark.Run(self.bm_spec)
    samples = self.bm_spec.GetSamples()
    metrics = [sample.metric for sample in samples]
    self.assertEqual(
        metrics,
        [
            'Time to Create',
            'Time to Ready',
            'response_time_0',
            'response_time_1',
            'response_time_2',
        ],
    )

  def testBenchmarkRunGivesCorrectSamplesForTwoModels(self):
    self.enter_context(flagsaver.flagsaver(create_second_model=True))
    self.bm_spec.ai_model.existing_endpoints = [
        'model1',
    ]
    self.bm_spec.ai_model.Create()
    ai_model_create_benchmark.Run(self.bm_spec)
    samples = self.bm_spec.GetSamples()
    metrics = [sample.metric for sample in samples]
    self.assertEqual(
        metrics,
        [
            'Time to Create',
            'Time to Ready',
            'response_time_0',
            'response_time_1',
            'response_time_2',
            'Time to Create',
            'Time to Ready',
            'response_time_0',
            'response_time_1',
            'response_time_2',
        ],
    )
    model1_sample = samples[0]
    model2_sample = samples[5]
    self.assertTrue(model1_sample.metadata['First Model'])
    self.assertFalse(model2_sample.metadata['First Model'])


if __name__ == '__main__':
  unittest.main()
