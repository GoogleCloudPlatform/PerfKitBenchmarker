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
from perfkitbenchmarker import test_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ai_model_throughput_benchmark
from tests import pkb_common_test_case
from tests.resources import fake_managed_ai_model

FLAGS = flags.FLAGS


class AiModelThroughputBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(zone=['us-west-1a']))
    config_spec = benchmark_config_spec.BenchmarkConfigSpec(
        ai_model_throughput_benchmark.BENCHMARK_NAME, flag_values=FLAGS
    )
    self.bm_spec = benchmark_spec.BenchmarkSpec(
        ai_model_throughput_benchmark, config_spec, 'benchmark_uid'
    )
    self.bm_spec.ai_model = fake_managed_ai_model.FakeManagedAiModel()
    self.bm_spec.resources.append(self.bm_spec.ai_model)
    self.bm_spec.ai_model.existing_endpoints = [
        'model1',
    ]

  def testBenchmarkPasses(self):
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    metrics = [sample.metric for sample in samples]
    self.assertEqual(
        metrics,
        [
            'success_rate',
            'total_responses',
            'median_response_time',
            'mean_response_time',
        ],
    )

  def testMoreRequestsSent(self):
    self.enter_context(flagsaver.flagsaver(ai_parallel_requests=10))
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    responses_samples = [s for s in samples if s.metric == 'total_responses']
    self.assertNotEmpty(responses_samples)
    responses_sample = responses_samples[0]
    self.assertEqual(responses_sample.value, 10)


if __name__ == '__main__':
  unittest.main()
