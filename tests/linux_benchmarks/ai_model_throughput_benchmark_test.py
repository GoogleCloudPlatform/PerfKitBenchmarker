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

import time
from typing import Any
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import test_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ai_model_throughput_benchmark
from tests import pkb_common_test_case
from tests.resources import fake_managed_ai_model


FLAGS = flags.FLAGS


class TimedFakeManagedAiModel(fake_managed_ai_model.FakeManagedAiModel):
  """Fake managed AI model for testing."""

  def __init__(self, **kwargs: Any) -> Any:
    super().__init__(**kwargs)
    self.wait_time = 0

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    time.sleep(self.wait_time)
    return [prompt]


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
    self.bm_spec.ai_model = TimedFakeManagedAiModel()
    self.bm_spec.resources.append(self.bm_spec.ai_model)
    self.bm_spec.ai_model.existing_endpoints = [
        'model1',
    ]

  @flagsaver.flagsaver(ai_parallel_requests=1, ai_test_duration=5)
  def testBenchmarkPassesWithCorrectMetrics(self):
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

  @parameterized.named_parameters(
      ('tiny test', 1, 2),
      ('longer test', 10, 3),
  )
  def testCorrectRequestsReturned(self, duration, qps):
    self.enter_context(flagsaver.flagsaver(ai_parallel_requests=qps))
    self.enter_context(flagsaver.flagsaver(ai_test_duration=duration))
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    responses_samples = [s for s in samples if s.metric == 'total_responses']
    self.assertNotEmpty(responses_samples)
    responses_sample = responses_samples[0]
    self.assertGreaterEqual(responses_sample.value, duration * qps)

  def testTooMuchQpsThrowsError(self):
    self.enter_context(flagsaver.flagsaver(ai_parallel_requests=50))
    self.enter_context(flagsaver.flagsaver(ai_test_duration=2))
    def idle_timer_mock():
      time.sleep(3)
    self.enter_context(
        mock.patch.object(
            ai_model_throughput_benchmark,
            '_UnitTestIdleTime',
            side_effect=idle_timer_mock,
        )
    )
    with self.assertRaises(errors.Benchmarks.RunError):
      ai_model_throughput_benchmark.Run(self.bm_spec)

  @flagsaver.flagsaver(ai_parallel_requests=2, ai_test_duration=5)
  def testWaitDoesntTriggerError(self):
    self.bm_spec.ai_model.wait_time = 15
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    responses_samples = [s for s in samples if s.metric == 'total_responses']
    self.assertNotEmpty(responses_samples)
    responses_sample = responses_samples[0]
    self.assertEqual(responses_sample.value, 10)
    time_samples = [s for s in samples if s.metric == 'median_response_time']
    self.assertNotEmpty(time_samples)
    time_sample = time_samples[0]
    self.assertGreaterEqual(time_sample.value, 15)


if __name__ == '__main__':
  unittest.main()
