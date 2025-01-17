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

import multiprocessing
import time
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import test_util
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.linux_benchmarks import ai_model_throughput_benchmark
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.scripts import throughput_load_driver
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
    self.bm_spec.vm_groups = {
        'default': [mock.create_autospec(virtual_machine.BaseVirtualMachine)]
    }

    def _run_command_mock(command: str):
      return command, '', 0

    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            '_RunCommand',
            side_effect=_run_command_mock,
        )
    )

    def _run_throughput_directly(
        command: str,
        throughput_script: str,
        vm: virtual_machine.BaseVirtualMachine,
        burst_requests: int,
    ) -> list[throughput_load_driver.CommandResponse]:
      """Calls throughput_load_driver directly rather than via VM."""
      del throughput_script
      del vm
      return throughput_load_driver.BurstRequestsOverTime(
          command,
          burst_requests,
          throughput_load_driver.TEST_DURATION.value,
          throughput_load_driver.BURST_TIME.value,
      )

    self.enter_context(
        mock.patch.object(
            ai_model_throughput_benchmark,
            '_BurstRequestsOverTime',
            side_effect=_run_throughput_directly,
        )
    )

  @flagsaver.flagsaver(ai_starting_requests=1, ai_test_duration=5)
  def testBenchmarkPassesWithCorrectMetrics(self):
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    metrics = [sample.metric for sample in samples]
    self.assertEqual(
        metrics,
        [
            'success_rate',
            'num_responses',
            'median_response_time',
            'mean_response_time',
            'max_throughput',
        ],
    )

  @flagsaver.flagsaver(ai_starting_requests=1, ai_test_duration=5)
  def testBenchmarkPassesWithCorrectMetricMetadata(self):
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    metadatas = [sample.metadata for sample in samples]
    expected = {
        'parallel_requests': 1,
        'test_duration': 5,
        'First Model': True,
    }
    self.assertEqual(metadatas[0], {**metadatas[0], **expected})

  @parameterized.named_parameters(
      ('tiny test', 1, 2),
      ('longer test', 10, 3),
  )
  def testCorrectRequestsReturned(self, duration, qps):
    self.enter_context(flagsaver.flagsaver(ai_starting_requests=qps))
    self.enter_context(flagsaver.flagsaver(ai_test_duration=duration))
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    responses_samples = [s for s in samples if s.metric == 'num_responses']
    self.assertNotEmpty(responses_samples)
    responses_sample = responses_samples[0]
    self.assertGreaterEqual(responses_sample.value, duration * qps)

  @flagsaver.flagsaver(
      ai_starting_requests=50,
      ai_test_duration=4,
      ai_throw_on_client_errors=True,
  )
  def testTooMuchQpsThrowsError(self):
    def idle_timer_mock():
      time.sleep(3)

    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            '_UnitTestIdleTime',
            side_effect=idle_timer_mock,
        )
    )
    with self.assertRaises(throughput_load_driver.ClientError):
      ai_model_throughput_benchmark.Run(self.bm_spec)

  @flagsaver.flagsaver(
      ai_starting_requests=2,
      ai_test_duration=5,
      ai_throw_on_client_errors=True,
  )
  def testWaitDoesntTriggerError(self):
    def _run_command_mock(command: str):
      time.sleep(15)
      return command, '', 0

    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            '_RunCommand',
            side_effect=_run_command_mock,
        )
    )
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    responses_samples = [s for s in samples if s.metric == 'num_responses']
    self.assertNotEmpty(responses_samples)
    responses_sample = responses_samples[0]
    self.assertEqual(responses_sample.value, 10)
    time_samples = [s for s in samples if s.metric == 'median_response_time']
    self.assertNotEmpty(time_samples)
    time_sample = time_samples[0]
    self.assertGreaterEqual(time_sample.value, 15)

  @flagsaver.flagsaver(
      ai_starting_requests=2,
      ai_max_requests=12,
      ai_test_duration=5,
  )
  def testMaxQpsReached(self):
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    throughput_samples = [s for s in samples if s.metric == 'max_throughput']
    self.assertNotEmpty(throughput_samples)
    throughput_sample = throughput_samples[0]
    self.assertEqual(throughput_sample.value, 11)

  @flagsaver.flagsaver(
      ai_starting_requests=2,
      ai_max_requests=20,
      ai_test_duration=1,
  )
  def testMaxThroughputReachedByTooManyProcesses(self):
    # Arrange
    def time_prompt_queue_checker(
        ai_model: managed_ai_model.BaseManagedAiModel,
        output_queue: multiprocessing.Queue,
    ):
      del ai_model
      end_time = 0.5
      # qsize is not guaranteed to be accurate, but seems to work ok.
      if output_queue.qsize() > 11:
        end_time = 200
      output_queue.put(
          throughput_load_driver.CommandResponse(0, end_time, 'response', '', 0)
      )
      time.sleep(0.5)

    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            '_TimeCommand',
            side_effect=time_prompt_queue_checker,
        )
    )
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    throughput_samples = [s for s in samples if s.metric == 'max_throughput']
    self.assertNotEmpty(throughput_samples)
    throughput_sample = throughput_samples[0]
    self.assertEqual(throughput_sample.value, 11)

  @flagsaver.flagsaver(
      ai_starting_requests=2,
      ai_max_requests=20,
      ai_test_duration=1,
  )
  def testMaxThroughputReachedByTimeoutWithMock(self):
    # Arrange
    short_responses = [
        throughput_load_driver.CommandResponse(0, 0.5, 'response', '', 0)
    ] * 11
    long_responses = [
        throughput_load_driver.CommandResponse(0, 100, 'response', '', 0)
    ] * 11
    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            'BurstRequestsOverTime',
            # 2 passes, 3rd fails.
            side_effect=[short_responses, short_responses, long_responses],
        )
    )
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    throughput_samples = [s for s in samples if s.metric == 'max_throughput']
    self.assertNotEmpty(throughput_samples)
    throughput_sample = throughput_samples[0]
    # steps go 2 -> 5 -> 8, so 5 is the max.
    self.assertEqual(throughput_sample.value, 5)

  @flagsaver.flagsaver(
      ai_starting_requests=2,
      ai_max_requests=20,
      ai_test_duration=10,
  )
  def testMaxThroughputReachedWithNotEnoughResponses(self):
    # Arrange
    one_response = [
        throughput_load_driver.CommandResponse(0, 0.5, 'response', '', 0)
    ]
    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            'BurstRequestsOverTime',
            # 2nd has expected number, 3rd does not.
            side_effect=[
                one_response * 20,
                one_response * 50,
                one_response * 60,
            ]
            + [one_response] * 10,
        )
    )
    # Act
    samples = ai_model_throughput_benchmark.Run(self.bm_spec)
    # Assert
    throughput_samples = [s for s in samples if s.metric == 'max_throughput']
    self.assertNotEmpty(throughput_samples)
    throughput_sample = throughput_samples[0]
    # steps go 2 -> 5 -> 8, so 5 is the max.
    self.assertEqual(throughput_sample.value, 5)


if __name__ == '__main__':
  unittest.main()
