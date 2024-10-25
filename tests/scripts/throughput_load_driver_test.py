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
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import test_util
from perfkitbenchmarker.scripts import throughput_load_driver
from tests import pkb_common_test_case
from pyfakefs import fake_filesystem


FLAGS = flags.FLAGS


class ThroughputLoadDriverTest(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()

    def _run_command_mock(command: str):
      return command, '', 0

    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            '_RunCommand',
            side_effect=_run_command_mock,
        )
    )
    fs = fake_filesystem.FakeFilesystem()
    fake_open = fake_filesystem.FakeFileOpen(fs)
    self.enter_context(
        mock.patch.object(throughput_load_driver, 'open', fake_open)
    )
    fs.create_dir('/tmp')

  @parameterized.named_parameters(
      ('tiny test', 1, 2),
      ('longer test', 10, 3),
  )
  def testCorrectRequestsReturned(self, duration, qps):
    self.enter_context(flagsaver.flagsaver(ai_starting_requests=qps))
    self.enter_context(flagsaver.flagsaver(ai_test_duration=duration))
    # Act
    responses = throughput_load_driver.Run()
    # Assert
    self.assertGreaterEqual(len(responses), duration * qps)

  @flagsaver.flagsaver(
      _ai_throughput_parallel_requests=50,
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
      throughput_load_driver.Run()

  @flagsaver.flagsaver(
      _ai_throughput_parallel_requests=2,
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
    responses = throughput_load_driver.Run()
    # Assert
    self.assertLen(responses, 10)
    for response in responses:
      self.assertGreaterEqual(response.end_time - response.start_time, 15)

  @flagsaver.flagsaver(
      _ai_throughput_parallel_requests=2,
      ai_test_duration=5,
      ai_throw_on_client_errors=True,
  )
  def testPastTimeoutThrows(self):
    orig_fail_latency = throughput_load_driver._FAIL_LATENCY
    orig_queue_wait_time = throughput_load_driver._QUEUE_WAIT_TIME
    throughput_load_driver._FAIL_LATENCY = 2
    throughput_load_driver._QUEUE_WAIT_TIME = 4
    try:

      def _run_command_mock(command: str):
        time.sleep(10)
        return command, '', 0

      self.enter_context(
          mock.patch.object(
              throughput_load_driver,
              '_RunCommand',
              side_effect=_run_command_mock,
          )
      )
      # Act
      with self.assertRaises(throughput_load_driver.ClientError):
        throughput_load_driver.Run()
    finally:
      throughput_load_driver._FAIL_LATENCY = orig_fail_latency
      throughput_load_driver._QUEUE_WAIT_TIME = orig_queue_wait_time

  @flagsaver.flagsaver(
      _ai_throughput_parallel_requests=2,
      ai_test_duration=10,
  )
  def testPastTimeoutCompletes(self):
    orig_fail_latency = throughput_load_driver._FAIL_LATENCY
    orig_queue_wait_time = throughput_load_driver._QUEUE_WAIT_TIME
    throughput_load_driver._FAIL_LATENCY = 2
    throughput_load_driver._QUEUE_WAIT_TIME = 4
    try:

      def _run_command_mock(command: str):
        time.sleep(9)
        return command, '', 0

      self.enter_context(
          mock.patch.object(
              throughput_load_driver,
              '_RunCommand',
              side_effect=_run_command_mock,
          )
      )
      # Act
      responses = throughput_load_driver.Run()
      # Assert - most processes have not finished & were abandoned.
      self.assertGreaterEqual(len(responses), 10)
      self.assertLessEqual(len(responses), 11)
    finally:
      throughput_load_driver._FAIL_LATENCY = orig_fail_latency
      throughput_load_driver._QUEUE_WAIT_TIME = orig_queue_wait_time

  @flagsaver.flagsaver(
      _ai_throughput_parallel_requests=2,
      ai_test_duration=5,
  )
  def testInputOutput(self):
    def _run_command_mock(command: str):
      del command
      return 'Response', 'Error', 0

    self.enter_context(
        mock.patch.object(
            throughput_load_driver,
            '_RunCommand',
            side_effect=_run_command_mock,
        )
    )
    # Act
    throughput_load_driver.Run()
    responses = throughput_load_driver.ReadJsonResponses(2)
    # Assert
    self.assertLen(responses, 10)
    for response in responses:
      self.assertEqual(response.response, 'Response')
      self.assertEqual(response.error, 'Error')


if __name__ == '__main__':
  unittest.main()
