"""Tests for Messaging Service Scripts end-to-end main process code."""

import asyncio
import functools
import itertools
import json
import multiprocessing as mp
import os
import typing
from typing import Any
import unittest
from unittest import mock

from perfkitbenchmarker.scripts.messaging_service_scripts.common import app
from perfkitbenchmarker.scripts.messaging_service_scripts.common import errors
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import latency_runner
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import main_process
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import publisher
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import receiver
from tests import pkb_common_test_case

AGGREGATE_E2E_METRICS = {
    'e2e_latency_failure_counter': {
        'value': 0,
        'unit': '',
        'metadata': {}
    },
    'e2e_latency_mean': {
        'value': 500.0,
        'unit': 'milliseconds',
        'metadata': {
            'samples': [500]
        }
    },
    'e2e_latency_mean_without_cold_start': {
        'value': 500.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_latency_p50': {
        'value': 500.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_latency_p99': {
        'value': 500.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_latency_p99_9': {
        'value': 500.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_latency_percentage_received': {
        'value': 100.0,
        'unit': '%',
        'metadata': {}
    },
    'e2e_acknowledge_latency_failure_counter': {
        'value': 0,
        'unit': '',
        'metadata': {}
    },
    'e2e_acknowledge_latency_mean': {
        'value': 1000.0,
        'unit': 'milliseconds',
        'metadata': {
            'samples': [1000]
        }
    },
    'e2e_acknowledge_latency_mean_without_cold_start': {
        'value': 1000.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_acknowledge_latency_p50': {
        'value': 1000.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_acknowledge_latency_p99': {
        'value': 1000.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_acknowledge_latency_p99_9': {
        'value': 1000.0,
        'unit': 'milliseconds',
        'metadata': {}
    },
    'e2e_acknowledge_latency_percentage_received': {
        'value': 100.0,
        'unit': '%',
        'metadata': {}
    }
}


def AsyncTest(test_method):
  """Run an async method synchronously for testing."""

  @functools.wraps(test_method)
  def Wrapped(self, *args, **kwargs):
    return asyncio.run(test_method(self, *args, **kwargs))

  return Wrapped


def Just(value=None):
  """Wrap a value (by default None) in a future that returns immediately."""
  future = asyncio.Future()
  future.set_result(value)
  return future


def GetMockCoro(return_value=None):
  """Gets a Mock Coroutine."""

  async def MockCoro(*args, **kwargs):  # pylint: disable=unused-argument
    return return_value

  return mock.Mock(wraps=MockCoro)


class MessagingServiceScriptsE2EMainProcessTest(
    pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.pipe_mock = self.enter_context(
        mock.patch.object(mp, 'Pipe', side_effect=self._GetPipeMocks))
    self.process_mock = self.enter_context(mock.patch.object(mp, 'Process'))
    self.subprocess_mock = self.process_mock.return_value
    self.flags_mock = self.enter_context(mock.patch('absl.flags.FLAGS'))
    self.app_mock = self.enter_context(mock.patch.object(app, 'App'))

  def _GetPipeMocks(self):
    return mock.Mock(name='pipe_writer'), mock.Mock(name='pipe_reader')

  def _GetSubprocessInWriter(self, worker):
    return typing.cast(Any, getattr(worker, 'subprocess_in_writer'))

  def _GetSubprocessInReader(self, worker):
    return typing.cast(Any, getattr(worker, 'subprocess_in_reader'))

  def _GetSubprocessOutWriter(self, worker):
    return typing.cast(Any, getattr(worker, 'subprocess_out_writer'))

  def _GetSubprocessOutReader(self, worker):
    return typing.cast(Any, getattr(worker, 'subprocess_out_reader'))

  @mock.patch.object(
      main_process.BaseWorker, '_join_subprocess', return_value=Just())
  @mock.patch.object(
      main_process.BaseWorker, '_read_subprocess_output', return_value=Just())
  @AsyncTest
  async def testStartStop(self, read_subprocess_output_mock,
                          join_subprocess_mock):
    worker = main_process.PublisherWorker()
    self.assertEqual(self.pipe_mock.call_count, 2)
    self.assertEqual(self._GetSubprocessInWriter(worker)._extract_mock_name(),
                     'pipe_writer')
    self.assertEqual(self._GetSubprocessInReader(worker)._extract_mock_name(),
                     'pipe_reader')
    self.assertEqual(self._GetSubprocessOutWriter(worker)._extract_mock_name(),
                     'pipe_writer')
    self.assertEqual(self._GetSubprocessOutReader(worker)._extract_mock_name(),
                     'pipe_reader')
    self.assertEqual(worker.subprocess_func, publisher.main)
    await worker.start()
    self.process_mock.assert_called_once_with(
        target=publisher.main,
        kwargs={
            'input_conn': worker.subprocess_in_reader,
            'output_conn': worker.subprocess_out_writer,
            'serialized_flags': self.flags_mock.flags_into_string(),
            'app': self.app_mock.get_instance.return_value,
            'pinned_cpus': None,
        })
    read_subprocess_output_mock.assert_called_once_with(protocol.Ready, None)
    await worker.stop()
    self.subprocess_mock.terminate.assert_called_once_with()
    join_subprocess_mock.assert_called_once_with(None)

  @mock.patch.object(
      main_process.BaseWorker, '_join_subprocess', return_value=Just())
  @mock.patch.object(
      main_process.BaseWorker, '_read_subprocess_output', return_value=Just())
  @AsyncTest
  async def testStartWithPinnedCpus(self, *_):
    worker = main_process.ReceiverWorker({3, 1, 4})
    await worker.start()
    self.process_mock.assert_called_once_with(
        target=receiver.main,
        kwargs={
            'input_conn': worker.subprocess_in_reader,
            'output_conn': worker.subprocess_out_writer,
            'serialized_flags': self.flags_mock.flags_into_string(),
            'app': self.app_mock.get_instance.return_value,
            'pinned_cpus': {3, 1, 4},
        })
    await worker.stop()

  @mock.patch.object(
      main_process.BaseWorker,
      '_join_subprocess',
      side_effect=(errors.EndToEnd.SubprocessTimeoutError, Just()))
  @mock.patch.object(
      main_process.BaseWorker, '_read_subprocess_output', return_value=Just())
  @AsyncTest
  async def testStopKill(self, read_subprocess_output_mock,
                         join_subprocess_mock):
    worker = main_process.PublisherWorker()
    await worker.start()
    read_subprocess_output_mock.assert_called_once_with(protocol.Ready, None)
    await worker.stop()
    self.subprocess_mock.terminate.assert_called_once_with()
    self.subprocess_mock.kill.assert_called_once_with()
    join_subprocess_mock.assert_has_calls([mock.call(None), mock.call(None)])

  @mock.patch.object(asyncio, 'sleep', wraps=asyncio.sleep)
  @mock.patch.object(
      main_process.BaseWorker,
      '_join_subprocess',
      side_effect=(errors.EndToEnd.SubprocessTimeoutError, Just()))
  @AsyncTest
  async def testReadSubprocessOutput(self, _, sleep_mock):
    worker = main_process.PublisherWorker()
    with mock.patch.object(
        main_process.BaseWorker, '_read_subprocess_output',
        return_value=Just()):
      await worker.start()
    worker.subprocess_out_reader.poll.side_effect = [False, True]
    worker.subprocess_out_reader.recv.return_value = 'hola'
    self.assertEqual(await worker._read_subprocess_output(str), 'hola')
    sleep_mock.assert_called_once_with(worker.SLEEP_TIME)
    self.assertEqual(self._GetSubprocessOutReader(worker).poll.call_count, 2)
    self._GetSubprocessOutReader(worker).recv.assert_called_once_with()
    await worker.stop()

  @mock.patch.object(
      main_process.BaseWorker,
      '_join_subprocess',
      side_effect=(errors.EndToEnd.SubprocessTimeoutError, Just()))
  @AsyncTest
  async def testReadSubprocessOutputTimeout(self, _):
    worker = main_process.PublisherWorker()
    with mock.patch.object(
        main_process.BaseWorker, '_read_subprocess_output',
        return_value=Just()):
      await worker.start()
    worker.subprocess_out_reader.poll.side_effect = itertools.repeat(False)
    self._GetSubprocessOutReader(worker).recv.assert_not_called()
    with self.assertRaises(errors.EndToEnd.SubprocessTimeoutError):
      await worker._read_subprocess_output(str, 0.2)
    await worker.stop()

  @mock.patch.object(
      main_process.BaseWorker,
      '_join_subprocess',
      side_effect=(errors.EndToEnd.SubprocessTimeoutError, Just()))
  @AsyncTest
  async def testReadSubprocessUnexpectedObject(self, _):
    worker = main_process.PublisherWorker()
    with mock.patch.object(
        main_process.BaseWorker, '_read_subprocess_output',
        return_value=Just()):
      await worker.start()
    worker.subprocess_out_reader.poll.return_value = True
    worker.subprocess_out_reader.recv.return_value = 42
    with self.assertRaises(errors.EndToEnd.ReceivedUnexpectedObjectError):
      await worker._read_subprocess_output(str)
    self._GetSubprocessOutReader(worker).recv.assert_called_once_with()
    await worker.stop()

  @mock.patch.object(main_process.BaseWorker, 'start', return_value=Just())
  @mock.patch.object(main_process.BaseWorker, 'stop', return_value=Just())
  @mock.patch.object(
      main_process.BaseWorker,
      '_read_subprocess_output',
      return_value=Just(protocol.AckPublish(publish_timestamp=1000)))
  @AsyncTest
  async def testPublish(self, read_subprocess_output_mock, *_):
    worker = main_process.PublisherWorker()
    await worker.start()
    self.assertEqual(await worker.publish(), 1000)
    self._GetSubprocessInWriter(worker).send.assert_called_once_with(
        protocol.Publish())
    read_subprocess_output_mock.assert_called_once_with(protocol.AckPublish,
                                                        None)
    await worker.stop()

  @mock.patch.object(main_process.BaseWorker, 'start', return_value=Just())
  @mock.patch.object(main_process.BaseWorker, 'stop', return_value=Just())
  @mock.patch.object(
      main_process.BaseWorker,
      '_read_subprocess_output',
      return_value=Just(protocol.AckPublish(publish_error='blahblah')))
  @AsyncTest
  async def testPublishError(self, *_):
    worker = main_process.PublisherWorker()
    await worker.start()
    with self.assertRaises(errors.EndToEnd.SubprocessFailedOperationError):
      await worker.publish()
    await worker.stop()

  @mock.patch.object(main_process.BaseWorker, 'start', return_value=Just())
  @mock.patch.object(main_process.BaseWorker, 'stop', return_value=Just())
  @mock.patch.object(
      main_process.BaseWorker, '_read_subprocess_output', return_value=Just())
  @AsyncTest
  async def testStartConsumption(self, read_subprocess_output_mock, *_):
    worker = main_process.ReceiverWorker()
    await worker.start()
    await worker.start_consumption()
    self._GetSubprocessInWriter(worker).send.assert_called_once_with(
        protocol.Consume())
    read_subprocess_output_mock.assert_called_once_with(protocol.AckConsume,
                                                        None)
    await worker.stop()

  @mock.patch.object(main_process.BaseWorker, 'start', return_value=Just())
  @mock.patch.object(main_process.BaseWorker, 'stop', return_value=Just())
  @mock.patch.object(
      main_process.BaseWorker,
      '_read_subprocess_output',
      return_value=Just(protocol.ReceptionReport(receive_error='blahblah')))
  @AsyncTest
  async def testReceive(self, *_):
    worker = main_process.ReceiverWorker()
    await worker.start()
    with self.assertRaises(errors.EndToEnd.SubprocessFailedOperationError):
      await worker.receive()
    await worker.stop()


class MessagingServiceScriptsEndToEndLatencyRunnerTest(
    pkb_common_test_case.PkbCommonTestCase):

  mock_coro = GetMockCoro()
  mock_sleep_coro = GetMockCoro()

  def setUp(self):
    super().setUp()
    self.publisher_mock = self.enter_context(
        mock.patch.object(main_process, 'PublisherWorker'))
    self.receiver_mock = self.enter_context(
        mock.patch.object(main_process, 'ReceiverWorker'))
    self.set_start_method_mock = self.enter_context(
        mock.patch.object(mp, 'set_start_method'))

    self.publisher_instance_mock = self.publisher_mock.return_value

    self.receiver_instance_mock = self.receiver_mock.return_value

    self.parent_mock = mock.Mock()
    self.parent_mock.attach_mock(self.publisher_instance_mock, 'publisher')
    self.parent_mock.attach_mock(self.publisher_instance_mock, 'receiver')

  def _SetupWorkerMocks(self, publish_timestamp, receive_timestamp,
                        ack_timestamp):
    self.publisher_instance_mock.start.return_value = Just()
    self.publisher_instance_mock.stop.return_value = Just()
    self.publisher_instance_mock.publish.return_value = Just(publish_timestamp)

    self.receiver_instance_mock.start.return_value = Just()
    self.receiver_instance_mock.stop.return_value = Just()
    self.receiver_instance_mock.start_consumption.return_value = Just()
    self.receiver_instance_mock.receive.return_value = Just(
        (receive_timestamp, ack_timestamp))

  @mock.patch.object(asyncio, 'run')
  @mock.patch.object(
      latency_runner.EndToEndLatencyRunner, '_async_run_phase', new=mock_coro)
  def testRunPhase(self, asyncio_run_mock):
    runner = latency_runner.EndToEndLatencyRunner(mock.Mock())
    runner.run_phase(13, 14)
    asyncio_run_mock.assert_called_once()
    self.mock_coro.assert_called_once_with(13, 14)

  @mock.patch.object(asyncio, 'sleep', new=mock_sleep_coro)
  @mock.patch.object(latency_runner, 'print')
  @AsyncTest
  async def testAsyncRunPhase(self, print_mock):
    self._SetupWorkerMocks(1_000_000_000, 1_500_000_000, 2_000_000_000)
    runner = latency_runner.EndToEndLatencyRunner(mock.Mock())
    metrics = await runner._async_run_phase(1, 42)
    print_mock.assert_called_once_with(json.dumps(metrics))
    self.assertEqual(metrics, AGGREGATE_E2E_METRICS)

  @mock.patch.object(asyncio, 'sleep', new=mock_sleep_coro)
  @mock.patch.object(latency_runner, 'print')
  @mock.patch.object(os, 'sched_getaffinity', return_value={1, 2, 3, 4, 5, 6})
  @mock.patch.object(os, 'sched_setaffinity')
  @AsyncTest
  async def testPinnedCpus(self, sched_setaffinity_mock, sched_getaffinity_mock,
                           *_):
    try:
      self._SetupWorkerMocks(1_000_000_000, 1_500_000_000, 2_000_000_000)
      self.publisher_mock.CPUS_REQUIRED = 1
      self.receiver_mock.CPUS_REQUIRED = 1
      runner_cls = latency_runner.EndToEndLatencyRunner
      runner_cls.on_startup()
      sched_getaffinity_mock.assert_called_once_with(0)
      main_pinned_cpus = runner_cls.MAIN_PINNED_CPUS
      publisher_pinned_cpus = runner_cls.PUBLISHER_PINNED_CPUS
      receiver_pinned_cpus = runner_cls.RECEIVER_PINNED_CPUS
      self.assertTrue(main_pinned_cpus, 'non-empty and non-none')
      self.assertTrue(publisher_pinned_cpus, 'non-empty and non-none')
      self.assertTrue(receiver_pinned_cpus, 'non-empty and non-none')
      self.assertLess(main_pinned_cpus, {1, 2, 3, 4, 5, 6})
      self.assertLess(publisher_pinned_cpus, {1, 2, 3, 4, 5, 6})
      self.assertLess(receiver_pinned_cpus, {1, 2, 3, 4, 5, 6})
      self.assertLen(
          main_pinned_cpus | publisher_pinned_cpus | receiver_pinned_cpus,
          len(main_pinned_cpus) + len(publisher_pinned_cpus) +
          len(receiver_pinned_cpus), 'test for disjointness')
      sched_setaffinity_mock.assert_called_once_with(0, main_pinned_cpus)
      runner = latency_runner.EndToEndLatencyRunner(mock.Mock())
      await runner._async_run_phase(1, 42)
      self.publisher_mock.assert_called_once_with(publisher_pinned_cpus)
      self.receiver_mock.assert_called_once_with(receiver_pinned_cpus)
    finally:
      runner_cls.MAIN_PINNED_CPUS = None
      runner_cls.PUBLISHER_PINNED_CPUS = None
      runner_cls.RECEIVER_PINNED_CPUS = None

  @mock.patch.object(asyncio, 'sleep', new=mock_sleep_coro)
  @mock.patch.object(latency_runner, 'print')
  @mock.patch.object(os, 'sched_getaffinity', return_value={1, 2})
  @mock.patch.object(os, 'sched_setaffinity')
  @AsyncTest
  async def testPinnedCpusNotEnoughCpus(self, sched_setaffinity_mock,
                                        sched_getaffinity_mock, *_):
    self._SetupWorkerMocks(1_000_000_000, 1_500_000_000, 2_000_000_000)
    self.publisher_mock.CPUS_REQUIRED = 1
    self.receiver_mock.CPUS_REQUIRED = 1
    runner_cls = latency_runner.EndToEndLatencyRunner
    runner_cls.on_startup()
    sched_getaffinity_mock.assert_called_once_with(0)
    self.assertIsNone(runner_cls.MAIN_PINNED_CPUS)
    self.assertIsNone(runner_cls.PUBLISHER_PINNED_CPUS)
    self.assertIsNone(runner_cls.RECEIVER_PINNED_CPUS)
    sched_setaffinity_mock.assert_not_called()
    runner = latency_runner.EndToEndLatencyRunner(mock.Mock())
    await runner._async_run_phase(1, 42)
    self.publisher_mock.assert_called_once_with(None)
    self.receiver_mock.assert_called_once_with(None)


if __name__ == '__main__':
  unittest.main()
