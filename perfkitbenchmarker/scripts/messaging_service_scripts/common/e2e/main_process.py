"""Utils for end-to-end latency measurement.

In this module we define helper classes to spawn and connect to subprocesses to
to measure end-to-end latency accurately.
"""
import asyncio
import multiprocessing as mp
import time
from typing import Any, Callable, Optional, Set, Tuple, Type

from absl import flags

from perfkitbenchmarker.scripts.messaging_service_scripts.common import app
from perfkitbenchmarker.scripts.messaging_service_scripts.common import errors
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import publisher
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import receiver


class BaseWorker:
  """Base Worker class which represents a worker in the main process."""

  SLEEP_TIME = 0.1
  DEFAULT_TIMEOUT = 10
  CPUS_REQUIRED = 1

  def __init__(self,
               subprocess_func: Callable[..., Any],
               pinned_cpus: Optional[Set[int]] = None):
    self.subprocess_func = subprocess_func
    self.subprocess = None
    self.subprocess_in_writer, self.subprocess_in_reader = mp.Pipe()
    self.subprocess_out_writer, self.subprocess_out_reader = mp.Pipe()
    self.pinned_cpus = pinned_cpus

  async def start(self, timeout: Optional[float] = None):
    """Launches the subprocess then waits for a Ready message.

    Args:
      timeout: Timeout in seconds. Defaults to BaseWorker.DEFAULT_TIMEOUT.
    """
    self.subprocess = mp.Process(
        target=self.subprocess_func,
        kwargs={
            'input_conn': self.subprocess_in_reader,
            'output_conn': self.subprocess_out_writer,
            'serialized_flags': flags.FLAGS.flags_into_string(),
            'app': app.App.get_instance(),
            'pinned_cpus': self.pinned_cpus,
        })
    self.subprocess.start()
    await self._read_subprocess_output(protocol.Ready, timeout)

  async def stop(self, timeout: Optional[float] = None):
    """Attempts to gracefully shutdown the subprocess, else just kills it.

    Args:
      timeout: Timeout in seconds. Defaults to BaseWorker.DEFAULT_TIMEOUT.
    """
    try:
      self.subprocess.terminate()
      await self._join_subprocess(timeout)
    except errors.EndToEnd.SubprocessTimeoutError:
      self.subprocess.kill()
      await self._join_subprocess(timeout)

  async def _read_subprocess_output(self,
                                    message_type: Type[Any],
                                    timeout: Optional[float] = None):
    """Attempts to read the subprocess output with a timeout."""
    timeout = self.DEFAULT_TIMEOUT if timeout is None else timeout
    deadline = time.time() + timeout
    while not self.subprocess_out_reader.poll():
      await asyncio.sleep(self.SLEEP_TIME)
      if time.time() > deadline:
        raise errors.EndToEnd.SubprocessTimeoutError
    message = self.subprocess_out_reader.recv()
    if not isinstance(message, message_type):
      raise errors.EndToEnd.ReceivedUnexpectedObjectError(
          'Unexpected subprocess output')
    return message

  async def _join_subprocess(self, timeout=None):
    timeout = self.DEFAULT_TIMEOUT if timeout is None else timeout
    deadline = time.time() + timeout
    while self.subprocess.exitcode is None:
      await asyncio.sleep(self.SLEEP_TIME)
      if time.time() > deadline:
        raise errors.EndToEnd.SubprocessTimeoutError


class PublisherWorker(BaseWorker):
  """Represents a Publisher worker subprocess to measure end-to-end latency.

  Be aware that this object may spawn subprocesses (in particular, with the
  start method).

  The lifecycle of this object is:
    - start() to spawn the subprocess
    - publish() to send messages, many times as desired
    - stop() to shutdown the subprocess

  You must await for the result of each async call before sending a new one.
  Not thread-safe.
  """

  def __init__(self, pinned_cpus: Optional[Set[int]] = None):
    super().__init__(publisher.main, pinned_cpus)

  async def publish(self, timeout: Optional[float] = None):
    """Commands the worker to send a message.

    Args:
      timeout: Timeout in seconds. Defaults to BaseWorker.DEFAULT_TIMEOUT.

    Returns:
      An int representing the timestamp registered just after the worker sent
      the message.
    """
    self.subprocess_in_writer.send(protocol.Publish())
    response = await self._read_subprocess_output(protocol.AckPublish, timeout)
    if response.publish_error is not None:
      raise errors.EndToEnd.SubprocessFailedOperationError(
          response.publish_error)
    return response.publish_timestamp


class ReceiverWorker(BaseWorker):
  """Represents a Receiver worker subprocess to measure end-to-end latency.

  Be aware that this object may spawn subprocesses (in particular, with the
  start method).

  The lifecycle of this object is:
    - start() to spawn the subprocess
    - start_consumption() to send messages, many times as desired (usually
      before another Publisher.publish() call)
    - receive() up to once per start_consumption() call
    - stop() to shutdown the subprocess

  You must await for the result of each async call before sending a new one.
  Not thread safe.
  """

  def __init__(self, pinned_cpus: Optional[Set[int]] = None):
    super().__init__(receiver.main, pinned_cpus)

  async def start_consumption(self, timeout: Optional[float] = None):
    """Commands the worker to start polling.

    Then waits for the worker's ACK.

    You shouldn't assume the worker is already polling when the ACK is received.
    It's only guaranteed the worker's ACK will be sent back just before polling.
    To be safe you should wait an extra time.

    Args:
      timeout: Timeout in seconds. Defaults to BaseWorker.DEFAULT_TIMEOUT.
    """
    self.subprocess_in_writer.send(protocol.Consume())
    await self._read_subprocess_output(protocol.AckConsume, timeout)

  async def receive(self, timeout: Optional[float] = None) -> Tuple[int, int]:
    """Awaits the worker to receive a message and returns reported timestamps.

    Args:
      timeout: Timeout in seconds. Defaults to BaseWorker.DEFAULT_TIMEOUT.

    Returns:
      A tuple of ints timestamps in nanoseconds
      (receive_timestamp, ack_timestamp)
    """
    report = await self._read_subprocess_output(protocol.ReceptionReport,
                                                timeout)
    if report.receive_error is not None:
      raise errors.EndToEnd.SubprocessFailedOperationError(report.receive_error)
    return report.receive_timestamp, report.ack_timestamp
