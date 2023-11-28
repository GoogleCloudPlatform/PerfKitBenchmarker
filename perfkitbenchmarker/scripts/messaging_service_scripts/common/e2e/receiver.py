"""Module for the receiver subprocess for end-to-end latency measurement."""

import abc
import logging
from multiprocessing import connection
import os
import time
from typing import Any, Iterable, Optional

from absl import flags
from perfkitbenchmarker.scripts.messaging_service_scripts.common import client as common_client
from perfkitbenchmarker.scripts.messaging_service_scripts.common import errors
from perfkitbenchmarker.scripts.messaging_service_scripts.common import log_utils
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import worker_utils

FLAGS = flags.FLAGS


# setting dummy root logger, before serialized_flags are parsed
logger = logging.getLogger('')


class ReceiverRunner:
  """Controls the flow of the receiver process.

  The whole flow is modeled with a state machine. Each state has its own class.
  """

  @classmethod
  def main(
      cls,
      input_conn: connection.Connection,
      output_conn: connection.Connection,
      serialized_flags: str,
      app: Any,
      iterations: Optional[int] = None,
      pinned_cpus: Optional[Iterable[Any]] = None,
  ):
    """Runs the code for the receiver worker subprocess.

    Intended to be called with the multiprocessing.Process stdlib function.

    Args:
      input_conn: A connection object created with multiprocessing.Pipe to read
        data from the main process.
      output_conn: A connection object created with multiprocessing.Pipe to
        write data to the main process.
      serialized_flags: Flags from the main process serialized with
        flags.FLAGS.flags_into_string.
      app: Main process' app instance.
      iterations: Optional. The number of times the main loop will be run. If
        left unset, it will run forever (or until terminated by the main
        process).
      pinned_cpus: Optional. An iterable of CPU IDs to be passed to
        os.sched_setaffinity if set.
    """
    global logger
    if pinned_cpus is not None:
      os.sched_setaffinity(0, pinned_cpus)
    app.promote_to_singleton_instance()
    FLAGS(serialized_flags.splitlines(), known_only=True)
    # setting actual logger, after actual log level in FLAGS is parsed
    logger = log_utils.get_logger(__name__, 'receiver.log')
    client = app.get_client_class().from_flags()
    communicator = worker_utils.Communicator(input_conn, output_conn)
    runner = cls(client, communicator)
    runner.process_state()  # this ensures greeting the main process always
    runner.run(iterations)

  def __init__(
      self,
      client: common_client.BaseMessagingServiceClient,
      communicator: worker_utils.Communicator,
  ):
    self.client = client
    self.communicator = communicator
    self.state = BootingState(self)
    self.received_seqs = set()
    self.iteration_count = 0

  def run(self, iterations=None) -> None:
    iterations = iterations if iterations is not None else float('inf')
    try:
      while self.iteration_count < iterations:
        self.process_state()
    finally:
      self.close()

  def process_state(self) -> None:
    self.state = self.state.process_state()

  def await_message_received(self) -> Optional[protocol.ReceptionReport]:
    """Awaits a message from the messaging service and gets a reception report.

    The default implementation calls self.client.pull_message, and then
    self.process_message to get the reception report, but this may be
    overriden.

    Returns:
      A ReceptionReport object.
    """
    message = self.client.pull_message(common_client.TIMEOUT)
    return self.process_message(message) if message is not None else None

  def process_message(self, message: Any) -> protocol.ReceptionReport:
    """Record the reception time for a message, ACK it (recording its time).

    This code should be thread-safe, as it might get run concurrently
    in a secondary thread (in subclasses).

    Args:
      message: A message object, as gotten by the client.

    Returns:
      A ReceptionReport object with the decoded seq, reception time and
      acknowledge time.
    """

    pull_timestamp = time.time_ns()
    self.client.acknowledge_received_message(message)
    ack_timestamp = time.time_ns()
    seq = self.client.decode_seq_from_message(message)
    return protocol.ReceptionReport(
        seq=seq,
        receive_timestamp=pull_timestamp,
        ack_timestamp=ack_timestamp,
    )

  def got_purge_command(self) -> bool:
    peeked_obj = self.communicator.peek()
    is_purge_cmd = isinstance(peeked_obj, protocol.Purge)
    if is_purge_cmd:
      self.communicator.await_from_main(protocol.Purge)
    return is_purge_cmd

  def register_reception_report(self, reception_report) -> None:
    self.received_seqs.add(reception_report.seq)

  # TODO(odiego): Rename consume to receive
  def is_consume_cmd_fulfilled(self, consume_cmd) -> bool:
    return consume_cmd.seq in self.received_seqs

  def send_reception_report(self, reception_report) -> None:
    self.communicator.send(reception_report)

  def handle_error(self, error: Exception) -> None:
    self.communicator.send(protocol.ReceptionReport(receive_error=repr(error)))

  def notify_purge_done(self) -> None:
    self.communicator.send(protocol.PurgeAck())

  def close(self) -> None:
    self.client.close()

  def before_boot(self) -> None:
    """Runs a start-up action in the BootingState, before greeting main process.

    By default this is a no-op. Intended to be overriden in subclasses if
    needed.
    """
    pass


class ReceiverState(abc.ABC):
  """Base class for all receiver runner states."""

  def __init__(self, receiver_runner: ReceiverRunner):
    self.receiver_runner = receiver_runner

  def get_next_state(self, state_cls, *args, **kwargs):
    return state_cls(self.receiver_runner, *args, **kwargs)

  @abc.abstractmethod
  def process_state(self) -> 'ReceiverState':
    pass


class BootingState(ReceiverState):
  """The initial state.

  In this state, we greet the main process. Signaling that we are ready to work.

  Possible next states:
  - ReadyState
  """

  def process_state(self) -> ReceiverState:
    self.receiver_runner.before_boot()
    self.receiver_runner.communicator.greet()
    logger.debug('Receiver ready!')
    return self.get_next_state(ReadyState)


class ReadyState(ReceiverState):
  """The state where we are idle, awaiting consume commands from main process.

  Possible next states:
  - PullingState
  """

  # TODO(odiego): Rename consume to receive
  def process_state(self) -> ReceiverState:
    logger.debug('Awaiting for consume request from main...')
    consume_cmd = self.receiver_runner.communicator.await_from_main(
        protocol.Consume, protocol.AckConsume()
    )
    deadline = time.time() + common_client.TIMEOUT
    return self.get_next_state(
        PullingState, consume_cmd=consume_cmd, deadline=deadline
    )


class PullingState(ReceiverState):
  """The state where we pull messages from the messaging service.

  Possible next states:
  - PullingState if pull operation itself got no message (and deadline is
    not exceeded).
  - PullSuccessState if the messaging service pull call was successful.
  - PullErrorState if an error was encountered (or deadline was reached).
  """

  # TODO(odiego): Rename consume to receive
  def __init__(
      self,
      receiver_runner: ReceiverRunner,
      consume_cmd: protocol.Consume,
      deadline: float,
  ):
    super().__init__(receiver_runner)
    self.consume_cmd = consume_cmd
    self.deadline = deadline

  # TODO(odiego): Rename consume to receive
  def process_state(self) -> ReceiverState:
    logger.debug('Getting message (expected_seq=%d)', self.consume_cmd.seq)
    try:
      if time.time() > self.deadline:
        logger.debug(
            'Deadline exceeded for message (expected_seq=%d)',
            self.consume_cmd.seq,
        )
        raise errors.EndToEnd.PullTimeoutOnReceiverError
      reception_report = self.receiver_runner.await_message_received()
    except Exception as e:  # pylint: disable=broad-except
      return self.get_next_state(PullErrorState, error=e)
    else:
      if reception_report is None:
        logger.debug('No message received. Retrying...')
        return self.get_next_state(
            PullingState, consume_cmd=self.consume_cmd, deadline=self.deadline
        )
      return self.get_next_state(
          PullSuccessState,
          consume_cmd=self.consume_cmd,
          deadline=self.deadline,
          reception_report=reception_report,
      )


class PullSuccessState(ReceiverState):
  """State reached after a successful message pull.

  In this step we check if we received a purge message command from the main
  process (might happen if some kind of error was encountered in another
  system component).

  Possible next states:
  - PullingState if there are more messages to pull (according to the
    current consume_cmd).
  - PurgingState if received a purge message from the main process.
  - ReadyState if the consume command was fulfilled.
  """

  # TODO(odiego): Rename consume to receive
  def __init__(
      self,
      receiver_runner: ReceiverRunner,
      consume_cmd: protocol.Consume,
      deadline: float,
      reception_report: protocol.ReceptionReport,
  ):
    super().__init__(receiver_runner)
    self.consume_cmd = consume_cmd
    self.deadline = deadline
    self.reception_report = reception_report

  # TODO(odiego): Rename consume to receive
  def process_state(self) -> ReceiverState:
    logger.debug(
        'Received message (expected_seq=%d)', self.reception_report.seq
    )
    if self.receiver_runner.got_purge_command():
      logger.debug('Got purge command from main!')
      return self.get_next_state(PurgingState, notify_to_main_process=True)
    self.receiver_runner.register_reception_report(self.reception_report)
    if self.receiver_runner.is_consume_cmd_fulfilled(self.consume_cmd):
      logger.debug('Consume command fulfilled for seq=%d', self.consume_cmd.seq)
      self.receiver_runner.send_reception_report(self.reception_report)
      logger.debug('Sent reception report to main!')
      self.receiver_runner.iteration_count += 1
      return self.get_next_state(ReadyState)
    else:
      return self.get_next_state(
          PullingState, consume_cmd=self.consume_cmd, deadline=self.deadline
      )


class PullErrorState(ReceiverState):
  """State reached when pulling from the message service errors out.

  Possible next states:
    - PurgeState
  """

  def __init__(self, receiver_runner: ReceiverRunner, error: Exception):
    super().__init__(receiver_runner)
    self.error = error

  def process_state(self) -> ReceiverState:
    logger.debug('Got an error while pulling a message.', exc_info=self.error)
    self.receiver_runner.handle_error(self.error)
    return self.get_next_state(PurgingState)


class PurgingState(ReceiverState):
  """State representing a purge.

  Possible next states:
    - ReadyState
  """

  def __init__(
      self,
      receiver_runner: ReceiverRunner,
      notify_to_main_process: bool = False,
  ):
    super().__init__(receiver_runner)
    self.notify_to_main_process = notify_to_main_process

  def process_state(self) -> ReceiverState:
    logger.debug('Purging messages in messaging service...')
    self.receiver_runner.client.purge_messages()
    logger.debug('Purged messages in messaging service.')
    if self.notify_to_main_process:
      logger.debug('Notifying finished purge to main process.')
      self.receiver_runner.notify_purge_done()
    self.receiver_runner.iteration_count += 1
    return self.get_next_state(ReadyState)
