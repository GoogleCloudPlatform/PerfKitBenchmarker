"""Module for the end-to-end latency receiver subprocess for streaming pull."""

import queue
from typing import Any, Optional

from perfkitbenchmarker.scripts.messaging_service_scripts.common import client as common_client
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import receiver
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import worker_utils


class StreamingPullReceiverRunner(receiver.ReceiverRunner):
  """Subclass of ReceiverRunner for StreamingPull."""

  def __init__(
      self,
      client: common_client.BaseMessagingServiceClient,
      communicator: worker_utils.Communicator,
  ):
    super().__init__(client, communicator)
    self.messages_received = queue.Queue()

  def on_message_received(self, message: Any):
    receiver.logger.debug('Receiving message...')
    reception_report = self.process_message(message)
    receiver.logger.debug('Received message (seq=%d)', reception_report.seq)
    self.messages_received.put(reception_report)

  def before_boot(self):
    receiver.logger.debug('Starting streaming pull...')
    self.client.start_streaming_pull(self.on_message_received)

  def await_message_received(self) -> Optional[protocol.ReceptionReport]:
    try:
      return self.messages_received.get(timeout=common_client.TIMEOUT)
    except queue.Empty:
      return None
