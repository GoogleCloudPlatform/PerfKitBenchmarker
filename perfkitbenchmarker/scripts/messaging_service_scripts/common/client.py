"""This module contains the base cloud client class."""
import abc
import base64
from typing import Any, Callable, Type, TypeVar

TIMEOUT = 10

# Workaround for forward declaration
BaseMessagingServiceClientT = TypeVar(
    'BaseMessagingServiceClientT', bound='BaseMessagingServiceClient')


class BaseMessagingServiceClient(metaclass=abc.ABCMeta):
  """Generic Messaging Service Client class.

  This is a base class to all messaging service client interfaces: GCP Cloud
  PubSub, AWS SQS, etc.
  """

  def generate_message(self, seq: int, message_size: int) -> str:
    """Generates a message str.

    This message consists of 2 parts: a 6 chars-long space padded base64 encoded
    representation of an unsigned 32-bit integer (the sequence number), and then
    ASCII spaces for padding.

    Args:
      seq: The sequence number. Must be an unsigned 32-bit integer.
      message_size: The message size in bytes. The minimum is 6 (in order to
        encode the sequence number).

    Returns:
      A str with the structure detailed in the description.
    """
    if message_size < 6:
      raise ValueError('Minimum message_size is 6.')
    encoded_seq_str = base64.b64encode(
        seq.to_bytes(4, byteorder='big', signed=False)).decode('ascii')
    encoded_seq_str = (encoded_seq_str + '      ')[:6]
    message = encoded_seq_str + ' ' * (message_size - 6)
    return message

  def decode_seq_from_message(self, message: Any) -> int:
    """Gets the seq number from the str contents of a message.

    The contents of the message should follow the format used in the output of
    generate_message.

    Args:
      message: The message as returned by pull_message.

    Returns:
      An int. The message sequence number.
    """
    seq_b64 = self._get_first_six_bytes_from_payload(message) + b'=='
    return int.from_bytes(base64.b64decode(seq_b64), 'big', signed=False)

  @abc.abstractmethod
  def _get_first_six_bytes_from_payload(self, message: Any) -> bytes:
    """Gets the first 6 bytes of a message (as returned by pull_message)."""
    pass

  @classmethod
  @abc.abstractmethod
  def from_flags(
      cls: Type[BaseMessagingServiceClientT]
  ) -> BaseMessagingServiceClientT:
    """Gets an actual instance based upon the FLAGS values."""
    pass

  @abc.abstractmethod
  def publish_message(self, message_payload: str) -> Any:
    """Publishes a single message to the messaging service.

    Args:
      message_payload: Message, created by 'generate_message'. This
        message will be the one that we publish/pull from the messaging service.

    Returns:
      Return response to publish a message from the provider. For GCP PubSub
      we make a call to '.publish' that publishes the message and we return
      the result from this call.
    """

  @abc.abstractmethod
  def pull_message(self, timeout: float = TIMEOUT) -> Any:
    """Pulls a single message from the messaging service.

    Args:
      timeout: The number of seconds to wait before timing out.

    Returns:
      Return response to pull a message from the provider. For GCP PubSub
      we make a call to '.pull' that pulls a message and we return
      the result from this call. If it times out, this is guaranteed to return
      None.
    """

  @abc.abstractmethod
  def acknowledge_received_message(self, response: Any):
    """Acknowledge that the pulled message was received.

    It tries to acknowledge the message that was pulled with _pull_message.

    Args:
      response: Response from _pull_message.
    """

  @abc.abstractmethod
  def purge_messages(self) -> None:
    """Purges all the messages for the underlying service."""

  def start_streaming_pull(self, callback: Callable[[Any], None]) -> None:
    """Opens a streaming connection to pull messages asynchronously.

    Implementation is optional. Just make sure that you don't try to run it with
    services that don't support it, because it will fail.

    Args:
      callback: A callable that receives a message argument. It will be called
        each time a message is received and might run in a different thread than
        the main one!
    """
    raise NotImplementedError

  def close(self):
    """Closes the underlying client objects.

    Optional override. By default it does nothing.
    """
