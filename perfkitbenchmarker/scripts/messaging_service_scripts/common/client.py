"""This module contains the base cloud client class."""
import abc
import random
import string
from typing import Any, Type, TypeVar

MESSAGE_CHARACTERS = string.ascii_letters + string.digits
TIMEOUT = 10

# Workaround for forward declaration
BaseMessagingServiceClientT = TypeVar(
    'BaseMessagingServiceClientT', bound='BaseMessagingServiceClient')


class BaseMessagingServiceClient(metaclass=abc.ABCMeta):
  """Generic Messaging Service Client class.

  This is a base class to all messaging service client interfaces: GCP Cloud
  PubSub, AWS SQS, etc.
  """

  def generate_random_message(self, message_size: int) -> str:
    message = ''.join(
        random.choice(MESSAGE_CHARACTERS) for _ in range(message_size))
    return message

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
      message_payload: Message, created by 'generate_random_message'. This
        message will be the one that we publish/pull from the messaging service.

    Returns:
      Return response to publish a message from the provider. For GCP PubSub
      we make a call to '.publish' that publishes the message and we return
      the result from this call.
    """

  @abc.abstractmethod
  def pull_message(self) -> Any:
    """Pulls a single message from the messaging service.

    Returns:
      Return response to pull a message from the provider. For GCP PubSub
      we make a call to '.pull' that pulls a message and we return
      the result from this call.
    """

  @abc.abstractmethod
  def acknowledge_received_message(self, response: Any):
    """Acknowledge that the pulled message was received.

    It tries to acknowledge the message that was pulled with _pull_message.

    Args:
      response: Response from _pull_message.
    """

  def close(self):
    """Closes the underlying client objects.

    Optional override. By default it does nothing.
    """
