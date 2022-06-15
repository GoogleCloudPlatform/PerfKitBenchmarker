"""Classes for communication between subprocesses in end-to-end benchmarks."""

import dataclasses
from typing import Optional


@dataclasses.dataclass
class Ready:
  """Message to signal Ready state to main process."""
  pass


@dataclasses.dataclass
class Publish:
  """Message to signal Publish command to subprocess."""
  seq: int


@dataclasses.dataclass
class AckPublish:
  """Message acknowledging Publish and reporting results to main process."""
  seq: Optional[int] = None
  publish_timestamp: Optional[int] = None
  publish_error: Optional[str] = None


# TODO(odiego): Rename to Receive
@dataclasses.dataclass
class Consume:
  """Message to signal Consume command to subprocess."""
  seq: int


# TODO(odiego): Rename to AckReceive
@dataclasses.dataclass
class AckConsume:
  """Message acknowledging Consume command to main process."""
  pass


@dataclasses.dataclass
class ReceptionReport:
  """Message reporting Consume command results to main process."""
  seq: Optional[int] = None
  receive_timestamp: Optional[int] = None
  ack_timestamp: Optional[int] = None
  receive_error: Optional[str] = None


@dataclasses.dataclass
class Purge:
  """Message to signal a Purge command from the main process."""
  pass


@dataclasses.dataclass
class PurgeAck:
  """Message acknowledging a Purge command to main process."""
  pass
