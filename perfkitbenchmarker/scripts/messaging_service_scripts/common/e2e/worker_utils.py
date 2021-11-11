"""Utils for the worker subprocesses."""
from multiprocessing import connection
from typing import Any, Optional, Type

from perfkitbenchmarker.scripts.messaging_service_scripts.common import errors
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol


class Communicator:
  """Allows easy communication from workers to main process."""

  def __init__(self,
               input_conn: connection.Connection,
               output_conn: connection.Connection):
    self.input_conn = input_conn
    self.output_conn = output_conn

  def send(self, obj: Any):
    """Sends an object to the main process."""
    return self.output_conn.send(obj)

  def greet(self):
    """Greets the main process with a Ready object."""
    self.output_conn.send(protocol.Ready())

  def await_from_main(self,
                      obj_class: Type[Any],
                      ack_obj: Optional[Any] = None) -> Any:
    """Awaits an incoming object from the main process, then returns it.

    Args:
      obj_class: The class the incoming object should be (otherwise raise an
        error).
      ack_obj: Optional. If specified, this object will be sent back to the main
        process once an incoming object has been received.

    Returns:
      The incoming object

    Raises:
      ReceivedUnexpectedObjectError: If it gets an object with an unexpected
      type.
    """
    incoming_obj = self.input_conn.recv()
    if not isinstance(incoming_obj, obj_class):
      raise errors.EndToEnd.ReceivedUnexpectedObjectError(
          f'Unexpected object type: {type(obj_class)!r},'
          f' object: {incoming_obj!r}'
      )
    if ack_obj is not None:
      self.output_conn.send(ack_obj)
    return incoming_obj
