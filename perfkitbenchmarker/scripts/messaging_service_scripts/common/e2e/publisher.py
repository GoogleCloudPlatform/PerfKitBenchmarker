"""Module for the publisher subprocess for end-to-end latency measurement."""
import itertools
import logging
from multiprocessing import connection
import os
import time
from typing import Any, Iterable, Optional

from absl import flags

from perfkitbenchmarker.scripts.messaging_service_scripts.common import log_utils
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import worker_utils

FLAGS = flags.FLAGS


# setting dummy root logger, before serialized_flags are parsed
logger = logging.getLogger('')


def main(input_conn: connection.Connection,
         output_conn: connection.Connection,
         serialized_flags: str,
         app: Any,
         iterations: Optional[int] = None,
         pinned_cpus: Optional[Iterable[Any]] = None):
  """Runs the code for the publisher worker subprocess.

  Intended to be called with the multiprocessing.Process stdlib function.

  Args:
    input_conn: A connection object created with multiprocessing.Pipe to read
      data from the main process.
    output_conn: A connection object created with multiprocessing.Pipe to write
      data to the main process.
    serialized_flags: Flags from the main process serialized with
      flags.FLAGS.flags_into_string.
    app: Main process' app instance.
    iterations: Optional. The number of times the main loop will be run. If left
      unset, it will run forever (or until terminated by the main process).
    pinned_cpus: Optional. An iterable of CPU IDs to be passed to
      os.sched_setaffinity if set.
  """
  global logger
  if pinned_cpus is not None:
    os.sched_setaffinity(0, pinned_cpus)
  FLAGS(serialized_flags.splitlines(), known_only=True)
  # setting actual logger, after actual log level in FLAGS is parsed
  logger = log_utils.get_logger(__name__, 'publisher.log')
  client = app.get_client_class().from_flags()
  communicator = worker_utils.Communicator(input_conn, output_conn)
  communicator.greet()
  times_iterable = itertools.repeat(0) if iterations is None else range(
      iterations)
  for _ in times_iterable:
    publish_obj = communicator.await_from_main(protocol.Publish)
    message_payload = client.generate_message(
        publish_obj.seq, FLAGS.message_size)
    publish_timestamp = time.time_ns()
    try:
      client.publish_message(message_payload)
    except Exception as e:  # pylint: disable=broad-except
      communicator.send(protocol.AckPublish(publish_error=repr(e)))
    else:
      communicator.send(
          protocol.AckPublish(publish_timestamp=publish_timestamp))
