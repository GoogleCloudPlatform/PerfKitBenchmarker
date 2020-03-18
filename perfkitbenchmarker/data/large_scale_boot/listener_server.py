# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
# Lint as: python3

"""Server to wait for incoming curls from booted VMs for large scale boot.

Large scale boot benchmark spins up launcher vms which, in turn, spins up boot
vms and measure their boot time. The launcher vm is an empty linux vm that
could be on any cloud. In order to ensure portability, the listening server is
http server written in python.

This script is downloaded onto cloud launcher VMs by the benchmark. It will
wait for incoming curl requests from booted vms. When it gets an incoming curl
request, it first double check that the other vm a valid vm, is reachable,
then record the system time in nanoseconds.
"""

import functools
import logging
import multiprocessing
import socket
import subprocess
import sys
import threading
import time
from http import server


# Amount of time in seconds to attempt calling a client VM.
MAX_TIME_SECONDS = 30
# entry to stop processing from the timing queue
_STOP_QUEUE_ENTRY = 'stop'


def ConfirmIPAccessible(client_host, port=22, timeout=MAX_TIME_SECONDS):
  """Confirm the given host's port is accessible and return the access time."""
  netcat_command = 'nc -zv -w 1 {client} {port}'.format(
      client=client_host,
      port=port)
  start_time = time.time()
  while time.time() <= (start_time + timeout):
    p = subprocess.Popen(netcat_command, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    _, stderr = p.communicate()
    if 'open' in stderr.decode('utf-8'):
      # return the system time in nanoseconds
      return 'Pass:%s:%d' % (client_host, time.time()*1e9)

  logging.warning('Could not netcat to port %s on client vm %s.',
                  port, client_host)
  return 'Fail:%s:%d' % (client_host, time.time()*1e9)


def StoreResult(result_str, queue):
  """Stores a given result string to results queue."""
  if result_str:
    queue.put(result_str)


def WriteResultsToFile(results_path, queue):
  """Write anything in results queue to a file."""
  with open(results_path, 'a') as writer:
    while True:
      result = queue.get()
      if result == _STOP_QUEUE_ENTRY:
        logging.info('Told to stop writing to file %s from queue', results_path)
        return
      writer.write('{}\n'.format(result))
      writer.flush()


class RequestHandler(server.BaseHTTPRequestHandler):
  """Request handler for incoming curl requests from booted vms."""

  def __init__(self, pool, launcher, queue, *args, **kwargs):
    """Creates a RequestHandler for a http request received by the server.

    Args:
      pool: multiprocessing process pool object.
      launcher: name string of the launcher vm that the server is on.
      queue: multiprocessing queue object.
      *args: Other argments to apply to the request handler.
      **kwargs: Keyword arguments to apply to the request handler.
    """
    self.process_pool = pool
    self.launcher = launcher
    self.timing_queue = queue
    # BaseHTTPRequestHandler calls do_GET inside __init__
    # So we have to call super().__init__ after setting attributes.
    super(RequestHandler, self).__init__(*args, **kwargs)

  def do_GET(self):  # pylint: disable=g-bad-name
    """Process GET requests."""
    self.send_response(200)
    self.send_header('Content-type', 'text/plain')
    self.end_headers()
    self.wfile.write(bytes('OK', 'UTF-8'))

    # Check that we are not getting random curls on the internet.
    client_host = self.client_address[0]
    client_check_str = self.headers.get('X-Header', None)
    if client_check_str != self.launcher:
      logging.error('Got curl with unknown X-Header: %s', client_check_str)
      self.shutdown()
      return

    # Process this client
    logging.info(client_host)
    store_results_func = functools.partial(StoreResult, queue=self.timing_queue)
    self.process_pool.apply_async(ConfirmIPAccessible,
                                  args=(client_host,),
                                  callback=store_results_func)

  def shutdown(self):
    """Shut down the server."""
    t = threading.Thread(target=self.server.shutdown)
    logging.info('Server shut down.')
    t.start()


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  if len(sys.argv) != 3:
    raise ValueError('Got unexpected number of command-line arguments. '
                     'There should be 2 command-line arguments, first being '
                     'the server port and second being the results file path.')
  server_address = ('', int(sys.argv[1]))
  results_file_path = sys.argv[2]
  hostname = socket.gethostname()
  process_pool = multiprocessing.Pool()
  multiprocessing_manager = multiprocessing.Manager()
  timing_queue = multiprocessing_manager.Queue()

  # Start the worker to move results from queue to file first.
  process_pool.apply_async(WriteResultsToFile,
                           args=(results_file_path, timing_queue,))

  # The start the server to listen and put results on queue.
  handler = functools.partial(
      RequestHandler, process_pool, hostname, timing_queue)
  listener = server.HTTPServer(server_address, handler)
  logging.info('Starting httpserver...\n')
  try:
    listener.serve_forever()
  except KeyboardInterrupt:
    logging.info('^C received, shutting down server')
    listener.server_close()
  timing_queue.add(_STOP_QUEUE_ENTRY)
