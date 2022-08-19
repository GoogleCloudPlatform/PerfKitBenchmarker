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
from http import server
import logging
import multiprocessing
import os
import subprocess
import sys
import threading
import time


# Amount of time in seconds to attempt calling a client VM if VM calling in.
MAX_TIME_SECONDS = 30
# Amount of time in seconds to attempt calling a client VM if VM not calling in.
MAX_TIME_SECONDS_NO_CALLING = 1200
# entry to stop processing from the timing queue
_STOP_QUEUE_ENTRY = 'stop'
# Tag for undefined hostname, should be synced with large_scale_boot_benchmark.
UNDEFINED_HOSTNAME = 'UNDEFINED'
# Tag for sequential hostname, should be synced with large_scale_boot_benchmark.
SEQUENTIAL_IP = 'SEQUENTIAL_IP'
# Multiplier for nanoseconds
NANO = 1e9


def ConfirmIPAccessible(client_host, port, timeout=MAX_TIME_SECONDS):
  """Confirm the given host's port is accessible and return the access time."""
  netcat_command = 'nc -zv -w 1 {client} {port}'.format(
      client=client_host,
      port=port)
  start_time = time.time()
  while time.time() <= (start_time + timeout):
    p = subprocess.Popen(netcat_command, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    _, stderr = p.communicate()
    # different versions of netcat uses different stderr strings.
    if any(word in stderr.decode('utf-8') for word in ['open', 'succeeded']):
      # return the system time in nanoseconds
      return 'Pass:%s:%d' % (client_host, time.time() * NANO)

  logging.warning('Could not netcat to port %s on client vm %s.',
                  port, client_host)
  return 'Fail:%s:%d' % (client_host, time.time() * NANO)


def WaitForRunningStatus(client_host, timeout=MAX_TIME_SECONDS):
  """Wait for the VM to report running status.

  Status command generated from data/large_scale_boot/vm_status.sh.jinja2.

  Args:
    client_host: client host to check for running status.
    timeout: Max timeout to wait before declaring failure.

  Returns:
    host status string.
  """
  with open('/tmp/pkb/vm_status.sh', 'r') as reader:
    command = reader.read()
  start_time = time.time()
  while time.time() <= (start_time + timeout):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,
                         universal_newlines=True, stderr=subprocess.PIPE)
    status, _ = p.communicate()
    if 'running' in status.lower():
      return 'Running:%s:%d' % (client_host, time.time() * NANO)

  logging.warning('Client vm %s not running yet.', client_host)
  return 'Fail:%s:%d' % (client_host, time.time() * NANO)


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


def BuildHostNames(name_pattern, count, use_public_ip):
  """Derieve host names from either name pattern or boot logs.

  See large_scale_boot benchmark for name_pattern. For example, SEQUENTIAL_IP
  name pattern is in the form of 'SEQUENTIAL_IP_{public_dns}_{start_index}'.

  Args:
    name_pattern: Name pattern to build host names with.
    count: count of vms.
    use_public_ip: hostnames should be public ip.

  Returns:
    hostnames or host ips to access.
  """
  if name_pattern == UNDEFINED_HOSTNAME:
    return WaitForHostNames(use_public_ip)
  elif SEQUENTIAL_IP in name_pattern:
    public_dns = name_pattern.split('_')[-2]
    start_vm_index = int(name_pattern.split('_')[-1])
    if public_dns:
      return [public_dns.replace('VMID', str(vm_id))
              for vm_id in range(start_vm_index, count + start_vm_index)]
    else:
      return GenerateHostIPs(start_vm_index, count)

  else:
    return [name_pattern.replace('VM_ID', str(vm_id))
            for vm_id in range(1, count + 1)]


def WaitForHostNames(use_public_ip, timeout=MAX_TIME_SECONDS_NO_CALLING):
  """Wait for boot logs to complete and grep the newly created ips.

  After boot_script.sh completes, it will print out [completed].
  In boot_script.sh output, outputs will be of the following formats:

  GCP:
    networkInterfaces[0].accessConfigs[0].natIP: 34.94.81.165
  AWS:
    PRIVATEIPADDRESSES True ip-10-0-0-143.ec2.internal 10.0.0.143
    ASSOCIATION amazon ec2-100-24-107-67.compute-1.amazonaws.com 100.24.107.67

  Args:
    use_public_ip: whether to use public_ip hostname.
    timeout: Amount of time in seconds to wait for boot.
  Returns:
    hosts to netcat.
  """
  start_time = time.time()
  while time.time() <= (start_time + timeout):
    if os.system('grep completed log') != 0:
      time.sleep(1)
      continue
    with open('log', 'r') as f:
      hostnames = []
      for line in f:
        # look for GCP public ip
        if 'natIP' in line:
          hostnames.append(line.split()[1])
        # look for amazon public ip if set
        if use_public_ip and 'ASSOCIATION' in line:
          hostnames.append(line.split()[3])
        # look for amazon private ip if public ip is not set
        if not use_public_ip and 'PRIVATEIPADDRESSES' in line:
          hostnames.append(line.split()[2])
    return set(hostnames)
  raise ValueError('Boot did not complete successfully before timeout of %s '
                   'seconds.' % MAX_TIME_SECONDS_NO_CALLING)


def GenerateHostIPs(boot_vm_index, count):
  """Logic must be aligned with large_scale_boot/boot_script.sh."""
  hostnames = []
  for vm_id in range(boot_vm_index, boot_vm_index + count):
    hostnames.append('10.0.{octet3}.{octet4}'.format(
        octet3=vm_id // 256,
        octet4=vm_id % 256))
  return hostnames


def ActAsClient(pool, queue, port, name_pattern, vms_count, use_public_ip):
  """Use as a client."""
  store_results = functools.partial(StoreResult, queue=queue)
  all_jobs = []
  for host_name in BuildHostNames(name_pattern, vms_count, use_public_ip):
    job = pool.apply_async(
        ConfirmIPAccessible,
        args=(host_name, port, MAX_TIME_SECONDS_NO_CALLING,),
        callback=store_results)
    all_jobs.append(job)
    if vms_count == 1:
      status_job = pool.apply_async(
          WaitForRunningStatus,
          args=(host_name, MAX_TIME_SECONDS_NO_CALLING,),
          callback=store_results)
      all_jobs.append(status_job)
  logging.info([async_job.get() for async_job in all_jobs])
  queue.put(_STOP_QUEUE_ENTRY)


def ActAsServer(pool, queue, port, host_name, listening_server):
  """Use as a server."""
  handler = functools.partial(RequestHandler, pool, host_name, queue, port)
  listener = server.HTTPServer(listening_server, handler)
  logging.info('Starting httpserver...\n')
  try:
    listener.serve_forever()
  except KeyboardInterrupt:
    logging.info('^C received, shutting down server')
    listener.server_close()
  queue.put(_STOP_QUEUE_ENTRY)


class RequestHandler(server.BaseHTTPRequestHandler):
  """Request handler for incoming curl requests from booted vms."""

  def __init__(self, pool, launcher, queue, access_port, *args, **kwargs):
    """Creates a RequestHandler for a http request received by the server.

    Args:
      pool: multiprocessing process pool object.
      launcher: name string of the launcher vm that the server is on.
      queue: multiprocessing queue object.
      access_port: port number to call on the booted vms.
      *args: Other argments to apply to the request handler.
      **kwargs: Keyword arguments to apply to the request handler.
    """
    self.process_pool = pool
    self.launcher = launcher
    self.timing_queue = queue
    self.access_port = access_port
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
                                  args=(client_host, self.access_port,),
                                  callback=store_results_func)

  def shutdown(self):
    """Shut down the server."""
    t = threading.Thread(target=self.server.shutdown)
    logging.info('Server shut down.')
    t.start()


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  if len(sys.argv) != 9:
    raise ValueError('Got unexpected number of command-line arguments. '
                     'There should be at most 7 command-line arguments: '
                     '1. name of the server vm, '
                     '2. server port, '
                     '3. results file, '
                     '4. port to access the boot VMs, '
                     '5. whether to use the listening server, '
                     '6. launched vm naming pattern, '
                     '7. number of launched vms.'
                     '8. whether to use public ip address.')
  hostname = sys.argv[1]
  server_address = ('', int(sys.argv[2]))
  results_file_path = sys.argv[3]
  clients_port = sys.argv[4]
  use_listening_server = sys.argv[5] == 'True'
  vms_name_pattern = sys.argv[6]
  num_vms = int(sys.argv[7])
  using_public_ip = sys.argv[8] == 'True'
  process_pool = multiprocessing.Pool()
  multiprocessing_manager = multiprocessing.Manager()
  timing_queue = multiprocessing_manager.Queue()

  # Start the worker to move results from queue to file first.
  process_pool.apply_async(WriteResultsToFile,
                           args=(results_file_path, timing_queue,))
  if use_listening_server:
    ActAsServer(process_pool, timing_queue, clients_port, hostname,
                server_address)

  # The start the server to listen and put results on queue.
  else:
    ActAsClient(process_pool, timing_queue, clients_port,
                vms_name_pattern, num_vms, using_public_ip)
