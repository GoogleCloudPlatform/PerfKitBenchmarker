# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing memcached server installation and cleanup functions."""

import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import INSTALL_DIR

FLAGS = flags.FLAGS

DOWNLOAD_URL = 'http://memcached.org/files/memcached-1.4.33.tar.gz'
MEMCACHED_DIR_NAME = 'memcached'
MEMCACHED_DIR = '%s/%s' % (INSTALL_DIR, MEMCACHED_DIR_NAME)

MEMCACHED_PORT = 11211

flags.DEFINE_integer('memcached_size_mb', 64,
                     'Size of memcached cache in megabytes.')


def _Install(vm):
  """Installs the memcached server on the VM."""
  vm.Install('build_tools')
  vm.Install('event')
  vm.RemoteCommand('cd {0} && wget {1} -O memcached.tar.gz'.format(
                   INSTALL_DIR, DOWNLOAD_URL))
  out, _ = vm.RemoteCommand('cd %s && tar -xzvf memcached.tar.gz' % INSTALL_DIR)
  # The directory name should be the first line of stdout
  memcached_dir = out.split('\n', 1)[0]
  # Rename the directory to a standard name
  vm.RemoteCommand('cd {0} && mv {1} {2}'.format(
                   INSTALL_DIR, memcached_dir, MEMCACHED_DIR_NAME))
  # Make memcached
  vm.RemoteCommand('cd {0} && ./configure && make'.format(MEMCACHED_DIR))


def YumInstall(vm):
  """Installs the memcache package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the memcache package on the VM."""
  _Install(vm)


@vm_util.Retry(poll_interval=5, timeout=300,
               retryable_exceptions=(errors.Resource.RetryableCreationError))
def _WaitForServerUp(server):
  """Block until the memcached server is up and responsive.

  Will timeout after 5 minutes, and raise an exception. Before the timeout
  expires any exceptions are caught and the status check is retried.

  We check the status of the server by issuing a 'stats' command. This should
  return many lines of form 'STAT <name> <value>\\r\\n' if the server is up and
  running.

  Args:
    server: VirtualMachine memcached has been installed on.

  Raises:
    errors.Resource.RetryableCreationError when response is not as expected or
      if there is an error connecting to the port or otherwise running the
      remote check command.
  """
  address = server.internal_ip
  port = MEMCACHED_PORT

  logging.info("Trying to connect to memcached at %s:%s", address, port)
  try:
    out, _ = server.RemoteCommand(
        '(echo -e "stats\n" ; sleep 1)| netcat %s %s' % (address, port))
    if out.startswith('STAT '):
      logging.info("memcached server stats received. Server up and running.")
      return
  except errors.VirtualMachine.RemoteCommandError as e:
    raise errors.Resource.RetryableCreationError(
        "memcached server not up yet: %s." % str(e))
  else:
    raise errors.Resource.RetryableCreationError(
        "memcached server not up yet. Expected 'STAT' but got '%s'." % out)


def ConfigureAndStart(server):
  """Prepare the memcached server on a VM.

  Args:
    server: VirtualMachine to install and start memcached on.
  """
  server.Install('memcached_server')

  for scratch_disk in server.scratch_disks:
    server.RemoteCommand('sudo umount %s' % scratch_disk.mount_point)

  server.RemoteCommand('cd {mcdir}; ./memcached -m {size} '
                       '&> /dev/null &'.format(
                           mcdir=MEMCACHED_DIR, size=FLAGS.memcached_size_mb))
  _WaitForServerUp(server)
  logging.info("memcached server configured and started.")


def StopMemcached(server):
  out, _ = server.RemoteCommand(
      '(echo -e "quit\n" ; sleep 1)| netcat %s %s' %
      (server.internal_ip, MEMCACHED_PORT))


def FlushMemcachedServer(ip, port):
  vm_util.IssueCommand(
      '(echo -e "flush_all\n" ; sleep 1)| netcat %s %s' % (ip, port))


def Uninstall(vm):
  vm.RemoteCommand('pkill memcached')
  vm.RemoteCommand('rm -rf %s' % MEMCACHED_DIR)
