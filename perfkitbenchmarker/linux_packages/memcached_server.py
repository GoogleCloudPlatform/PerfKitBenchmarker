# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

FLAGS = flags.FLAGS


MEMCACHED_PORT = 11211

flags.DEFINE_integer('memcached_size_mb', 64,
                     'Size of memcached cache in megabytes.')

flags.DEFINE_integer('memcached_num_threads', 4,
                     'Number of worker threads.')


def _Install(vm):
  """Installs the memcached server on the VM."""
  vm.InstallPackages('memcached')
  vm.InstallPackages('libmemcached-tools')


def YumInstall(vm):
  """Installs the memcache package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the memcache package on the VM."""
  _Install(vm)


@vm_util.Retry(poll_interval=5, timeout=300,
               retryable_exceptions=(errors.Resource.RetryableCreationError))
def _WaitForServerUp(vm):
  """Block until the memcached server is up and responsive.

  Will timeout after 5 minutes, and raise an exception. Before the timeout
  expires any exceptions are caught and the status check is retried.

  We check the status of the server by issuing a 'stats' command. This should
  return many lines of form 'STAT <name> <value>' if the server is up and
  running.

  Args:
    vm: VirtualMachine memcached has been installed on.

  Raises:
    errors.Resource.RetryableCreationError when response is not as expected or
      if there is an error connecting to the port or otherwise running the
      remote check command.
  """
  address = vm.internal_ip
  port = MEMCACHED_PORT

  logging.info('Trying to connect to memcached at %s:%s', address, port)
  try:
    out, _ = vm.RemoteCommand(
        '(echo -e "stats\n")| netcat -q 1 %s %s' % (address, port))
    if out.startswith('STAT '):
      logging.info('memcached server stats received. Server up and running.')
      return
  except errors.VirtualMachine.RemoteCommandError as e:
    raise errors.Resource.RetryableCreationError(
        'memcached server not up yet: %s.' % str(e))
  else:
    raise errors.Resource.RetryableCreationError(
        'memcached server not up yet. Expected "STAT" but got "%s".' % out)


def ConfigureAndStart(vm):
  """Prepare the memcached server on a VM.

  Args:
    vm: VirtualMachine to install and start memcached on.
  """
  vm.Install('memcached_server')

  for scratch_disk in vm.scratch_disks:
    vm.RemoteCommand('sudo umount %s' % scratch_disk.mount_point)

  # update security config to allow incoming network
  vm.RemoteCommand(
      'sudo sed -i "s/-l .*/-l 0.0.0.0/g" /etc/memcached.conf')
  # update memory size
  vm.RemoteCommand(
      'sudo sed -i "s/-m .*/-m {size}/g" /etc/memcached.conf'.format(
          size=FLAGS.memcached_size_mb))
  # update default port
  vm.RemoteCommand(
      'sudo sed -i "s/-p .*/-m {port}/g" /etc/memcached.conf'.format(
          port=MEMCACHED_PORT))

  vm.RemoteCommand(
      'echo "-t {threads}" | sudo tee -a /etc/memcached.conf'.format(
          threads=FLAGS.memcached_num_threads))

  # restart the default running memcached to run it with custom configurations.
  vm.RemoteCommand('sudo service memcached restart')

  _WaitForServerUp(vm)
  logging.info('memcached server configured and started.')


def GetVersion(vm):
  """Returns the version of the memcached server installed."""
  results, _ = vm.RemoteCommand('memcached -help |grep -m 1 "memcached"'
                                '| tr -d "\n"')
  return results


def StopMemcached(vm):
  vm.RemoteCommand('sudo service memcached stop')


def FlushMemcachedServer(ip, port):
  vm_util.IssueCommand(
      '(echo -e "flush_all\n" ; sleep 1)| netcat %s %s' % (ip, port))


def AptUninstall(vm):
  """Removes the memcache package on the VM."""
  vm.RemoteCommand('sudo apt-get --purge autoremove -y memcached')
