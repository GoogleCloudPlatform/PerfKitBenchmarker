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
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

MEMCACHED_PORT = 11211

flags.DEFINE_integer('memcached_size_mb', 64,
                     'Size of memcached cache in megabytes.')

flags.DEFINE_integer('memcached_num_threads', 4,
                     'Number of worker threads.')
flags.DEFINE_string('memcached_version', '1.6.9',
                    'Memcached version to use.')

DIR = linux_packages.INSTALL_DIR


def _Install(vm):
  """Installs the memcached server on the VM."""
  vm.InstallPackages('wget')
  vm.Install('build_tools')
  vm.Install('event')
  vm.RemoteCommand(
      f'cd {DIR}; '
      'wget https://www.memcached.org/files/'
      f'memcached-{FLAGS.memcached_version}.tar.gz --no-check-certificate; '
      f'tar -zxvf memcached-{FLAGS.memcached_version}.tar.gz; '
      f'cd memcached-{FLAGS.memcached_version}; '
      './configure && make && sudo make install')


def YumInstall(vm):
  """Installs the memcache package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the memcache package on the VM."""
  _Install(vm)


@vm_util.Retry(poll_interval=5, timeout=300,
               retryable_exceptions=(errors.Resource.RetryableCreationError))
def _WaitForServerUp(vm, port=MEMCACHED_PORT):
  """Block until the memcached server is up and responsive.

  Will timeout after 5 minutes, and raise an exception. Before the timeout
  expires any exceptions are caught and the status check is retried.

  We check the status of the server by issuing a 'stats' command. This should
  return many lines of form 'STAT <name> <value>' if the server is up and
  running.

  Args:
    vm: VirtualMachine memcached has been installed on.
    port: int. Memcached port to use.

  Raises:
    errors.Resource.RetryableCreationError when response is not as expected or
      if there is an error connecting to the port or otherwise running the
      remote check command.
  """
  address = vm.internal_ip

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


def ConfigureAndStart(vm, port=MEMCACHED_PORT, smp_affinity=False):
  """Prepare the memcached server on a VM.

  Args:
    vm: VirtualMachine to install and start memcached on.
    port: int. Memcached port to use.
    smp_affinity: Boolean. Whether or not to set smp_affinity.
  """
  vm.Install('memcached_server')
  if smp_affinity:
    vm.SetSmpAffinity()

  for scratch_disk in vm.scratch_disks:
    vm.RemoteCommand('sudo umount %s' % scratch_disk.mount_point)

  vm.RemoteCommand(
      'ulimit -n 32768; '
      'sudo nohup memcached '
      # Increase maximum memcached server connections
      '-u $USER -c 32768 '
      f'-t {FLAGS.memcached_num_threads} '
      # update default port
      f'-p {port} '
      # update memory size
      f'-m {FLAGS.memcached_size_mb} '
      # update security config to allow incoming network
      '-l 0.0.0.0 -v &> log &')

  _WaitForServerUp(vm, port)
  logging.info('memcached server configured and started.')


def GetVersion(vm):
  """Returns the version of the memcached server installed."""
  results, _ = vm.RemoteCommand('memcached -help |grep -m 1 "memcached"'
                                '| tr -d "\n"')
  return results


def StopMemcached(vm):
  vm.RemoteCommand('sudo pkill -9 memcached', ignore_failure=True)


def FlushMemcachedServer(ip, port):
  vm_util.IssueCommand(
      '(echo -e "flush_all\n" ; sleep 1)| netcat %s %s' % (ip, port))


def AptUninstall(vm):
  """Removes the memcache package on the VM."""
  del vm
