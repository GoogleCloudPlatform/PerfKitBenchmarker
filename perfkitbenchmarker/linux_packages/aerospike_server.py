# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing aerospike server installation and cleanup functions."""

import logging

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import INSTALL_DIR

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/aerospike/aerospike-server.git'
GIT_TAG = '3.7.5'
AEROSPIKE_DIR = '%s/aerospike-server' % INSTALL_DIR
AEROSPIKE_CONF_PATH = '%s/as/etc/aerospike_dev.conf' % AEROSPIKE_DIR

AEROSPIKE_DEFAULT_TELNET_PORT = 3003

MEMORY = 'memory'
DISK = 'disk'
flags.DEFINE_enum('aerospike_storage_type', MEMORY, [MEMORY, DISK],
                  'The type of storage to use for Aerospike data. The type of '
                  'disk is controlled by the "data_disk_type" flag.')
flags.DEFINE_integer('aerospike_replication_factor', 1,
                     'Replication factor for aerospike server.')
flags.DEFINE_integer('aerospike_transaction_threads_per_queue', 4,
                     'Number of threads per transaction queue.')


def _Install(vm):
  """Installs the Aerospike server on the VM."""
  vm.Install('build_tools')
  vm.Install('lua5_1')
  vm.Install('openssl')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, AEROSPIKE_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1} && git submodule update --init '
                   '&& make'.format(AEROSPIKE_DIR, GIT_TAG))


def YumInstall(vm):
  """Installs the memtier package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the memtier package on the VM."""
  _Install(vm)


@vm_util.Retry(poll_interval=5, timeout=300,
               retryable_exceptions=(errors.Resource.RetryableCreationError))
def _WaitForServerUp(server):
  """Block until the Aerospike server is up and responsive.

  Will timeout after 5 minutes, and raise an exception. Before the timeout
  expires any exceptions are caught and the status check is retried.

  We check the status of the server by connecting to Aerospike's out
  of band telnet management port and issue a 'status' command. This should
  return 'ok' if the server is ready. Per the aerospike docs, this always
  returns 'ok', i.e. if the server is not up the connection will fail or we
  would get no response at all.

  Args:
    server: VirtualMachine Aerospike has been installed on.

  Raises:
    errors.Resource.RetryableCreationError when response is not 'ok' or if there
      is an error connecting to the telnet port or otherwise running the remote
      check command.
  """
  address = server.internal_ip
  port = AEROSPIKE_DEFAULT_TELNET_PORT

  logging.info("Trying to connect to Aerospike at %s:%s" % (address, port))
  try:
    out, _ = server.RemoteCommand(
        '(echo -e "status\n" ; sleep 1)| netcat -q 1 %s %s' % (address, port))
    if out.startswith('ok'):
      logging.info("Aerospike server status is OK. Server up and running.")
      return
  except errors.VirtualMachine.RemoteCommandError as e:
    raise errors.Resource.RetryableCreationError(
        "Aerospike server not up yet: %s." % str(e))
  else:
    raise errors.Resource.RetryableCreationError(
        "Aerospike server not up yet. Expected 'ok' but got '%s'." % out)


def ConfigureAndStart(server, seed_node_ips=None):
  """Prepare the Aerospike server on a VM.

  Args:
    server: VirtualMachine to install and start Aerospike on.
    seed_node_ips: internal IP addresses of seed nodes in the cluster.
      Leave unspecified for a single-node deployment.
  """
  server.Install('aerospike_server')
  seed_node_ips = seed_node_ips or [server.internal_ip]

  if FLAGS.aerospike_storage_type == DISK:
    devices = [scratch_disk.GetDevicePath()
               for scratch_disk in server.scratch_disks]
  else:
    devices = []

  server.RenderTemplate(
      data.ResourcePath('aerospike.conf.j2'), AEROSPIKE_CONF_PATH,
      {'devices': devices,
       'memory_size': int(server.total_memory_kb * 0.8),
       'seed_addresses': seed_node_ips,
       'transaction_threads_per_queue':
       FLAGS.aerospike_transaction_threads_per_queue,
       'replication_factor': FLAGS.aerospike_replication_factor})

  for scratch_disk in server.scratch_disks:
    if scratch_disk.mount_point:
      server.RemoteCommand('sudo umount %s' % scratch_disk.mount_point)

  server.RemoteCommand('cd %s && make init' % AEROSPIKE_DIR)
  server.RemoteCommand('cd %s; nohup sudo make start &> /dev/null &' %
                       AEROSPIKE_DIR)
  _WaitForServerUp(server)
  logging.info("Aerospike server configured and started.")


def Uninstall(vm):
  vm.RemoteCommand('rm -rf %s' % AEROSPIKE_DIR)
