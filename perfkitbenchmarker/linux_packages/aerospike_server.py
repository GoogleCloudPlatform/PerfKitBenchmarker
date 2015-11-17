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

import time

from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/aerospike/aerospike-server.git'
GIT_TAG = '3.3.19'
AEROSPIKE_DIR = '%s/aerospike-server' % vm_util.VM_TMP_DIR
AEROSPIKE_CONF_PATH = '%s/as/etc/aerospike_dev.conf' % AEROSPIKE_DIR

MEMORY = 'memory'
DISK = 'disk'
flags.DEFINE_enum('aerospike_storage_type', MEMORY, [MEMORY, DISK],
                  'The type of storage to use for Aerospike data. The type of '
                  'disk is controlled by the "data_disk_type" flag.')


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
    if FLAGS.data_disk_type == disk.LOCAL:
      devices = server.GetLocalDisks()
    else:
      devices = [scratch_disk.GetDevicePath()
                 for scratch_disk in server.scratch_disks]
  else:
    devices = []

  server.RenderTemplate(data.ResourcePath('aerospike.conf.j2'),
                        AEROSPIKE_CONF_PATH,
                        {'devices': devices,
                         'seed_addresses': seed_node_ips})

  for scratch_disk in server.scratch_disks:
    server.RemoteCommand('sudo umount %s' % scratch_disk.mount_point)

  server.RemoteCommand('cd %s && make init' % AEROSPIKE_DIR)
  server.RemoteCommand('cd %s; nohup sudo make start &> /dev/null &' %
                       AEROSPIKE_DIR)
  time.sleep(5)  # Wait for server to come up
