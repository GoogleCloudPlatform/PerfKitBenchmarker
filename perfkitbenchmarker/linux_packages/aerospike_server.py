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

import enum
import logging

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/aerospike/aerospike-server.git'
GIT_TAG = '6.0.0.2'
AEROSPIKE_DIR = '%s/aerospike-server' % linux_packages.INSTALL_DIR

AEROSPIKE_VERSION_NAME_FOR_OS = {
    os_types.UBUNTU2004: 'ubuntu20_amd64',
    os_types.RHEL8: 'el8_amd64',
}


@enum.unique
class AerospikeEdition(enum.Enum):
  """Aerospike Edition constant."""

  COMNUNITY = 'comnunity'
  ENTERPRISE = 'enterprise'


MEMORY = 'memory'
DISK = 'disk'

DEFAULT_VERSION = '6.2.0'
DEFAULT_INSTALL_URL = (
    'https://enterprise.aerospike.com/enterprise/download/server/{}/artifact/{}'
)


# Link could be found here
# https://aerospike.com/download/#servers

_AEROSPIKE_ENTERPRISE_VERSION = flags.DEFINE_string(
    'aerospike_enterprise_version', DEFAULT_VERSION, 'Aerospike version to use'
)

_AEROSPIKE_EDITION = flags.DEFINE_enum_class(
    'aerospike_edition',
    AerospikeEdition.COMNUNITY,
    AerospikeEdition,
    'The type of edition aerospike uses.',
)
flags.DEFINE_enum(
    'aerospike_storage_type',
    MEMORY,
    [MEMORY, DISK],
    (
        'The type of storage to use for Aerospike data. The type of '
        'disk is controlled by the "data_disk_type" flag.'
    ),
)

_AEROSPIKE_ZEROIZE_DISK = flags.DEFINE_bool(
    'aerospike_zeroize_disk',
    True,
    (
        'Whether to zeroize the disk. If set to false, it will run blkddiscard'
        ' to zeroize the header. If set to true, it will wipe the full disk.'
    ),
)
flags.DEFINE_integer(
    'aerospike_replication_factor',
    1,
    'Replication factor for aerospike server.',
)
flags.DEFINE_integer(
    'aerospike_service_threads', 4, 'Number of threads per transaction queue.'
)
flags.DEFINE_integer(
    'aerospike_vms', 1, 'Number of vms (nodes) for aerospike server.'
)
flags.DEFINE_integer(
    'aerospike_proto_fd_max',
    80000,
    'Maximum number of allowed file descriptors to be opened in the VM.'
)

MIN_FREE_KBYTES = 1160000


_AEROSPIKE_CONFIGS = {
    AerospikeEdition.COMNUNITY: 'aerospike_community.conf.j2',
    AerospikeEdition.ENTERPRISE: 'aerospike_enterprise.conf.j2',
}


def _GetAerospikeDir(idx=None):
  if _AEROSPIKE_EDITION.value == AerospikeEdition.COMNUNITY:
    if idx is None:
      return f'{linux_packages.INSTALL_DIR}/aerospike-server'
    else:
      return f'{linux_packages.INSTALL_DIR}/{idx}/aerospike-server'
  else:
    return '/etc/aerospike'


def _GetAerospikeConfig(idx=None):
  if _AEROSPIKE_EDITION.value == AerospikeEdition.COMNUNITY:
    return f'{_GetAerospikeDir(idx)}/as/etc/aerospike_dev.conf'
  else:
    return '~/aerospike/aerospike.conf'


def _InstallFromGit(vm):
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, _GetAerospikeDir()))
  # Comment out Werror flag and compile. With newer compilers gcc7xx,
  # compilation is broken due to warnings.
  vm.RemoteCommand(
      'cd {0} && git checkout {1} && git submodule update --init '
      '&& sed -i "s/COMMON_CFLAGS += -Werror/# $COMMON_CFLAGS += -Werror/" '
      '{0}/make_in/Makefile.in '
      '&& make'.format(_GetAerospikeDir(), GIT_TAG)
  )
  for idx in range(FLAGS.aerospike_instances):
    vm.RemoteCommand(
        f'mkdir {linux_packages.INSTALL_DIR}/{idx}; '
        f'cp -rf {_GetAerospikeDir()} {_GetAerospikeDir(idx)}'
    )


def _InstallFromPackage(vm):
  """Installs the aerospike_server package on the VM."""
  if FLAGS.aerospike_instances != 1:
    raise NotImplementedError(
        'Only support one instance of aerospike on enterprise'
    )
  if FLAGS.os_type not in AEROSPIKE_VERSION_NAME_FOR_OS:
    raise ValueError(
        f'Unsupported OS type: {FLAGS.os_type}. Supported OS types are: '
        + ', '.join(AEROSPIKE_VERSION_NAME_FOR_OS.keys())
    )
  # https://docs.aerospike.com/server/operations/install/linux/ubuntu
  vm.RemoteCommand(
      'wget -O aerospike.tgz '
      + DEFAULT_INSTALL_URL.format(
          _AEROSPIKE_ENTERPRISE_VERSION.value,
          AEROSPIKE_VERSION_NAME_FOR_OS[FLAGS.os_type],
      )
  )
  # Create log directory
  vm.InstallPackages('python3')
  vm.InstallPackages('dpkg')
  vm.InstallPackages('netcat')
  vm.RemoteCommand('sudo mkdir -p /var/log/aerospike')

  vm.RemoteCommand('mkdir -p aerospike')
  vm.RemoteCommand('tar -xvf aerospike.tgz -C aerospike --strip-components=1')
  vm.RemoteCommand('cd ./aerospike && sudo ./asinstall')
  # lincense key file needs to be in prepvosional data directory
  vm.DownloadPreprovisionedData('./aerospike', 'aerospike', 'features.conf')
  vm.RemoteCommand(
      'sudo mv ./aerospike/features.conf /etc/aerospike/features.conf'
  )


def _Install(vm):
  """Installs the Aerospike server on the VM."""
  vm.Install('build_tools')
  vm.Install('lua5_1')
  vm.Install('openssl')
  vm.Install('wget')
  if _AEROSPIKE_EDITION.value == AerospikeEdition.COMNUNITY:
    _InstallFromGit(vm)
  else:
    _InstallFromPackage(vm)


def YumInstall(vm):
  """Installs the aerospike_server package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the aerospike_server package on the VM."""
  vm.InstallPackages('netcat-openbsd zlib1g-dev')
  _Install(vm)


@vm_util.Retry(
    poll_interval=5,
    timeout=300,
    retryable_exceptions=(errors.Resource.RetryableCreationError),
)
def _WaitForServerUp(server, idx=None):
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
    idx: aerospike process index.

  Raises:
    errors.Resource.RetryableCreationError when response is not 'ok' or if there
      is an error connecting to the telnet port or otherwise running the remote
      check command.
  """
  port = f'{idx + 3}003'
  logging.info('Trying to connect to Aerospike at port %s', port)
  try:
    out, _ = server.RemoteCommand(
        f'sudo asinfo -v "status -p {port}"', ignore_failure=True
    )
    if out.startswith('ok'):
      logging.info('Aerospike server status is OK. Server up and running.')
      return
  except errors.VirtualMachine.RemoteCommandError as e:
    raise errors.Resource.RetryableCreationError(
        'Aerospike server not up yet: %s.' % str(e)
    )
  else:
    raise errors.Resource.RetryableCreationError(
        "Aerospike server not up yet. Expected 'ok' but got '%s'." % out
    )
  finally:
    server.RemoteCommand('sudo systemctl status aerospike')


def WipeDisk(server, devices):
  """Wipe the disk on the server."""
  for scratch_disk in server.scratch_disks:
    if scratch_disk.mount_point:
      server.RemoteCommand(
          f'sudo umount {scratch_disk.mount_point}', ignore_failure=True
      )

  # https://docs.aerospike.com/server/operations/plan/ssd/ssd_init
  # Expect to exit 1 with `No space left on device` error.
  def _WipeDevice(device):
    if _AEROSPIKE_ZEROIZE_DISK.value:
      server.RobustRemoteCommand(
          f'sudo dd if=/dev/zero of={device} bs=1M', ignore_failure=True
      )
    else:
      server.RobustRemoteCommand(f'sudo blkdiscard {device}')
      server.RobustRemoteCommand(f'sudo blkdiscard -z --length 8MiB {device}')

    background_tasks.RunThreaded(_WipeDevice, devices)

  @vm_util.Retry(
      poll_interval=5,
      timeout=300,
      retryable_exceptions=(errors.Resource.RetryableCreationError),
  )
  def _ZeroizeHeader(device):
    try:
      server.RemoteCommand(f'sudo sudo blkdiscard -z --length 8MiB {device}')
    except errors.Resource.RetryableCreationError as e:
      raise errors.VirtualMachine.RemoteCommandError(
          f'Device {device} header not zeroized: {e}.'
      )

  for device in devices:
    _ZeroizeHeader(device)


def BuildAndStartCommunityAerospike(server, idx):
  """Build and start commmunity version of aerospike."""
  server.RemoteCommand(f'cd {_GetAerospikeDir(idx)} && make init')
  # Persist the nohup command past the ssh session
  # "sh -c 'cd /wherever; nohup ./whatever > /dev/null 2>&1 &'"
  log_file = f'~/aerospike-{server.name}-{idx}.log'
  cmd = (
      f"sh -c 'cd {_GetAerospikeDir(idx)} && nohup sudo make start > "
      f"{log_file} 2>&1 &'"
  )
  server.RemoteCommand(cmd)
  server.PullFile(vm_util.GetTempDir(), log_file)
  _WaitForServerUp(server, idx)
  logging.info('Aerospike server configured and started.')
  server.RemoteCommand(
      'sudo asadm -e "show best-practices"'
  )
  # In certain cases where file descriptor is not enough, the server will
  # hanging when processing disk IOs. Logging to expose more debugging info.
  server.RemoteCommand(
      'sudo cat /var/log/aerospike/aerospike.log | grep "descriptor"'
  )


def RestartEnterpriseAerospike(server):
  server.RemoteCommand('sudo mv ~/aerospike/aerospike.conf /etc/aerospike/')
  server.RemoteCommand('sudo systemctl restart aerospike')
  _WaitForServerUp(server, 0)


def ConfigureAndStart(server, seed_node_ips=None):
  """Prepare the Aerospike server on a VM.

  Args:
    server: VirtualMachine to install and start Aerospike on.
    seed_node_ips: internal IP addresses of seed nodes in the cluster. Leave
      unspecified for a single-node deployment.
  """
  server.Install('aerospike_server')
  seed_node_ips = seed_node_ips or [server.internal_ip]

  if FLAGS.aerospike_storage_type == DISK:
    devices = [
        scratch_disk.GetDevicePath() for scratch_disk in server.scratch_disks
    ]
    WipeDisk(server, devices)
  else:
    devices = []

  # Linux best practice based on:
  # https://docs.aerospike.com/server/operations/install/linux/bestpractices#linux-best-practices
  server.RemoteCommand(
      f'echo {MIN_FREE_KBYTES * FLAGS.aerospike_instances} '
      '| sudo tee /proc/sys/vm/min_free_kbytes'
  )
  server.RemoteCommand('echo 0 | sudo tee /proc/sys/vm/swappiness')
  server.RemoteCommand('ulimit -n %d' % FLAGS.aerospike_proto_fd_max)
  for idx in range(FLAGS.aerospike_instances):
    current_devices = []
    if devices:
      num_device_per_instance = int(len(devices) / FLAGS.aerospike_instances)
      current_devices = devices[
          idx * num_device_per_instance : (idx + 1) * num_device_per_instance
      ]
    server.RenderTemplate(
        data.ResourcePath(_AEROSPIKE_CONFIGS[_AEROSPIKE_EDITION.value]),
        _GetAerospikeConfig(idx),
        {
            'devices': current_devices,
            'port_prefix': 3 + idx,
            'memory_size': int(
                server.total_memory_kb * 0.8 / FLAGS.aerospike_instances
            ),
            'seed_addresses': seed_node_ips,
            'service_threads': FLAGS.aerospike_service_threads,
            'replication_factor': FLAGS.aerospike_replication_factor,
            'proto_fd_max': FLAGS.aerospike_proto_fd_max,
            'node_id': GetNodeId(server),
            'rack_id': GetNodeId(server),
            'enable_strong_consistency': (
                FLAGS.aerospike_enable_strong_consistency
            ),
        },
    )
    if _AEROSPIKE_EDITION.value == AerospikeEdition.COMNUNITY:
      BuildAndStartCommunityAerospike(server, idx)
    else:
      RestartEnterpriseAerospike(server)


def Uninstall(vm):
  del vm


def GetNodeId(vm):
  # Assuming the instance name always follows the patten `pkb-xxxx-x`.
  return vm.name.split('-')[2]


def EnableStrongConsistency(vm, namespaces):
  """Enable the strong consistency feature for the given namespaces.

  Args:
    vm: the vm instance where the Aerospike server is running.
    namespaces: the Aerospike namespace to enable strong consistency.
  """
  # Grant necessary permissions
  vm.RemoteCommand(
      'sudo asadm -e "manage acl grant user admin roles sys-admin data-admin'
      ' read-write" --enable'
  )
  vm.RemoteCommand('sudo systemctl status aerospike')
  # Enable Strong Consistency
  for namespace in namespaces:
    out, _ = vm.RemoteCommand(
        f'sudo asinfo -v "roster:namespace={namespace}" | grep -oE'
        r' "observed_nodes=([@a-zA-Z0-9,]+)[\:;]?"'
    )
    nodes = out.split('=')[1]
    vm.RemoteCommand(
        f'sudo asinfo -v "roster-set:namespace={namespace};nodes={nodes}"'
    )
  vm.RemoteCommand('sudo asinfo -v "recluster:"')
