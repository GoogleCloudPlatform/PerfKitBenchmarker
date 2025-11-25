# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing KVrocks installation and cluster setup functions."""

import logging
import os
from typing import Any, Dict, List, TYPE_CHECKING

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

if TYPE_CHECKING:
  from perfkitbenchmarker import linux_virtual_machine  # pylint: disable=g-import-not-at-top


class KvrocksError(errors.Error):
  """Base error for KVrocks."""


class KvrocksNodeIdNotSetError(KvrocksError):
  """Error raised when the node id is not set."""


class KvrocksClusterNotReadyError(KvrocksError):
  """Error raised when the cluster is not ready."""


class KvrocksInstanceNotUpError(KvrocksError):
  """Error raised when a kvrocks instance is not up."""


FLAGS = flags.FLAGS

flags.DEFINE_boolean(
    'kvrocks_enable_profiling',
    True,
    'If true, configure kvrocks nodes for profiling.',
)

DEFAULT_PORT = 6380
KVROCKS_DIR = '~/kvrocks'
KVROCKS_BINARY_PATH = os.path.join(KVROCKS_DIR, 'kvrocks')
KVROCKS_EXPORTER_BINARY_PATH = os.path.join(KVROCKS_DIR, 'kvrocks_exporter')
NUM_KV_INSTANCES = 4
INSTANCE_TO_PORT_MAP = {
    i: DEFAULT_PORT + i - 1 for i in range(1, NUM_KV_INSTANCES + 1)
}
INSTANCE_TO_KEY_RANGE = {
    1: '0-4095',
    2: '4096-8191',
    3: '8192-12287',
    4: '12288-16383',
}

# These paths are relative to the runfiles dir after adding to BUILD data deps
KVROCKS_CONF_JINJA = (
    'third_party/py/perfkitbenchmarker/data/kvrocks/kvrocks.conf.jinja2'
)


def GetPassword() -> str:
  """Generate a password for this session."""
  password = FLAGS.run_uri + '_P3rfk1tbenchm4rker'
  return password


def ConfigureNodeForProfiling(vm):
  """Configures a KVrocks node for profiling."""
  RunRedisCommand(vm, 'CONFIG SET profiling-sample-commands hmset')
  RunRedisCommand(vm, 'CONFIG SET profiling-sample-ratio 1')
  RunRedisCommand(vm, 'CONFIG SET profiling-sample-record-threshold-ms 20')


def Install(vm: 'linux_virtual_machine.BaseLinuxVirtualMachine') -> None:
  """Installs KVrocks on the VM."""
  vm.RemoteCommand('sudo dnf clean all')
  # For Rocky Linux 9, crb must be enabled for "Development Tools"
  vm.RemoteCommand('sudo dnf install -y dnf-plugins-core')
  vm.RemoteCommand('sudo dnf config-manager --set-enabled crb')
  vm.RemoteCommand('sudo dnf update -y')
  vm.InstallPackages('python3 python3-pip wget docker lvm2 redis fio iproute')
  vm.RemoteCommand('pip install jinja2')

  # Use precompiled binaries
  vm.RemoteCommand(f'mkdir -p {KVROCKS_DIR}')
  vm.PushDataFile(KVROCKS_BINARY_EXPERIMENTAL, KVROCKS_BINARY_PATH)
  vm.RemoteCommand(f'chmod +x {KVROCKS_BINARY_PATH}')
  vm.PushDataFile(
      KVROCKS_EXPORTER_BINARY_EXPERIMENTAL, KVROCKS_EXPORTER_BINARY_PATH
  )
  vm.RemoteCommand(f'chmod +x {KVROCKS_EXPORTER_BINARY_PATH}')

  vm.bin_path = KVROCKS_BINARY_PATH

  logging.info('KVrocks binaries placed on %s', vm.name)


@vm_util.Retry(
    max_retries=10,
    poll_interval=1,
    retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,),
)
def _WaitForExporterToStart(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine',
):
  """Waits for the kvrocks_exporter to listen on its port."""
  vm.RemoteCommand("ss -tln | grep -q ':9180'")


def StartExporter(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine',
) -> None:
  """Starts the KVrocks exporter on the given VM."""
  logging.info('Checking presence of scratch disks on %s', vm.name)
  if vm.scratch_disks:
    logging.info('Setting up scratch disks for exporter on %s', vm.name)
  # The exporter logs to /scratch, so make sure it exists.
  vm.RemoteCommand('sudo mkdir -p /scratch')
  vm.RemoteCommand('sudo chmod a+rwx /scratch')

  exporter_cmd = (
      'nohup'
      f" {KVROCKS_EXPORTER_BINARY_PATH} --kvrocks.password='{GetPassword()}'"
      ' --web.listen-address=0.0.0.0:9180 > /scratch/kvrocks_exporter.log'
      ' 2>&1 &'
  )
  vm.RemoteCommand(exporter_cmd)
  try:
    _WaitForExporterToStart(vm)
    logging.info('KVrocks Exporter started on %s', vm.name)
  except errors.VirtualMachine.RemoteCommandError as e:
    log_file = '/scratch/kvrocks_exporter.log'
    try:
      stdout, _ = vm.RemoteCommand(f'cat {log_file}')
      logging.error('KVROCKS EXPORTER LOG FOR %s:\n%s', vm.name, stdout)
    except errors.VirtualMachine.RemoteCommandError:
      logging.error(
          'Could not retrieve kvrocks_exporter log file from %s:%s',
          vm.name,
          log_file,
      )
    raise errors.VirtualMachine.RemoteCommandError(
        f'KVrocks exporter failed to start on {vm.name}.'
    ) from e


def _RenderKvrocksConfig(vm: 'linux_virtual_machine.BaseLinuxVirtualMachine'):
  """Renders and writes KVrocks config files on the VM."""
  vm.PushDataFile(KVROCKS_CONF_JINJA)
  for instance_num, port in INSTANCE_TO_PORT_MAP.items():
    conf_file = f'/scratch/kv-{instance_num}/kvrocks_{port}.conf'
    data_dir = f'/scratch/kv-{instance_num}'
    template_data = {
        'instance_num': instance_num,
        'port': port,
        'password': GetPassword(),
        'bind_ip': vm.internal_ip,
        'data_dir': data_dir,
    }
    vm.RenderTemplate(KVROCKS_CONF_JINJA, conf_file, template_data)
    # The template has a hardcoded data directory. Overwrite it to use the
    # scratch disk.
    vm.RemoteCommand(f"sudo sed -i 's|^dir .*|dir {data_dir}|' {conf_file}")


def _GetKvrocksBinaryPath(vm):
  return getattr(vm, 'bin_path', f'{KVROCKS_DIR}/kvrocks')


def RerunKvrocks(vm: 'linux_virtual_machine.BaseLinuxVirtualMachine'):
  """Stops and starts KVrocks instances."""
  vm.RemoteCommand('killall --wait kvrocks || true')
  for i in range(1, NUM_KV_INSTANCES + 1):
    port = INSTANCE_TO_PORT_MAP[i]
    conf_file = f'/scratch/kv-{i}/kvrocks_{port}.conf'
    bin_path = _GetKvrocksBinaryPath(vm)
    cmd = (
        f'nohup {bin_path} -c {conf_file} > /scratch/kv-{i}/kvrocks.log 2>&1 &'
    )
    vm.RemoteCommand(cmd)


def _GetInstanceNodeId(instance: int, node_idx: int, port: int) -> str:
  return f'kvrockskvrockskvrockskvrocksnode{instance}_{node_idx}_{port}'


def _CreateClusterNodesConf(
    server_vms: List['linux_virtual_machine.BaseLinuxVirtualMachine'],
):
  """Creates the cluster.conf file content."""
  master_node_idx = 0
  master_ip = server_vms[master_node_idx].internal_ip
  lines = []

  # SCENARIO 1 Topology
  for instance in range(1, NUM_KV_INSTANCES + 1):
    port = INSTANCE_TO_PORT_MAP[instance]
    node_id = _GetInstanceNodeId(instance, master_node_idx, port)
    lines.append(
        f'{node_id} {master_ip} {port} master -'
        f' {INSTANCE_TO_KEY_RANGE[instance]}'
    )

  for slave_idx in range(len(server_vms)):
    if slave_idx == master_node_idx:
      continue
    slave_ip = server_vms[slave_idx].internal_ip
    for instance in range(1, NUM_KV_INSTANCES + 1):
      port = INSTANCE_TO_PORT_MAP[instance]
      slave_node_id = _GetInstanceNodeId(instance, slave_idx, port)
      master_node_id = _GetInstanceNodeId(
          instance, master_node_idx, INSTANCE_TO_PORT_MAP[instance]
      )
      lines.append(f'{slave_node_id} {slave_ip} {port} slave {master_node_id}')

  return '\n'.join(lines)


def _SetupScratchDisks(vm: 'linux_virtual_machine.BaseLinuxVirtualMachine'):
  """Stripe, mount, and create directories on scratch disks."""
  if vm.scratch_disks:
    scratch_disk = vm.scratch_disks[0]  # Assuming a single striped disk
    mount_point = scratch_disk.mount_point
    vm.RemoteCommand(f'sudo mkdir -p {mount_point}/kv-{{1,2,3,4}}')
    vm.RemoteCommand(f'sudo chmod -R a+rwx {mount_point}')
  else:
    logging.warning('No scratch disks found on %s to setup.', vm.name)


@vm_util.Retry(
    max_retries=15,
    poll_interval=2,
    retryable_exceptions=(
        errors.VirtualMachine.RemoteCommandError,
        KvrocksInstanceNotUpError,
    ),
)
def _ValidateInstanceUp(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine', port: int
):
  """Checks if a kvrocks instance is up by running PING."""
  cli_command = (
      f'redis-cli -h {vm.internal_ip} -p {port} -a {GetPassword()} PING'
  )
  stdout, _ = vm.RemoteCommand(cli_command)
  if not stdout or 'PONG' not in stdout:
    raise KvrocksInstanceNotUpError(f'Did not get PONG from {vm.name}:{port}')


def _WaitForAllInstancesUp(vm: 'linux_virtual_machine.BaseLinuxVirtualMachine'):
  """Waits for all kvrocks instances on a VM to be up."""
  for port in INSTANCE_TO_PORT_MAP.values():
    _ValidateInstanceUp(vm, port)


@vm_util.Retry(
    max_retries=15,
    poll_interval=2,
    retryable_exceptions=(
        errors.VirtualMachine.RemoteCommandError,
        KvrocksClusterNotReadyError,
    ),
)
def _CheckClusterReadyOnInstance(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine', port: int
):
  """Checks if cluster is ready on an instance."""
  cli_command = (
      f"redis-cli -h {vm.internal_ip} -p {port} -a '{GetPassword()}' CLUSTER"
      ' INFO'
  )
  stdout, _ = vm.RemoteCommand(cli_command)
  if not stdout or 'cluster_state:ok' not in stdout:
    raise KvrocksClusterNotReadyError(f'Cluster not ready on {vm.name}:{port}.')


def _WaitForClusterReady(vm: 'linux_virtual_machine.BaseLinuxVirtualMachine'):
  """Waits for cluster to be ready on all instances of a VM."""
  for port in INSTANCE_TO_PORT_MAP.values():
    _CheckClusterReadyOnInstance(vm, port)


@vm_util.Retry(
    max_retries=10,
    poll_interval=2,
    retryable_exceptions=(
        errors.VirtualMachine.RemoteCommandError,
        KvrocksNodeIdNotSetError,
    ),
)
def _CheckNodeIdIsSet(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine', vm_idx: int
):
  """Checks if the node ID has been set for all instances on a VM."""
  for instance, port in INSTANCE_TO_PORT_MAP.items():
    expected_node_id = _GetInstanceNodeId(instance, vm_idx, port)
    # Use CLUSTER MYID to get the node's own ID, which works before the
    # cluster is fully initialized.
    current_node_id, _ = RunRedisCommand(vm, 'CLUSTERX MYID', port, retry=False)
    if not current_node_id or current_node_id.strip() != expected_node_id:
      raise KvrocksNodeIdNotSetError(
          f'Node ID for {vm.name}:{port} is'
          f' "{current_node_id.strip() if current_node_id else "None"}",'
          f' expected "{expected_node_id}".'
      )


def StartCluster(
    server_vms: List['linux_virtual_machine.BaseLinuxVirtualMachine'],
) -> None:
  """Creates and starts the KVrocks cluster."""

  background_tasks.RunThreaded(_SetupScratchDisks, server_vms)
  background_tasks.RunThreaded(_RenderKvrocksConfig, server_vms)
  background_tasks.RunThreaded(RerunKvrocks, server_vms)

  # Wait for instances to start
  background_tasks.RunThreaded(_WaitForAllInstancesUp, server_vms)

  # Assign node IDs
  def _SetNodeId(vm, vm_idx):
    for instance, port in INSTANCE_TO_PORT_MAP.items():
      node_id = _GetInstanceNodeId(instance, vm_idx, port)
      RunRedisCommand(vm, f'CLUSTERX SETNODEID {node_id}', port)

  background_tasks.RunThreaded(
      _SetNodeId, [((vm, i), {}) for i, vm in enumerate(server_vms)]
  )
  # Wait for node IDs to be set correctly.
  background_tasks.RunThreaded(
      _CheckNodeIdIsSet, [((vm, i), {}) for i, vm in enumerate(server_vms)]
  )

  # Set cluster topology on all nodes
  topology = _CreateClusterNodesConf(server_vms)

  def _SetTopology(vm):
    for port in INSTANCE_TO_PORT_MAP.values():
      RunRedisCommand(
          vm,
          f'CLUSTERX SETNODES "{topology}" 1',
          port,
      )

  background_tasks.RunThreaded(_SetTopology, server_vms)

  # Wait for cluster to form
  background_tasks.RunThreaded(_WaitForClusterReady, server_vms)
  logging.info('KVrocks cluster started on %s', [vm.name for vm in server_vms])

  if FLAGS.kvrocks_enable_profiling:
    logging.info('Enabling profiling on kvrocks nodes.')
    background_tasks.RunThreaded(ConfigureNodeForProfiling, server_vms)


def RunRedisCommand(
    vm: 'linux_virtual_machine.BaseLinuxVirtualMachine',
    command: str,
    port: int = DEFAULT_PORT,
    retry: bool = True,
) -> tuple[str | None, str | None]:
  """Runs a Redis command on the given VM."""
  cli_command = (
      f"redis-cli -h {vm.internal_ip} -p {port} -a '{GetPassword()}' {command}"
  )

  if not retry:
    return vm.RemoteCommand(cli_command)

  @vm_util.Retry(
      max_retries=5,
      poll_interval=10,
      retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,),
  )
  def _Run():
    return vm.RemoteCommand(cli_command)

  try:
    return _Run()
  except vm_util.RetriesExceededRetryError as e:
    instance_num = -1
    for num, p in INSTANCE_TO_PORT_MAP.items():
      if p == port:
        instance_num = num
        break
    if instance_num != -1:
      log_file = f'/scratch/kv-{instance_num}/kvrocks.log'
      try:
        stdout, _ = vm.RemoteCommand(f'cat {log_file}')
        logging.error('KVROCKS LOG FOR %s PORT %d:\n%s', vm.name, port, stdout)
      except errors.VirtualMachine.RemoteCommandError:
        logging.error(
            'Could not retrieve kvrocks log file from %s:%s',
            vm.name,
            log_file,
        )
    logging.error(
        'Failed to run Redis command on %s:%d after 5 attempts: %s',
        vm.name,
        port,
        command,
    )
    raise errors.VirtualMachine.RemoteCommandError(
        f'Failed to run Redis command on {vm.name}:{port}: {command}.'
        f' Error: {e}'
    ) from e


def GetMetadata() -> Dict[str, Any]:
  """Returns metadata about the KVrocks server."""
  # TODO(user): Add relevant metadata
  return {'kvrocks_version': 'unknown'}


def GetPort() -> int:
  return DEFAULT_PORT
