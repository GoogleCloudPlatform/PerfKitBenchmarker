# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Standardized Receive Flow Steering (RFS) tuning kit."""

import logging
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'rps_sock_flow_entries', 0, 'Global RFS table size (rps_sock_flow_entries).'
)
flags.DEFINE_integer(
    'rps_flow_cnt', 0, 'Per-queue RFS flow count (rps_flow_cnt).'
)

_RFS_GLOBAL_PATH = 'net.core.rps_sock_flow_entries'


def _GetPhysicalInterfaces(vm):
  """Finds physical, non-virtual network interfaces."""
  stdout, _ = vm.RemoteCommand('find /sys/class/net -maxdepth 2 -name device')
  # Expected output: /sys/class/net/eth0/device
  interfaces = []
  for line in stdout.splitlines():
    if line.startswith('/sys/class/net/'):
      iface = line.split('/')[4]
      if iface != 'lo':
        interfaces.append(iface)
  return interfaces


def Configure(vm):
  """Configures RFS on the VM and backs up current settings."""
  if not FLAGS.rps_sock_flow_entries and not FLAGS.rps_flow_cnt:
    return

  # Back up original settings if not already backed up
  if not hasattr(vm, 'rfs_original_settings'):
    vm.rfs_original_settings = {}

    # Backup global
    stdout, _ = vm.RemoteCommand(f'sysctl -n {_RFS_GLOBAL_PATH}')
    vm.rfs_original_settings['global'] = stdout.strip()

    # Backup per-interface/queue
    vm.rfs_original_settings['queues'] = {}
    interfaces = _GetPhysicalInterfaces(vm)
    for iface in interfaces:
      # Find all rx-N/rps_flow_cnt files
      find_cmd = f'find /sys/class/net/{iface}/queues/rx-* -name rps_flow_cnt'
      stdout, _ = vm.RemoteCommand(find_cmd, ignore_failure=True)
      for path in stdout.splitlines():
        val, _ = vm.RemoteCommand(f'cat {path}')
        vm.rfs_original_settings['queues'][path] = val.strip()

  # Apply new settings
  if FLAGS.rps_sock_flow_entries:
    vm.RemoteCommand(
        f'sudo sysctl -w {_RFS_GLOBAL_PATH}={FLAGS.rps_sock_flow_entries}'
    )

  if FLAGS.rps_flow_cnt:
    interfaces = _GetPhysicalInterfaces(vm)
    for iface in interfaces:
      queues_pattern = f'/sys/class/net/{iface}/queues/rx-*/rps_flow_cnt'
      vm.RemoteCommand(
          f'echo {FLAGS.rps_flow_cnt} | sudo tee {queues_pattern}',
          ignore_failure=True,
      )


def Restore(vm):
  """Restores original RFS settings on the VM."""
  if not hasattr(vm, 'rfs_original_settings'):
    logging.info('No RFS settings to restore on %s', vm.name)
    return

  settings = vm.rfs_original_settings

  # Restore global
  vm.RemoteCommand(f"sudo sysctl -w {_RFS_GLOBAL_PATH}={settings['global']}")

  # Restore per-queue
  for path, val in settings['queues'].items():
    vm.RemoteCommand(f'echo {val} | sudo tee {path}', ignore_failure=True)

  del vm.rfs_original_settings


def GetMetadata():
  """Returns RFS metadata based on current flags."""
  metadata = {}
  if FLAGS.rps_sock_flow_entries:
    metadata['rps_sock_flow_entries'] = FLAGS.rps_sock_flow_entries
  if FLAGS.rps_flow_cnt:
    metadata['rps_flow_cnt'] = FLAGS.rps_flow_cnt
  return metadata


def GetSoftnetStat(vm):
  """Reads /proc/net/softnet_stat and returns the rps_flow_steer count (column 10)."""
  # Each line is a CPU. Column 10 (0-indexed 9) is rps_flow_steer.
  stdout, _ = vm.RemoteCommand('cat /proc/net/softnet_stat')
  total_rps_flow_steer = 0
  for line in stdout.splitlines():
    parts = line.split()
    if len(parts) >= 10:
      total_rps_flow_steer += int(parts[9], 16)
  return total_rps_flow_steer
