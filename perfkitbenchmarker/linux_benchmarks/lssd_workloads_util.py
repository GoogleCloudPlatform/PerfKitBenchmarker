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
"""Utility functions for VM local SSD (LSSD) preconditioning and setup."""

import logging
from absl import flags
from perfkitbenchmarker import linux_virtual_machine

LSSD_WORKLOAD_PRECONDITION_DISK = flags.DEFINE_bool(
    'lssd_workload_precondition_disk',
    False,
    'Whether to precondition the LSSD disk on a VM using FIO and format it.',
)


def PreconditionDisk(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
) -> None:
  """Preconditions the LSSD disk on a VM using FIO.

  Args:
    vm: The virtual machine instance.
  """

  disk = vm.scratch_disks[0]

  logging.info('Prefilling disk with FIO...')

  # Unmount the disk before running blkdiscard to avoid "Device or resource
  # busy" error.
  # Striped disk mounted at both DATA_DIR and /mnt/md0
  logging.info('Unmounting disk for raw FIO prefill...')
  vm.RemoteCommand(f'sudo umount -l {disk.mount_point}', ignore_failure=True)
  vm.RemoteCommand(
      f'sudo umount -l {disk.device_path}', ignore_failure=True
  )

  logging.info('Discarding blocks on disk %s...', disk.GetDevicePath())
  vm.RemoteCommand(f'sudo blkdiscard -f {disk.device_path}')

  fio_cmd = (
      'sudo fio --ioengine=libaio --direct=1 --rw=write --bs=1M '
      '--iodepth=64 --size=100% --group_reporting '
      f'--name=precondition --filename={disk.device_path}'
  )
  logging.info('Running FIO command: %s', fio_cmd)
  stdout, stderr = vm.RobustRemoteCommand(fio_cmd)
  logging.info('FIO Prefill Summary:\n%s', stdout)
  if stderr:
    logging.warning('FIO Prefill Warnings/Errors:\n%s', stderr)


def CreateFileSystemAndMount(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
    fs_type: str = 'xfs',
) -> None:
  """Creates a filesystem on the LSSD disk and mounts it.

  Args:
    vm: The virtual machine instance.
    fs_type: The filesystem type (e.g. 'xfs' or 'ext4').
  """
  disk = vm.scratch_disks[0]

  logging.info(
      'Formatting disk %s with filesystem type %s with nodiscard',
      disk.device_path,
      fs_type,
  )
  if fs_type == 'xfs':
    # -K in mkfs.xfs means do not discard blocks at mkfs time
    vm.RemoteCommand(f'sudo mkfs.xfs -f -K {disk.device_path}')
  elif fs_type == 'ext4':
    # nodiscard in mke2fs is done via -E nodiscard
    vm.RemoteCommand(
        'sudo mke2fs -F -E lazy_itable_init=0,nodiscard -O '
        f'^has_journal -t ext4 -b 4096 {disk.device_path}'
    )
  else:
    raise ValueError(f'Unsupported filesystem type: {fs_type}')

  logging.info('Remounting disk')
  vm.MountDisk(
      disk.device_path, disk.mount_point
  )

