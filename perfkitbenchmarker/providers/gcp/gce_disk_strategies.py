# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing strategies to prepare disks.

This module abstract out the disk algorithm for formatting and creating
scratch disks.
"""
from absl import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies


FLAGS = flags.FLAGS


def GetCreateDiskStrategy(
    vm, disk_spec: disk.BaseDiskSpec, disk_count: int
) -> disk_strategies.CreateDiskStrategy:
  return disk_strategies.EmptyCreateDiskStrategy(vm, disk_spec, disk_count)


class SetUpGcsFuseDiskStrategy(disk_strategies.SetUpDiskStrategy):
  """Strategies to set up ram disks."""

  DEFAULT_MOUNT_OPTIONS = [
      'allow_other',
      'dir_mode=755',
      'file_mode=755',
      'implicit_dirs',
  ]

  def SetUpDiskOnLinux(self, vm, disk_spec):
    """Performs Linux specific setup of ram disk."""
    scratch_disk = disk.BaseDisk(disk_spec)
    vm.Install('gcsfuse')
    vm.RemoteCommand(
        f'sudo mkdir -p {disk_spec.mount_point} && sudo chmod a+w'
        f' {disk_spec.mount_point}'
    )

    opts = ','.join(self.DEFAULT_MOUNT_OPTIONS + FLAGS.mount_options)
    bucket = FLAGS.gcsfuse_bucket
    target = disk_spec.mount_point
    vm.RemoteCommand(f'sudo mount -t gcsfuse -o {opts} {bucket} {target}')
    vm.scratch_disks.append(scratch_disk)
