# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing installation functions for SPEC JBB 2015."""

from absl import flags

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'specjbb2015'
SPEC_JBB_2015_ISO = 'SPECjbb2015-1_03.iso'
SPEC_DIR = 'spec'


def Install(vm) -> None:
  """Prepares a SPEC client by copying SPEC to the VM."""
  mount_dir = 'spec_mnt'
  vm.RemoteCommand(f'mkdir -p {mount_dir} {SPEC_DIR}')
  vm.InstallPreprovisionedBenchmarkData(_BENCHMARK_NAME, [SPEC_JBB_2015_ISO],
                                        '~/')
  vm.RemoteCommand(
      f'sudo mount -t iso9660 -o loop {SPEC_JBB_2015_ISO} {mount_dir}')
  vm.RemoteCommand(f'cp -r {mount_dir}/* {SPEC_DIR}')
  vm.RemoteCommand(f'sudo umount {mount_dir} && sudo rm -rf {mount_dir}')


def Uninstall(vm) -> None:
  """Cleanup Specjbb on the target vm."""
  vm.RemoteCommand(f'sudo umount {SPEC_DIR}', ignore_failure=True)
  vm.RemoteCommand(
      f'rm -rf {SPEC_DIR} {SPEC_JBB_2015_ISO}', ignore_failure=True)


def AptInstall(vm) -> None:
  Install(vm)


def YumInstall(vm) -> None:
  Install(vm)
