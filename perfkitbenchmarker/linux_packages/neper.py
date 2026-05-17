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

"""Module containing neper installation and cleanup functions."""

from perfkitbenchmarker import linux_packages

NEPER_DIR = f'{linux_packages.INSTALL_DIR}/neper'
GIT_REPO = 'https://github.com/google/neper.git'
GIT_COMMIT = 'c1419ebdd26ab934e2391da6a89c7323adb4b4fb'


def _Install(vm):
  """Installs the neper package on the VM."""
  vm.Install('build_tools')
  vm.Install('numactl')
  vm.InstallPackages('autoconf automake git')
  vm.RemoteCommand(f'rm -rf {NEPER_DIR} && git clone {GIT_REPO} {NEPER_DIR}')
  vm.RemoteCommand(f'cd {NEPER_DIR} && git checkout {GIT_COMMIT} && make')


def YumInstall(vm):
  """Installs the neper package on the VM for Yum-based systems."""
  _Install(vm)


def AptInstall(vm):
  """Installs the neper package on the VM for Apt-based systems."""
  _Install(vm)


def GetPath():
  """Returns the path to the tcp_rr binary on the VM."""
  return f'{NEPER_DIR}/tcp_rr'
