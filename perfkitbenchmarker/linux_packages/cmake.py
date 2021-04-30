# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing cmake installation and cleanup functions."""

import logging
from absl import flags
from perfkitbenchmarker import os_types

_CMAKE_KITWARE = flags.DEFINE_bool(
    'cmake_kitware', False,
    'Whether to install cmake from the Kitware repo. Default is to install '
    'from the (possibily outdated) OS distro.')

# Needed to configure the kitware debian repo
_UBUNTU_VERSION_NAMES = {
    os_types.UBUNTU1604: 'xenial',
    os_types.UBUNTU1804: 'bionic',
    os_types.UBUNTU2004: 'focal',
}
_KITWARE_KEY_URL = 'https://apt.kitware.com/keys/kitware-archive-latest.asc'
_KITWARE_DEB_CONFIG = 'deb https://apt.kitware.com/ubuntu/ {version} main'


def _Install(vm):
  """Installs the cmake package on the VM."""
  vm.InstallPackages('cmake')
  text, _ = vm.RemoteCommand('cmake --version')
  logging.info('Installed cmake version %s', text.splitlines()[0].split()[-1])


def YumInstall(vm):
  """Installs the cmake package on the VM."""
  if _CMAKE_KITWARE.value:
    raise ValueError('Installing cmake from kitware only supported via apt.')
  _Install(vm)


def AptInstall(vm):
  """Installs the cmake package on the VM."""
  if _CMAKE_KITWARE.value:
    _AddCmakeAptRepo(vm)
  _Install(vm)


def _AddCmakeAptRepo(vm):
  vm.Install('curl')
  vm.RemoteCommand(f'curl --silent {_KITWARE_KEY_URL} | gpg --dearmor | '
                   'sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null')
  repo = _KITWARE_DEB_CONFIG.format(version=_UBUNTU_VERSION_NAMES[vm.OS_TYPE])
  vm.RemoteCommand(f'sudo apt-add-repository "{repo}"')
  vm.AptUpdate()
