# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing fortran installation and cleanup functions."""

import logging
from absl import flags

VERSION = flags.DEFINE_integer('fortran_version', None,
                               'Version of gfortran to install')


def GetLibPath(vm):
  """Get fortran library path."""
  out, _ = vm.RemoteCommand('find /usr/lib/ | grep fortran.a')
  return out[:-1]


def YumInstall(vm):
  """Installs the fortran package on the VM."""
  if VERSION.value:
    _YumInstallVersion(vm, VERSION.value)
  else:
    vm.InstallPackages('gcc-gfortran libgfortran')
  _LogFortranVersion(vm)


def AptInstall(vm):
  """Installs the fortan package on the VM."""
  if VERSION.value:
    _AptInstallVersion(vm, VERSION.value)
  else:
    vm.InstallPackages('gfortran')
  _LogFortranVersion(vm)


def _YumInstallVersion(vm, version):
  """Does yum install for the version of fortran."""
  vm.InstallPackages('centos-release-scl-rh')
  devtoolset = f'devtoolset-{version}'
  vm.InstallPackages(f'{devtoolset}-gcc-gfortran')
  # Sets the path to use the newly installed version
  vm.RemoteCommand(f'echo "source scl_source enable {devtoolset}" >> .bashrc')
  # SCL's sudo is broken, remove it to use the normal /bin/sudo
  vm.RemoteCommand(f'sudo rm /opt/rh/{devtoolset}/root/usr/bin/sudo')
  # Normally a gfortran-{version} symlink is installed, create one
  sym_link = f'/usr/bin/gfortran-{version}'
  real_path = f'/opt/rh/{devtoolset}/root/usr/bin/gfortran'
  vm.RemoteCommand(
      f'sudo alternatives --install {sym_link} fortran {real_path} 100')


def _AptInstallVersion(vm, version):
  """Does an apt install for the version of fortran."""
  vm.Install('ubuntu_toolchain')
  vm.InstallPackages(f'gfortran-{version}')
  # make symlink so that 'gfortran' will use the newly installed version
  vm.RemoteCommand('sudo update-alternatives --install /usr/bin/gfortran '
                   f'gfortran /usr/bin/gfortran-{version} 100')


def _LogFortranVersion(vm):
  """Logs the version of gfortran."""
  txt, _ = vm.RemoteCommand('gfortran -dumpversion')
  logging.info('Version of fortran: %s', txt.strip())
