# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing Glibc Benchmark installation and cleanup functions."""
from perfkitbenchmarker.linux_packages import INSTALL_DIR

GLIBC_DIR = '%s/glibc' % INSTALL_DIR
GLIBC_TAR = 'glibc-2.27.tar.xz'

BINUTILS_DIR = '%s/binutils' % INSTALL_DIR
BINUTILS_TAR = 'binutils-2.30.tar.gz'


def _Install(vm):
  """Installs the Glibc Benchmark package on the VM."""
  vm.Install('node_js')
  vm.Install('build_tools')
  vm.InstallPackages('bison')

  vm.RemoteCommand('cd {0} && mkdir binutils'.format(INSTALL_DIR))
  vm.RemoteCommand(
      'cd {0} && '
      'wget http://ftp.gnu.org/gnu/binutils/binutils-2.30.tar.gz'.format(
          BINUTILS_DIR))
  vm.RemoteCommand('cd {0} && tar xvf {1}'.format(BINUTILS_DIR, BINUTILS_TAR))
  vm.RemoteCommand('cd {0} && mkdir binutils-build && '
                   'cd binutils-build/ && '
                   '../binutils-2.30/configure --prefix=/opt/binutils && '
                   'make -j 4 && sudo make install'.format(BINUTILS_DIR))

  vm.Install('gcc5')

  vm.RemoteCommand('cd {0} && mkdir glibc'.format(INSTALL_DIR))
  vm.RemoteCommand(
      'cd {0} && '
      'wget https://fossies.org/linux/misc/glibc-2.27.tar.xz'.format(GLIBC_DIR))
  vm.RemoteCommand('cd {0} && tar xvf {1}'.format(GLIBC_DIR, GLIBC_TAR))
  vm.RemoteCommand(
      'cd {0} && mkdir glibc-build && cd glibc-build && '
      '../glibc-2.27/configure --prefix=/usr/local/glibc --disable-profile '
      '--enable-add-ons --with-headers=/usr/include '
      '--with-binutils=/opt/binutils/bin && make && sudo make install'.format(
          GLIBC_DIR))


def YumInstall(vm):
  """Installs the Glibc Benchmark package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Glibc Benchmark package on the VM."""
  _Install(vm)
