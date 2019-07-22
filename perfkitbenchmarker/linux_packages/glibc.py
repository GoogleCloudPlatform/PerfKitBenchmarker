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
import posixpath
from perfkitbenchmarker.linux_packages import INSTALL_DIR

PACKAGE_NAME = 'glibc'
GLIBC_DIR = '%s/glibc' % INSTALL_DIR
GLIBC_VERSION = '2.29'
GLIBC_TAR = 'glibc-{}.tar.xz'.format(GLIBC_VERSION)

BINUTILS_DIR = '%s/binutils' % INSTALL_DIR
BINUTILS_TAR = 'binutils-2.30.tar.gz'
PREPROVISIONED_DATA = {
    BINUTILS_TAR:
        '8c3850195d1c093d290a716e20ebcaa72eda32abf5e3d8611154b39cff79e9ea',
    GLIBC_TAR:
        'f3eeb8d57e25ca9fc13c2af3dae97754f9f643bc69229546828e3a240e2af04b'
}
PACKAGE_DATA_URL = {
    BINUTILS_TAR: posixpath.join(
        'https://ftp.gnu.org/gnu/binutils', BINUTILS_TAR),
    GLIBC_TAR: posixpath.join(
        'https://fossies.org/linux/misc', GLIBC_TAR)
}


def _Install(vm):
  """Installs the Glibc Benchmark package on the VM."""
  vm.Install('node_js')
  vm.Install('build_tools')
  vm.InstallPackages('bison')

  vm.RemoteCommand('cd {0} && mkdir binutils'.format(INSTALL_DIR))
  vm.InstallPreprovisionedPackageData(
      PACKAGE_NAME, [BINUTILS_TAR], BINUTILS_DIR)
  vm.RemoteCommand('cd {0} && tar xvf {1}'.format(BINUTILS_DIR, BINUTILS_TAR))
  vm.RemoteCommand('cd {0} && mkdir binutils-build && '
                   'cd binutils-build/ && '
                   '../binutils-2.30/configure --prefix=/opt/binutils && '
                   'make -j 4 && sudo make install'.format(BINUTILS_DIR))

  vm.Install('gcc5')

  vm.RemoteCommand('cd {0} && mkdir glibc'.format(INSTALL_DIR))
  vm.InstallPreprovisionedPackageData(
      PACKAGE_NAME, [GLIBC_TAR], GLIBC_DIR)
  vm.RemoteCommand('cd {0} && tar xvf {1}'.format(GLIBC_DIR, GLIBC_TAR))
  vm.RemoteCommand(
      'cd {0} && mkdir glibc-build && cd glibc-build && '
      '../glibc-{1}/configure --prefix=/usr/local/glibc --disable-profile '
      '--enable-add-ons --with-headers=/usr/include '
      '--with-binutils=/opt/binutils/bin && make && sudo make install'.format(
          GLIBC_DIR, GLIBC_VERSION))


def YumInstall(vm):
  """Installs the Glibc Benchmark package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Glibc Benchmark package on the VM."""
  _Install(vm)
