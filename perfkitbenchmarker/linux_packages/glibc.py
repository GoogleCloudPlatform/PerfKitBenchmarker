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
import re
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import INSTALL_DIR

PACKAGE_NAME = 'glibc'
GLIBC_DIR = '%s/glibc' % INSTALL_DIR
GLIBC_VERSION = '2.31'
GLIBC_TAR = 'glibc-{}.tar.xz'.format(GLIBC_VERSION)

BINUTILS_DIR = '%s/binutils' % INSTALL_DIR
BINUTILS_VERSION = '2.34'
BINUTILS_TAR = 'binutils-{}.tar.gz'.format(BINUTILS_VERSION)
PREPROVISIONED_DATA = {
    BINUTILS_TAR:
        '53537d334820be13eeb8acb326d01c7c81418772d626715c7ae927a7d401cab3',
    GLIBC_TAR:
        '9246fe44f68feeec8c666bb87973d590ce0137cca145df014c72ec95be9ffd17'
}
PACKAGE_DATA_URL = {
    BINUTILS_TAR: posixpath.join(
        'https://ftp.gnu.org/gnu/binutils', BINUTILS_TAR),
    GLIBC_TAR: posixpath.join(
        'https://fossies.org/linux/misc', GLIBC_TAR)
}

_GCC_VERSION_RE = re.compile(r'gcc\ version\ (.*?)\ ')


def GetGccVersion(vm):
  """Get the currently installed gcc version."""
  _, stderr = vm.RemoteCommand('gcc -v')
  match = _GCC_VERSION_RE.search(stderr)
  if not match:
    raise errors.Benchmarks.RunError('Invalid gcc version %s' % stderr)
  return match.group(1)


def _Install(vm):
  """Installs the Glibc Benchmark package on the VM."""
  # The included version of gcc-7.4 in Ubuntu 1804 does not work out of the box
  # without gcc-snapshot.
  vm.InstallPackages('gcc-snapshot')
  GetGccVersion(vm)
  vm.Install('build_tools')
  # bison and texinfo are required for compiling newer versions of glibc > 2.27.
  vm.InstallPackages('bison texinfo')

  vm.RemoteCommand('cd {0} && mkdir binutils'.format(INSTALL_DIR))
  vm.InstallPreprovisionedPackageData(
      PACKAGE_NAME, [BINUTILS_TAR], BINUTILS_DIR)
  vm.RemoteCommand('cd {0} && tar xvf {1}'.format(BINUTILS_DIR, BINUTILS_TAR))
  vm.RemoteCommand('cd {0} && mkdir binutils-build && '
                   'cd binutils-build/ && '
                   '../binutils-{1}/configure --prefix=/opt/binutils && '
                   'make -j 4 && sudo make install'.format(
                       BINUTILS_DIR, BINUTILS_VERSION))

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
