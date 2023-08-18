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
"""Module containing Jemalloc installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import linux_packages

JE_INSTALL_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'jemalloc/install')
JE_DIR = posixpath.join(JE_INSTALL_DIR, 'jemalloc-5.1.0')
JE_TAR = 'jemalloc-5.1.0.tar.bz2'


def _Install(vm):
  """Installs Jemalloc on the VM."""
  vm.RemoteCommand(f'mkdir -p {JE_INSTALL_DIR}')
  vm.RemoteCommand(
      f'cd {JE_INSTALL_DIR} && '
      'wget'
      f' https://github.com/jemalloc/jemalloc/releases/download/5.1.0/{JE_TAR}'
  )
  vm.RemoteCommand(f'cd {JE_INSTALL_DIR} && tar xvf {JE_TAR}')

  vm.RemoteCommand(
      'export CC=$GCC_INSTALL_DIR/bin/gcc && export'
      f' CXX=$GCC_INSTALL_DIR/bin/g++ && cd {JE_DIR} && ./configure'
      f' --with-lg-page=16 --prefix={JE_INSTALL_DIR} && make && make install'
  )


def YumInstall(vm):
  _Install(vm)


def AptInstall(vm):
  _Install(vm)
