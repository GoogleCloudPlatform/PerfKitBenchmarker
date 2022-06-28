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


"""Module containing sysbench installation and cleanup functions."""

GIT_REPO = 'https://github.com/akopytov/sysbench'
# release 1.0.20; committed Apr 24, 2020
RELEASE_TAG = '1.0.20'
SYSBENCH_DIR = '~/sysbench'


def _Install(vm):
  """Installs the sysbench package on the VM."""
  vm.RemoteCommand(
      f'git clone {GIT_REPO} {SYSBENCH_DIR} --branch {RELEASE_TAG}')
  vm.RemoteCommand(f'cd {SYSBENCH_DIR} && ./autogen.sh '
                   '&& ./configure --with-pgsql')
  vm.RemoteCommand(f'cd {SYSBENCH_DIR} && make -j && sudo make install')


def Uninstall(vm):
  """Uninstalls the sysbench package on the VM."""
  vm.RemoteCommand(f'cd {SYSBENCH_DIR} && sudo make uninstall')


def YumInstall(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages(
      'make automake libtool pkgconfig libaio-devel mariadb-devel '
      'openssl-devel postgresql-devel'
  )
  _Install(vm)


def AptInstall(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages(
      'make automake libtool pkg-config libaio-dev libmysqlclient-dev '
      'libssl-dev libpq-dev'
  )
  _Install(vm)
