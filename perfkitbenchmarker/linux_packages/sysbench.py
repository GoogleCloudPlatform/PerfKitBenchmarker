# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
GIT_TAG = 'df89d34c410a2277e19f77e47e535d0890b2029b'
SYSBENCH_DIR = '~/sysbench'


def _Install(vm):
  """Installs the sysbench package on the VM."""
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, SYSBENCH_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(SYSBENCH_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && make && sudo make install'.format(SYSBENCH_DIR))


def _Uninstall(vm):
  """Uninstalls the sysbench package on the VM."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(SYSBENCH_DIR))


def YumInstall(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallEpelRepo()
  vm.InstallPackages(
      'make automake libtool pkgconfig libaio-devel mariadb-devel '
      'openssl-devel postgresql-devel'
  )
  _Install(vm)


def YumUninstall(vm):
  """Uninstalls sysbench."""
  _Uninstall(vm)


def AptInstall(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages(
      'make automake libtool pkg-config libaio-dev libmysqlclient-dev '
      'libssl-dev libpq-dev'
  )
  _Install(vm)


def AptUninstall(vm):
  """Uninstalls sysbench."""
  _Uninstall(vm)
