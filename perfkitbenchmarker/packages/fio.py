# Copyright 2014 Google Inc. All rights reserved.
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

"""Module containing fio installation and cleanup functions."""

FIO_DIR = 'pkb/fio'
GIT_REPO = 'git://git.kernel.dk/fio.git'
GIT_TAG = 'fio-2.1.14'


def _Install(vm):
  """Installs the fio package on the VM."""
  vm.Install('build_tools')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, FIO_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(FIO_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && ./configure && make'.format(FIO_DIR))


def YumInstall(vm):
  """Installs the fio package on the VM."""
  vm.InstallPackages('libaio-devel libaio bc')
  _Install(vm)


def AptInstall(vm):
  """Installs the fio package on the VM."""
  vm.InstallPackages('libaio-dev libaio1 bc')
  _Install(vm)
