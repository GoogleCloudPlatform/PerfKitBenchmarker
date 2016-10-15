# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing multichase installation and cleanup functions."""

GIT_PATH = 'https://github.com/google/multichase'
GIT_VERSION = '8a00c4006d253c6aa5079f5e702279ee20e0df68'
INSTALL_PATH = 'multichase'


def _Install(vm):
  """Installs the multichase package on the VM."""
  vm.Install('build_tools')
  vm.RemoteCommand('rm -rf {path} && mkdir -p {path}'.format(
      path=INSTALL_PATH))
  vm.RemoteCommand('git clone --recursive {git_path} {dir}'.format(
      dir=INSTALL_PATH, git_path=GIT_PATH))
  vm.RemoteCommand('cd {dir} && git checkout {version} && make'.format(
      dir=INSTALL_PATH, version=GIT_VERSION))


def YumInstall(vm):
  """Installs the multichase package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the multichase package on the VM."""
  _Install(vm)


def Uninstall(vm):
  vm.RemoteCommand('rm -rf {0}'.format(INSTALL_PATH))
