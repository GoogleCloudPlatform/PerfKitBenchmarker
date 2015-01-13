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


"""Module containing unixbench installation and cleanup functions."""

UNIXBENCH_TAR = 'UnixBench5.1.3.tgz'
UNIXBENCH_URL = 'http://byte-unixbench.googlecode.com/files/' + UNIXBENCH_TAR
UNIXBENCH_DIR = 'pkb/UnixBench'


def _Install(vm):
  """Installs the unixbench package on the VM."""
  vm.Install('build_tools')
  vm.RemoteCommand('wget {0} -P pkb'.format(UNIXBENCH_URL))
  vm.RemoteCommand('cd pkb && tar xvzf {0}'.format(UNIXBENCH_TAR))


def YumInstall(vm):
  """Installs the unixbench package on the VM."""
  vm.InstallPackages('libX11-devel mesa-libGL-devel '
                     'perl-Time-HiRes libXext-devel')
  _Install(vm)


def AptInstall(vm):
  """Installs the unixbench package on the VM."""
  vm.InstallPackages('libx11-dev libgl1-mesa-dev libxext-dev')
  _Install(vm)
