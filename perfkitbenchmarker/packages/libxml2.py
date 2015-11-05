# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

import posixpath
from perfkitbenchmarker import vm_util

"""Module containing libxml2 installation and cleanup functions."""


def YumInstall(vm):
  """Installs the libxml2 package on the VM."""
  vm.InstallPackages('libxml2')
  vm.InstallPackages('libxml2-devel')


def AptInstall(vm):
  """Installs the libxml2 package-version 2.7.6 on the VM."""
  vm.InstallPackages('gcc build-essential')
  vm.RemoteCommand('cd %s && wget xmlsoft.org/sources/libxml2-2.7.6.tar.gz && '
                   'tar xzf libxml2-2.7.6.tar.gz && cd libxml2-2.7.6 && '
                   './configure && make && sudo make install'
                   % posixpath.join(vm_util.VM_TMP_DIR, 'web-release'))
