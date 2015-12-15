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

"""Module containing oldisim dependencies installation functions."""

YUM_PACKAGES = ('bc gengetopt libevent-devel '
                'google-perftools-devel scons')
APT_PACKAGES = ('bc gengetopt libevent-dev '
                'libgoogle-perftools-dev scons')
ZYP_PACKAGES = ('bc gengetopt libevent-dev '
                'gperftools-dev scons')

SUSE11_TOOLS_REPO = 'http://download.opensuse.org/repositories/' \
                    'devel:/tools/SLE_11_SP4/devel:tools.repo'
SUSE12_TOOLS_REPO = 'http://download.opensuse.org/repositories/' \
                    'devel:/tools/SLE_12/devel:tools.repo'
SUSE11_PATRICKFREY_REPO = 'http://download.opensuse.org/repositories' \
                          '/home:/PatrickFrey/SLE_11_SP4/home:PatrickFrey.repo'


def YumInstall(vm):
  """Installs oldisim dependencies on the VM."""
  vm.Install('build_tools')
  vm.InstallEpelRepo()
  vm.InstallPackages(YUM_PACKAGES)


def AptInstall(vm):
  """Installs oldisim dependencies on the VM."""
  vm.Install('build_tools')
  vm.InstallPackages(APT_PACKAGES)


def ZypperInstall(vm):
  """Installs oldisim dependencies on the VM."""
  vm.Install('build_tools')
  if vm.GetSUSEVersion() >= 12:
    vm.AddRepository(SUSE12_TOOLS_REPO)
  elif vm.GetSUSEVersion() == 11:
    vm.AddRepository(SUSE11_TOOLS_REPO)
    vm.AddRepository(SUSE11_PATRICKFREY_REPO)
  vm.InstallPackages(ZYP_PACKAGES)
