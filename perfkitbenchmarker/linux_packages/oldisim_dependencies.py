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

import os

YUM_PACKAGES = ('bc gengetopt libevent-devel '
                'google-perftools-devel scons')
APT_PACKAGES = ('bc gengetopt libevent-dev '
                'libgoogle-perftools-dev scons')

OLDISIM_GIT = 'https://github.com/GoogleCloudPlatform/oldisim.git'
OLDISIM_DIR = 'oldisim'
OLDISIM_VERSION = 'v0.1'
BINARY_BASE = 'release/workloads/search'


def _Install(vm, packages):
  vm.Install('build_tools')
  vm.InstallPackages(packages)
  vm.RemoteCommand('git clone --recursive %s' % OLDISIM_GIT)
  vm.RemoteCommand('cd %s && git checkout %s && '
                   'scons -j$(cat /proc/cpuinfo | grep processor | wc -l)' %
                   (OLDISIM_DIR, OLDISIM_VERSION))


def YumInstall(vm):
  """Installs oldisim dependencies on the VM."""
  vm.InstallEpelRepo()
  _Install(vm, YUM_PACKAGES)


def AptInstall(vm):
  """Installs oldisim dependencies on the VM."""
  _Install(vm, APT_PACKAGES)


def Path(name):
  """Returns the path of a file within the package."""
  return os.path.join(OLDISIM_DIR, name)


def BinaryPath(name):
  """Returns the path of a binary within the package."""
  return os.path.join(OLDISIM_DIR, BINARY_BASE, name)
