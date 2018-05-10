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


"""Module containing build tools installation and cleanup functions."""
from perfkitbenchmarker import os_types


def YumInstall(vm):
  """Installs build tools on the VM."""
  vm.InstallPackageGroup('Development Tools')


def AptInstall(vm):
  """Installs build tools on the VM."""
  vm.InstallPackages('build-essential git libtool autoconf automake')


def _GetVersion(vm, pkg):
  """Get version of package."""
  _, err = vm.RemoteCommand('{pkg} -v'.format(pkg=pkg), ignore_failure=True)
  return err


def Reinstall(vm, version='4.7'):
  """Install specific version of gcc.

  Args:
    vm: VirtualMachine object.
    version: string. GCC version.
  """
  # TODO(user): Make this work on yum based systems.
  if vm.BASE_OS_TYPE != os_types.DEBIAN:
    return
  for pkg in ('gcc', 'gfortran', 'g++'):
    version_string = _GetVersion(vm, pkg)
    if version in version_string:
      continue
    else:
      new_pkg = pkg + '-' + version
      vm.RemoteCommand('sudo apt-get remove {pkg} -y'.format(pkg=pkg),
                       ignore_failure=True)
      vm.InstallPackages(new_pkg)
      vm.RemoteCommand('sudo ln -s /usr/bin/{new_pkg} /usr/bin/{pkg}'.format(
          new_pkg=new_pkg, pkg=pkg))
