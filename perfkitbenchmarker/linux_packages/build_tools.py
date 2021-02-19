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
import logging
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types

FLAGS = flags.FLAGS
flags.DEFINE_string('gcc_version', None, 'Version of gcc to use. Benchmarks '
                    'that utilize gcc compilation should ensure reinstallation '
                    'of GCC. Default is set by the OS package manager.')


def YumInstall(vm):
  """Installs build tools on the VM."""
  vm.InstallPackageGroup('Development Tools')


def AptInstall(vm):
  """Installs build tools on the VM."""
  vm.InstallPackages('build-essential git libtool autoconf automake')
  if FLAGS.gcc_version:
    Reinstall(vm, version=FLAGS.gcc_version)


def GetVersion(vm, pkg):
  """Get version of package using -dumpversion."""
  out, _ = vm.RemoteCommand(
      '{pkg} -dumpversion'.format(pkg=pkg), ignore_failure=True)
  return out.rstrip()


def GetVersionInfo(vm, pkg):
  """Get compiler version info for package using --version."""
  out, _ = vm.RemoteCommand(
      '{pkg} --version'.format(pkg=pkg), ignore_failure=True)
  # return first line of pkg --version
  return out.splitlines()[0] if out else None


def Reinstall(vm, version='4.7'):
  """Install specific version of gcc.

  Args:
    vm: VirtualMachine object.
    version: string. GCC version.
  Raises:
    Error: If this is ran on a non debian based system.
  """
  # TODO(user): Make this work on yum based systems.
  if vm.BASE_OS_TYPE != os_types.DEBIAN:
    raise errors.Error('Updating GCC only works on Debian based systems.')
  vm.Install('ubuntu_toolchain')
  for pkg in ('gcc', 'gfortran', 'g++'):
    version_string = GetVersion(vm, pkg)
    if version in version_string:
      logging.info('Have expected version of %s: %s', pkg, version_string)
      continue
    else:
      new_pkg = pkg + '-' + version
      vm.InstallPackages(new_pkg)
      vm.RemoteCommand('sudo rm -f /usr/bin/{pkg}'.format(pkg=pkg))
      vm.RemoteCommand('sudo ln -s /usr/bin/{new_pkg} /usr/bin/{pkg}'.format(
          new_pkg=new_pkg, pkg=pkg))
      logging.info('Updated version of %s: Old: %s New: %s', pkg,
                   version_string, GetVersion(vm, pkg))
