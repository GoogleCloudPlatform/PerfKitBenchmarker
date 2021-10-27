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
from perfkitbenchmarker import os_types

FLAGS = flags.FLAGS
flags.DEFINE_string('gcc_version', None, 'Version of gcc to use. Benchmarks '
                    'that utilize gcc compilation should ensure reinstallation '
                    'of GCC. Default is set by the OS package manager.')
flags.DEFINE_boolean('force_build_gcc_from_source', False, 'Whether to force '
                     'building GCC from source.')
flags.DEFINE_boolean('build_fortran', False, 'Whether to build fortran '
                     'alongside c and c++ when building GCC.')

GCC_TAR = 'gcc-{version}.tar.gz'
GCC_URL = 'https://ftp.gnu.org/gnu/gcc/gcc-{version}/' + GCC_TAR
PREPROVISIONED_DATA = {
    GCC_TAR.format(version='9.2.0'):
        'a931a750d6feadacbeecb321d73925cd5ebb6dfa7eff0802984af3aef63759f4'
}
PACKAGE_DATA_URL = {
    GCC_TAR.format(version='9.2.0'): GCC_URL.format(version='9.2.0')
}


def YumInstall(vm):
  """Installs build tools on the VM."""
  vm.InstallPackageGroup('Development Tools')
  if FLAGS.gcc_version:
    Reinstall(vm, version=FLAGS.gcc_version)


def AptInstall(vm):
  """Installs build tools on the VM."""
  vm.InstallPackages('build-essential git libtool autoconf automake')
  if FLAGS.gcc_version:
    Reinstall(vm, version=FLAGS.gcc_version)


def BuildGccFromSource(vm, gcc_version):
  """Install a specific version of gcc by compiling from source.

  Args:
    vm: VirtualMachine object.
    gcc_version: string. GCC version.

  Taken from: https://gist.github.com/nchaigne/ad06bc867f911a3c0d32939f1e930a11
  """
  if gcc_version == '9' or gcc_version == '9.2':
    gcc_version = '9.2.0'
  logging.info('Compiling GCC %s', gcc_version)

  # build GCC on scratch disks for speed if possible
  build_dir = vm.GetScratchDir() if vm.scratch_disks else 'build_tools'
  gcc_tar = GCC_TAR.format(version=gcc_version)
  if gcc_tar in PREPROVISIONED_DATA:
    vm.InstallPreprovisionedPackageData(
        'build_tools', PREPROVISIONED_DATA.keys(), build_dir)
  else:
    vm.RemoteCommand(f'cd {build_dir} && '
                     f'wget {GCC_URL.format(version=gcc_version)}')
  vm.RemoteCommand(f'cd {build_dir} && tar xzvf {gcc_tar}')
  vm.RemoteCommand(f'cd {build_dir} && mkdir -p obj.gcc-{gcc_version}')
  vm.RemoteCommand(f'cd {build_dir}/gcc-{gcc_version} && '
                   './contrib/download_prerequisites')
  enable_languages = 'c,c++' + (',fortran' if FLAGS.build_fortran else '')
  vm.RemoteCommand(f'cd {build_dir}/obj.gcc-{gcc_version} && '
                   f'../gcc-{gcc_version}/configure '
                   f'--disable-multilib --enable-languages={enable_languages}')
  # TODO(user): Measure GCC compilation time as a benchmark.
  vm.RemoteCommand(f'cd {build_dir}/obj.gcc-{gcc_version} && '
                   f'time make -j {vm.NumCpusForBenchmark()}')
  vm.RemoteCommand(f'cd {build_dir}/obj.gcc-{gcc_version} && sudo make install')
  vm.RemoteCommand('sudo rm -rf /usr/bin/gcc && '
                   'sudo ln -s /usr/local/bin/gcc /usr/bin/gcc')
  vm.RemoteCommand('sudo rm -rf /usr/bin/g++ && '
                   'sudo ln -s /usr/local/bin/g++ /usr/bin/g++')
  if FLAGS.build_fortran:
    vm.RemoteCommand('sudo rm -rf /usr/bin/gfortran && '
                     'sudo ln -s /usr/local/bin/gfortran /usr/bin/gfortran')

  if '11' in gcc_version:
    # https://stackoverflow.com/a/65384705
    vm.RemoteCommand(
        f'sudo cp {build_dir}/obj.gcc-{gcc_version}/x86_64-pc-linux-gnu/'
        'libstdc++-v3/src/.libs/* /usr/lib/x86_64-linux-gnu/',
        ignore_failure=True)
    vm.RemoteCommand(
        f'sudo cp {build_dir}/obj.gcc-{gcc_version}/aarch64-unknown-linux-gnu/'
        'libstdc++-v3/src/.libs/* /usr/lib/aarch64-linux-gnu/',
        ignore_failure=True)


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


def Reinstall(vm, version: str):
  """Install specific version of gcc.

  Args:
    vm: VirtualMachine object.
    version: string. GCC version.
  """
  if vm.BASE_OS_TYPE != os_types.DEBIAN or FLAGS.force_build_gcc_from_source:
    BuildGccFromSource(vm, version)
    logging.info('GCC info: %s', GetVersion(vm, 'gcc'))
    return
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
