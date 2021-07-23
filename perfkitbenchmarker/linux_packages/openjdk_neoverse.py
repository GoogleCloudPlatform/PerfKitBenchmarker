"""Module containing OpenJDK installation for ARM Neoverse."""

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types

FLAGS = flags.FLAGS
GCC_VERSION = 10
NEOVERSE_CFLAGS = ('-mcpu=neoverse-n1 -march=armv8.2-a -mtune=neoverse-n1 '
                   '-lgcc_s -fPIC ')
NEOVERSE_LDFLAGS = '-Wl,--allow-multiple-definition -Wl,-lgcc_s -lgcc_s'


def InstallNeoverseCompiledOpenJDK(vm, jdk_version):
  """Compiles and installs OpenJDK for Neoverse ARM on the VM.

  Installation instructions inpired by
  https://metebalci.com/blog/custom-openjdk-10-builds-on-ubuntu-16.04/

  Args:
     vm:  VM to build OpenJDK on
     jdk_version:  an integer indicating the jdk version to build
  """

  os_type = FLAGS.os_type
  if os_type != os_types.UBUNTU1804:
    raise errors.Config.InvalidValue(f'OS Type must be {os_types.UBUNTU1804},'
                                     f'current os type is {FLAGS.os_type}.')
  if int(jdk_version) != 11:
    raise errors.Config.InvalidValue('OpenJDK Version must begin with 11, '
                                     f'current version is {jdk_version}.')
  vm.Install('ubuntu_toolchain')
  # This command fails without Ubuntu 1804 - gcc-10 isn't available.
  vm.InstallPackages(f'gcc-{GCC_VERSION} g++-{GCC_VERSION}')

  vm.InstallPackages('mercurial autoconf build-essential '
                     'unzip zip libx11-dev libxext-dev libxrender-dev '
                     'libxrandr-dev libxtst-dev libxt-dev libcups2-dev '
                     'libfontconfig1-dev libasound2-dev')
  vm.RemoteCommand('cd /scratch/ && hg clone '
                   f'http://hg.openjdk.java.net/jdk-updates/jdk{jdk_version}u/')

  vm.RemoteCommand(
      f'cd /scratch/jdk{jdk_version}u/ && '
      f'CC=/usr/bin/gcc-{GCC_VERSION} CXX=/usr/bin/g++-{GCC_VERSION} '
      'sh ./configure --disable-warnings-as-errors '
      f'--with-extra-cflags="{NEOVERSE_CFLAGS}" '
      f'--with-extra-cxxflags="{NEOVERSE_CFLAGS}" '
      f'--with-extra-ldflags="{NEOVERSE_LDFLAGS}" '
      f'--with-boot-jdk=/usr/lib/jvm/java-{jdk_version}-openjdk-arm64')
  vm.RemoteCommand(f'cd /scratch/jdk{jdk_version}u/ && make JOBS=50')

  # The build output folder is different for other OpenJDK versions e.g. 13
  build_dir = '/scratch/jdk11u/build/linux-aarch64-normal-server-release'
  for binary in ['java', 'javac']:
    vm.RemoteCommand(f'sudo update-alternatives --install /usr/bin/{binary} '
                     f'{binary} {build_dir}/jdk/bin/{binary} 1')
    vm.RemoteCommand(f'sudo update-alternatives --set {binary} '
                     f'{build_dir}/jdk/bin/{binary}')

    stdout, _ = vm.RemoteCommand(f'{binary} --version', should_log=True)
