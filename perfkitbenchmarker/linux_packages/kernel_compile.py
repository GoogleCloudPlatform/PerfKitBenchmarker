import os

from perfkitbenchmarker.linux_packages import INSTALL_DIR

URL = 'https://www.kernel.org/pub/linux/kernel/v4.x/linux-4.4.25.tar.gz'
TARBALL = 'linux-4.4.25.tar.gz'
UNTAR_DIR = 'linux-4.4.25'
KERNEL_TARBALL = os.path.join(INSTALL_DIR, TARBALL)


def _Install(vm):
  vm.Install('build_tools')
  vm.Install('wget')
  vm.InstallPackages('bc')
  vm.RemoteCommand('mkdir -p {0} && '
                   'cd {0} && wget {1}'.format(INSTALL_DIR, URL))


def AptInstall(vm):
  _Install(vm)


def YumInstall(vm):
  _Install(vm)


def Cleanup(vm):
  vm.RemoteCommand('cd {} && rm -f {}'.format(INSTALL_DIR, TARBALL))
