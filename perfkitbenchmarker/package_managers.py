# Copyright 2014 Google Inc. All rights reserved.
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #   http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

"""Module containing mixin classes for package management.

These classes allow installation on both Debian and RHEL based linuxes.
They also handle some intial setup (especially on RHEL based linuxes
since by default sudo commands without a tty don't work) and
can restore the VM to the state it was in before packages were
installed.

To install a package on a VM, just call vm.Install(package_name).
The package name is just the name of the package module (i.e. the
file name minus .py). The framework will take care of all cleanup
for you.
"""

import gflags as flags

from perfkitbenchmarker import errors
from perfkitbenchmarker import packages
from perfkitbenchmarker import vm_util


RHEL = 'rhel'
DEBIAN = 'debian'

flags.DEFINE_enum('os_type', DEBIAN,
                  [DEBIAN, RHEL],
                  'The version of linux that the VM\'s os is based on. '
                  'This will determine the package manager used, among '
                  'other things.')


class BasePackageMixin(object):

  def Startup(self):
    """Performs any necessary setup on the VM specific to the OS."""
    pass

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    pass

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    pass

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Deletes the temp directory, restores packages, and uninstalls all
    Perfkit packages.
    """
    for package_name in self._installed_packages:
      self.Uninstall(package_name)
    self.RestorePackages()
    self.RemoteCommand('rm -rf pkb')

  def Install(self, package_name):
    """Installs a Perfkit package on the VM."""
    pass

  def Uninstall(self, package_name):
    """Uninstalls a Perfkit package on the VM."""
    pass

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for Perfkit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    pass

  def GetServiceName(self, package_name):
    """Returns the service name of a Perfkit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    pass


class YumMixin(BasePackageMixin):

  def Startup(self):
    """Eliminates the need to have a tty to run sudo commands."""
    self.RemoteCommand('sudo sed -i "/requiretty/d" /etc/sudoers',
                       login_shell=True)

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand('mkdir -p pkb')
    self.RemoteCommand('rpm -qa > pkb/package_snapshot1')

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand('rpm -qa > pkb/package_snapshot2')
    stdout, _ = self.RemoteCommand(
        'grep -F -x -v -f pkb/package_snapshot1 pkb/package_snapshot2')
    packages = stdout.replace('\n', ' ')
    self.RemoteCommand('sudo rpm -e %s' % packages)

  def InstallPackages(self, packages):
    """Installs packages using the yum package manager."""
    self.RemoteCommand('sudo yum install -y %s' % packages)

  def InstallPackageGroup(self, package_group):
    """Installs a 'package group' using the yum package manager."""
    self.RemoteCommand('sudo yum groupinstall -y "%s"' % package_group)

  def Install(self, package_name):
    """Installs a Perfkit package on the VM."""
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.YumInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a Perfkit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'YumUninstall'):
      package.YumUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for Perfkit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.YumGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a Perfkit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.YumGetServiceName(self)


class AptMixin(BasePackageMixin):

  def Startup(self):
    """Runs apt-get update so InstallPackages shouldn't need to."""
    self.AptUpdate()

  def AptUpdate(self):
    """Updates the package lists on VMs using apt."""
    self.RemoteCommand('sudo apt-get update')

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand('mkdir -p pkb')
    self.RemoteCommand('dpkg --get-selections > pkb/package_snapshot')

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand('sudo dpkg --clear-selections')
    self.RemoteCommand('sudo dpkg --set-selections < pkb/package_snapshot')
    self.RemoteCommand('sudo DEBIAN_FRONTEND=\'noninteractive\' '
                       'apt-get --purge -y dselect-upgrade')

  @vm_util.Retry()
  def InstallPackages(self, packages):
    """Installs packages using the apt package manager."""
    try:
      install_command = ('sudo DEBIAN_FRONTEND=\'noninteractive\' '
                         '/usr/bin/apt-get -y install %s' % (packages))
      self.RemoteCommand(install_command)
    except errors.VmUtil.SshConnectionError as e:
      self.AptUpdate()
      raise e

  def Install(self, package_name):
    """Installs a Perfkit package on the VM."""
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.AptInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a Perfkit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'AptUninstall'):
      package.AptUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for Perfkit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a Perfkit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetServiceName(self)
