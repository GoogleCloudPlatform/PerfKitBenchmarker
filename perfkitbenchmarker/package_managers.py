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

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import packages
from perfkitbenchmarker import vm_util


RHEL = 'rhel'
DEBIAN = 'debian'
EPEL_RPM = ('http://dl.fedoraproject.org/pub/epel/'
            '6/x86_64/epel-release-6-8.noarch.rpm')

flags.DEFINE_enum('os_type', DEBIAN,
                  [DEBIAN, RHEL],
                  'The version of linux that the VM\'s os is based on. '
                  'This will determine the package manager used, among '
                  'other things.')


class BasePackageMixin(object):

  def __init__(self):
    self._installed_packages = set()
    super(BasePackageMixin, self).__init__()

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
    PerfKit packages.
    """
    for package_name in self._installed_packages:
      self.Uninstall(package_name)
    self.RestorePackages()
    self.RemoteCommand('rm -rf %s' % vm_util.VM_TMP_DIR)

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    pass

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    pass

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    pass

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    pass


class YumMixin(BasePackageMixin):

  def Startup(self):
    """Eliminates the need to have a tty to run sudo commands."""
    self.RemoteCommand('echo \'Defaults:%s !requiretty\' | '
                       'sudo tee /etc/sudoers.d/pkb' % self.user_name,
                       login_shell=True)
    self.RemoteCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)

  def InstallEpelRepo(self):
    """Installs the Extra Packages for Enterprise Linux repository."""
    self.RemoteCommand('sudo rpm -ivh --force %s' % EPEL_RPM)

  def PackageCleanup(self):
    """Cleans up all installed packages.

    Performs the normal package cleanup, then deletes the file
    added to the /etc/sudoers.d directory during startup.
    """
    super(YumMixin, self).PackageCleanup()
    self.RemoteCommand('sudo rm /etc/sudoers.d/pkb')

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand('rpm -qa > %s/rpm_package_list' % vm_util.VM_TMP_DIR)

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand(
        'rpm -qa | grep --fixed-strings --line-regexp --invert-match --file '
        '%s/rpm_package_list | xargs --no-run-if-empty sudo rpm -e' %
        vm_util.VM_TMP_DIR,
        ignore_failure=True)

  def InstallPackages(self, packages):
    """Installs packages using the yum package manager."""
    self.RemoteCommand('sudo yum install -y %s' % packages)

  def InstallPackageGroup(self, package_group):
    """Installs a 'package group' using the yum package manager."""
    self.RemoteCommand('sudo yum groupinstall -y "%s"' % package_group)

  def Install(self, package_name):
    """Installs a PerfKit package on the VM."""
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.YumInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'YumUninstall'):
      package.YumUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.YumGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

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
    self.RemoteCommand('mkdir -p %s' % vm_util.VM_TMP_DIR)

  def AptUpdate(self):
    """Updates the package lists on VMs using apt."""
    self.RemoteCommand('sudo apt-get update')

  def SnapshotPackages(self):
    """Grabs a snapshot of the currently installed packages."""
    self.RemoteCommand(
        'dpkg --get-selections > %s/dpkg_selections' % vm_util.VM_TMP_DIR)

  def RestorePackages(self):
    """Restores the currently installed packages to those snapshotted."""
    self.RemoteCommand('sudo dpkg --clear-selections')
    self.RemoteCommand(
        'sudo dpkg --set-selections < %s/dpkg_selections' % vm_util.VM_TMP_DIR)
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
    """Installs a PerfKit package on the VM."""
    if package_name not in self._installed_packages:
      package = packages.PACKAGES[package_name]
      package.AptInstall(self)
      self._installed_packages.add(package_name)

  def Uninstall(self, package_name):
    """Uninstalls a PerfKit package on the VM."""
    package = packages.PACKAGES[package_name]
    if hasattr(package, 'AptUninstall'):
      package.AptUninstall(self)

  def GetPathToConfig(self, package_name):
    """Returns the path to the config file for PerfKit packages.

    This function is mostly useful when config files locations
    don't match across distributions (such as mysql). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetPathToConfig(self)

  def GetServiceName(self, package_name):
    """Returns the service name of a PerfKit package.

    This function is mostly useful when service names don't
    match across distributions (such as mongodb). Packages don't
    need to implement it if this is not the case.
    """
    package = packages.PACKAGES[package_name]
    return package.AptGetServiceName(self)
